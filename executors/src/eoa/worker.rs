use alloy::consensus::{SignableTransaction, Signed, Transaction, TypedTransaction};
use alloy::network::{TransactionBuilder, TransactionBuilder7702};
use alloy::primitives::{Address, B256, Bytes, U256};
use alloy::providers::Provider;
use alloy::rpc::types::TransactionRequest as AlloyTransactionRequest;
use alloy::transports::{RpcError, TransportErrorKind};
use engine_core::signer::AccountSigner;
use engine_core::{
    chain::{Chain, ChainService, RpcCredentials},
    credentials::SigningCredential,
    error::{AlloyRpcErrorToEngineError, RpcErrorKind},
    signer::{EoaSigner, EoaSigningOptions},
};
use hex;
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio::time::sleep;
use twmq::{
    DurableExecution, FailHookData, NackHookData, SuccessHookData, UserCancellable,
    error::TwmqError,
    hooks::TransactionContext,
    job::{BorrowedJob, JobResult, RequeuePosition, ToJobResult},
};

use crate::eoa::store::{
    BorrowedTransactionData, EoaExecutorStore, EoaHealth, EoaTransactionRequest, ScopedEoaExecutorStore, TransactionData,
    TransactionStoreError,
};

// ========== JOB DATA ==========
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EoaExecutorWorkerJobData {
    pub eoa_address: Address,
    pub chain_id: u64,
    pub worker_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EoaExecutorWorkerResult {
    pub recovered_transactions: u32,
    pub confirmed_transactions: u32,
    pub failed_transactions: u32,
    pub sent_transactions: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone, thiserror::Error)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "errorCode")]
pub enum EoaExecutorWorkerError {
    #[error("Chain service error for chainId {chain_id}: {message}")]
    ChainServiceError { chain_id: u64, message: String },

    #[error("Store error: {message}")]
    StoreError { message: String },

    #[error("RPC error: {message}")]
    RpcError { message: String },

    #[error("Transaction signing failed: {message}")]
    SigningError { message: String },

    #[error("Work still remaining: {message}")]
    WorkRemaining { message: String },

    #[error("Internal error: {message}")]
    InternalError { message: String },

    #[error("User cancelled")]
    UserCancelled,
}

impl From<TwmqError> for EoaExecutorWorkerError {
    fn from(error: TwmqError) -> Self {
        EoaExecutorWorkerError::InternalError {
            message: format!("Queue error: {}", error),
        }
    }
}

impl From<TransactionStoreError> for EoaExecutorWorkerError {
    fn from(error: TransactionStoreError) -> Self {
        EoaExecutorWorkerError::StoreError {
            message: error.to_string(),
        }
    }
}

impl UserCancellable for EoaExecutorWorkerError {
    fn user_cancelled() -> Self {
        EoaExecutorWorkerError::UserCancelled
    }
}

// ========== SIMPLE ERROR CLASSIFICATION ==========
#[derive(Debug)]
enum SendResult {
    Success,
    PossiblySent,         // "nonce too low", "already known" etc
    DeterministicFailure, // Invalid signature, malformed tx, insufficient funds etc
}

fn classify_send_error(error: &RpcError<TransportErrorKind>) -> SendResult {
    let error_str = error.to_string().to_lowercase();

    // Transaction possibly made it to mempool
    if error_str.contains("nonce too low")
        || error_str.contains("already known")
        || error_str.contains("replacement transaction underpriced")
    {
        return SendResult::PossiblySent;
    }

    // Clear failures that didn't consume nonce
    if error_str.contains("invalid signature")
        || error_str.contains("malformed")
        || error_str.contains("insufficient funds")
        || error_str.contains("gas limit")
        || error_str.contains("intrinsic gas too low")
    {
        return SendResult::DeterministicFailure;
    }

    // Default: assume possibly sent for safety
    SendResult::PossiblySent
}

fn is_retryable_rpc_error(kind: &RpcErrorKind) -> bool {
    match kind {
        RpcErrorKind::TransportHttpError { status, .. } if *status >= 400 && *status < 500 => false,
        RpcErrorKind::UnsupportedFeature { .. } => false,
        _ => true,
    }
}

// ========== PREPARED TRANSACTION ==========
#[derive(Debug, Clone)]
struct PreparedTransaction {
    transaction_id: String,
    signed_tx: Signed<TypedTransaction>,
    nonce: u64,
}

// ========== MAIN WORKER ==========
pub struct EoaExecutorWorker<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    pub chain_service: Arc<CS>,
    pub store: Arc<EoaExecutorStore>,
    pub eoa_signer: Arc<EoaSigner>,
    pub max_inflight: u64,
    pub max_recycled_nonces: u64,
}

impl<CS> DurableExecution for EoaExecutorWorker<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    type Output = EoaExecutorWorkerResult;
    type ErrorData = EoaExecutorWorkerError;
    type JobData = EoaExecutorWorkerJobData;

    #[tracing::instrument(skip_all, fields(eoa = %job.job.data.eoa_address, chain_id = job.job.data.chain_id))]
    async fn process(
        &self,
        job: &BorrowedJob<Self::JobData>,
    ) -> JobResult<Self::Output, Self::ErrorData> {
        let data = &job.job.data;

        // 1. ACQUIRE LOCK AGGRESSIVELY
        tracing::info!("Acquiring EOA lock aggressively");
        self.store
            .acquire_eoa_lock_aggressively(data.eoa_address, data.chain_id, &data.worker_id)
            .await
            .map_err_nack(Some(Duration::from_secs(10)), RequeuePosition::Last)?;

        // 2. GET CHAIN
        let chain = self
            .chain_service
            .get_chain(data.chain_id)
            .map_err(|e| EoaExecutorWorkerError::ChainServiceError {
                chain_id: data.chain_id,
                message: format!("Failed to get chain: {}", e),
            })
            .map_err_nack(Some(Duration::from_secs(10)), RequeuePosition::Last)?;

        // 3. CREATE SCOPED STORE (validates lock ownership)
        let scoped = ScopedEoaExecutorStore::build(
            &self.store,
            data.eoa_address,
            data.chain_id,
            data.worker_id.clone(),
        )
        .await
        .map_err_nack(Some(Duration::from_secs(10)), RequeuePosition::Last)?;

        // 4. CRASH RECOVERY
        let recovered = self
            .recover_borrowed_state(&scoped, &chain)
            .await
            .map_err_nack(Some(Duration::from_secs(10)), RequeuePosition::Last)?;

        // 5. CONFIRM FLOW
        let (confirmed, failed) = self
            .confirm_flow(&scoped, &chain)
            .await
            .map_err_nack(Some(Duration::from_secs(10)), RequeuePosition::Last)?;

        // 6. SEND FLOW
        let sent = self
            .send_flow(&scoped, &chain)
            .await
            .map_err_nack(Some(Duration::from_secs(10)), RequeuePosition::Last)?;

        // 7. CHECK FOR REMAINING WORK
        let pending_count = scoped
            .peek_pending_transactions(1000)
            .await
            .map_err_nack(Some(Duration::from_secs(10)), RequeuePosition::Last)?
            .len();
        let borrowed_count = scoped
            .peek_borrowed_transactions()
            .await
            .map_err_nack(Some(Duration::from_secs(10)), RequeuePosition::Last)?
            .len();
        let recycled_count = scoped
            .peek_recycled_nonces()
            .await
            .map_err_nack(Some(Duration::from_secs(10)), RequeuePosition::Last)?
            .len();

        if pending_count > 0 || borrowed_count > 0 || recycled_count > 0 {
            return Err(EoaExecutorWorkerError::WorkRemaining {
                message: format!(
                    "Work remaining: {} pending, {} borrowed, {} recycled",
                    pending_count, borrowed_count, recycled_count
                ),
            })
            .map_err_nack(Some(Duration::from_secs(5)), RequeuePosition::Last);
        }

        // Only succeed if no work remains
        Ok(EoaExecutorWorkerResult {
            recovered_transactions: recovered,
            confirmed_transactions: confirmed,
            failed_transactions: failed,
            sent_transactions: sent,
        })
    }

    async fn on_success(
        &self,
        _job: &BorrowedJob<Self::JobData>,
        _success_data: SuccessHookData<'_, Self::Output>,
        _tx: &mut TransactionContext<'_>,
    ) {
        // No additional operations needed for EOA worker success
    }

    async fn on_nack(
        &self,
        _job: &BorrowedJob<Self::JobData>,
        _nack_data: NackHookData<'_, Self::ErrorData>,
        _tx: &mut TransactionContext<'_>,
    ) {
        // No additional operations needed for EOA worker nack
    }

    async fn on_fail(
        &self,
        _job: &BorrowedJob<Self::JobData>,
        _fail_data: FailHookData<'_, Self::ErrorData>,
        _tx: &mut TransactionContext<'_>,
    ) {
        // EOA locks use a takeover pattern - no explicit release needed
        // Other workers will forcefully take over the lock when needed
    }
}

impl<CS> EoaExecutorWorker<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    // ========== CRASH RECOVERY ==========
    async fn recover_borrowed_state(
        &self,
        scoped: &ScopedEoaExecutorStore<'_>,
        chain: &impl Chain,
    ) -> Result<u32, EoaExecutorWorkerError> {
        let mut borrowed_transactions = scoped.peek_borrowed_transactions().await?;
        
        // Sort borrowed transactions by nonce to ensure proper ordering
        borrowed_transactions.sort_by_key(|tx| tx.signed_transaction.nonce());
        
        // Check for stale borrowed transactions (older than 5 minutes)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        for borrowed in &borrowed_transactions {
            if now - borrowed.borrowed_at > 300 { // 5 minutes
                tracing::warn!(
                    transaction_id = %borrowed.transaction_id,
                    nonce = borrowed.signed_transaction.nonce(),
                    age_seconds = now - borrowed.borrowed_at,
                    "Found stale borrowed transaction - possible worker crash or system issue"
                );
            }
        }
        
        let mut recovered_count = 0;

        for borrowed in borrowed_transactions {
            tracing::info!(
                transaction_id = %borrowed.transaction_id,
                nonce = borrowed.signed_transaction.nonce(),
                "Recovering borrowed transaction"
            );

            // Rebroadcast the transaction
            let send_result = chain
                .provider()
                .send_tx_envelope(borrowed.signed_transaction.clone().into())
                .await;
            let nonce = borrowed.signed_transaction.nonce();

            match send_result {
                Ok(_) => {
                    // Transaction was sent successfully
                    scoped
                        .move_borrowed_to_submitted(
                            nonce,
                            &format!("{:?}", borrowed.hash),
                            &borrowed.transaction_id,
                        )
                        .await?;
                    tracing::info!(transaction_id = %borrowed.transaction_id, nonce = nonce, "Moved recovered transaction to submitted");
                }
                Err(e) => {
                    match classify_send_error(&e) {
                        SendResult::PossiblySent => {
                            // Transaction possibly sent, move to submitted
                            scoped
                                .move_borrowed_to_submitted(
                                    nonce,
                                    &format!("{:?}", borrowed.hash),
                                    &borrowed.transaction_id,
                                )
                                .await?;
                            tracing::info!(transaction_id = %borrowed.transaction_id, nonce = nonce, "Moved recovered transaction to submitted (possibly sent)");
                        }
                        SendResult::DeterministicFailure => {
                            // Transaction is broken, recycle nonce and requeue
                            scoped
                                .move_borrowed_to_recycled(nonce, &borrowed.transaction_id)
                                .await?;
                            tracing::warn!(transaction_id = %borrowed.transaction_id, nonce = nonce, error = %e, "Recycled failed transaction");
                        }
                        SendResult::Success => {
                            // This case is handled by Ok(_) above
                            unreachable!()
                        }
                    }
                }
            }

            recovered_count += 1;
        }

        Ok(recovered_count)
    }

    // ========== CONFIRM FLOW ==========
    async fn confirm_flow(
        &self,
        scoped: &ScopedEoaExecutorStore<'_>,
        chain: &impl Chain,
    ) -> Result<(u32, u32), EoaExecutorWorkerError> {
        // Get fresh on-chain transaction count
        let current_chain_nonce = chain
            .provider()
            .get_transaction_count(scoped.eoa())
            .await
            .map_err(|e| {
                let engine_error = e.to_engine_error(chain);
                EoaExecutorWorkerError::RpcError {
                    message: format!("Failed to get transaction count: {}", engine_error),
                }
            })?;

        let cached_nonce = scoped.get_cached_transaction_count().await?;

        if current_chain_nonce == cached_nonce {
            tracing::debug!("No nonce progress, skipping confirm flow");
            return Ok((0, 0));
        }

        tracing::info!(
            current_chain_nonce = current_chain_nonce,
            cached_nonce = cached_nonce,
            "Processing confirmations"
        );

        // Get all hashes below the current chain nonce
        let pending_hashes = scoped.get_hashes_below_nonce(current_chain_nonce).await?;

        let mut confirmed_count = 0;
        let mut failed_count = 0;

        // Process receipts in parallel batches
        let batch_size = 10;
        for batch in pending_hashes.chunks(batch_size) {
            let receipt_futures: Vec<_> = batch
                .iter()
                .map(|(nonce, hash)| async {
                    let receipt = chain
                        .provider()
                        .get_transaction_receipt(hash.parse::<B256>().unwrap())
                        .await;
                    (*nonce, hash.clone(), receipt)
                })
                .collect();

            let results = futures::future::join_all(receipt_futures).await;

            for (nonce, hash, receipt_result) in results {
                match receipt_result {
                    Ok(Some(receipt)) => {
                        // Transaction confirmed!
                        if let Ok(Some(tx_id)) = self.store.get_transaction_id_for_hash(&hash).await
                        {
                            scoped
                                .succeed_transaction(
                                    &tx_id,
                                    &hash,
                                    &serde_json::to_string(&receipt).unwrap(),
                                )
                                .await?;
                            confirmed_count += 1;
                            tracing::info!(transaction_id = %tx_id, nonce = nonce, "Transaction confirmed");
                        }
                    }
                    Ok(None) | Err(_) => {
                        // Transaction failed or dropped
                        if let Ok(Some(tx_id)) = self.store.get_transaction_id_for_hash(&hash).await
                        {
                            scoped.fail_and_requeue_transaction(&tx_id).await?;
                            failed_count += 1;
                            tracing::warn!(transaction_id = %tx_id, nonce = nonce, "Transaction failed, requeued");
                        }
                    }
                }
            }
        }

        // Update cached transaction count
        scoped
            .update_cached_transaction_count(current_chain_nonce)
            .await?;

        Ok((confirmed_count, failed_count))
    }

    // ========== SEND FLOW ==========
    async fn send_flow(
        &self,
        scoped: &ScopedEoaExecutorStore<'_>,
        chain: &impl Chain,
    ) -> Result<u32, EoaExecutorWorkerError> {
        // 1. Check and update EOA health
        self.check_and_update_eoa_health(scoped, chain).await?;

        let health = scoped.check_eoa_health().await?;
        if health.map(|h| h.balance.is_zero()).unwrap_or(true) {
            tracing::warn!("EOA has insufficient balance, skipping send flow");
            return Ok(0);
        }

        let mut total_sent = 0;

        // 2. Process recycled nonces first
        total_sent += self.process_recycled_nonces(scoped, chain).await?;

        // 3. Only proceed to new nonces if we successfully used all recycled nonces
        let remaining_recycled = scoped.peek_recycled_nonces().await?.len();
        if remaining_recycled == 0 {
            let inflight_budget = scoped.get_inflight_budget(self.max_inflight).await?;
            if inflight_budget > 0 {
                total_sent += self
                    .process_new_transactions(scoped, chain, inflight_budget)
                    .await?;
            }
        } else {
            tracing::warn!(
                "Still have {} recycled nonces, not sending new transactions",
                remaining_recycled
            );
        }

        Ok(total_sent)
    }

    async fn process_recycled_nonces(
        &self,
        scoped: &ScopedEoaExecutorStore<'_>,
        chain: &impl Chain,
    ) -> Result<u32, EoaExecutorWorkerError> {
        let recycled_nonces = scoped.peek_recycled_nonces().await?;

        // If too many recycled nonces, nuke them
        if recycled_nonces.len() as u64 > self.max_recycled_nonces {
            tracing::warn!(
                "Too many recycled nonces ({}), nuking them",
                recycled_nonces.len()
            );
            scoped.nuke_recycled_nonces().await?;
            return Ok(0);
        }

        if recycled_nonces.is_empty() {
            return Ok(0);
        }

        // Get pending transactions (one per recycled nonce)
        let pending_txs = scoped
            .peek_pending_transactions(recycled_nonces.len() as u64)
            .await?;
        let mut sent_count = 0;

        // Process each recycled nonce sequentially with delays
        for (i, nonce) in recycled_nonces.into_iter().enumerate() {
            if i > 0 {
                sleep(Duration::from_millis(50)).await; // 50ms delay between consecutive nonces
            }

            if let Some(tx_id) = pending_txs.get(i) {
                // Try to send transaction with this recycled nonce
                match self
                    .send_single_transaction_with_recycled_nonce(scoped, chain, tx_id, nonce)
                    .await
                {
                    Ok(true) => sent_count += 1,
                    Ok(false) => {} // Failed to send, but handled
                    Err(e) => tracing::error!("Error processing recycled nonce {}: {}", nonce, e),
                }
            } else {
                // No pending transactions, send no-op
                match self.send_noop_transaction(scoped, chain, nonce).await {
                    Ok(true) => sent_count += 1,
                    Ok(false) => {} // Failed to send no-op
                    Err(e) => tracing::error!("Error sending no-op for nonce {}: {}", nonce, e),
                }
            }
        }

        Ok(sent_count)
    }

    async fn process_new_transactions(
        &self,
        scoped: &ScopedEoaExecutorStore<'_>,
        chain: &impl Chain,
        budget: u64,
    ) -> Result<u32, EoaExecutorWorkerError> {
        if budget == 0 {
            return Ok(0);
        }

        // 1. Get pending transactions
        let pending_txs = scoped.peek_pending_transactions(budget).await?;
        if pending_txs.is_empty() {
            return Ok(0);
        }

        let optimistic_nonce = scoped.get_optimistic_nonce().await?;

        // 2. Build and sign all transactions in parallel
        let build_tasks: Vec<_> = pending_txs
            .iter()
            .enumerate()
            .map(|(i, tx_id)| {
                let expected_nonce = optimistic_nonce + i as u64;
                self.build_and_sign_single_transaction(scoped, tx_id, expected_nonce, chain)
            })
            .collect();

        let prepared_results = futures::future::join_all(build_tasks).await;

        // 3. Move successful transactions to borrowed state serially (to maintain nonce order)
        let mut prepared_txs = Vec::new();
        for (i, result) in prepared_results.into_iter().enumerate() {
            match result {
                Ok(prepared) => {
                    let borrowed_data = BorrowedTransactionData {
                        transaction_id: prepared.transaction_id.clone(),
                        signed_transaction: prepared.signed_tx.clone(),
                        hash: *prepared.signed_tx.hash(),
                        borrowed_at: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs(),
                    };

                    match scoped
                        .atomic_move_pending_to_borrowed_with_new_nonce(
                            &prepared.transaction_id,
                            prepared.nonce,
                            &borrowed_data,
                        )
                        .await
                    {
                        Ok(()) => prepared_txs.push(prepared),
                        Err(TransactionStoreError::OptimisticNonceChanged { .. }) => {
                            tracing::debug!(
                                "Nonce changed for transaction {}, skipping",
                                prepared.transaction_id
                            );
                            break; // Stop processing if nonce changed
                        }
                        Err(TransactionStoreError::TransactionNotInPendingQueue { .. }) => {
                            tracing::debug!(
                                "Transaction {} already processed, skipping",
                                prepared.transaction_id
                            );
                            continue;
                        }
                        Err(e) => {
                            tracing::error!(
                                "Failed to move transaction {} to borrowed: {}",
                                prepared.transaction_id,
                                e
                            );
                            continue;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to build transaction {}: {}", pending_txs[i], e);
                    // Individual transaction failure doesn't stop the worker
                    continue;
                }
            }
        }

        // 4. Send all prepared transactions sequentially with delays
        let mut sent_count = 0;
        for (i, prepared) in prepared_txs.iter().enumerate() {
            if i > 0 {
                sleep(Duration::from_millis(50)).await; // 50ms delay between consecutive nonces
            }

            match Self::send_prepared_transaction(scoped, chain, prepared).await {
                Ok(true) => sent_count += 1,
                Ok(false) => {} // Failed to send, but handled
                Err(e) => tracing::error!("Error sending transaction: {}", e),
            }
        }

        Ok(sent_count)
    }

    // ========== TRANSACTION BUILDING & SENDING ==========
    async fn build_and_sign_single_transaction(
        &self,
        scoped: &ScopedEoaExecutorStore<'_>,
        transaction_id: &str,
        nonce: u64,
        chain: &impl Chain,
    ) -> Result<PreparedTransaction, EoaExecutorWorkerError> {
        // Get transaction data
        let tx_data = scoped
            .get_transaction_data(transaction_id)
            .await?
            .ok_or_else(|| EoaExecutorWorkerError::StoreError {
                message: format!("Transaction not found: {}", transaction_id),
            })?;

        // Build and sign transaction
        let signed_tx = self
            .build_and_sign_transaction(&tx_data, nonce, chain)
            .await?;

        Ok(PreparedTransaction {
            transaction_id: transaction_id.to_string(),
            signed_tx,
            nonce,
        })
    }

    async fn send_prepared_transaction(
        scoped: &ScopedEoaExecutorStore<'_>,
        chain: &impl Chain,
        prepared: &PreparedTransaction,
    ) -> Result<bool, EoaExecutorWorkerError> {
        // Send to RPC
        let send_result = chain
            .provider()
            .send_tx_envelope(prepared.signed_tx.clone().into())
            .await;

        match send_result {
            Ok(_) => {
                // Transaction was sent successfully
                scoped
                    .move_borrowed_to_submitted(
                        prepared.nonce,
                        &format!("{:?}", prepared.signed_tx.hash()),
                        &prepared.transaction_id,
                    )
                    .await?;

                tracing::info!(
                    transaction_id = %prepared.transaction_id,
                    nonce = prepared.nonce,
                    hash = ?prepared.signed_tx.hash(),
                    "Successfully sent transaction"
                );

                Ok(true)
            }
            Err(e) => {
                match classify_send_error(&e) {
                    SendResult::PossiblySent => {
                        // Move to submitted state
                        scoped
                            .move_borrowed_to_submitted(
                                prepared.nonce,
                                &format!("{:?}", prepared.signed_tx.hash()),
                                &prepared.transaction_id,
                            )
                            .await?;

                        tracing::info!(
                            transaction_id = %prepared.transaction_id,
                            nonce = prepared.nonce,
                            hash = ?prepared.signed_tx.hash(),
                            "Transaction possibly sent"
                        );

                        Ok(true)
                    }
                    SendResult::DeterministicFailure => {
                        // Move back to recycled/pending
                        scoped
                            .move_borrowed_to_recycled(prepared.nonce, &prepared.transaction_id)
                            .await?;

                        tracing::warn!(
                            transaction_id = %prepared.transaction_id,
                            nonce = prepared.nonce,
                            error = %e,
                            "Transaction failed deterministically, recycled nonce"
                        );

                        Ok(false)
                    }
                    SendResult::Success => {
                        // This case is handled by Ok(_) above
                        unreachable!()
                    }
                }
            }
        }
    }

    async fn send_single_transaction_with_recycled_nonce(
        &self,
        scoped: &ScopedEoaExecutorStore<'_>,
        chain: &impl Chain,
        transaction_id: &str,
        nonce: u64,
    ) -> Result<bool, EoaExecutorWorkerError> {
        // Build and sign transaction
        match self
            .build_and_sign_single_transaction(scoped, transaction_id, nonce, chain)
            .await
        {
            Ok(prepared) => {
                let borrowed_data = BorrowedTransactionData {
                    transaction_id: transaction_id.to_string(),
                    signed_transaction: prepared.signed_tx.clone(),
                    hash: *prepared.signed_tx.hash(),
                    borrowed_at: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                };

                // Atomically move from pending to borrowed with recycled nonce
                match scoped
                    .atomic_move_pending_to_borrowed_with_recycled_nonce(
                        transaction_id,
                        nonce,
                        &borrowed_data,
                    )
                    .await
                {
                    Ok(()) => {
                        // Successfully moved to borrowed, now send
                        Self::send_prepared_transaction(scoped, chain, &prepared).await
                    }
                    Err(TransactionStoreError::NonceNotInRecycledSet { .. }) => {
                        // Nonce was consumed by another worker
                        Ok(false)
                    }
                    Err(TransactionStoreError::TransactionNotInPendingQueue { .. }) => {
                        // Transaction was processed by another worker
                        Ok(false)
                    }
                    Err(e) => Err(e.into()),
                }
            }
            Err(EoaExecutorWorkerError::StoreError { .. }) => {
                // Individual transaction failed (e.g., gas estimation revert)
                // Just skip this transaction, don't fail the worker
                tracing::warn!(
                    "Skipping transaction {} due to build failure",
                    transaction_id
                );
                Ok(false)
            }
            Err(e) => Err(e), // Other errors (RPC, signing) should propagate
        }
    }

    async fn send_noop_transaction(
        &self,
        _scoped: &ScopedEoaExecutorStore<'_>,
        _chain: &impl Chain,
        nonce: u64,
    ) -> Result<bool, EoaExecutorWorkerError> {
        // TODO: Implement proper no-op transaction for recycled nonces
        // This requires handling signing credentials and creating atomic operations
        // for consuming recycled nonces without pending transactions
        tracing::warn!(
            nonce = nonce,
            "No-op transaction not implemented - recycled nonce will remain unconsumed"
        );
        Ok(false)
    }

    // ========== HELPER METHODS ==========
    async fn check_and_update_eoa_health(
        &self,
        scoped: &ScopedEoaExecutorStore<'_>,
        chain: &impl Chain,
    ) -> Result<(), EoaExecutorWorkerError> {
        let current_health = scoped.check_eoa_health().await?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let should_update = current_health
            .as_ref()
            .map(|h| now - h.balance_fetched_at > 300) // 5 minutes
            .unwrap_or(true);

        if should_update {
            let balance = chain
                .provider()
                .get_balance(scoped.eoa())
                .await
                .map_err(|e| {
                    let engine_error = e.to_engine_error(chain);
                    EoaExecutorWorkerError::RpcError {
                        message: format!("Failed to get balance: {}", engine_error),
                    }
                })?;

            let health = EoaHealth {
                balance,
                balance_fetched_at: now,
                last_confirmation_at: current_health.as_ref().and_then(|h| h.last_confirmation_at),
                nonce_resets: current_health.map(|h| h.nonce_resets).unwrap_or_default(),
            };

            scoped.update_health_data(&health).await?;
        }

        Ok(())
    }

    async fn build_and_sign_transaction(
        &self,
        tx_data: &TransactionData,
        nonce: u64,
        chain: &impl Chain,
    ) -> Result<Signed<TypedTransaction>, EoaExecutorWorkerError> {
        // Build transaction request from stored data
        let mut tx_request = AlloyTransactionRequest::default()
            .with_from(tx_data.user_request.from)
            .with_value(tx_data.user_request.value)
            .with_input(tx_data.user_request.data.clone())
            .with_chain_id(tx_data.user_request.chain_id)
            .with_nonce(nonce);

        if let Some(to) = tx_data.user_request.to {
            tx_request = tx_request.with_to(to);
        }

        if let Some(gas_limit) = tx_data.user_request.gas_limit {
            tx_request = tx_request.with_gas_limit(gas_limit);
        }

        // Apply transaction type specific settings
        if let Some(type_data) = &tx_data.user_request.transaction_type_data {
            tx_request = match type_data {
                crate::eoa::store::EoaTransactionTypeData::Eip1559(data) => {
                    let mut req = tx_request;
                    if let Some(max_fee) = data.max_fee_per_gas {
                        req = req.with_max_fee_per_gas(max_fee);
                    }
                    if let Some(max_priority) = data.max_priority_fee_per_gas {
                        req = req.with_max_priority_fee_per_gas(max_priority);
                    }
                    req
                }
                crate::eoa::store::EoaTransactionTypeData::Legacy(data) => {
                    if let Some(gas_price) = data.gas_price {
                        tx_request.with_gas_price(gas_price)
                    } else {
                        tx_request
                    }
                }
                crate::eoa::store::EoaTransactionTypeData::Eip7702(data) => {
                    let mut req = tx_request;
                    if let Some(authorization_list) = &data.authorization_list {
                        req = req.with_authorization_list(authorization_list.clone());
                    }
                    if let Some(max_fee) = data.max_fee_per_gas {
                        req = req.with_max_fee_per_gas(max_fee);
                    }
                    if let Some(max_priority) = data.max_priority_fee_per_gas {
                        req = req.with_max_priority_fee_per_gas(max_priority);
                    }
                    req
                }
            };
        }

        // Estimate gas if needed
        if tx_request.gas.is_none() {
            match chain.provider().estimate_gas(tx_request.clone()).await {
                Ok(gas_limit) => {
                    tx_request = tx_request.with_gas_limit(gas_limit * 110 / 100); // 10% buffer
                }
                Err(e) => {
                    // Check if this is a revert
                    if let RpcError::ErrorResp(error_payload) = &e {
                        if let Some(revert_data) = error_payload.as_revert_data() {
                            // This is a revert - the transaction is fundamentally broken
                            // This should fail the individual transaction, not the worker
                            return Err(EoaExecutorWorkerError::StoreError {
                                message: format!(
                                    "Transaction reverted during gas estimation: {} (revert: {})",
                                    error_payload.message,
                                    hex::encode(&revert_data)
                                ),
                            });
                        }
                    }

                    // Not a revert - could be RPC issue, this should nack the worker
                    let engine_error = e.to_engine_error(chain);
                    return Err(EoaExecutorWorkerError::RpcError {
                        message: format!("Gas estimation failed: {}", engine_error),
                    });
                }
            }
        }

        // Build typed transaction
        let typed_tx =
            tx_request
                .build_typed_tx()
                .map_err(|e| EoaExecutorWorkerError::StoreError {
                    message: format!("Failed to build typed transaction: {:?}", e),
                })?;

        // Sign transaction
        let signing_options = EoaSigningOptions {
            from: tx_data.user_request.from,
            chain_id: Some(tx_data.user_request.chain_id),
        };

        let signature = self
            .eoa_signer
            .sign_transaction(
                signing_options,
                typed_tx.clone(),
                tx_data.user_request.signing_credential.clone(),
            )
            .await
            .map_err(|engine_error| EoaExecutorWorkerError::SigningError {
                message: format!("Failed to sign transaction: {}", engine_error),
            })?;

        let signed_tx = typed_tx.into_signed(signature.parse().map_err(|e| {
            EoaExecutorWorkerError::StoreError {
                message: format!("Failed to parse signature: {}", e),
            }
        })?);

        Ok(signed_tx)
    }
}
