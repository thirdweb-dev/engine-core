use alloy::consensus::{
    SignableTransaction, Signed, Transaction, TxEip4844Variant, TxEip4844WithSidecar,
    TypedTransaction,
};
use alloy::network::{TransactionBuilder, TransactionBuilder7702};
use alloy::primitives::{Address, B256, Bytes, U256};
use alloy::providers::{PendingTransactionBuilder, Provider};
use alloy::rpc::types::TransactionRequest as AlloyTransactionRequest;
use alloy::signers::Signature;
use alloy::transports::{RpcError, TransportErrorKind};
use engine_core::error::EngineError;
use engine_core::signer::AccountSigner;
use engine_core::transaction::TransactionTypeData;
use engine_core::{
    chain::{Chain, ChainService},
    credentials::SigningCredential,
    error::{AlloyRpcErrorToEngineError, RpcErrorKind},
    signer::{EoaSigner, EoaSigningOptions},
};
use hex;
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use thirdweb_core::iaw::IAWError;
use tokio::time::sleep;
use twmq::Queue;
use twmq::redis::AsyncCommands;
use twmq::redis::aio::ConnectionManager;
use twmq::{
    DurableExecution, FailHookData, NackHookData, SuccessHookData, UserCancellable,
    error::TwmqError,
    hooks::TransactionContext,
    job::{BorrowedJob, JobResult, RequeuePosition, ToJobResult},
};

use crate::eoa::store::{
    AtomicEoaExecutorStore, BorrowedTransactionData, CleanupReport, ConfirmedTransaction,
    EoaExecutorStore, EoaExecutorStoreKeys, EoaHealth, EoaTransactionRequest, PendingTransaction,
    ReplacedTransaction, SubmissionErrorFail, SubmissionErrorNack, SubmissionResult,
    SubmittedTransaction, TransactionData, TransactionStoreError,
};
use crate::webhook::WebhookJobHandler;

// ========== SPEC-COMPLIANT CONSTANTS ==========
const MAX_INFLIGHT_PER_EOA: u64 = 100; // Default from spec
const MAX_RECYCLED_THRESHOLD: u64 = 50; // Circuit breaker from spec
const TARGET_TRANSACTIONS_PER_EOA: u64 = 10; // Fleet management from spec
const MIN_TRANSACTIONS_PER_EOA: u64 = 1; // Fleet management from spec
const HEALTH_CHECK_INTERVAL: u64 = 300; // 5 minutes in seconds
const NONCE_STALL_TIMEOUT: u64 = 300_000; // 5 minutes in milliseconds - after this time, attempt gas bump

// Retry constants for preparation phase
const MAX_PREPARATION_RETRIES: u32 = 3;
const PREPARATION_RETRY_DELAY_MS: u64 = 100;

// ========== JOB DATA ==========
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EoaExecutorWorkerJobData {
    pub eoa_address: Address,
    pub chain_id: u64,
    pub noop_signing_credential: SigningCredential,
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
    StoreError {
        message: String,
        inner_error: TransactionStoreError,
    },

    #[error("Transaction not found: {transaction_id}")]
    TransactionNotFound { transaction_id: String },

    #[error("Transaction simulation failed: {message}")]
    TransactionSimulationFailed {
        message: String,
        inner_error: EngineError,
    },

    #[error("Transaction build failed: {message}")]
    TransactionBuildFailed { message: String },

    #[error("RPC error encountered during generic operation: {message}")]
    RpcError {
        message: String,
        inner_error: EngineError,
    },

    #[error("Error encountered when broadcasting transaction: {message}")]
    TransactionSendError {
        message: String,
        inner_error: EngineError,
    },

    #[error("Signature parsing failed: {message}")]
    SignatureParsingFailed { message: String },

    #[error("Transaction signing failed: {message}")]
    SigningError {
        message: String,
        inner_error: EngineError,
    },

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
            inner_error: error,
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
pub enum SendErrorClassification {
    PossiblySent,         // "nonce too low", "already known" etc
    DeterministicFailure, // Invalid signature, malformed tx, insufficient funds etc
}

#[derive(PartialEq, Eq, Debug)]
pub enum SendContext {
    Rebroadcast,
    InitialBroadcast,
}

#[tracing::instrument(skip_all, fields(error = %error, context = ?context))]
fn classify_send_error(
    error: &RpcError<TransportErrorKind>,
    context: SendContext,
) -> SendErrorClassification {
    if !error.is_error_resp() {
        return SendErrorClassification::DeterministicFailure;
    }

    let error_str = error.to_string().to_lowercase();

    // Deterministic failures that didn't consume nonce (spec-compliant)
    if error_str.contains("invalid signature")
        || error_str.contains("malformed transaction")
        || (context == SendContext::InitialBroadcast && error_str.contains("insufficient funds"))
        || error_str.contains("invalid transaction format")
        || error_str.contains("nonce too high")
    // Should trigger nonce reset
    {
        return SendErrorClassification::DeterministicFailure;
    }

    // Transaction possibly made it to mempool (spec-compliant)
    if error_str.contains("nonce too low")
        || error_str.contains("already known")
        || error_str.contains("replacement transaction underpriced")
    {
        return SendErrorClassification::PossiblySent;
    }

    // Additional common failures that didn't consume nonce
    if error_str.contains("malformed")
        || error_str.contains("gas limit")
        || error_str.contains("intrinsic gas too low")
    {
        return SendErrorClassification::DeterministicFailure;
    }

    tracing::warn!(
        "Unknown send error: {}. PLEASE REPORT FOR ADDING CORRECT CLASSIFICATION [NOTIFY]",
        error_str
    );

    // Default: assume possibly sent for safety
    SendErrorClassification::PossiblySent
}

fn should_trigger_nonce_reset(error: &RpcError<TransportErrorKind>) -> bool {
    let error_str = error.to_string().to_lowercase();

    // "nonce too high" should trigger nonce reset as per spec
    error_str.contains("nonce too high")
}

fn should_update_balance_threshold(error: &EngineError) -> bool {
    match error {
        EngineError::RpcError { kind, .. }
        | EngineError::PaymasterError { kind, .. }
        | EngineError::BundlerError { kind, .. } => match kind {
            RpcErrorKind::ErrorResp(resp) => {
                let message = resp.message.to_lowercase();
                message.contains("insufficient funds")
                    || message.contains("insufficient balance")
                    || message.contains("out of gas")
                    || message.contains("insufficient eth")
                    || message.contains("balance too low")
                    || message.contains("not enough funds")
                    || message.contains("insufficient native token")
            }
            _ => false,
        },
        _ => false,
    }
}

fn is_retryable_rpc_error(kind: &RpcErrorKind) -> bool {
    match kind {
        RpcErrorKind::TransportHttpError { status, .. } if *status >= 400 && *status < 500 => false,
        RpcErrorKind::UnsupportedFeature { .. } => false,
        _ => true,
    }
}

fn is_retryable_preparation_error(error: &EoaExecutorWorkerError) -> bool {
    match error {
        EoaExecutorWorkerError::RpcError { inner_error, .. } => {
            // extract the RpcErrorKind from the inner error
            if let EngineError::RpcError { kind, .. } = inner_error {
                is_retryable_rpc_error(kind)
            } else {
                false
            }
        }
        EoaExecutorWorkerError::ChainServiceError { .. } => true, // Network related
        EoaExecutorWorkerError::StoreError { inner_error, .. } => {
            matches!(inner_error, TransactionStoreError::RedisError { .. })
        }
        EoaExecutorWorkerError::TransactionSimulationFailed { .. } => false, // Deterministic
        EoaExecutorWorkerError::TransactionBuildFailed { .. } => false,      // Deterministic
        EoaExecutorWorkerError::SigningError { inner_error, .. } => match inner_error {
            // if vault error, it's not retryable
            EngineError::VaultError { .. } => false,
            // if iaw error, it's retryable only if it's a network error
            EngineError::IawError { error, .. } => matches!(error, IAWError::NetworkError { .. }),
            _ => false,
        },
        EoaExecutorWorkerError::TransactionNotFound { .. } => false, // Deterministic
        EoaExecutorWorkerError::InternalError { .. } => false,       // Deterministic
        EoaExecutorWorkerError::UserCancelled => false,              // Deterministic
        EoaExecutorWorkerError::TransactionSendError { .. } => false, // Different context
        EoaExecutorWorkerError::SignatureParsingFailed { .. } => false, // Deterministic
        EoaExecutorWorkerError::WorkRemaining { .. } => false,       // Different context
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfirmedTransactionWithRichReceipt {
    pub nonce: u64,
    pub hash: String,
    pub transaction_id: String,
    pub receipt: alloy::rpc::types::TransactionReceipt,
}

// ========== MAIN WORKER ==========
/// EOA Executor Worker
///
/// ## Core Workflow:
/// 1. **Acquire Lock Aggressively** - Takes over stalled workers using force acquisition. This is a lock over EOA:CHAIN
/// 2. **Crash Recovery** - Rebroadcasts borrowed transactions, handles deterministic failures
/// 3. **Confirmation Flow** - Fetches receipts, confirms transactions, handles nonce sync, requeues replaced transactions
/// 4. **Send Flow** - Processes recycled nonces first, then new transactions with in-flight budget control
/// 5. **Lock Release** - Explicit release in finally pattern as per spec
///
/// ## Key Features:
/// - **Atomic Operations**: All state transitions use Redis WATCH/MULTI/EXEC for durability
/// - **Borrowed State**: Mid-send crash recovery with atomic pending->borrowed->submitted transitions
/// - **Nonce Management**: Optimistic nonce tracking with recycled nonce priority
/// - **Error Classification**: Spec-compliant deterministic vs. possibly-sent error handling
/// - **Circuit Breakers**: Automatic recycled nonce nuking when threshold exceeded
/// - **Health Monitoring**: Balance checking with configurable thresholds
pub struct EoaExecutorWorker<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    pub chain_service: Arc<CS>,
    pub webhook_queue: Arc<Queue<WebhookJobHandler>>,

    pub redis: ConnectionManager,
    pub namespace: Option<String>,

    pub eoa_signer: Arc<EoaSigner>,
    pub max_inflight: u64, // Note: Spec uses MAX_INFLIGHT_PER_EOA constant
    pub max_recycled_nonces: u64, // Note: Spec uses MAX_RECYCLED_THRESHOLD constant
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

        // 1. GET CHAIN
        let chain = self
            .chain_service
            .get_chain(data.chain_id)
            .map_err(|e| EoaExecutorWorkerError::ChainServiceError {
                chain_id: data.chain_id,
                message: format!("Failed to get chain: {}", e),
            })
            .map_err_nack(Some(Duration::from_secs(10)), RequeuePosition::Last)?;

        // 2. CREATE SCOPED STORE (acquires lock)
        let scoped = EoaExecutorStore::new(
            self.redis.clone(),
            self.namespace.clone(),
            data.eoa_address,
            data.chain_id,
        )
        .acquire_eoa_lock_aggressively(&job.lease_token)
        .await
        .map_err_nack(Some(Duration::from_secs(10)), RequeuePosition::Last)?;

        // initiate health data if doesn't exist
        self.get_eoa_health(&scoped, &chain)
            .await
            .map_err_nack(Some(Duration::from_secs(10)), RequeuePosition::Last)?;

        // Execute main workflow with proper error handling
        self.execute_main_workflow(&scoped, &chain).await
    }

    async fn on_success(
        &self,
        job: &BorrowedJob<Self::JobData>,
        _success_data: SuccessHookData<'_, Self::Output>,
        _tx: &mut TransactionContext<'_>,
    ) {
        self.release_eoa_lock(&job.job.data).await;
    }

    async fn on_nack(
        &self,
        job: &BorrowedJob<Self::JobData>,
        _nack_data: NackHookData<'_, Self::ErrorData>,
        _tx: &mut TransactionContext<'_>,
    ) {
        self.release_eoa_lock(&job.job.data).await;
    }

    async fn on_fail(
        &self,
        job: &BorrowedJob<Self::JobData>,
        _fail_data: FailHookData<'_, Self::ErrorData>,
        _tx: &mut TransactionContext<'_>,
    ) {
        self.release_eoa_lock(&job.job.data).await;
    }
}

impl SubmissionResult {
    /// Convert a send result to a SubmissionResult for batch processing
    /// This handles the specific RpcError<TransportErrorKind> type from alloy
    pub fn from_send_result<SR>(
        borrowed_transaction: &BorrowedTransactionData,
        send_result: Result<SR, RpcError<TransportErrorKind>>,
        send_context: SendContext,
        user_data: Option<TransactionData>,
        chain: &impl Chain,
    ) -> Self {
        match send_result {
            Ok(_) => SubmissionResult::Success(borrowed_transaction.into()),
            Err(ref rpc_error) => {
                match classify_send_error(rpc_error, send_context) {
                    SendErrorClassification::PossiblySent => {
                        SubmissionResult::Success(borrowed_transaction.into())
                    }
                    SendErrorClassification::DeterministicFailure => {
                        // Transaction failed, should be retried
                        let engine_error = rpc_error.to_engine_error(chain);
                        let error = EoaExecutorWorkerError::TransactionSendError {
                            message: format!("Transaction send failed: {}", rpc_error),
                            inner_error: engine_error,
                        };
                        SubmissionResult::Nack(SubmissionErrorNack {
                            transaction_id: borrowed_transaction.transaction_id.clone(),
                            error,
                            user_data,
                        })
                    }
                }
            }
        }
    }

    /// Helper method for when we need to create a failure result
    pub fn from_failure(
        transaction_id: String,
        error: EoaExecutorWorkerError,
        user_data: Option<TransactionData>,
    ) -> Self {
        SubmissionResult::Fail(SubmissionErrorFail {
            transaction_id,
            error,
            user_data,
        })
    }
}

impl<CS> EoaExecutorWorker<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    /// Execute the main EOA worker workflow
    async fn execute_main_workflow(
        &self,
        scoped: &AtomicEoaExecutorStore,
        chain: &impl Chain,
    ) -> JobResult<EoaExecutorWorkerResult, EoaExecutorWorkerError> {
        // 1. CRASH RECOVERY
        let recovered = self
            .recover_borrowed_state(scoped, chain)
            .await
            .map_err_nack(Some(Duration::from_secs(10)), RequeuePosition::Last)?;

        // 2. CONFIRM FLOW
        let confirmations_report = self
            .confirm_flow(scoped, chain)
            .await
            .map_err_nack(Some(Duration::from_secs(10)), RequeuePosition::Last)?;

        // 3. SEND FLOW
        let sent = self
            .send_flow(scoped, chain)
            .await
            .map_err_nack(Some(Duration::from_secs(10)), RequeuePosition::Last)?;

        // 4. CHECK FOR REMAINING WORK
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
        let submitted_count = scoped
            .get_submitted_transactions_count()
            .await
            .map_err_nack(Some(Duration::from_secs(10)), RequeuePosition::Last)?;

        // NACK here is a yield, when you think of the queue as a distributed EOA scheduler
        if pending_count > 0 || borrowed_count > 0 || recycled_count > 0 || submitted_count > 0 {
            return Err(EoaExecutorWorkerError::WorkRemaining {
                message: format!(
                    "Work remaining: {} pending, {} borrowed, {} recycled, {} submitted",
                    pending_count, borrowed_count, recycled_count, submitted_count
                ),
            })
            .map_err_nack(Some(Duration::from_secs(2)), RequeuePosition::Last);
        }

        // Only succeed if no work remains
        Ok(EoaExecutorWorkerResult {
            recovered_transactions: recovered,
            confirmed_transactions: confirmations_report.moved_to_success as u32,
            failed_transactions: confirmations_report.moved_to_pending as u32,
            sent_transactions: sent,
        })
    }

    /// Release EOA lock following the spec's finally pattern
    async fn release_eoa_lock(&self, job_data: &EoaExecutorWorkerJobData) {
        let keys = EoaExecutorStoreKeys::new(
            job_data.eoa_address,
            job_data.chain_id,
            self.namespace.clone(),
        );

        let lock_key = keys.eoa_lock_key_name();
        let mut conn = self.redis.clone();
        if let Err(e) = conn.del::<&str, ()>(&lock_key).await {
            tracing::error!(
                eoa = %job_data.eoa_address,
                chain_id = %job_data.chain_id,
                error = %e,
                "Failed to release EOA lock"
            );
        }
    }

    // ========== CRASH RECOVERY ==========
    #[tracing::instrument(skip_all)]
    async fn recover_borrowed_state(
        &self,
        scoped: &AtomicEoaExecutorStore,
        chain: &impl Chain,
    ) -> Result<u32, EoaExecutorWorkerError> {
        let mut borrowed_transactions = scoped.peek_borrowed_transactions().await?;

        if borrowed_transactions.is_empty() {
            return Ok(0);
        }

        tracing::warn!(
            "Recovering {} borrowed transactions. This indicates a worker crash or system issue",
            borrowed_transactions.len()
        );

        // Sort borrowed transactions by nonce to ensure proper ordering
        borrowed_transactions.sort_by_key(|tx| tx.signed_transaction.nonce());

        // Rebroadcast all transactions in parallel
        let rebroadcast_futures: Vec<_> = borrowed_transactions
            .iter()
            .map(|borrowed| {
                let tx_envelope = borrowed.signed_transaction.clone().into();
                let nonce = borrowed.signed_transaction.nonce();
                let transaction_id = borrowed.transaction_id.clone();

                tracing::info!(
                    transaction_id = %transaction_id,
                    nonce = nonce,
                    "Recovering borrowed transaction"
                );

                async move {
                    let send_result = chain.provider().send_tx_envelope(tx_envelope).await;
                    (borrowed, send_result)
                }
            })
            .collect();

        let rebroadcast_results = futures::future::join_all(rebroadcast_futures).await;

        // Convert results to SubmissionResult for batch processing
        let submission_results: Vec<SubmissionResult> = rebroadcast_results
            .into_iter()
            .map(|(borrowed, send_result)| {
                SubmissionResult::from_send_result(
                    borrowed,
                    send_result,
                    SendContext::Rebroadcast,
                    None, // We'll let the batch operation fetch user data
                    chain,
                )
            })
            .collect();

        // TODO: Implement post-processing analysis for balance threshold updates and nonce resets
        // Currently we lose the granular error handling that was in the individual atomic operations.
        // Consider:
        // 1. Analyzing submission_results for specific error patterns
        // 2. Calling update_balance_threshold if needed
        // 3. Detecting nonce reset conditions
        // 4. Or move this logic into the batch processor itself

        // Process all results in one batch operation
        let report = scoped
            .process_borrowed_transactions(submission_results, self.webhook_queue.clone())
            .await?;

        // TODO: Handle post-processing updates here if needed
        // For now, we skip the individual error analysis that was done in the old atomic approach

        tracing::info!(
            "Recovered {} transactions: {} submitted, {} recycled, {} failed",
            report.total_processed,
            report.moved_to_submitted,
            report.moved_to_pending,
            report.failed_transactions
        );

        Ok(report.total_processed as u32)
    }

    // ========== CONFIRM FLOW ==========
    #[tracing::instrument(skip_all)]
    async fn confirm_flow(
        &self,
        scoped: &AtomicEoaExecutorStore,
        chain: &impl Chain,
    ) -> Result<CleanupReport, EoaExecutorWorkerError> {
        // Get fresh on-chain transaction count
        let current_chain_nonce = chain
            .provider()
            .get_transaction_count(scoped.eoa())
            .await
            .map_err(|e| {
                let engine_error = e.to_engine_error(chain);
                EoaExecutorWorkerError::RpcError {
                    message: format!("Failed to get transaction count: {}", engine_error),
                    inner_error: engine_error,
                }
            })?;

        let cached_nonce = match scoped.get_cached_transaction_count().await {
            Err(e) => match e {
                TransactionStoreError::NonceSyncRequired { .. } => {
                    scoped.reset_nonces(current_chain_nonce).await?;
                    current_chain_nonce
                }
                _ => return Err(e.into()),
            },
            Ok(cached_nonce) => cached_nonce,
        };

        let submitted_count = scoped.get_submitted_transactions_count().await?;

        // no nonce progress
        if current_chain_nonce == cached_nonce {
            let current_health = self.get_eoa_health(scoped, chain).await?;
            let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
            // No nonce progress - check if we should attempt gas bumping for stalled nonce
            let time_since_movement = now.saturating_sub(current_health.last_nonce_movement_at);

            // if there are waiting transactions, we can attempt a gas bump
            if time_since_movement > NONCE_STALL_TIMEOUT && submitted_count > 0 {
                tracing::info!(
                    time_since_movement = time_since_movement,
                    stall_timeout = NONCE_STALL_TIMEOUT,
                    current_chain_nonce = current_chain_nonce,
                    "Nonce has been stalled, attempting gas bump"
                );

                // Attempt gas bump for the next expected nonce
                if let Err(e) = self
                    .attempt_gas_bump_for_stalled_nonce(scoped, chain, current_chain_nonce)
                    .await
                {
                    tracing::warn!(
                        error = %e,
                        "Failed to attempt gas bump for stalled nonce"
                    );
                }
            }

            tracing::debug!("No nonce progress, skipping confirm flow");
            return Ok(CleanupReport::default());
        }

        tracing::info!(
            current_chain_nonce = current_chain_nonce,
            cached_nonce = cached_nonce,
            "Processing confirmations"
        );

        // Get all pending transactions below the current chain nonce
        let waiting_txs = scoped
            .get_submitted_transactions_below_nonce(current_chain_nonce)
            .await?;

        if waiting_txs.is_empty() {
            tracing::debug!("No waiting transactions to confirm");
            return Ok(CleanupReport::default());
        }

        // Fetch receipts and categorize transactions
        let (confirmed_txs, replaced_txs) = self
            .fetch_confirmed_transaction_receipts(chain, waiting_txs)
            .await;

        // Process confirmed transactions
        let successes: Vec<ConfirmedTransaction> = confirmed_txs
            .into_iter()
            .map(|tx| {
                let receipt_data = match serde_json::to_string(&tx.receipt) {
                    Ok(receipt_json) => receipt_json,
                    Err(e) => {
                        tracing::warn!(
                            transaction_id = %tx.transaction_id,
                            hash = %tx.hash,
                            error = %e,
                            "Failed to serialize receipt as JSON, using debug format"
                        );
                        format!("{:?}", tx.receipt)
                    }
                };

                tracing::info!(
                    transaction_id = %tx.transaction_id,
                    nonce = tx.nonce,
                    hash = %tx.hash,
                    "Transaction confirmed"
                );

                ConfirmedTransaction {
                    hash: tx.hash,
                    transaction_id: tx.transaction_id,
                    receipt_data,
                }
            })
            .collect();

        let report = scoped
            .clean_submitted_transactions(&successes, current_chain_nonce - 1)
            .await?;

        // Update cached transaction count
        scoped
            .update_cached_transaction_count(current_chain_nonce)
            .await?;

        Ok(report)
    }

    // ========== SEND FLOW ==========
    #[tracing::instrument(skip_all)]
    async fn send_flow(
        &self,
        scoped: &AtomicEoaExecutorStore,
        chain: &impl Chain,
    ) -> Result<u32, EoaExecutorWorkerError> {
        // 1. Get EOA health (initializes if needed) and check if we should update balance
        let mut health = self.get_eoa_health(scoped, chain).await?;
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;

        // Update balance if it's stale
        // TODO: refactor this, very ugly
        if health.balance <= health.balance_threshold {
            if now - health.balance_fetched_at > HEALTH_CHECK_INTERVAL {
                let balance = chain
                    .provider()
                    .get_balance(scoped.eoa())
                    .await
                    .map_err(|e| {
                        let engine_error = e.to_engine_error(chain);
                        EoaExecutorWorkerError::RpcError {
                            message: format!("Failed to get balance: {}", engine_error),
                            inner_error: engine_error,
                        }
                    })?;

                health.balance = balance;
                health.balance_fetched_at = now;
                scoped.update_health_data(&health).await?;
            }

            if health.balance <= health.balance_threshold {
                tracing::warn!(
                    "EOA has insufficient balance (<= {} wei), skipping send flow",
                    health.balance_threshold
                );
                return Ok(0);
            }
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
        scoped: &AtomicEoaExecutorStore,
        chain: &impl Chain,
    ) -> Result<u32, EoaExecutorWorkerError> {
        let recycled_nonces = scoped.peek_recycled_nonces().await?;

        if recycled_nonces.is_empty() {
            return Ok(0);
        }

        let mut total_sent = 0;
        let mut remaining_nonces = recycled_nonces;

        // Loop to handle preparation failures and refill with new transactions
        while !remaining_nonces.is_empty() {
            // Get pending transactions to match with recycled nonces
            let pending_txs = scoped
                .peek_pending_transactions(remaining_nonces.len() as u64)
                .await?;

            if pending_txs.is_empty() {
                tracing::debug!("No pending transactions available for recycled nonces");
                break;
            }

            // Pair recycled nonces with pending transactions
            let mut build_tasks = Vec::new();
            let mut nonce_tx_pairs = Vec::new();

            for (i, nonce) in remaining_nonces.iter().enumerate() {
                if let Some(p_tx) = pending_txs.get(i) {
                    build_tasks.push(self.build_and_sign_single_transaction_with_retries(
                        scoped, p_tx, *nonce, chain,
                    ));
                    nonce_tx_pairs.push((*nonce, p_tx.clone()));
                } else {
                    // No more pending transactions for this recycled nonce
                    tracing::debug!("No pending transaction for recycled nonce {}", nonce);
                    break;
                }
            }

            if build_tasks.is_empty() {
                break;
            }

            // Build and sign all transactions in parallel
            let prepared_results = futures::future::join_all(build_tasks).await;

            // Separate successful preparations from failures
            let mut prepared_txs = Vec::new();
            let mut failed_tx_ids = Vec::new();
            let mut balance_threshold_update_needed = false;

            for (i, result) in prepared_results.into_iter().enumerate() {
                match result {
                    Ok(borrowed_data) => {
                        prepared_txs.push(borrowed_data);
                    }
                    Err(e) => {
                        // Track balance threshold issues
                        if let EoaExecutorWorkerError::TransactionSimulationFailed {
                            inner_error,
                            ..
                        } = &e
                        {
                            if should_update_balance_threshold(inner_error) {
                                balance_threshold_update_needed = true;
                            }
                        } else if let EoaExecutorWorkerError::RpcError { inner_error, .. } = &e {
                            if should_update_balance_threshold(inner_error) {
                                balance_threshold_update_needed = true;
                            }
                        }

                        let (_nonce, pending_tx) = &nonce_tx_pairs[i];
                        tracing::warn!(
                            "Failed to build recycled transaction {}: {}",
                            pending_tx.transaction_id,
                            e
                        );

                        // For deterministic build failures, fail the transaction immediately
                        if !is_retryable_preparation_error(&e) {
                            failed_tx_ids.push(pending_tx.transaction_id.clone());
                        }
                    }
                }
            }

            // Fail deterministic failures from pending state
            for tx_id in failed_tx_ids {
                if let Err(e) = scoped
                    .fail_pending_transaction(
                        &tx_id,
                        "Deterministic preparation failure",
                        self.webhook_queue.clone(),
                    )
                    .await
                {
                    tracing::error!("Failed to fail pending transaction {}: {}", tx_id, e);
                }
            }

            // Update balance threshold if needed
            if balance_threshold_update_needed {
                if let Err(e) = self.update_balance_threshold(scoped, chain).await {
                    tracing::error!(
                        "Failed to update balance threshold after build failures: {}",
                        e
                    );
                }
            }

            if prepared_txs.is_empty() {
                // No successful preparations, try again with more pending transactions
                // Remove the nonces we couldn't use from our list
                remaining_nonces = remaining_nonces
                    .into_iter()
                    .skip(nonce_tx_pairs.len())
                    .collect();
                continue;
            }

            // Move prepared transactions to borrowed state with recycled nonces
            let moved_count = scoped
                .atomic_move_pending_to_borrowed_with_recycled_nonces(&prepared_txs)
                .await?;

            tracing::debug!(
                moved_count = moved_count,
                total_prepared = prepared_txs.len(),
                "Moved transactions to borrowed state using recycled nonces"
            );

            // Actually send the transactions to the blockchain
            let send_tasks: Vec<_> = prepared_txs
                .iter()
                .map(|borrowed_tx| {
                    let signed_tx = borrowed_tx.signed_transaction.clone();
                    async move { chain.provider().send_tx_envelope(signed_tx.into()).await }
                })
                .collect();

            let send_results = futures::future::join_all(send_tasks).await;

            // Process send results and update states
            let mut submission_results = Vec::new();
            for (i, send_result) in send_results.into_iter().enumerate() {
                let borrowed_tx = &prepared_txs[i];
                let user_data = scoped
                    .get_transaction_data(&borrowed_tx.transaction_id)
                    .await?;

                let submission_result = SubmissionResult::from_send_result(
                    borrowed_tx,
                    send_result,
                    SendContext::InitialBroadcast,
                    user_data,
                    chain,
                );
                submission_results.push(submission_result);
            }

            // Use batch processing to handle all submission results
            let processing_report = scoped
                .process_borrowed_transactions(submission_results, self.webhook_queue.clone())
                .await?;

            tracing::debug!(
                "Processed {} borrowed transactions: {} moved to submitted, {} moved to pending, {} failed",
                processing_report.total_processed,
                processing_report.moved_to_submitted,
                processing_report.moved_to_pending,
                processing_report.failed_transactions
            );

            total_sent += processing_report.moved_to_submitted;

            // Remove the nonces we successfully processed from our list
            remaining_nonces = remaining_nonces.into_iter().skip(moved_count).collect();

            // If we didn't use all available nonces, we ran out of pending transactions
            if moved_count < nonce_tx_pairs.len() {
                break;
            }
        }

        Ok(total_sent as u32)
    }

    async fn process_new_transactions(
        &self,
        scoped: &AtomicEoaExecutorStore,
        chain: &impl Chain,
        budget: u64,
    ) -> Result<u32, EoaExecutorWorkerError> {
        if budget == 0 {
            return Ok(0);
        }

        let mut total_sent = 0;
        let mut remaining_budget = budget;

        // Loop to handle preparation failures and refill with new transactions
        while remaining_budget > 0 {
            // 1. Get pending transactions
            let pending_txs = scoped.peek_pending_transactions(remaining_budget).await?;
            if pending_txs.is_empty() {
                break;
            }

            let optimistic_nonce = scoped.get_optimistic_transaction_count().await?;

            // 2. Build and sign all transactions in parallel
            let build_tasks: Vec<_> = pending_txs
                .iter()
                .enumerate()
                .map(|(i, tx)| {
                    let expected_nonce = optimistic_nonce + i as u64;
                    self.build_and_sign_single_transaction_with_retries(
                        scoped,
                        tx,
                        expected_nonce,
                        chain,
                    )
                })
                .collect();

            let prepared_results = futures::future::join_all(build_tasks).await;

            // 3. Separate successful preparations from failures
            let mut prepared_txs = Vec::new();
            let mut failed_tx_ids = Vec::new();
            let mut balance_threshold_update_needed = false;

            for (i, result) in prepared_results.into_iter().enumerate() {
                match result {
                    Ok(borrowed_data) => {
                        prepared_txs.push(borrowed_data);
                    }
                    Err(e) => {
                        // Track balance threshold issues
                        if let EoaExecutorWorkerError::TransactionSimulationFailed {
                            inner_error,
                            ..
                        } = &e
                        {
                            if should_update_balance_threshold(inner_error) {
                                balance_threshold_update_needed = true;
                            }
                        } else if let EoaExecutorWorkerError::RpcError { inner_error, .. } = &e {
                            if should_update_balance_threshold(inner_error) {
                                balance_threshold_update_needed = true;
                            }
                        }

                        let pending_tx = &pending_txs[i];
                        tracing::warn!(
                            "Failed to build transaction {}: {}",
                            pending_tx.transaction_id,
                            e
                        );

                        // For deterministic build failures, fail the transaction immediately
                        if !is_retryable_preparation_error(&e) {
                            failed_tx_ids.push(pending_tx.transaction_id.clone());
                        }
                    }
                }
            }

            // 4. Fail deterministic failures from pending state
            for tx_id in failed_tx_ids {
                if let Err(e) = scoped
                    .fail_pending_transaction(
                        &tx_id,
                        "Deterministic preparation failure",
                        self.webhook_queue.clone(),
                    )
                    .await
                {
                    tracing::error!("Failed to fail pending transaction {}: {}", tx_id, e);
                }
            }

            // Update balance threshold if needed
            if balance_threshold_update_needed {
                if let Err(e) = self.update_balance_threshold(scoped, chain).await {
                    tracing::error!(
                        "Failed to update balance threshold after build failures: {}",
                        e
                    );
                }
            }

            if prepared_txs.is_empty() {
                // No successful preparations, try again with remaining budget
                remaining_budget = remaining_budget.saturating_sub(pending_txs.len() as u64);
                continue;
            }

            // 5. Move prepared transactions to borrowed state
            let moved_count = scoped
                .atomic_move_pending_to_borrowed_with_incremented_nonces(&prepared_txs)
                .await?;

            tracing::debug!(
                moved_count = moved_count,
                total_prepared = prepared_txs.len(),
                "Moved transactions to borrowed state using incremented nonces"
            );

            // 6. Actually send the transactions to the blockchain
            let send_tasks: Vec<_> = prepared_txs
                .iter()
                .map(|borrowed_tx| {
                    let signed_tx = borrowed_tx.signed_transaction.clone();
                    async move { chain.provider().send_tx_envelope(signed_tx.into()).await }
                })
                .collect();

            let send_results = futures::future::join_all(send_tasks).await;

            // 7. Process send results and update states
            let mut submission_results = Vec::new();
            for (i, send_result) in send_results.into_iter().enumerate() {
                let borrowed_tx = &prepared_txs[i];
                let user_data = scoped
                    .get_transaction_data(&borrowed_tx.transaction_id)
                    .await?;

                let submission_result = SubmissionResult::from_send_result(
                    borrowed_tx,
                    send_result,
                    SendContext::InitialBroadcast,
                    user_data,
                    chain,
                );
                submission_results.push(submission_result);
            }

            // 8. Use batch processing to handle all submission results
            let processing_report = scoped
                .process_borrowed_transactions(submission_results, self.webhook_queue.clone())
                .await?;

            tracing::debug!(
                "Processed {} borrowed transactions: {} moved to submitted, {} moved to pending, {} failed",
                processing_report.total_processed,
                processing_report.moved_to_submitted,
                processing_report.moved_to_pending,
                processing_report.failed_transactions
            );

            total_sent += processing_report.moved_to_submitted;
            remaining_budget = remaining_budget.saturating_sub(moved_count as u64);

            // If we didn't use all our budget, we ran out of pending transactions
            if moved_count < pending_txs.len() {
                break;
            }
        }

        Ok(total_sent as u32)
    }

    // ========== TRANSACTION BUILDING & SENDING ==========
    async fn build_and_sign_single_transaction_with_retries(
        &self,
        scoped: &AtomicEoaExecutorStore,
        pending_transaction: &PendingTransaction,
        nonce: u64,
        chain: &impl Chain,
    ) -> Result<BorrowedTransactionData, EoaExecutorWorkerError> {
        let mut last_error = None;

        // Internal retry loop for retryable errors
        for attempt in 0..=MAX_PREPARATION_RETRIES {
            if attempt > 0 {
                // Simple exponential backoff
                let delay = PREPARATION_RETRY_DELAY_MS * (2_u64.pow(attempt - 1));
                sleep(Duration::from_millis(delay)).await;

                tracing::debug!(
                    transaction_id = %pending_transaction.transaction_id,
                    attempt = attempt,
                    "Retrying transaction preparation"
                );
            }

            match self
                .build_and_sign_single_transaction(scoped, pending_transaction, nonce, chain)
                .await
            {
                Ok(result) => return Ok(result),
                Err(error) => {
                    if is_retryable_preparation_error(&error) && attempt < MAX_PREPARATION_RETRIES {
                        tracing::warn!(
                            transaction_id = %pending_transaction.transaction_id,
                            attempt = attempt,
                            error = %error,
                            "Retryable error during transaction preparation, will retry"
                        );
                        last_error = Some(error);
                        continue;
                    } else {
                        // Either deterministic error or exceeded max retries
                        return Err(error);
                    }
                }
            }
        }

        // This should never be reached, but just in case
        Err(
            last_error.unwrap_or_else(|| EoaExecutorWorkerError::InternalError {
                message: "Unexpected error in retry loop".to_string(),
            }),
        )
    }

    async fn build_and_sign_single_transaction(
        &self,
        scoped: &AtomicEoaExecutorStore,
        pending_transaction: &PendingTransaction,
        nonce: u64,
        chain: &impl Chain,
    ) -> Result<BorrowedTransactionData, EoaExecutorWorkerError> {
        // Get transaction data
        let tx_data = scoped
            .get_transaction_data(&pending_transaction.transaction_id)
            .await?
            .ok_or_else(|| EoaExecutorWorkerError::TransactionNotFound {
                transaction_id: pending_transaction.transaction_id.clone(),
            })?;

        // Build and sign transaction
        let signed_tx = self
            .build_and_sign_transaction(&tx_data, nonce, chain)
            .await?;

        Ok(BorrowedTransactionData {
            transaction_id: pending_transaction.transaction_id.clone(),
            hash: signed_tx.hash().to_string(),
            signed_transaction: signed_tx,
            borrowed_at: chrono::Utc::now().timestamp_millis().max(0) as u64,
            queued_at: pending_transaction.queued_at,
        })
    }

    async fn send_noop_transaction(
        &self,
        scoped: &AtomicEoaExecutorStore,
        chain: &impl Chain,
        nonce: u64,
        credential: SigningCredential,
    ) -> Result<String, EoaExecutorWorkerError> {
        // Create a minimal transaction to consume the recycled nonce
        // Send 0 ETH to self with minimal gas
        let eoa = scoped.eoa();

        // Build no-op transaction (send 0 to self)
        let tx_request = AlloyTransactionRequest::default()
            .with_from(eoa)
            .with_to(eoa) // Send to self
            .with_value(U256::ZERO) // Send 0 value
            .with_input(Bytes::new()) // No data
            .with_chain_id(scoped.chain_id())
            .with_nonce(nonce)
            .with_gas_limit(21000); // Minimal gas for basic transfer

        let tx_request = self.estimate_gas_fees(chain, tx_request).await?;
        let built_tx = tx_request.build_typed_tx().map_err(|e| {
            EoaExecutorWorkerError::TransactionBuildFailed {
                message: format!("Failed to build typed transaction for no-op: {e:?}"),
            }
        })?;

        let tx = self.sign_transaction(eoa, credential, built_tx).await?;

        chain
            .provider()
            .send_tx_envelope(tx.into())
            .await
            .map_err(|e| EoaExecutorWorkerError::TransactionSendError {
                message: format!("Failed to send no-op transaction: {e:?}"),
                inner_error: e.to_engine_error(chain),
            })
            .map(|pending| pending.tx_hash().to_string())
    }

    // ========== GAS BUMP METHODS ==========

    /// Attempt to gas bump a stalled transaction for the next expected nonce
    async fn attempt_gas_bump_for_stalled_nonce(
        &self,
        scoped: &AtomicEoaExecutorStore,
        chain: &impl Chain,
        expected_nonce: u64,
    ) -> Result<bool, EoaExecutorWorkerError> {
        tracing::info!(
            nonce = expected_nonce,
            "Attempting gas bump for stalled nonce"
        );

        // Get all transaction IDs for this nonce
        let submitted_transactions = scoped
            .get_submitted_transactions_for_nonce(expected_nonce)
            .await?;

        if submitted_transactions.is_empty() {
            tracing::debug!(
                nonce = expected_nonce,
                "No transactions found for stalled nonce"
            );
            return Ok(false);
        }

        // Load transaction data for all IDs and find the newest one
        let mut newest_transaction: Option<(String, TransactionData)> = None;
        let mut newest_submitted_at = 0u64;

        for SubmittedTransaction { transaction_id, .. } in submitted_transactions {
            if let Some(tx_data) = scoped.get_transaction_data(&transaction_id).await? {
                // Find the most recent attempt for this transaction
                if let Some(latest_attempt) = tx_data.attempts.last() {
                    let submitted_at = latest_attempt.sent_at;
                    if submitted_at > newest_submitted_at {
                        newest_submitted_at = submitted_at;
                        newest_transaction = Some((transaction_id, tx_data));
                    }
                }
            }
        }

        if let Some((transaction_id, tx_data)) = newest_transaction {
            tracing::info!(
                transaction_id = %transaction_id,
                nonce = expected_nonce,
                "Found newest transaction for gas bump"
            );

            // Get the latest attempt to extract gas values from
            // Build typed transaction -> manually bump -> sign
            let typed_tx = match self
                .build_typed_transaction(&tx_data, expected_nonce, chain)
                .await
            {
                Ok(tx) => tx,
                Err(e) => {
                    // Check if this is a balance threshold issue during simulation
                    if let EoaExecutorWorkerError::TransactionSimulationFailed {
                        inner_error, ..
                    } = &e
                    {
                        if should_update_balance_threshold(inner_error) {
                            if let Err(e) = self.update_balance_threshold(scoped, chain).await {
                                tracing::error!("Failed to update balance threshold: {}", e);
                            }
                        }
                    } else if let EoaExecutorWorkerError::RpcError { inner_error, .. } = &e {
                        if should_update_balance_threshold(inner_error) {
                            if let Err(e) = self.update_balance_threshold(scoped, chain).await {
                                tracing::error!("Failed to update balance threshold: {}", e);
                            }
                        }
                    }

                    tracing::warn!(
                        transaction_id = %transaction_id,
                        nonce = expected_nonce,
                        error = %e,
                        "Failed to build typed transaction for gas bump"
                    );
                    return Ok(false);
                }
            };
            let bumped_typed_tx = self.apply_gas_bump_to_typed_transaction(typed_tx, 120); // 20% increase
            let bumped_tx = match self
                .sign_transaction(
                    tx_data.user_request.from,
                    tx_data.user_request.signing_credential,
                    bumped_typed_tx,
                )
                .await
            {
                Ok(tx) => tx,
                Err(e) => {
                    tracing::warn!(
                        transaction_id = %transaction_id,
                        nonce = expected_nonce,
                        error = %e,
                        "Failed to sign transaction for gas bump"
                    );
                    return Ok(false);
                }
            };

            // Record the gas bump attempt
            scoped
                .add_gas_bump_attempt(
                    &SubmittedTransaction {
                        nonce: expected_nonce,
                        hash: bumped_tx.hash().to_string(),
                        transaction_id: transaction_id.to_string(),
                        queued_at: tx_data.created_at,
                    },
                    bumped_tx.clone(),
                )
                .await?;

            // Send the bumped transaction
            let tx_envelope = bumped_tx.into();
            match chain.provider().send_tx_envelope(tx_envelope).await {
                Ok(_) => {
                    tracing::info!(
                        transaction_id = %transaction_id,
                        nonce = expected_nonce,
                        "Successfully sent gas bumped transaction"
                    );
                    return Ok(true);
                }
                Err(e) => {
                    tracing::warn!(
                        transaction_id = %transaction_id,
                        nonce = expected_nonce,
                        error = %e,
                        "Failed to send gas bumped transaction"
                    );
                    // Don't fail the worker, just log the error
                    return Ok(false);
                }
            }
        }

        Ok(false)
    }

    // ========== HEALTH ACCESSOR ==========

    /// Get EOA health, initializing it if it doesn't exist
    /// This method ensures the health data is always available for the worker
    async fn get_eoa_health(
        &self,
        scoped: &AtomicEoaExecutorStore,
        chain: &impl Chain,
    ) -> Result<EoaHealth, EoaExecutorWorkerError> {
        let store_health = scoped.check_eoa_health().await?;
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;

        match store_health {
            Some(health) => Ok(health),
            None => {
                // Initialize with fresh data from chain
                let balance = chain
                    .provider()
                    .get_balance(scoped.eoa())
                    .await
                    .map_err(|e| {
                        let engine_error = e.to_engine_error(chain);
                        EoaExecutorWorkerError::RpcError {
                            message: format!(
                                "Failed to get balance during initialization: {}",
                                engine_error
                            ),
                            inner_error: engine_error,
                        }
                    })?;

                let health = EoaHealth {
                    balance,
                    balance_threshold: U256::ZERO,
                    balance_fetched_at: now,
                    last_confirmation_at: now,
                    last_nonce_movement_at: now,
                    nonce_resets: Vec::new(),
                };

                // Save to store
                scoped.update_health_data(&health).await?;
                Ok(health)
            }
        }
    }

    #[tracing::instrument(skip_all, fields(eoa = %scoped.eoa(), chain_id = %chain.chain_id()))]
    async fn update_balance_threshold(
        &self,
        scoped: &AtomicEoaExecutorStore,
        chain: &impl Chain,
    ) -> Result<(), EoaExecutorWorkerError> {
        let mut health = self.get_eoa_health(scoped, chain).await?;

        tracing::info!("Updating balance threshold");
        let balance_threshold = chain
            .provider()
            .get_balance(scoped.eoa())
            .await
            .map_err(|e| {
                let engine_error = e.to_engine_error(chain);
                EoaExecutorWorkerError::RpcError {
                    message: format!("Failed to get balance: {}", engine_error),
                    inner_error: engine_error,
                }
            })?;

        health.balance_threshold = balance_threshold;
        scoped.update_health_data(&health).await?;
        Ok(())
    }

    /// Fetch receipts for all submitted transactions and categorize them
    async fn fetch_confirmed_transaction_receipts(
        &self,
        chain: &impl Chain,
        submitted_txs: Vec<SubmittedTransaction>,
    ) -> (
        Vec<ConfirmedTransactionWithRichReceipt>,
        Vec<ReplacedTransaction>,
    ) {
        // Fetch all receipts in parallel
        let receipt_futures: Vec<_> = submitted_txs
            .iter()
            .filter_map(|tx| match tx.hash.parse::<B256>() {
                Ok(hash_bytes) => Some(async move {
                    let receipt = chain.provider().get_transaction_receipt(hash_bytes).await;
                    (tx, receipt)
                }),
                Err(_) => {
                    tracing::warn!("Invalid hash format: {}, skipping", tx.hash);
                    None
                }
            })
            .collect();

        let receipt_results = futures::future::join_all(receipt_futures).await;

        // Categorize transactions
        let mut confirmed_txs = Vec::new();
        let mut failed_txs = Vec::new();

        for (tx, receipt_result) in receipt_results {
            match receipt_result {
                Ok(Some(receipt)) => {
                    confirmed_txs.push(ConfirmedTransactionWithRichReceipt {
                        nonce: tx.nonce,
                        hash: tx.hash.clone(),
                        transaction_id: tx.transaction_id.clone(),
                        receipt,
                    });
                }
                Ok(None) | Err(_) => {
                    failed_txs.push(ReplacedTransaction {
                        hash: tx.hash.clone(),
                        transaction_id: tx.transaction_id.clone(),
                    });
                }
            }
        }

        (confirmed_txs, failed_txs)
    }

    // ========== HELPER METHODS ==========
    async fn estimate_gas_fees(
        &self,
        chain: &impl Chain,
        tx: AlloyTransactionRequest,
    ) -> Result<AlloyTransactionRequest, EoaExecutorWorkerError> {
        // Check what fees are missing and need to be estimated

        // If we have gas_price set, we're doing legacy - don't estimate EIP-1559
        if tx.gas_price.is_some() {
            return Ok(tx);
        }

        // If we have both EIP-1559 fees set, don't estimate
        if tx.max_fee_per_gas.is_some() && tx.max_priority_fee_per_gas.is_some() {
            return Ok(tx);
        }

        // Try EIP-1559 fees first, fall back to legacy if unsupported
        match chain.provider().estimate_eip1559_fees().await {
            Ok(eip1559_fees) => {
                tracing::debug!(
                    "Using EIP-1559 fees: max_fee={}, max_priority_fee={}",
                    eip1559_fees.max_fee_per_gas,
                    eip1559_fees.max_priority_fee_per_gas
                );

                let mut result = tx;
                // Only set fees that are missing
                if result.max_fee_per_gas.is_none() {
                    result = result.with_max_fee_per_gas(eip1559_fees.max_fee_per_gas);
                }
                if result.max_priority_fee_per_gas.is_none() {
                    result =
                        result.with_max_priority_fee_per_gas(eip1559_fees.max_priority_fee_per_gas);
                }

                Ok(result)
            }
            Err(eip1559_error) => {
                // Check if this is an "unsupported feature" error
                if let RpcError::UnsupportedFeature(_) = &eip1559_error {
                    tracing::debug!("EIP-1559 not supported, falling back to legacy gas price");

                    // Fall back to legacy gas price only if no gas price is set
                    if tx.authorization_list().is_none() {
                        match chain.provider().get_gas_price().await {
                            Ok(gas_price) => {
                                tracing::debug!("Using legacy gas price: {}", gas_price);
                                Ok(tx.with_gas_price(gas_price))
                            }
                            Err(legacy_error) => Err(EoaExecutorWorkerError::RpcError {
                                message: format!(
                                    "Failed to get legacy gas price: {}",
                                    legacy_error
                                ),
                                inner_error: legacy_error.to_engine_error(chain),
                            }),
                        }
                    } else {
                        Err(EoaExecutorWorkerError::TransactionBuildFailed {
                            message: "EIP7702 transactions not supported on chain".to_string(),
                        })
                    }
                } else {
                    // Other EIP-1559 error
                    Err(EoaExecutorWorkerError::RpcError {
                        message: format!("Failed to estimate EIP-1559 fees: {}", eip1559_error),
                        inner_error: eip1559_error.to_engine_error(chain),
                    })
                }
            }
        }
    }

    async fn build_typed_transaction(
        &self,
        tx_data: &TransactionData,
        nonce: u64,
        chain: &impl Chain,
    ) -> Result<TypedTransaction, EoaExecutorWorkerError> {
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

        // Handle gas fees - either from user settings or estimation
        tx_request = if let Some(type_data) = &tx_data.user_request.transaction_type_data {
            // User provided gas settings - respect them first
            match type_data {
                TransactionTypeData::Eip1559(data) => {
                    let mut req = tx_request;
                    if let Some(max_fee) = data.max_fee_per_gas {
                        req = req.with_max_fee_per_gas(max_fee);
                    }
                    if let Some(max_priority) = data.max_priority_fee_per_gas {
                        req = req.with_max_priority_fee_per_gas(max_priority);
                    }

                    // if either not set, estimate the other one
                    if req.max_fee_per_gas.is_none() || req.max_priority_fee_per_gas.is_none() {
                        req = self.estimate_gas_fees(chain, req).await?;
                    }

                    req
                }
                TransactionTypeData::Legacy(data) => {
                    if let Some(gas_price) = data.gas_price {
                        tx_request.with_gas_price(gas_price)
                    } else {
                        // User didn't provide gas price, estimate it
                        self.estimate_gas_fees(chain, tx_request).await?
                    }
                }
                TransactionTypeData::Eip7702(data) => {
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

                    // if either not set, estimate the other one
                    if req.max_fee_per_gas.is_none() || req.max_priority_fee_per_gas.is_none() {
                        req = self.estimate_gas_fees(chain, req).await?;
                    }

                    req
                }
            }
        } else {
            // No user settings - estimate appropriate fees
            self.estimate_gas_fees(chain, tx_request).await?
        };

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
                            return Err(EoaExecutorWorkerError::TransactionSimulationFailed {
                                message: format!(
                                    "Transaction reverted during gas estimation: {} (revert: {})",
                                    error_payload.message,
                                    hex::encode(&revert_data)
                                ),
                                inner_error: e.to_engine_error(chain),
                            });
                        }
                    }

                    // Not a revert - could be RPC issue, this should nack the worker
                    let engine_error = e.to_engine_error(chain);
                    return Err(EoaExecutorWorkerError::RpcError {
                        message: format!("Gas estimation failed: {}", engine_error),
                        inner_error: engine_error,
                    });
                }
            }
        }

        // Build typed transaction
        tx_request
            .build_typed_tx()
            .map_err(|e| EoaExecutorWorkerError::TransactionBuildFailed {
                message: format!("Failed to build typed transaction: {:?}", e),
            })
    }

    async fn sign_transaction(
        &self,
        from: Address,
        credential: SigningCredential,
        typed_tx: TypedTransaction,
    ) -> Result<Signed<TypedTransaction>, EoaExecutorWorkerError> {
        let signing_options = EoaSigningOptions {
            from,
            chain_id: typed_tx.chain_id(),
        };

        let signature = self
            .eoa_signer
            .sign_transaction(signing_options, typed_tx.clone(), credential)
            .await
            .map_err(|engine_error| EoaExecutorWorkerError::SigningError {
                message: format!("Failed to sign transaction: {}", engine_error),
                inner_error: engine_error,
            })?;

        let signature = signature.parse::<Signature>().map_err(|e| {
            EoaExecutorWorkerError::SignatureParsingFailed {
                message: format!("Failed to parse signature: {}", e),
            }
        })?;

        Ok(typed_tx.into_signed(signature))
    }

    async fn build_and_sign_transaction(
        &self,
        tx_data: &TransactionData,
        nonce: u64,
        chain: &impl Chain,
    ) -> Result<Signed<TypedTransaction>, EoaExecutorWorkerError> {
        let typed_tx = self.build_typed_transaction(tx_data, nonce, chain).await?;
        self.sign_transaction(
            tx_data.user_request.from,
            tx_data.user_request.signing_credential.clone(),
            typed_tx,
        )
        .await
    }

    fn apply_gas_bump_to_typed_transaction(
        &self,
        mut typed_tx: TypedTransaction,
        bump_multiplier: u32, // e.g., 120 for 20% increase
    ) -> TypedTransaction {
        match &mut typed_tx {
            TypedTransaction::Eip1559(tx) => {
                tx.max_fee_per_gas = tx.max_fee_per_gas * bump_multiplier as u128 / 100;
                tx.max_priority_fee_per_gas =
                    tx.max_priority_fee_per_gas * bump_multiplier as u128 / 100;
            }
            TypedTransaction::Legacy(tx) => {
                tx.gas_price = tx.gas_price * bump_multiplier as u128 / 100;
            }
            TypedTransaction::Eip2930(tx) => {
                tx.gas_price = tx.gas_price * bump_multiplier as u128 / 100;
            }
            TypedTransaction::Eip7702(tx) => {
                tx.max_fee_per_gas = tx.max_fee_per_gas * bump_multiplier as u128 / 100;
                tx.max_priority_fee_per_gas =
                    tx.max_priority_fee_per_gas * bump_multiplier as u128 / 100;
            }
            TypedTransaction::Eip4844(tx) => match tx {
                TxEip4844Variant::TxEip4844(tx) => {
                    tx.max_fee_per_gas = tx.max_fee_per_gas * bump_multiplier as u128 / 100;
                    tx.max_priority_fee_per_gas =
                        tx.max_priority_fee_per_gas * bump_multiplier as u128 / 100;
                }
                TxEip4844Variant::TxEip4844WithSidecar(TxEip4844WithSidecar { tx, .. }) => {
                    tx.max_fee_per_gas = tx.max_fee_per_gas * bump_multiplier as u128 / 100;
                    tx.max_priority_fee_per_gas =
                        tx.max_priority_fee_per_gas * bump_multiplier as u128 / 100;
                }
            },
        }
        typed_tx
    }
}
