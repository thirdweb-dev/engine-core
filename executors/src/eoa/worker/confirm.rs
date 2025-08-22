use alloy::{primitives::B256, providers::Provider};
use engine_core::{chain::Chain, error::AlloyRpcErrorToEngineError};
use serde::{Deserialize, Serialize};

use crate::eoa::{
    EoaExecutorStore,
    store::{
        CleanupReport, ConfirmedTransaction, ReplacedTransaction, SubmittedTransactionDehydrated,
        TransactionStoreError,
    },
    worker::{
        EoaExecutorWorker,
        error::{EoaExecutorWorkerError, should_update_balance_threshold},
    },
};

const NONCE_STALL_LIMIT_MS: u64 = 300_000; // 5 minutes in milliseconds - after this time, attempt gas bump

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfirmedTransactionWithRichReceipt {
    pub nonce: u64,
    pub transaction_hash: String,
    pub transaction_id: String,
    pub receipt: alloy::rpc::types::TransactionReceipt,
}

impl<C: Chain> EoaExecutorWorker<C> {
    // ========== CONFIRM FLOW ==========
    #[tracing::instrument(skip_all, fields(worker_id = self.store.worker_id))]
    pub async fn confirm_flow(&self) -> Result<CleanupReport, EoaExecutorWorkerError> {
        // Get fresh on-chain transaction count
        let current_chain_transaction_count = self
            .chain
            .provider()
            .get_transaction_count(self.eoa)
            .await
            .map_err(|e| {
                let engine_error = e.to_engine_error(&self.chain);
                EoaExecutorWorkerError::RpcError {
                    message: format!("Failed to get transaction count: {engine_error}"),
                    inner_error: engine_error,
                }
            })?;

        if self.store.is_manual_reset_scheduled().await? {
            tracing::info!("Manual reset scheduled, executing now");
            self.store
                .reset_nonces(current_chain_transaction_count)
                .await?;
        }

        let cached_transaction_count = match self.store.get_cached_transaction_count().await {
            Err(e) => match e {
                TransactionStoreError::NonceSyncRequired { .. } => {
                    tracing::warn!(
                        cached_transaction_count = current_chain_transaction_count,
                        "Nonce sync required, store was uninitialized, updating cached transaction count with current chain transaction count"
                    );
                    self.store
                        .update_cached_transaction_count(current_chain_transaction_count)
                        .await?;
                    current_chain_transaction_count
                }
                _ => return Err(e.into()),
            },
            Ok(cached_nonce) => cached_nonce,
        };

        let submitted_count = self.store.get_submitted_transactions_count().await?;

        // no nonce progress
        if current_chain_transaction_count <= cached_transaction_count {
            let current_health = self.get_eoa_health().await?;
            let now = EoaExecutorStore::now();
            // No nonce progress - check if we should attempt gas bumping for stalled nonce
            let time_since_movement = now.saturating_sub(current_health.last_nonce_movement_at);

            // if there are waiting transactions, we can attempt a gas bump
            if time_since_movement > NONCE_STALL_LIMIT_MS && submitted_count > 0 {
                tracing::info!(
                    time_since_movement = time_since_movement,
                    stall_timeout = NONCE_STALL_LIMIT_MS,
                    current_chain_nonce = current_chain_transaction_count,
                    cached_transaction_count = cached_transaction_count,
                    "Nonce has been stalled, attempting gas bump"
                );

                // Attempt gas bump for the next expected nonce
                if let Err(e) = self
                    .attempt_gas_bump_for_stalled_nonce(current_chain_transaction_count)
                    .await
                {
                    tracing::warn!(
                        error = ?e,
                        "Failed to attempt gas bump for stalled nonce"
                    );
                }
            }

            tracing::debug!("No nonce progress, still going ahead with confirm flow");
            // return Ok(CleanupReport::default());
        }

        tracing::info!(
            current_chain_nonce = current_chain_transaction_count,
            cached_transaction_count = cached_transaction_count,
            "Processing confirmations"
        );

        // Get all pending transactions below the current chain transaction count
        // ie, if transaction count is 1, nonce 0 should have mined
        let waiting_txs = self
            .store
            .get_submitted_transactions_below_chain_transaction_count(
                current_chain_transaction_count,
            )
            .await?;

        if waiting_txs.is_empty() {
            tracing::debug!("No waiting transactions to confirm");
            return Ok(CleanupReport::default());
        }

        // Fetch receipts and categorize transactions
        let (confirmed_txs, _replaced_txs) =
            self.fetch_confirmed_transaction_receipts(waiting_txs).await;

        // Process confirmed transactions
        let successes: Vec<ConfirmedTransaction> = confirmed_txs
            .into_iter()
            .map(|tx| {
                let receipt_data = match serde_json::to_string(&tx.receipt) {
                    Ok(receipt_json) => receipt_json,
                    Err(e) => {
                        tracing::warn!(
                            transaction_id = ?tx.transaction_id,
                            hash = tx.transaction_hash,
                            error = ?e,
                            "Failed to serialize receipt as JSON, using debug format"
                        );
                        format!("{:?}", tx.receipt)
                    }
                };

                tracing::info!(
                    transaction_id = ?tx.transaction_id,
                    nonce = tx.nonce,
                    hash = tx.transaction_hash,
                    "Transaction confirmed"
                );

                ConfirmedTransaction {
                    transaction_hash: tx.transaction_hash,
                    transaction_id: tx.transaction_id,
                    receipt: tx.receipt,
                    receipt_serialized: receipt_data,
                }
            })
            .collect();

        let report = self
            .store
            .clean_submitted_transactions(
                &successes,
                current_chain_transaction_count - 1,
                self.webhook_queue.clone(),
            )
            .await?;

        if current_chain_transaction_count != cached_transaction_count {
            if current_chain_transaction_count < cached_transaction_count {
                tracing::error!(
                    current_chain_transaction_count = current_chain_transaction_count,
                    cached_transaction_count = cached_transaction_count,
                    "Fresh fetched chain transaction count is lower than cached transaction count. \
                    This indicates a re-org or RPC block lag. Engine will use the newest fetched transaction count from now (assuming re-org).\
                    Transactions already confirmed will not be attempted again, even if their nonce was higher than the new chain transaction count.
                    In case this is RPC misbehaviour not reflective of actual chain state, Engine's nonce management might be affected."
                );
            }

            self.store
                .update_cached_transaction_count(current_chain_transaction_count)
                .await?;
        }

        Ok(report)
    }

    /// Fetch receipts for all submitted transactions and categorize them
    async fn fetch_confirmed_transaction_receipts(
        &self,
        submitted_txs: Vec<SubmittedTransactionDehydrated>,
    ) -> (
        Vec<ConfirmedTransactionWithRichReceipt>,
        Vec<ReplacedTransaction>,
    ) {
        // Fetch all receipts in parallel
        let receipt_futures: Vec<_> = submitted_txs
            .iter()
            .filter_map(|tx| match tx.transaction_hash.parse::<B256>() {
                Ok(hash_bytes) => Some(async move {
                    let receipt = self
                        .chain
                        .provider()
                        .get_transaction_receipt(hash_bytes)
                        .await;
                    (tx, receipt)
                }),
                Err(_) => {
                    tracing::warn!("Invalid hash format: {}, skipping", tx.transaction_hash);
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
                        transaction_hash: tx.transaction_hash.clone(),
                        transaction_id: tx.transaction_id.clone(),
                        receipt,
                    });
                }
                Ok(None) | Err(_) => {
                    failed_txs.push(ReplacedTransaction {
                        transaction_hash: tx.transaction_hash.clone(),
                        transaction_id: tx.transaction_id.clone(),
                    });
                }
            }
        }

        (confirmed_txs, failed_txs)
    }

    // ========== GAS BUMP METHODS ==========

    /// Attempt to gas bump a stalled transaction for the next expected nonce
    async fn attempt_gas_bump_for_stalled_nonce(
        &self,
        expected_nonce: u64,
    ) -> Result<bool, EoaExecutorWorkerError> {
        tracing::info!(
            nonce = expected_nonce,
            "Attempting gas bump for stalled nonce"
        );

        // Get all transaction IDs for this nonce
        let submitted_transactions = self
            .store
            .get_submitted_transactions_for_nonce(expected_nonce)
            .await?;

        // Load transaction data for all IDs and find the newest one
        let newest_transaction = if submitted_transactions.len() == 1 {
            submitted_transactions.first()
        } else {
            submitted_transactions
                .iter()
                .max_by_key(|tx| tx.submitted_at)
        };

        let newest_transaction_data = match newest_transaction {
            Some(tx) => self.store.get_transaction_data(&tx.transaction_id).await?,
            None => None,
        };

        if let Some(newest_transaction_data) = newest_transaction_data {
            tracing::info!(
                transaction_id = ?newest_transaction_data.transaction_id,
                nonce = expected_nonce,
                "Found newest transaction for gas bump"
            );

            // Get the latest attempt to extract gas values from
            // Build typed transaction -> manually bump -> sign
            let typed_tx = match self
                .build_typed_transaction(&newest_transaction_data.user_request, expected_nonce)
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
                            if let Err(e) = self.update_balance_threshold().await {
                                tracing::error!("Failed to update balance threshold: {}", e);
                            }
                        }
                    } else if let EoaExecutorWorkerError::RpcError { inner_error, .. } = &e {
                        if should_update_balance_threshold(inner_error) {
                            if let Err(e) = self.update_balance_threshold().await {
                                tracing::error!("Failed to update balance threshold: {}", e);
                            }
                        }
                    }

                    tracing::warn!(
                        transaction_id = ?newest_transaction_data.transaction_id,
                        nonce = expected_nonce,
                        error = ?e,
                        "Failed to build typed transaction for gas bump"
                    );
                    return Ok(false);
                }
            };
            let bumped_typed_tx = self.apply_gas_bump_to_typed_transaction(typed_tx, 120); // 20% increase
            let bumped_tx = match self
                .sign_transaction(
                    bumped_typed_tx,
                    &newest_transaction_data.user_request.signing_credential,
                )
                .await
            {
                Ok(tx) => tx,
                Err(e) => {
                    tracing::warn!(
                        transaction_id = ?newest_transaction_data.transaction_id,
                        nonce = expected_nonce,
                        error = ?e,
                        "Failed to sign transaction for gas bump"
                    );
                    return Ok(false);
                }
            };

            // Record the gas bump attempt
            self.store
                .add_gas_bump_attempt(
                    &SubmittedTransactionDehydrated {
                        nonce: expected_nonce,
                        transaction_hash: bumped_tx.hash().to_string(),
                        transaction_id: newest_transaction_data.transaction_id.clone(),
                        submitted_at: EoaExecutorStore::now(),
                        queued_at: newest_transaction_data.created_at,
                    },
                    bumped_tx.clone(),
                )
                .await?;

            // Send the bumped transaction
            let tx_envelope = bumped_tx.into();
            match self.chain.provider().send_tx_envelope(tx_envelope).await {
                Ok(_) => {
                    tracing::info!(
                        transaction_id = ?newest_transaction_data.transaction_id,
                        nonce = expected_nonce,
                        "Successfully sent gas bumped transaction"
                    );
                    Ok(true)
                }
                Err(e) => {
                    tracing::warn!(
                        transaction_id = ?newest_transaction_data.transaction_id,
                        nonce = expected_nonce,
                        error = ?e,
                        "Failed to send gas bumped transaction"
                    );
                    // Don't fail the worker, just log the error
                    Ok(false)
                }
            }
        } else {
            tracing::debug!(
                nonce = expected_nonce,
                "Successfully retrieved all transactions for this nonce, but failed to find newest transaction for gas bump, sending noop"
            );

            let noop_tx = self.send_noop_transaction(expected_nonce).await?;
            self.store.process_noop_transactions(&[noop_tx]).await?;
            Ok(true)
        }
    }
}
