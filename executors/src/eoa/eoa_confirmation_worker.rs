use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{BlockNumberOrTag, TransactionReceipt};
use alloy::transports::{RpcError, TransportErrorKind};
use engine_core::{
    chain::{Chain, ChainService, RpcCredentials},
    error::{AlloyRpcErrorToEngineError, EngineError, RpcErrorKind},
    execution_options::WebhookOptions,
};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use twmq::{
    FailHookData, NackHookData, Queue, SuccessHookData, UserCancellable,
    error::TwmqError,
    hooks::TransactionContext,
    job::{BorrowedJob, DelayOptions, JobResult, RequeuePosition, ToJobError, ToJobResult},
};

use crate::{
    transaction_registry::TransactionRegistry,
    webhook::{
        WebhookJobHandler,
        envelope::{ExecutorStage, WebhookCapable},
    },
};

use super::{
    nonce_manager::NonceManager,
    send::{EoaSendHandler, EoaSendJobData},
    transaction_store::{ActiveAttempt, ConfirmationData, TransactionStore},
};

// --- Job Payload ---
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EoaConfirmationWorkerJobData {
    pub eoa: Address,
    pub chain_id: u64,
    pub rpc_credentials: RpcCredentials,
}

// --- Success Result ---
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EoaConfirmationWorkerResult {
    pub eoa: Address,
    pub chain_id: u64,
    pub nonce_progression: Option<(u64, u64)>, // (from, to)
    pub transactions_confirmed: u32,
    pub transactions_requeued: u32,
}

// --- Error Types ---
#[derive(Serialize, Deserialize, Debug, Clone, thiserror::Error)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "errorCode")]
pub enum EoaConfirmationWorkerError {
    #[error("Chain service error for chainId {chain_id}: {message}")]
    ChainServiceError { chain_id: u64, message: String },

    #[error("RPC error: {message}")]
    RpcError { message: String, retryable: bool },

    #[error("Active transactions found - will continue monitoring")]
    ActiveTransactionsFound { count: u32 },

    #[error("Sync required for EOA {eoa}")]
    SyncRequired { eoa: Address },

    #[error("Invalid RPC Credentials: {message}")]
    InvalidRpcCredentials { message: String },

    #[error("Internal error: {message}")]
    InternalError { message: String },

    #[error("Worker cancelled by user")]
    UserCancelled,
}

impl From<TwmqError> for EoaConfirmationWorkerError {
    fn from(error: TwmqError) -> Self {
        EoaConfirmationWorkerError::InternalError {
            message: format!("Queue error: {}", error),
        }
    }
}

impl UserCancellable for EoaConfirmationWorkerError {
    fn user_cancelled() -> Self {
        EoaConfirmationWorkerError::UserCancelled
    }
}

// --- Handler ---
pub struct EoaConfirmationWorker<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    pub chain_service: Arc<CS>,
    pub nonce_manager: Arc<NonceManager>,
    pub transaction_store: Arc<TransactionStore>,
    pub send_queue: Arc<Queue<EoaSendHandler<CS>>>,
    pub webhook_queue: Arc<Queue<WebhookJobHandler>>,
    pub transaction_registry: Arc<TransactionRegistry>,
}

impl<CS> ExecutorStage for EoaConfirmationWorker<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    fn executor_name() -> &'static str {
        "eoa"
    }

    fn stage_name() -> &'static str {
        "confirmation_worker"
    }
}

impl<CS> WebhookCapable for EoaConfirmationWorker<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    fn webhook_queue(&self) -> &Arc<Queue<WebhookJobHandler>> {
        &self.webhook_queue
    }
}

impl<CS> twmq::DurableExecution for EoaConfirmationWorker<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    type Output = EoaConfirmationWorkerResult;
    type ErrorData = EoaConfirmationWorkerError;
    type JobData = EoaConfirmationWorkerJobData;

    #[tracing::instrument(skip(self, job), fields(eoa = %job.job.data.eoa, chain_id = job.job.data.chain_id, stage = Self::stage_name(), executor = Self::executor_name()))]
    async fn process(
        &self,
        job: &BorrowedJob<Self::JobData>,
    ) -> JobResult<Self::Output, Self::ErrorData> {
        let job_data = &job.job.data;

        // 1. Get Chain
        let chain = self
            .chain_service
            .get_chain(job_data.chain_id)
            .map_err(|e| EoaConfirmationWorkerError::ChainServiceError {
                chain_id: job_data.chain_id,
                message: format!("Failed to get chain instance: {}", e),
            })
            .map_err_fail()?;

        let chain_auth_headers = job_data
            .rpc_credentials
            .to_header_map()
            .map_err(|e| EoaConfirmationWorkerError::InvalidRpcCredentials {
                message: e.to_string(),
            })
            .map_err_fail()?;

        let chain = chain.with_new_default_headers(chain_auth_headers);

        // 2. Get current onchain nonce
        let onchain_nonce = match chain.provider().get_transaction_count(job_data.eoa).await {
            Ok(nonce) => nonce,
            Err(e) => {
                let engine_error = e.to_engine_error(&chain);
                return Err(EoaConfirmationWorkerError::RpcError {
                    message: format!("Failed to get transaction count: {}", engine_error),
                    retryable: true,
                }
                .nack(Some(Duration::from_secs(5)), RequeuePosition::Last));
            }
        };

        // 3. Get cached onchain nonce to detect progression
        let cached_nonce = self
            .nonce_manager
            .get_cached_onchain_nonce(job_data.eoa, job_data.chain_id)
            .await
            .map_err(|e| EoaConfirmationWorkerError::InternalError {
                message: format!("Failed to get cached nonce: {}", e),
            })
            .map_err_fail()?
            .unwrap_or(0);

        tracing::debug!(
            eoa = %job_data.eoa,
            onchain_nonce = %onchain_nonce,
            cached_nonce = %cached_nonce,
            "Checking nonce progression"
        );

        // 4. Check if nonce has progressed
        let nonce_progression = if onchain_nonce > cached_nonce {
            Some((cached_nonce, onchain_nonce))
        } else {
            None
        };

        // 5. Get all active transactions for this EOA
        let active_transaction_ids = self
            .transaction_store
            .get_active_transactions(job_data.eoa, job_data.chain_id)
            .await
            .map_err(|e| EoaConfirmationWorkerError::InternalError {
                message: format!("Failed to get active transactions: {}", e),
            })
            .map_err_fail()?;

        // 6. If no active transactions and no progression, we're done
        if active_transaction_ids.is_empty() && nonce_progression.is_none() {
            tracing::debug!(
                eoa = %job_data.eoa,
                "No active transactions and no nonce progression - stopping worker"
            );

            return Ok(EoaConfirmationWorkerResult {
                eoa: job_data.eoa,
                chain_id: job_data.chain_id,
                nonce_progression: None,
                transactions_confirmed: 0,
                transactions_requeued: 0,
            });
        }

        // 7. Process any nonce progression
        let mut transactions_confirmed = 0;
        let mut transactions_requeued = 0;

        if let Some((from_nonce, to_nonce)) = nonce_progression {
            tracing::info!(
                eoa = %job_data.eoa,
                from_nonce = %from_nonce,
                to_nonce = %to_nonce,
                "Processing nonce progression"
            );

            let (confirmed, requeued) = self
                .process_nonce_progression(job_data, &chain, from_nonce, to_nonce)
                .await
                .map_err_fail()?;

            transactions_confirmed += confirmed;
            transactions_requeued += requeued;

            // Update cached nonce
            if let Err(e) = self
                .nonce_manager
                .update_cached_onchain_nonce(job_data.eoa, job_data.chain_id, onchain_nonce)
                .await
            {
                tracing::error!(
                    eoa = %job_data.eoa,
                    error = %e,
                    "Failed to update cached nonce"
                );
            }
        }

        // 8. Check if we still have active transactions - if so, requeue worker
        let remaining_active = self
            .transaction_store
            .get_active_transactions(job_data.eoa, job_data.chain_id)
            .await
            .map_err(|e| EoaConfirmationWorkerError::InternalError {
                message: format!("Failed to get remaining active transactions: {}", e),
            })
            .map_err_fail()?;

        if !remaining_active.is_empty() {
            tracing::debug!(
                eoa = %job_data.eoa,
                active_count = remaining_active.len(),
                "Active transactions found - requeuing worker"
            );

            return Err(EoaConfirmationWorkerError::ActiveTransactionsFound {
                count: remaining_active.len() as u32,
            }
            .nack(Some(Duration::from_secs(3)), RequeuePosition::Last));
        }

        // 9. No more active transactions - worker can complete
        Ok(EoaConfirmationWorkerResult {
            eoa: job_data.eoa,
            chain_id: job_data.chain_id,
            nonce_progression,
            transactions_confirmed,
            transactions_requeued,
        })
    }

    async fn on_success(
        &self,
        job: &BorrowedJob<Self::JobData>,
        success_data: SuccessHookData<'_, Self::Output>,
        tx: &mut TransactionContext<'_>,
    ) {
        tracing::info!(
            eoa = %job.job.data.eoa,
            chain_id = job.job.data.chain_id,
            transactions_confirmed = success_data.result.transactions_confirmed,
            transactions_requeued = success_data.result.transactions_requeued,
            "EOA confirmation worker completed"
        );
    }

    async fn on_nack(
        &self,
        job: &BorrowedJob<Self::JobData>,
        nack_data: NackHookData<'_, Self::ErrorData>,
        tx: &mut TransactionContext<'_>,
    ) {
        tracing::debug!(
            eoa = %job.job.data.eoa,
            chain_id = job.job.data.chain_id,
            error = ?nack_data.error,
            "EOA confirmation worker nacked - will retry"
        );
    }

    async fn on_fail(
        &self,
        job: &BorrowedJob<Self::JobData>,
        fail_data: FailHookData<'_, Self::ErrorData>,
        tx: &mut TransactionContext<'_>,
    ) {
        tracing::error!(
            eoa = %job.job.data.eoa,
            chain_id = job.job.data.chain_id,
            error = ?fail_data.error,
            "EOA confirmation worker failed permanently"
        );
    }
}

// --- Core Logic ---

impl<CS> EoaConfirmationWorker<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    /// Process nonce progression and determine winners/losers
    async fn process_nonce_progression(
        &self,
        job_data: &EoaConfirmationWorkerJobData,
        chain: &impl Chain,
        from_nonce: u64,
        to_nonce: u64,
    ) -> Result<(u32, u32), EoaConfirmationWorkerError> {
        let mut transactions_confirmed = 0;
        let mut transactions_requeued = 0;

        // Process each nonce from cached to current onchain
        for nonce in from_nonce..to_nonce {
            let nonce_u256 = U256::from(nonce);

            // Get all our transactions competing for this nonce
            let competing_transaction_ids = self
                .transaction_store
                .get_transactions_by_nonce(job_data.eoa, job_data.chain_id, nonce_u256)
                .await
                .map_err(|e| EoaConfirmationWorkerError::InternalError {
                    message: format!("Failed to get transactions by nonce {}: {}", nonce, e),
                })?;

            if competing_transaction_ids.is_empty() {
                tracing::debug!(
                    nonce = %nonce,
                    "No competing transactions for nonce - chain progressed without us"
                );
                continue;
            }

            tracing::debug!(
                nonce = %nonce,
                competing_count = competing_transaction_ids.len(),
                "Processing competing transactions for nonce"
            );

            // Check each competing transaction
            let mut found_winner = false;
            for transaction_id in &competing_transaction_ids {
                if let Some(attempt) = self
                    .transaction_store
                    .get_active_attempt(transaction_id)
                    .await
                    .map_err(|e| EoaConfirmationWorkerError::InternalError {
                        message: format!(
                            "Failed to get active attempt for {}: {}",
                            transaction_id, e
                        ),
                    })?
                {
                    // Query receipt by hash
                    match chain
                        .provider()
                        .get_transaction_receipt(attempt.transaction_hash)
                        .await
                    {
                        Ok(Some(receipt)) => {
                            if receipt.status() {
                                // This transaction won!
                                tracing::info!(
                                    transaction_id = %transaction_id,
                                    transaction_hash = %attempt.transaction_hash,
                                    nonce = %nonce,
                                    block_number = %receipt.block_number.unwrap_or_default(),
                                    "Transaction confirmed on-chain"
                                );

                                let confirmation_data = ConfirmationData {
                                    transaction_hash: attempt.transaction_hash,
                                    receipt,
                                    confirmed_at: std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap()
                                        .as_secs(),
                                };

                                if let Err(e) = self
                                    .transaction_store
                                    .mark_transaction_confirmed(transaction_id, &confirmation_data)
                                    .await
                                {
                                    tracing::error!(
                                        transaction_id = %transaction_id,
                                        error = %e,
                                        "Failed to mark transaction as confirmed"
                                    );
                                }

                                transactions_confirmed += 1;
                                found_winner = true;
                            } else {
                                // Transaction reverted
                                tracing::warn!(
                                    transaction_id = %transaction_id,
                                    transaction_hash = %attempt.transaction_hash,
                                    nonce = %nonce,
                                    "Transaction reverted on-chain"
                                );

                                if let Err(e) = self
                                    .transaction_store
                                    .mark_transaction_failed(transaction_id, "Transaction reverted")
                                    .await
                                {
                                    tracing::error!(
                                        transaction_id = %transaction_id,
                                        error = %e,
                                        "Failed to mark transaction as failed"
                                    );
                                }
                            }
                        }
                        Ok(None) => {
                            // No receipt - transaction might still be pending or was replaced
                            tracing::debug!(
                                transaction_id = %transaction_id,
                                transaction_hash = %attempt.transaction_hash,
                                nonce = %nonce,
                                "No receipt found for transaction"
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                transaction_id = %transaction_id,
                                transaction_hash = %attempt.transaction_hash,
                                nonce = %nonce,
                                error = %e,
                                "Error getting receipt for transaction"
                            );
                        }
                    }
                }
            }

            // If nonce progressed but none of our transactions won, they all lost
            if !found_winner {
                for transaction_id in &competing_transaction_ids {
                    tracing::info!(
                        transaction_id = %transaction_id,
                        nonce = %nonce,
                        "Transaction lost nonce race - requeuing"
                    );

                    // Remove active attempt and requeue as new send job
                    if let Err(e) = self
                        .transaction_store
                        .remove_active_attempt(transaction_id)
                        .await
                    {
                        tracing::error!(
                            transaction_id = %transaction_id,
                            error = %e,
                            "Failed to remove active attempt for requeue"
                        );
                        continue;
                    }

                    // Create new send job (attempt_number will be incremented)
                    if let Err(e) = self.requeue_transaction(job_data, transaction_id).await {
                        tracing::error!(
                            transaction_id = %transaction_id,
                            error = %e,
                            "Failed to requeue transaction"
                        );
                    } else {
                        transactions_requeued += 1;
                    }
                }
            }
        }

        Ok((transactions_confirmed, transactions_requeued))
    }

    /// Requeue a transaction that lost a nonce race
    async fn requeue_transaction(
        &self,
        job_data: &EoaConfirmationWorkerJobData,
        transaction_id: &str,
    ) -> Result<(), EoaConfirmationWorkerError> {
        // Get original transaction data
        let tx_data = self
            .transaction_store
            .get_transaction_data(transaction_id)
            .await
            .map_err(|e| EoaConfirmationWorkerError::InternalError {
                message: format!(
                    "Failed to get transaction data for {}: {}",
                    transaction_id, e
                ),
            })?
            .ok_or_else(|| EoaConfirmationWorkerError::InternalError {
                message: format!("Transaction data not found for {}", transaction_id),
            })?;

        // Get current attempt number for new queue job ID
        let mut conn = self.transaction_store.redis.clone();
        let counter_key = self.transaction_store.attempt_counter_key(transaction_id);
        let attempt_number: u32 = conn.get(&counter_key).await.unwrap_or(0);

        // Create new send job with incremented attempt
        let requeue_job = self
            .send_queue
            .clone()
            .job(EoaSendJobData {
                transaction_id: tx_data.transaction_id.clone(),
                chain_id: tx_data.chain_id,
                from: tx_data.eoa,
                to: tx_data.to,
                value: tx_data.value,
                data: tx_data.data.into(),
                webhook_options: None, // TODO: Get from original job if needed
                assigned_nonce: None,  // Will get new nonce
                gas_limit: tx_data.gas_limit,
                signing_credential: Default::default(), // TODO: Get from original job
                rpc_credentials: job_data.rpc_credentials.clone(),
            })
            .with_id(&format!("{}_{}", transaction_id, attempt_number));

        // Queue the job (this would normally be done in a pipeline in the actual hook)
        tracing::info!(
            transaction_id = %transaction_id,
            queue_job_id = %format!("{}_{}", transaction_id, attempt_number),
            "Requeuing transaction after race loss"
        );

        Ok(())
    }
}
