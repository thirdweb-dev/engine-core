use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
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
        envelope::{ExecutorStage, HasTransactionMetadata, HasWebhookOptions, WebhookCapable},
    },
};

use super::{
    nonce_manager::NonceManager,
    send::{EoaSendHandler, EoaSendJobData},
};

// --- Job Payload ---
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EoaConfirmationJobData {
    pub transaction_id: String,
    pub chain_id: u64,
    pub eoa_address: Address,
    pub nonce: U256,
    pub transaction_hash: B256,
    pub webhook_options: Option<Vec<WebhookOptions>>,
    pub rpc_credentials: RpcCredentials,
}

impl HasWebhookOptions for EoaConfirmationJobData {
    fn webhook_options(&self) -> Option<Vec<WebhookOptions>> {
        self.webhook_options.clone()
    }
}

impl HasTransactionMetadata for EoaConfirmationJobData {
    fn transaction_id(&self) -> String {
        self.transaction_id.clone()
    }
}

// --- Success Result ---
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EoaConfirmationResult {
    pub transaction_hash: B256,
    pub nonce_confirmed: U256,
    pub block_number: U256,
    pub block_hash: B256,
    pub gas_used: U256,
    pub effective_gas_price: U256,
    pub status: bool,
}

// --- Error Types ---
#[derive(Serialize, Deserialize, Debug, Clone, thiserror::Error)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "errorCode")]
pub enum EoaConfirmationError {
    #[error("Chain service error for chainId {chain_id}: {message}")]
    ChainServiceError { chain_id: u64, message: String },

    #[error("Transaction not yet confirmed")]
    NotYetConfirmed,

    #[error("Transaction was replaced by another transaction")]
    TransactionReplaced {
        original_hash: B256,
        replacing_hash: Option<B256>,
        nonce: U256,
    },

    #[error("Transaction failed with revert")]
    TransactionReverted {
        transaction_hash: B256,
        revert_reason: Option<String>,
    },

    #[error("RPC error during confirmation: {message}")]
    RpcError { message: String, retryable: bool },

    #[error("Nonce conflict detected - multiple transactions for nonce {nonce}")]
    NonceConflict {
        nonce: U256,
        competing_hashes: Vec<B256>,
    },

    #[error("Invalid RPC Credentials: {message}")]
    InvalidRpcCredentials { message: String },

    #[error("Internal error: {message}")]
    InternalError { message: String },

    #[error("Transaction cancelled by user")]
    UserCancelled,
}

impl From<TwmqError> for EoaConfirmationError {
    fn from(error: TwmqError) -> Self {
        EoaConfirmationError::InternalError {
            message: format!("Queue error: {}", error),
        }
    }
}

impl UserCancellable for EoaConfirmationError {
    fn user_cancelled() -> Self {
        EoaConfirmationError::UserCancelled
    }
}

// --- Handler ---
pub struct EoaConfirmationHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    pub chain_service: Arc<CS>,
    pub nonce_manager: Arc<NonceManager>,
    pub webhook_queue: Arc<Queue<WebhookJobHandler>>,
    pub send_queue: Arc<Queue<EoaSendHandler<CS>>>,
    pub transaction_registry: Arc<TransactionRegistry>,
    pub max_quick_checks: u32,
}

impl<CS> ExecutorStage for EoaConfirmationHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    fn executor_name() -> &'static str {
        "eoa"
    }

    fn stage_name() -> &'static str {
        "confirm"
    }
}

impl<CS> WebhookCapable for EoaConfirmationHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    fn webhook_queue(&self) -> &Arc<Queue<WebhookJobHandler>> {
        &self.webhook_queue
    }
}

impl<CS> twmq::DurableExecution for EoaConfirmationHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    type Output = EoaConfirmationResult;
    type ErrorData = EoaConfirmationError;
    type JobData = EoaConfirmationJobData;

    #[tracing::instrument(skip(self, job), fields(transaction_id = job.job.id, stage = Self::stage_name(), executor = Self::executor_name()))]
    async fn process(
        &self,
        job: &BorrowedJob<Self::JobData>,
    ) -> JobResult<Self::Output, Self::ErrorData> {
        let job_data = &job.job.data;

        // 1. Get Chain
        let chain = self
            .chain_service
            .get_chain(job_data.chain_id)
            .map_err(|e| EoaConfirmationError::ChainServiceError {
                chain_id: job_data.chain_id,
                message: format!("Failed to get chain instance: {}", e),
            })
            .map_err_fail()?;

        let chain_auth_headers = job_data
            .rpc_credentials
            .to_header_map()
            .map_err(|e| EoaConfirmationError::InvalidRpcCredentials {
                message: e.to_string(),
            })
            .map_err_fail()?;

        let chain = chain.with_new_default_headers(chain_auth_headers);

        // 2. Get current onchain nonce
        let onchain_nonce = match chain
            .provider()
            .get_transaction_count(job_data.eoa_address)
            .await
        {
            Ok(nonce) => nonce,
            Err(e) => {
                let engine_error = e.to_engine_error(&chain);
                return Err(EoaConfirmationError::RpcError {
                    message: format!("Failed to get transaction count: {}", engine_error),
                    retryable: true,
                }
                .nack(Some(Duration::from_secs(5)), RequeuePosition::Last));
            }
        };

        // 3. Get cached nonce to detect progression
        let last_known_nonce = self
            .nonce_manager
            .get_cached_onchain_nonce(job_data.eoa_address, job_data.chain_id)
            .await
            .map_err(|e| EoaConfirmationError::InternalError {
                message: format!("Failed to get cached nonce: {}", e),
            })
            .map_err_fail()?
            .unwrap_or(U256::ZERO);

        tracing::debug!(
            nonce = %job_data.nonce,
            onchain_nonce = %onchain_nonce,
            last_known_nonce = %last_known_nonce,
            "Checking confirmation status"
        );

        // 4. If nonce hasn't moved, check specific transaction
        if onchain_nonce <= last_known_nonce {
            return self.check_specific_transaction(job, &chain).await;
        }

        // 5. Nonce moved! Check all in-flight transactions for this range
        self.process_nonce_progression(job, &chain, last_known_nonce, onchain_nonce)
            .await
    }

    async fn on_success(
        &self,
        job: &BorrowedJob<Self::JobData>,
        success_data: SuccessHookData<'_, Self::Output>,
        tx: &mut TransactionContext<'_>,
    ) {
        // 1. Remove nonce assignment (transaction confirmed)
        self.nonce_manager.add_remove_assignment_command(
            tx.pipeline(),
            job.job.data.eoa_address,
            job.job.data.chain_id,
            success_data.result.nonce_confirmed,
        );

        // 2. Update cached onchain nonce
        let cached_nonce = success_data.result.nonce_confirmed + U256::from(1);
        // TODO: Add method to update cached nonce via pipeline

        // 3. Remove from transaction registry (completed)
        self.transaction_registry
            .add_remove_command(tx.pipeline(), &job.job.data.transaction_id);

        // 4. Reset error counters on success
        // TODO: Update health to reset consecutive errors

        if let Err(e) = self.queue_success_webhook(job, success_data, tx) {
            tracing::error!(
                transaction_id = %job.job.data.transaction_id,
                error = %e,
                "Failed to queue success webhook"
            );
        }
    }

    async fn on_nack(
        &self,
        job: &BorrowedJob<Self::JobData>,
        nack_data: NackHookData<'_, Self::ErrorData>,
        tx: &mut TransactionContext<'_>,
    ) {
        if let Err(e) = self.queue_nack_webhook(job, nack_data, tx) {
            tracing::error!(
                transaction_id = %job.job.data.transaction_id,
                error = %e,
                "Failed to queue nack webhook"
            );
        }
    }

    async fn on_fail(
        &self,
        job: &BorrowedJob<Self::JobData>,
        fail_data: FailHookData<'_, Self::ErrorData>,
        tx: &mut TransactionContext<'_>,
    ) {
        // Handle different failure types
        match fail_data.error {
            EoaConfirmationError::TransactionReplaced { nonce, .. } => {
                // Transaction was replaced - requeue the original transaction
                self.requeue_replaced_transaction(job, *nonce, tx).await;
            }
            EoaConfirmationError::TransactionReverted { .. } => {
                // Transaction reverted - remove nonce assignment and don't requeue
                self.nonce_manager.add_remove_assignment_command(
                    tx.pipeline(),
                    job.job.data.eoa_address,
                    job.job.data.chain_id,
                    job.job.data.nonce,
                );
            }
            _ => {
                // Other failures - remove nonce assignment
                self.nonce_manager.add_remove_assignment_command(
                    tx.pipeline(),
                    job.job.data.eoa_address,
                    job.job.data.chain_id,
                    job.job.data.nonce,
                );
            }
        }

        // Remove from transaction registry
        self.transaction_registry
            .add_remove_command(tx.pipeline(), &job.job.data.transaction_id);

        if let Err(e) = self.queue_fail_webhook(job, fail_data, tx) {
            tracing::error!(
                transaction_id = %job.job.data.transaction_id,
                error = %e,
                "Failed to queue fail webhook"
            );
        }
    }
}

// --- Confirmation Logic ---

impl<CS> EoaConfirmationHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    async fn check_specific_transaction(
        &self,
        job: &BorrowedJob<Self::JobData>,
        chain: &impl Chain,
    ) -> JobResult<EoaConfirmationResult, EoaConfirmationError> {
        let job_data = &job.job.data;

        // Poll for specific transaction receipt
        match chain
            .provider()
            .get_transaction_receipt(job_data.transaction_hash)
            .await
        {
            Ok(Some(receipt)) => {
                // Found receipt - check if transaction succeeded
                if receipt.status() {
                    tracing::info!(
                        transaction_hash = %job_data.transaction_hash,
                        block_number = %receipt.block_number.unwrap_or_default(),
                        "Transaction confirmed successfully"
                    );

                    Ok(EoaConfirmationResult {
                        transaction_hash: job_data.transaction_hash,
                        nonce_confirmed: job_data.nonce,
                        block_number: receipt.block_number.unwrap_or_default(),
                        block_hash: receipt.block_hash.unwrap_or_default(),
                        gas_used: receipt.gas_used,
                        effective_gas_price: receipt.effective_gas_price.unwrap_or_default(),
                        status: true,
                    })
                } else {
                    // Transaction reverted
                    Err(EoaConfirmationError::TransactionReverted {
                        transaction_hash: job_data.transaction_hash,
                        revert_reason: None, // Could extract from logs if needed
                    }
                    .fail())
                }
            }
            Ok(None) => {
                // No receipt yet
                if job.job.attempts > self.max_quick_checks {
                    // After max quick checks, switch to slow path (longer delays)
                    Err(EoaConfirmationError::NotYetConfirmed
                        .nack(Some(Duration::from_secs(30)), RequeuePosition::Last))
                } else {
                    // Quick recheck
                    Err(EoaConfirmationError::NotYetConfirmed
                        .nack(Some(Duration::from_secs(3)), RequeuePosition::Last))
                }
            }
            Err(e) => {
                let engine_error = e.to_engine_error(chain);
                let retryable = matches!(engine_error,
                    EngineError::RpcError { kind: RpcErrorKind::OtherTransportError(_), .. } |
                    EngineError::RpcError { kind: RpcErrorKind::ErrorResp(resp), .. } if matches!(resp.code, -32005 | -32603)
                );

                if retryable {
                    Err(EoaConfirmationError::RpcError {
                        message: format!("RPC error getting receipt: {}", engine_error),
                        retryable: true,
                    }
                    .nack(Some(Duration::from_secs(5)), RequeuePosition::Last))
                } else {
                    Err(EoaConfirmationError::RpcError {
                        message: format!("Failed to get receipt: {}", engine_error),
                        retryable: false,
                    }
                    .fail())
                }
            }
        }
    }

    async fn process_nonce_progression(
        &self,
        job: &BorrowedJob<Self::JobData>,
        chain: &impl Chain,
        last_known_nonce: U256,
        onchain_nonce: U256,
    ) -> JobResult<EoaConfirmationResult, EoaConfirmationError> {
        let job_data = &job.job.data;

        tracing::info!(
            from_nonce = %last_known_nonce,
            to_nonce = %onchain_nonce,
            "Processing nonce progression"
        );

        // Check all nonces from last_known to current onchain
        for nonce_to_check in last_known_nonce.to::<u64>()..onchain_nonce.to::<u64>() {
            let nonce_u256 = U256::from(nonce_to_check);

            // Get all assignments for this nonce
            let assignments = self
                .nonce_manager
                .get_nonce_assignments(job_data.eoa_address, job_data.chain_id, nonce_u256)
                .await
                .map_err(|e| EoaConfirmationError::InternalError {
                    message: format!("Failed to get nonce assignments: {}", e),
                })
                .map_err_fail()?;

            for assignment in assignments {
                match chain
                    .provider()
                    .get_transaction_receipt(assignment.transaction_hash)
                    .await
                {
                    Ok(Some(receipt)) => {
                        // Found a receipt for this nonce
                        if assignment.transaction_id == job_data.transaction_id {
                            // This job won the race!
                            if receipt.status() {
                                tracing::info!(
                                    transaction_hash = %assignment.transaction_hash,
                                    nonce = %nonce_u256,
                                    "Transaction confirmed in nonce progression"
                                );

                                // Update cached nonce
                                if let Err(e) = self
                                    .nonce_manager
                                    .update_cached_onchain_nonce(
                                        job_data.eoa_address,
                                        job_data.chain_id,
                                        onchain_nonce,
                                    )
                                    .await
                                {
                                    tracing::error!("Failed to update cached nonce: {}", e);
                                }

                                return Ok(EoaConfirmationResult {
                                    transaction_hash: assignment.transaction_hash,
                                    nonce_confirmed: nonce_u256,
                                    block_number: receipt.block_number.unwrap_or_default(),
                                    block_hash: receipt.block_hash.unwrap_or_default(),
                                    gas_used: receipt.gas_used,
                                    effective_gas_price: receipt
                                        .effective_gas_price
                                        .unwrap_or_default(),
                                    status: true,
                                });
                            } else {
                                // Transaction reverted
                                return Err(EoaConfirmationError::TransactionReverted {
                                    transaction_hash: assignment.transaction_hash,
                                    revert_reason: None,
                                }
                                .fail());
                            }
                        } else {
                            // Different transaction won - will handle in requeue
                            tracing::info!(
                                winning_hash = %assignment.transaction_hash,
                                losing_transaction_id = %job_data.transaction_id,
                                nonce = %nonce_u256,
                                "Different transaction won nonce race"
                            );
                        }
                    }
                    Ok(None) => {
                        // No receipt for this hash yet
                        if nonce_u256 < onchain_nonce - U256::from(1) {
                            // Old nonce with no receipt - transaction was likely replaced
                            tracing::warn!(
                                transaction_hash = %assignment.transaction_hash,
                                nonce = %nonce_u256,
                                "Old transaction with no receipt - likely replaced"
                            );
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            transaction_hash = %assignment.transaction_hash,
                            nonce = %nonce_u256,
                            error = %e,
                            "Error getting receipt during nonce progression"
                        );
                    }
                }
            }
        }

        // Update cached nonce regardless
        if let Err(e) = self
            .nonce_manager
            .update_cached_onchain_nonce(job_data.eoa_address, job_data.chain_id, onchain_nonce)
            .await
        {
            tracing::error!("Failed to update cached nonce: {}", e);
        }

        // If we get here, our transaction wasn't found in the confirmed range
        if job_data.nonce < onchain_nonce {
            // Our nonce is old and we didn't find a receipt - transaction was replaced
            Err(EoaConfirmationError::TransactionReplaced {
                original_hash: job_data.transaction_hash,
                replacing_hash: None,
                nonce: job_data.nonce,
            }
            .fail())
        } else {
            // Our nonce is current or future - keep waiting
            Err(EoaConfirmationError::NotYetConfirmed
                .nack(Some(Duration::from_secs(5)), RequeuePosition::Last))
        }
    }

    async fn requeue_replaced_transaction(
        &self,
        job: &BorrowedJob<Self::JobData>,
        nonce: U256,
        tx: &mut TransactionContext<'_>,
    ) {
        tracing::info!(
            transaction_id = %job.job.data.transaction_id,
            nonce = %nonce,
            "Requeuing replaced transaction"
        );

        // Create a new send job without assigned nonce (will get new nonce)
        let requeue_job = self
            .send_queue
            .clone()
            .job(EoaSendJobData {
                transaction_id: job.job.data.transaction_id.clone(),
                chain_id: job.job.data.chain_id,
                from: job.job.data.eoa_address,
                to: Address::ZERO, // TODO: Get original transaction details
                value: U256::ZERO,
                data: Default::default(),
                gas_limit: None,
                max_fee_per_gas: None,
                max_priority_fee_per_gas: None,
                assigned_nonce: None, // Will get new nonce
                webhook_options: job.job.data.webhook_options.clone(),
                rpc_credentials: job.job.data.rpc_credentials.clone(),
            })
            .with_id(&format!("{}_retry", job.job.data.transaction_id));

        if let Err(e) = tx.queue_job(requeue_job) {
            tracing::error!(
                transaction_id = %job.job.data.transaction_id,
                error = %e,
                "Failed to requeue replaced transaction"
            );
        }
    }
}
