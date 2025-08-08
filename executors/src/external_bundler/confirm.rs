use alloy::primitives::{Address, Bytes, U256};
use engine_core::{
    chain::{Chain, ChainService, RpcCredentials},
    error::{AlloyRpcErrorToEngineError, EngineError},
    execution_options::WebhookOptions,
    rpc_clients::UserOperationReceipt,
};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use twmq::{
    DurableExecution, FailHookData, NackHookData, Queue, SuccessHookData, UserCancellable,
    error::TwmqError,
    hooks::TransactionContext,
    job::{BorrowedJob, JobResult, RequeuePosition, ToJobResult},
};

use crate::{
    metrics::{record_transaction_queued_to_confirmed, current_timestamp_ms, calculate_duration_seconds_from_twmq},
    transaction_registry::TransactionRegistry,
    webhook::{
        WebhookJobHandler,
        envelope::{ExecutorStage, HasWebhookOptions, WebhookCapable},
    },
};

use super::deployment::RedisDeploymentLock;

// --- Job Payload ---
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UserOpConfirmationJobData {
    pub transaction_id: String,
    pub chain_id: u64,
    pub account_address: Address,
    pub user_op_hash: Bytes,
    pub nonce: U256,
    pub deployment_lock_acquired: bool,
    pub webhook_options: Vec<WebhookOptions>,
    pub rpc_credentials: RpcCredentials,
    /// Original timestamp when the transaction was first queued (unix timestamp in milliseconds)
    #[serde(default)]
    pub original_queued_timestamp: Option<u64>,
}

// --- Success Result ---
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UserOpConfirmationResult {
    pub user_op_hash: Bytes,
    pub receipt: UserOperationReceipt,
    pub deployment_lock_released: bool,
}

// --- Error Types ---
#[derive(Serialize, Deserialize, Debug, Clone, thiserror::Error)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "errorCode")]
pub enum UserOpConfirmationError {
    #[error("Chain service error for chainId {chain_id}: {message}")]
    #[serde(rename_all = "camelCase")]
    ChainServiceError { chain_id: u64, message: String },

    #[error("Receipt not yet available for user operation {user_op_hash}")]
    #[serde(rename_all = "camelCase")]
    ReceiptNotAvailable {
        user_op_hash: Bytes,
        attempt_number: u32,
    },

    #[error("Failed to query user operation receipt: {message}")]
    #[serde(rename_all = "camelCase")]
    ReceiptQueryFailed {
        user_op_hash: Bytes,
        message: String,
        inner_error: Option<EngineError>,
    },

    #[error("Internal error: {message}")]
    #[serde(rename_all = "camelCase")]
    InternalError { message: String },

    #[error("Transaction cancelled by user")]
    UserCancelled,
}

impl From<TwmqError> for UserOpConfirmationError {
    fn from(error: TwmqError) -> Self {
        UserOpConfirmationError::InternalError {
            message: format!("Deserialization error for job data: {error}"),
        }
    }
}

impl UserCancellable for UserOpConfirmationError {
    fn user_cancelled() -> Self {
        UserOpConfirmationError::UserCancelled
    }
}

// --- Handler ---
pub struct UserOpConfirmationHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    pub chain_service: Arc<CS>,
    pub deployment_lock: RedisDeploymentLock,
    pub webhook_queue: Arc<Queue<WebhookJobHandler>>,
    pub transaction_registry: Arc<TransactionRegistry>,
    pub max_confirmation_attempts: u32,
    pub confirmation_retry_delay: Duration,
}

impl<CS> UserOpConfirmationHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    pub fn new(
        chain_service: Arc<CS>,
        deployment_lock: RedisDeploymentLock,
        webhook_queue: Arc<Queue<WebhookJobHandler>>,
        transaction_registry: Arc<TransactionRegistry>,
    ) -> Self {
        Self {
            chain_service,
            deployment_lock,
            webhook_queue,
            transaction_registry,
            max_confirmation_attempts: 20, // ~5 minutes with 15 second delays
            confirmation_retry_delay: Duration::from_secs(5),
        }
    }

    pub fn with_retry_config(mut self, max_attempts: u32, retry_delay: Duration) -> Self {
        self.max_confirmation_attempts = max_attempts;
        self.confirmation_retry_delay = retry_delay;
        self
    }
}

impl<CS> DurableExecution for UserOpConfirmationHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    type Output = UserOpConfirmationResult;
    type ErrorData = UserOpConfirmationError;
    type JobData = UserOpConfirmationJobData;

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
            .map_err(|e| UserOpConfirmationError::ChainServiceError {
                chain_id: job_data.chain_id,
                message: format!("Failed to get chain instance: {e}"),
            })
            .map_err_fail()?;

        let chain = chain.with_new_default_headers(
            job.job
                .data
                .rpc_credentials
                .to_header_map()
                .map_err(|e| UserOpConfirmationError::InternalError {
                    message: format!("Bad RPC Credential values, unserialisable into headers: {e}"),
                })
                .map_err_fail()?,
        );

        // 2. Query for User Operation Receipt
        let receipt_option = chain
            .bundler_client()
            .get_user_op_receipt(job_data.user_op_hash.clone())
            .await
            .map_err(|e| UserOpConfirmationError::ReceiptQueryFailed {
                user_op_hash: job_data.user_op_hash.clone(),
                message: e.to_string(),
                inner_error: Some(e.to_engine_bundler_error(&chain)),
            })
            .map_err_nack(Some(self.confirmation_retry_delay), RequeuePosition::Last)?;

        let receipt = match receipt_option {
            Some(receipt) => receipt,
            None => {
                // Receipt not available and max attempts reached - permanent failure
                if job.job.attempts >= self.max_confirmation_attempts {
                    return Err(UserOpConfirmationError::ReceiptNotAvailable {
                        user_op_hash: job_data.user_op_hash.clone(),
                        attempt_number: job.job.attempts,
                    })
                    .map_err_fail(); // FAIL - triggers on_fail hook which will release lock
                }

                return Err(UserOpConfirmationError::ReceiptNotAvailable {
                    user_op_hash: job_data.user_op_hash.clone(),
                    attempt_number: job.job.attempts,
                })
                .map_err_nack(Some(self.confirmation_retry_delay), RequeuePosition::Last);
                // NACK - triggers on_nack hook which keeps lock for retry
            }
        };

        // 3. We got a receipt - that's confirmation success!
        // Whether it reverted or not is just information in the receipt
        tracing::info!(
            transaction_id = job_data.transaction_id,
            user_op_hash = ?job_data.user_op_hash,
            transaction_hash = ?receipt.receipt.transaction_hash,
            success = ?receipt.success,
            "User operation confirmed on-chain"
        );

                    // 4. Record metrics if original timestamp is available
            if let Some(original_timestamp) = job_data.original_queued_timestamp {
                let confirmed_timestamp = current_timestamp_ms();
                let queued_to_confirmed_duration = calculate_duration_seconds_from_twmq(original_timestamp, confirmed_timestamp);
                record_transaction_queued_to_confirmed("erc4337-external", job_data.chain_id, queued_to_confirmed_duration);
            }

        // 5. Success! Lock cleanup will happen atomically in on_success hook
        Ok(UserOpConfirmationResult {
            user_op_hash: job_data.user_op_hash.clone(),
            receipt,
            deployment_lock_released: job_data.deployment_lock_acquired, // Will be released in hook
        })
    }

    async fn on_success(
        &self,
        job: &BorrowedJob<Self::JobData>,
        success_data: SuccessHookData<'_, Self::Output>,
        tx: &mut TransactionContext<'_>,
    ) {
        // Remove transaction from registry since confirmation is complete
        self.transaction_registry
            .add_remove_command(tx.pipeline(), &job.job.data.transaction_id);

        // Atomic cleanup: release lock + update cache if lock was acquired
        if job.job.data.deployment_lock_acquired {
            self.deployment_lock
                .release_lock_and_update_cache_with_pipeline(
                    tx.pipeline(),
                    job.job.data.chain_id,
                    &job.job.data.account_address,
                    true, // is_deployed = true
                );

            tracing::info!(
                transaction_id = job.job.data.transaction_id,
                account_address = ?job.job.data.account_address,
                "Added atomic lock release and cache update to transaction pipeline"
            );
        }

        // Queue success webhook
        if let Err(e) = self.queue_success_webhook(job, success_data, tx) {
            tracing::error!(
                transaction_id = job.job.data.transaction_id,
                error = ?e,
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
        // NEVER release lock on NACK - job will be retried with the same lock
        // Just queue webhook with current status
        if let Err(e) = self.queue_nack_webhook(job, nack_data, tx) {
            tracing::error!(
                transaction_id = job.job.data.transaction_id,
                error = ?e,
                "Failed to queue nack webhook"
            );
        }

        tracing::debug!(
            transaction_id = job.job.data.transaction_id,
            attempt = job.job.attempts,
            "Confirmation job NACKed, retaining lock for retry"
        );
    }

    async fn on_fail(
        &self,
        job: &BorrowedJob<Self::JobData>,
        fail_data: FailHookData<'_, Self::ErrorData>,
        tx: &mut TransactionContext<'_>,
    ) {
        // Remove transaction from registry since it failed permanently
        self.transaction_registry
            .add_remove_command(tx.pipeline(), &job.job.data.transaction_id);

        // Always release lock on permanent failure
        if job.job.data.deployment_lock_acquired {
            self.deployment_lock.release_lock_with_pipeline(
                tx.pipeline(),
                job.job.data.chain_id,
                &job.job.data.account_address,
            );

            let failure_reason = match fail_data.error {
                UserOpConfirmationError::ReceiptNotAvailable { .. } => {
                    "Max confirmation attempts exceeded"
                }
                _ => "Confirmation job failed permanently",
            };

            tracing::error!(
                transaction_id = job.job.data.transaction_id,
                account_address = ?job.job.data.account_address,
                reason = failure_reason,
                "Added lock release to transaction pipeline due to permanent failure"
            );
        }

        // Queue failure webhook
        if let Err(e) = self.queue_fail_webhook(job, fail_data, tx) {
            tracing::error!(
                transaction_id = job.job.data.transaction_id,
                error = ?e,
                "Failed to queue fail webhook"
            );
        }
    }
}

// --- Trait Implementations ---
impl<CS> ExecutorStage for UserOpConfirmationHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    fn executor_name() -> &'static str {
        "erc4337"
    }

    fn stage_name() -> &'static str {
        "confirm"
    }
}

impl HasWebhookOptions for UserOpConfirmationJobData {
    fn webhook_options(&self) -> Vec<WebhookOptions> {
        self.webhook_options.clone()
    }
}

impl<CS> WebhookCapable for UserOpConfirmationHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    fn webhook_queue(&self) -> &Arc<Queue<WebhookJobHandler>> {
        &self.webhook_queue
    }
}
