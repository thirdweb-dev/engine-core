use alloy::{
    primitives::{Address, Bytes, U256},
    rpc::types::UserOperationReceipt,
};
use engine_core::{
    chain::{Chain, ChainService},
    error::AlloyRpcErrorToEngineError,
};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use twmq::{
    DurableExecution, FailHookData, NackHookData, Queue, SuccessHookData,
    hooks::TransactionContext,
    job::{Job, JobResult, RequeuePosition, ToJobResult},
};

use crate::webhook::{
    WebhookJobHandler,
    envelope::{ExecutorStage, HasWebhookOptions, WebhookCapable},
};

use super::{deployment::RedisDeploymentLock, send::WebhookOptions};

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
    pub webhook_options: Option<WebhookOptions>,
    pub thirdweb_client_id: Option<String>,
    pub thirdweb_service_key: Option<String>,
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
#[serde(rename_all = "camelCase", tag = "errorCode")]
pub enum UserOpConfirmationError {
    #[error("Chain service error for chainId {chain_id}: {message}")]
    ChainServiceError { chain_id: u64, message: String },

    #[error("Receipt not yet available for user operation {user_op_hash}")]
    ReceiptNotAvailable {
        user_op_hash: Bytes,
        attempt_number: u32,
    },

    #[error("Failed to query user operation receipt: {message}")]
    ReceiptQueryFailed {
        user_op_hash: Bytes,
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        technical_details: Option<String>,
    },

    #[error("Internal error: {message}")]
    InternalError { message: String },
}

// --- Handler ---
pub struct UserOpConfirmationHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    pub chain_service: Arc<CS>,
    pub deployment_lock: RedisDeploymentLock,
    pub webhook_queue: Arc<Queue<WebhookJobHandler>>,
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
    ) -> Self {
        Self {
            chain_service,
            deployment_lock,
            webhook_queue,
            max_confirmation_attempts: 20, // ~5 minutes with 15 second delays
            confirmation_retry_delay: Duration::from_secs(15),
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

    async fn process(&self, job: &Job<Self::JobData>) -> JobResult<Self::Output, Self::ErrorData> {
        let job_data = &job.data;

        // 1. Get Chain
        let chain = self
            .chain_service
            .get_chain(job_data.chain_id)
            .map_err(|e| UserOpConfirmationError::ChainServiceError {
                chain_id: job_data.chain_id,
                message: format!("Failed to get chain instance: {}", e),
            })
            .fail_err()?;

        // 2. Query for User Operation Receipt
        let receipt_result = chain
            .bundler_client()
            .get_user_op_receipt(job_data.user_op_hash.clone())
            .await
            .map_err(|e| UserOpConfirmationError::ReceiptQueryFailed {
                user_op_hash: job_data.user_op_hash.clone(),
                message: e.to_string(),
                technical_details: Some(
                    serde_json::to_string(&e.to_engine_bundler_error(&chain)).unwrap_or_default(),
                ),
            });

        let receipt_option = match receipt_result {
            Ok(opt) => opt,
            Err(e) => {
                // Network/RPC errors might be temporary, retry
                return Err(e).nack_err(Some(self.confirmation_retry_delay), RequeuePosition::Last);
            }
        };

        let receipt = match receipt_option {
            Some(receipt) => receipt,
            None => {
                // Receipt not available and max attempts reached - permanent failure
                if job.attempts >= self.max_confirmation_attempts {
                    return Err(UserOpConfirmationError::ReceiptNotAvailable {
                        user_op_hash: job_data.user_op_hash.clone(),
                        attempt_number: job.attempts,
                    })
                    .fail_err(); // FAIL - triggers on_fail hook which will release lock
                }

                return Err(UserOpConfirmationError::ReceiptNotAvailable {
                    user_op_hash: job_data.user_op_hash.clone(),
                    attempt_number: job.attempts,
                })
                .nack_err(Some(self.confirmation_retry_delay), RequeuePosition::Last);
                // NACK - triggers on_nack hook which keeps lock for retry
            }
        };

        // 3. We got a receipt - that's confirmation success!
        // Whether it reverted or not is just information in the receipt
        tracing::info!(
            transaction_id = %job_data.transaction_id,
            user_op_hash = ?job_data.user_op_hash,
            success = %receipt.success,
            "User operation confirmed on-chain"
        );

        // 4. Success! Lock cleanup will happen atomically in on_success hook
        Ok(UserOpConfirmationResult {
            user_op_hash: job_data.user_op_hash.clone(),
            receipt,
            deployment_lock_released: job_data.deployment_lock_acquired, // Will be released in hook
        })
    }

    async fn on_success(
        &self,
        job: &Job<Self::JobData>,
        success_data: SuccessHookData<'_, Self::Output>,
        tx: &mut TransactionContext<'_>,
    ) {
        // Atomic cleanup: release lock + update cache if lock was acquired
        if job.data.deployment_lock_acquired {
            self.deployment_lock
                .release_lock_and_update_cache_with_pipeline(
                    tx.pipeline(),
                    job.data.chain_id,
                    &job.data.account_address,
                    true, // is_deployed = true
                );

            tracing::info!(
                transaction_id = %job.data.transaction_id,
                account_address = ?job.data.account_address,
                "Added atomic lock release and cache update to transaction pipeline"
            );
        }

        // Queue success webhook
        if let Err(e) = self.queue_success_webhook(job, success_data, tx) {
            tracing::error!(
                transaction_id = %job.data.transaction_id,
                error = %e,
                "Failed to queue success webhook"
            );
        }
    }

    async fn on_nack(
        &self,
        job: &Job<Self::JobData>,
        nack_data: NackHookData<'_, Self::ErrorData>,
        tx: &mut TransactionContext<'_>,
    ) {
        // NEVER release lock on NACK - job will be retried with the same lock
        // Just queue webhook with current status
        if let Err(e) = self.queue_nack_webhook(job, nack_data, tx) {
            tracing::error!(
                transaction_id = %job.data.transaction_id,
                error = %e,
                "Failed to queue nack webhook"
            );
        }

        tracing::debug!(
            transaction_id = %job.data.transaction_id,
            attempt = %job.attempts,
            "Confirmation job NACKed, retaining lock for retry"
        );
    }

    async fn on_fail(
        &self,
        job: &Job<Self::JobData>,
        fail_data: FailHookData<'_, Self::ErrorData>,
        tx: &mut TransactionContext<'_>,
    ) {
        // Always release lock on permanent failure
        if job.data.deployment_lock_acquired {
            self.deployment_lock.release_lock_with_pipeline(
                tx.pipeline(),
                job.data.chain_id,
                &job.data.account_address,
            );

            let failure_reason = match fail_data.error {
                UserOpConfirmationError::ReceiptNotAvailable { .. } => {
                    "Max confirmation attempts exceeded"
                }
                _ => "Confirmation job failed permanently",
            };

            tracing::error!(
                transaction_id = %job.data.transaction_id,
                account_address = ?job.data.account_address,
                reason = %failure_reason,
                "Added lock release to transaction pipeline due to permanent failure"
            );
        }

        // Queue failure webhook
        if let Err(e) = self.queue_fail_webhook(job, fail_data, tx) {
            tracing::error!(
                transaction_id = %job.data.transaction_id,
                error = %e,
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
        "confirmation"
    }
}

impl HasWebhookOptions for UserOpConfirmationJobData {
    fn webhook_url(&self) -> Option<String> {
        self.webhook_options
            .as_ref()
            .map(|opts| opts.webhook_url.clone())
    }

    fn transaction_id(&self) -> String {
        self.transaction_id.clone()
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
