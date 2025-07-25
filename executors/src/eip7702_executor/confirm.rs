use alloy::primitives::{Address, TxHash};
use alloy::providers::Provider;
use alloy::rpc::types::TransactionReceipt;
use engine_core::error::{AlloyRpcErrorToEngineError, EngineError};
use engine_core::{
    chain::{Chain, ChainService, RpcCredentials},
    execution_options::WebhookOptions,
};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use twmq::{
    FailHookData, NackHookData, Queue, SuccessHookData, UserCancellable,
    error::TwmqError,
    hooks::TransactionContext,
    job::{BorrowedJob, JobResult, RequeuePosition, ToJobError, ToJobResult},
};

use crate::eip7702_executor::send::Eip7702Sender;
use crate::{
    transaction_registry::TransactionRegistry,
    webhook::{
        WebhookJobHandler,
        envelope::{ExecutorStage, HasTransactionMetadata, HasWebhookOptions, WebhookCapable},
    },
};

// --- Job Payload ---
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Eip7702ConfirmationJobData {
    pub transaction_id: String,
    pub chain_id: u64,
    pub bundler_transaction_id: String,
    /// ! Deprecated todo: remove this field after all jobs are processed
    pub eoa_address: Option<Address>,

    // TODO: make non-optional after all jobs are processed
    pub sender_details: Option<Eip7702Sender>,

    pub rpc_credentials: RpcCredentials,
    #[serde(default)]
    pub webhook_options: Vec<WebhookOptions>,
}

impl HasWebhookOptions for Eip7702ConfirmationJobData {
    fn webhook_options(&self) -> Vec<WebhookOptions> {
        self.webhook_options.clone()
    }
}

impl HasTransactionMetadata for Eip7702ConfirmationJobData {
    fn transaction_id(&self) -> String {
        self.transaction_id.clone()
    }
}

// --- Success Result ---
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Eip7702ConfirmationResult {
    pub transaction_id: String,
    pub transaction_hash: TxHash,
    pub receipt: TransactionReceipt,

    #[serde(flatten)]
    pub sender_details: Eip7702Sender,
}

// --- Error Types ---
#[derive(Serialize, Deserialize, Debug, Clone, thiserror::Error)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "errorCode")]
pub enum Eip7702ConfirmationError {
    #[error("Chain service error for chainId {chain_id}: {message}")]
    #[serde(rename_all = "camelCase")]
    ChainServiceError { chain_id: u64, message: String },

    #[error("Failed to get transaction hash from bundler: {message}")]
    TransactionHashError { message: String },

    #[error("Failed to confirm transaction: {message}")]
    #[serde(rename_all = "camelCase")]
    ConfirmationError {
        message: String,
        inner_error: Option<EngineError>,
    },

    #[error("Receipt not yet available for transaction: {message}")]
    #[serde(rename_all = "camelCase")]
    ReceiptNotAvailable {
        message: String,
        transaction_hash: TxHash,
    },

    #[error("Transaction failed: {message}")]
    TransactionFailed {
        message: String,
        receipt: Box<TransactionReceipt>,
    },

    #[error("Invalid RPC Credentials: {message}")]
    InvalidRpcCredentials { message: String },

    #[error("Internal error: {message}")]
    InternalError { message: String },

    #[error("Transaction cancelled by user")]
    UserCancelled,
}

impl From<TwmqError> for Eip7702ConfirmationError {
    fn from(error: TwmqError) -> Self {
        Eip7702ConfirmationError::InternalError {
            message: format!("Deserialization error for job data: {}", error),
        }
    }
}

impl UserCancellable for Eip7702ConfirmationError {
    fn user_cancelled() -> Self {
        Eip7702ConfirmationError::UserCancelled
    }
}

// --- Handler ---
pub struct Eip7702ConfirmationHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    pub chain_service: Arc<CS>,
    pub webhook_queue: Arc<Queue<WebhookJobHandler>>,
    pub transaction_registry: Arc<TransactionRegistry>,
}

impl<CS> ExecutorStage for Eip7702ConfirmationHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    fn executor_name() -> &'static str {
        "eip7702"
    }

    fn stage_name() -> &'static str {
        "confirm"
    }
}

impl<CS> WebhookCapable for Eip7702ConfirmationHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    fn webhook_queue(&self) -> &Arc<Queue<WebhookJobHandler>> {
        &self.webhook_queue
    }
}

impl<CS> twmq::DurableExecution for Eip7702ConfirmationHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    type Output = Eip7702ConfirmationResult;
    type ErrorData = Eip7702ConfirmationError;
    type JobData = Eip7702ConfirmationJobData;

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
            .map_err(|e| Eip7702ConfirmationError::ChainServiceError {
                chain_id: job_data.chain_id,
                message: format!("Failed to get chain instance: {}", e),
            })
            .map_err_fail()?;

        let chain_auth_headers = job_data
            .rpc_credentials
            .to_header_map()
            .map_err(|e| Eip7702ConfirmationError::InvalidRpcCredentials {
                message: e.to_string(),
            })
            .map_err_fail()?;

        let chain = chain.with_new_default_headers(chain_auth_headers);

        // 2. Get transaction hash from bundler
        let transaction_hash_str = chain
            .bundler_client()
            .tw_get_transaction_hash(&job_data.bundler_transaction_id)
            .await
            .map_err(|e| Eip7702ConfirmationError::TransactionHashError {
                message: e.to_string(),
            })
            .map_err_fail()?;

        let transaction_hash = match transaction_hash_str {
            Some(hash) => hash.parse::<TxHash>().map_err(|e| {
                Eip7702ConfirmationError::TransactionHashError {
                    message: format!("Invalid transaction hash format: {}", e),
                }
                .fail()
            })?,
            None => {
                return Err(Eip7702ConfirmationError::TransactionHashError {
                    message: "Transaction not found".to_string(),
                })
                .map_err_nack(Some(Duration::from_secs(2)), RequeuePosition::Last);
            }
        };

        tracing::debug!(
            transaction_hash = ?transaction_hash,
            bundler_transaction_id = job_data.bundler_transaction_id,
            "Got transaction hash from bundler"
        );

        // 3. Wait for transaction confirmation
        let receipt = chain
            .provider()
            .get_transaction_receipt(transaction_hash)
            .await
            .map_err(|e| {
                // If transaction not found, nack and retry
                Eip7702ConfirmationError::ConfirmationError {
                    message: format!("Failed to get transaction receipt: {}", e),
                    inner_error: Some(e.to_engine_error(&chain)),
                }
                .nack(Some(Duration::from_secs(5)), RequeuePosition::Last)
            })?;

        let receipt = match receipt {
            Some(receipt) => receipt,
            None => {
                // Transaction not mined yet, nack and retry
                return Err(Eip7702ConfirmationError::ReceiptNotAvailable {
                    message: "Transaction not mined yet".to_string(),
                    transaction_hash,
                })
                .map_err_nack(Some(Duration::from_secs(2)), RequeuePosition::Last);
            }
        };

        // 4. Check transaction status
        let success = receipt.status();
        if !success {
            return Err(Eip7702ConfirmationError::TransactionFailed {
                message: "Transaction reverted".to_string(),
                receipt: Box::new(receipt),
            })
            .map_err_fail();
        }

        tracing::debug!(
            transaction_hash = ?transaction_hash,
            block_number = receipt.block_number,
            gas_used = ?receipt.gas_used,
            "Transaction confirmed successfully"
        );

        // todo: remove this after all jobs are processed
        let sender_details = job_data
            .sender_details
            .clone()
            .or_else(|| {
                job_data
                    .eoa_address
                    .map(|eoa_address| Eip7702Sender::Owner { eoa_address })
            })
            .ok_or_else(|| Eip7702ConfirmationError::InternalError {
                message: "No sender details found".to_string(),
            })
            .map_err_fail()?;

        Ok(Eip7702ConfirmationResult {
            transaction_id: job_data.transaction_id.clone(),
            transaction_hash,
            receipt,
            sender_details,
        })
    }

    async fn on_success(
        &self,
        job: &BorrowedJob<Eip7702ConfirmationJobData>,
        success_data: SuccessHookData<'_, Eip7702ConfirmationResult>,
        tx: &mut TransactionContext<'_>,
    ) {
        // Remove transaction from registry since confirmation is complete
        self.transaction_registry
            .add_remove_command(tx.pipeline(), &job.job.data.transaction_id);

        // Send webhook
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
        job: &BorrowedJob<Eip7702ConfirmationJobData>,
        nack_data: NackHookData<'_, Eip7702ConfirmationError>,
        tx: &mut TransactionContext<'_>,
    ) {
        // Don't modify transaction registry on NACK - job will be retried
        if let Err(e) = self.queue_nack_webhook(job, nack_data, tx) {
            tracing::error!(
                transaction_id = job.job.data.transaction_id,
                error = ?e,
                "Failed to queue nack webhook"
            );
        }
    }

    async fn on_fail(
        &self,
        job: &BorrowedJob<Eip7702ConfirmationJobData>,
        fail_data: FailHookData<'_, Eip7702ConfirmationError>,
        tx: &mut TransactionContext<'_>,
    ) {
        // Remove transaction from registry since it failed permanently
        self.transaction_registry
            .add_remove_command(tx.pipeline(), &job.job.data.transaction_id);

        tracing::error!(
            transaction_id = job.job.data.transaction_id,
            error = ?fail_data.error,
            "EIP-7702 confirmation job failed"
        );

        if let Err(e) = self.queue_fail_webhook(job, fail_data, tx) {
            tracing::error!(
                transaction_id = job.job.data.transaction_id,
                error = ?e,
                "Failed to queue fail webhook"
            );
        }
    }
}
