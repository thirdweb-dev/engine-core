use alloy::primitives::{Address, TxHash, Bytes};
use engine_core::{
    chain::{Chain, ChainService, RpcCredentials},
    error::EngineError,
    execution_options::WebhookOptions,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
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

// --- Job Payload ---
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Eip7702ConfirmationJobData {
    pub transaction_id: String,
    pub chain_id: u64,
    pub bundler_transaction_id: String,
    pub eoa_address: Address,
    pub rpc_credentials: RpcCredentials,
    pub webhook_options: Option<Vec<WebhookOptions>>,
}

impl HasWebhookOptions for Eip7702ConfirmationJobData {
    fn webhook_options(&self) -> Option<Vec<WebhookOptions>> {
        self.webhook_options.clone()
    }
}

impl HasTransactionMetadata for Eip7702ConfirmationJobData {
    fn transaction_id(&self) -> &str {
        &self.transaction_id
    }

    fn chain_id(&self) -> u64 {
        self.chain_id
    }
}

// --- Success Result ---
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Eip7702ConfirmationResult {
    pub transaction_id: String,
    pub transaction_hash: TxHash,
    pub eoa_address: Address,
    pub block_number: Option<u64>,
    pub gas_used: Option<u64>,
    pub status: bool,
}

// --- Error Types ---
#[derive(Serialize, Deserialize, Debug, Clone, thiserror::Error)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "errorCode")]
pub enum Eip7702ConfirmationError {
    #[error("Chain service error for chainId {chain_id}: {message}")]
    ChainServiceError { chain_id: u64, message: String },

    #[error("Failed to get transaction hash from bundler: {message}")]
    TransactionHashError { message: String },

    #[error("Failed to confirm transaction: {message}")]
    ConfirmationError { message: String },

    #[error("Transaction failed: {message}")]
    TransactionFailed { message: String },

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
        let transaction_hash = get_transaction_hash_from_bundler(&chain, &job_data.bundler_transaction_id)
            .await
            .map_err(|e| {
                match e {
                    EngineError::RpcError { message, .. } if message.contains("not found") || message.contains("pending") => {
                        // Transaction not ready yet, nack and retry
                        Eip7702ConfirmationError::TransactionHashError {
                            message: format!("Transaction not ready: {}", message),
                        }
                        .nack(Some(Duration::from_secs(5)), RequeuePosition::Last)
                    }
                    _ => Eip7702ConfirmationError::TransactionHashError {
                        message: e.to_string(),
                    }
                    .fail()
                }
            })?;

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
                }
                .nack(Some(Duration::from_secs(10)), RequeuePosition::Last)
            })?;

        let receipt = match receipt {
            Some(receipt) => receipt,
            None => {
                // Transaction not mined yet, nack and retry
                return Err(Eip7702ConfirmationError::ConfirmationError {
                    message: "Transaction not mined yet".to_string(),
                })
                .map_err_nack(Some(Duration::from_secs(10)), RequeuePosition::Last);
            }
        };

        // 4. Check transaction status
        let success = receipt.status();
        if !success {
            return Err(Eip7702ConfirmationError::TransactionFailed {
                message: "Transaction reverted".to_string(),
            })
            .map_err_fail();
        }

        tracing::debug!(
            transaction_hash = ?transaction_hash,
            block_number = receipt.block_number,
            gas_used = ?receipt.gas_used,
            "Transaction confirmed successfully"
        );

        Ok(Eip7702ConfirmationResult {
            transaction_id: job_data.transaction_id.clone(),
            transaction_hash,
            eoa_address: job_data.eoa_address,
            block_number: receipt.block_number,
            gas_used: receipt.gas_used,
            status: success,
        })
    }

    async fn on_success(
        &self,
        job: &BorrowedJob<Eip7702ConfirmationJobData>,
        success_data: SuccessHookData<'_, Eip7702ConfirmationResult>,
        tx: &mut TransactionContext<'_>,
    ) {
        // Update transaction registry
        if let Err(e) = self
            .transaction_registry
            .update_transaction_status(
                &job.job.data.transaction_id,
                crate::transaction_registry::TransactionStatus::Confirmed {
                    transaction_hash: success_data.result.transaction_hash,
                    block_number: success_data.result.block_number,
                    gas_used: success_data.result.gas_used,
                },
            )
            .await
        {
            tracing::error!(
                transaction_id = job.job.data.transaction_id,
                error = ?e,
                "Failed to update transaction registry"
            );
        }

        // Send webhook
        self.send_webhook_on_success(job, success_data, tx).await;
    }

    async fn on_nack(
        &self,
        job: &BorrowedJob<Eip7702ConfirmationJobData>,
        nack_data: NackHookData<'_, Eip7702ConfirmationError>,
        tx: &mut TransactionContext<'_>,
    ) {
        self.send_webhook_on_nack(job, nack_data, tx).await;
    }

    async fn on_fail(
        &self,
        job: &BorrowedJob<Eip7702ConfirmationJobData>,
        fail_data: FailHookData<'_, Eip7702ConfirmationError>,
        tx: &mut TransactionContext<'_>,
    ) {
        // Update transaction registry
        if let Err(e) = self
            .transaction_registry
            .update_transaction_status(
                &job.job.data.transaction_id,
                crate::transaction_registry::TransactionStatus::Failed {
                    reason: fail_data.error.to_string(),
                },
            )
            .await
        {
            tracing::error!(
                transaction_id = job.job.data.transaction_id,
                error = ?e,
                "Failed to update transaction registry"
            );
        }

        self.send_webhook_on_fail(job, fail_data, tx).await;
    }
}

// --- Helper Functions ---

async fn get_transaction_hash_from_bundler(
    chain: &impl Chain,
    bundler_transaction_id: &str,
) -> Result<TxHash, EngineError> {
    let bundler_client = chain.bundler_client();
    
    let params = json!([bundler_transaction_id]);

    let response: Value = bundler_client
        .inner
        .request("tw_getTransactionHash", params)
        .await
        .map_err(|e| EngineError::RpcError {
            message: format!("Failed to get transaction hash from bundler: {}", e),
            kind: crate::error::RpcErrorKind::Unknown,
        })?;

    let transaction_hash_str = response
        .as_str()
        .ok_or_else(|| EngineError::RpcError {
            message: "Invalid response from bundler: expected transaction hash string".to_string(),
            kind: crate::error::RpcErrorKind::Unknown,
        })?;

    let transaction_hash = transaction_hash_str.parse::<TxHash>()
        .map_err(|e| EngineError::ValidationError {
            message: format!("Invalid transaction hash format: {}", e),
        })?;

    Ok(transaction_hash)
}