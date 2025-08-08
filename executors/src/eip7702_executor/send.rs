use alloy::{
    eips::eip7702::Authorization,
    primitives::{Address, U256},
};
use engine_core::{
    chain::{Chain, ChainService, RpcCredentials},
    credentials::SigningCredential,
    error::{AlloyRpcErrorToEngineError, EngineError, RpcErrorKind},
    execution_options::{WebhookOptions, eip7702::Eip7702ExecutionOptions},
    signer::EoaSigner,
    transaction::InnerTransaction,
};
use engine_eip7702_core::delegated_account::DelegatedAccount;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{ops::Deref, sync::Arc, time::Duration};
use twmq::{
    FailHookData, NackHookData, Queue, SuccessHookData, UserCancellable,
    error::TwmqError,
    hooks::TransactionContext,
    job::{BorrowedJob, JobResult, ToJobError, ToJobResult},
};

use crate::{
    metrics::{record_transaction_queued_to_sent, current_timestamp_ms, calculate_duration_seconds_from_twmq},
    transaction_registry::TransactionRegistry,
    webhook::{
        WebhookJobHandler,
        envelope::{ExecutorStage, HasTransactionMetadata, HasWebhookOptions, WebhookCapable},
    },
};

use super::confirm::{Eip7702ConfirmationHandler, Eip7702ConfirmationJobData};

// --- Job Payload ---
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Eip7702SendJobData {
    pub transaction_id: String,
    pub chain_id: u64,
    pub transactions: Vec<InnerTransaction>,
    pub execution_options: Eip7702ExecutionOptions,
    pub signing_credential: SigningCredential,
    #[serde(default)]
    pub webhook_options: Vec<WebhookOptions>,
    pub rpc_credentials: RpcCredentials,
    pub nonce: Option<U256>,
}

impl HasWebhookOptions for Eip7702SendJobData {
    fn webhook_options(&self) -> Vec<WebhookOptions> {
        self.webhook_options.clone()
    }
}

impl HasTransactionMetadata for Eip7702SendJobData {
    fn transaction_id(&self) -> String {
        self.transaction_id.clone()
    }
}

// --- Success Result ---
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Eip7702SendResult {
    pub transaction_id: String,
    pub wrapped_calls: Value,
    pub signature: String,
    pub authorization: Option<Authorization>,

    #[serde(flatten)]
    pub sender_details: Eip7702Sender,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum Eip7702Sender {
    #[serde(rename_all = "camelCase")]
    Owner { eoa_address: Address },
    #[serde(rename_all = "camelCase")]
    SessionKey {
        session_key_address: Address,
        eoa_address: Address,
    },
}

// --- Error Types ---
#[derive(Serialize, Deserialize, Debug, Clone, thiserror::Error)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "errorCode")]
pub enum Eip7702SendError {
    #[error("Chain service error for chainId {chain_id}: {message}")]
    #[serde(rename_all = "camelCase")]
    ChainServiceError { chain_id: u64, message: String },

    #[error("Failed to sign authorization or wrapped calls: {inner_error}")]
    #[serde(rename_all = "camelCase")]
    SigningFailed { inner_error: EngineError },

    #[error("Failed to check delegation or add authorization: {inner_error}")]
    #[serde(rename_all = "camelCase")]
    DelegationCheckFailed { inner_error: EngineError },

    #[error("Failed to call bundler: {inner_error}")]
    #[serde(rename_all = "camelCase")]
    BundlerCallFailed { inner_error: EngineError },

    #[error("Invalid RPC Credentials: {message}")]
    InvalidRpcCredentials { message: String },

    #[error("Internal error: {message}")]
    InternalError { message: String },

    #[error("Transaction cancelled by user")]
    UserCancelled,
}

impl From<TwmqError> for Eip7702SendError {
    fn from(error: TwmqError) -> Self {
        Eip7702SendError::InternalError {
            message: format!("Deserialization error for job data: {error}"),
        }
    }
}

impl UserCancellable for Eip7702SendError {
    fn user_cancelled() -> Self {
        Eip7702SendError::UserCancelled
    }
}

// --- Handler ---
pub struct Eip7702SendHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    pub chain_service: Arc<CS>,
    pub eoa_signer: Arc<EoaSigner>,
    pub webhook_queue: Arc<Queue<WebhookJobHandler>>,
    pub confirm_queue: Arc<Queue<Eip7702ConfirmationHandler<CS>>>,
    pub transaction_registry: Arc<TransactionRegistry>,
}

impl<CS> ExecutorStage for Eip7702SendHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    fn executor_name() -> &'static str {
        "eip7702"
    }

    fn stage_name() -> &'static str {
        "send"
    }
}

impl<CS> WebhookCapable for Eip7702SendHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    fn webhook_queue(&self) -> &Arc<Queue<WebhookJobHandler>> {
        &self.webhook_queue
    }
}

impl<CS> twmq::DurableExecution for Eip7702SendHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    type Output = Eip7702SendResult;
    type ErrorData = Eip7702SendError;
    type JobData = Eip7702SendJobData;

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
            .map_err(|e| Eip7702SendError::ChainServiceError {
                chain_id: job_data.chain_id,
                message: format!("Failed to get chain instance: {e}"),
            })
            .map_err_fail()?;

        let chain_auth_headers = job_data
            .rpc_credentials
            .to_header_map()
            .map_err(|e| Eip7702SendError::InvalidRpcCredentials {
                message: e.to_string(),
            })
            .map_err_fail()?;

        let chain = chain.with_new_default_headers(chain_auth_headers);

        let owner_address = match &job_data.execution_options {
            Eip7702ExecutionOptions::Owner(o) => o.from,
            Eip7702ExecutionOptions::SessionKey(s) => s.session_key_address,
        };

        let account = DelegatedAccount::new(owner_address, chain);

        let session_key_target_address = match &job_data.execution_options {
            Eip7702ExecutionOptions::Owner(_) => None,
            Eip7702ExecutionOptions::SessionKey(s) => Some(s.account_address),
        };

        let transactions = match session_key_target_address {
            Some(target_address) => {
                account.session_key_transaction(target_address, &job_data.transactions)
            }
            None => account.owner_transaction(&job_data.transactions),
        };

        let transactions = transactions
            .add_authorization_if_needed(self.eoa_signer.deref(), &job_data.signing_credential)
            .await
            .map_err(|e| {
                let mapped_error = match e {
                    EngineError::RpcError { .. } => Eip7702SendError::DelegationCheckFailed {
                        inner_error: e.clone(),
                    },
                    _ => Eip7702SendError::SigningFailed {
                        inner_error: e.clone(),
                    },
                };

                if is_build_error_retryable(&e) {
                    mapped_error.nack_with_max_retries(
                        Some(Duration::from_secs(2)),
                        twmq::job::RequeuePosition::Last,
                        3,
                        job.attempts(),
                    )
                } else {
                    mapped_error.fail()
                }
            })?;

        let (wrapped_calls, signature) = transactions
            .build(self.eoa_signer.deref(), &job_data.signing_credential)
            .await
            .map_err(|e| Eip7702SendError::SigningFailed { inner_error: e })
            .map_err_fail()?;

        let transaction_id = transactions
            .account()
            .chain()
            .bundler_client()
            .tw_execute(
                owner_address,
                &wrapped_calls,
                &signature,
                transactions.authorization(),
            )
            .await
            .map_err(|e| {
                let engine_error = e.to_engine_bundler_error(transactions.account().chain());
                let wrapped_error = Eip7702SendError::BundlerCallFailed {
                    inner_error: engine_error.clone(),
                };
                if let EngineError::BundlerError { kind, .. } = &engine_error {
                    if is_retryable_rpc_error(kind) {
                        wrapped_error.nack_with_max_retries(
                            Some(Duration::from_secs(2)),
                            twmq::job::RequeuePosition::Last,
                            3,
                            job.attempts(),
                        )
                    } else {
                        wrapped_error.fail()
                    }
                } else {
                    wrapped_error.fail()
                }
            })?;

        tracing::debug!(transaction_id = ?transaction_id, "EIP-7702 transaction sent to bundler");

                    // Record metrics: transaction queued to sent
            let sent_timestamp = current_timestamp_ms();
            let queued_to_sent_duration = calculate_duration_seconds_from_twmq(job.job.created_at, sent_timestamp);
            record_transaction_queued_to_sent("eip7702", job_data.chain_id, queued_to_sent_duration);

        let sender_details = match session_key_target_address {
            Some(target_address) => Eip7702Sender::SessionKey {
                session_key_address: owner_address,
                eoa_address: target_address,
            },
            None => Eip7702Sender::Owner {
                eoa_address: owner_address,
            },
        };

        Ok(Eip7702SendResult {
            sender_details,
            transaction_id: transaction_id.clone(),
            wrapped_calls: serde_json::to_value(&wrapped_calls)
                .map_err(|e| Eip7702SendError::InternalError {
                    message: format!("Failed to serialize wrapped calls: {e}"),
                })
                .map_err_fail()?,
            signature,
            authorization: transactions.authorization().map(|f| f.inner().clone()),
        })
    }

    async fn on_success(
        &self,
        job: &BorrowedJob<Eip7702SendJobData>,
        success_data: SuccessHookData<'_, Eip7702SendResult>,
        tx: &mut TransactionContext<'_>,
    ) {
        // Update transaction registry: move from send queue to confirm queue
        self.transaction_registry.add_set_command(
            tx.pipeline(),
            &job.job.data.transaction_id,
            "eip7702_confirm",
        );

        // Send confirmation job
        let confirmation_job = self
            .confirm_queue
            .clone()
            .job(Eip7702ConfirmationJobData {
                transaction_id: job.job.data.transaction_id.clone(),
                chain_id: job.job.data.chain_id,
                bundler_transaction_id: success_data.result.transaction_id.clone(),
                sender_details: success_data.result.sender_details.clone(),
                rpc_credentials: job.job.data.rpc_credentials.clone(),
                webhook_options: job.job.data.webhook_options.clone(),
                original_queued_timestamp: Some(job.job.created_at),
            })
            .with_id(job.job.transaction_id());

        if let Err(e) = tx.queue_job(confirmation_job) {
            tracing::error!(
                transaction_id = job.job.data.transaction_id,
                error = ?e,
                "Failed to enqueue confirmation job"
            );
        }

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
        job: &BorrowedJob<Eip7702SendJobData>,
        nack_data: NackHookData<'_, Eip7702SendError>,
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
        job: &BorrowedJob<Eip7702SendJobData>,
        fail_data: FailHookData<'_, Eip7702SendError>,
        tx: &mut TransactionContext<'_>,
    ) {
        // Remove transaction from registry since it failed permanently
        self.transaction_registry
            .add_remove_command(tx.pipeline(), &job.job.data.transaction_id);

        tracing::error!(
            transaction_id = job.job.data.transaction_id,
            error = ?fail_data.error,
            "EIP-7702 send job failed"
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

/// Determines if an error should be retried based on its type and content
fn is_build_error_retryable(e: &EngineError) -> bool {
    match e {
        // Standard RPC errors - don't retry client errors (4xx)
        EngineError::RpcError { kind, .. } => !is_client_error(kind),

        // Paymaster and Bundler errors - more restrictive retry policy
        EngineError::PaymasterError { kind, .. } | EngineError::BundlerError { kind, .. } => {
            is_retryable_rpc_error(kind)
        }

        // Vault errors are never retryable (auth/encryption issues)
        EngineError::VaultError { .. } => false,

        // All other errors are not retryable by default
        _ => false,
    }
}

/// Check if an RPC error represents a client error (4xx) that shouldn't be retried
fn is_client_error(kind: &RpcErrorKind) -> bool {
    match kind {
        RpcErrorKind::TransportHttpError { status, .. } if *status >= 400 && *status < 500 => true,
        RpcErrorKind::UnsupportedFeature { .. } => true,
        _ => false,
    }
}

/// Determine if an RPC error from paymaster/bundler should be retried
fn is_retryable_rpc_error(kind: &RpcErrorKind) -> bool {
    match kind {
        // Don't retry client errors (4xx)
        RpcErrorKind::TransportHttpError { status, .. } if *status >= 400 && *status < 500 => false,

        // Don't retry 500 errors that contain revert data (contract reverts)
        RpcErrorKind::TransportHttpError { status: 500, body } => !contains_revert_data(body),

        // Don't retry specific JSON-RPC error codes
        RpcErrorKind::ErrorResp(resp) if is_non_retryable_rpc_code(resp.code) => false,

        // Retry other errors (network issues, temporary server errors, etc.)
        _ => true,
    }
}

/// Check if the error body contains revert data indicating a contract revert
fn contains_revert_data(body: &str) -> bool {
    // Common revert selectors that indicate contract execution failures
    const REVERT_SELECTORS: &[&str] = &[
        "0x08c379a0", // Error(string)
        "0x4e487b71", // Panic(uint256)
    ];

    // Check if body contains any revert selectors
    REVERT_SELECTORS.iter().any(|selector| body.contains(selector)) ||
    // Also check for common revert-related keywords
    body.contains("reverted during simulation") ||
    body.contains("execution reverted") ||
    body.contains("UserOperation reverted")
}

/// Check if an RPC error code should not be retried
fn is_non_retryable_rpc_code(code: i64) -> bool {
    match code {
        -32000 => true, // Invalid input / execution error
        -32001 => true, // Chain does not exist / invalid chain
        -32603 => true, // Internal error (often indicates invalid params)
        _ => false,
    }
}
