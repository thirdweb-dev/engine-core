use alloy::primitives::{Address, Bytes, U256};
use engine_aa_core::{
    account_factory::{AccountFactory, get_account_factory},
    smart_account::{DeterminedSmartAccount, SmartAccount, SmartAccountFromSalt},
    userop::{
        builder::{UserOpBuilder, UserOpBuilderConfig},
        deployment::{AcquireLockResult, DeploymentLock, DeploymentManager, DeploymentStatus},
    },
};
use engine_aa_types::VersionedUserOp;
use engine_core::{
    chain::{Chain, ChainService, RpcCredentials},
    credentials::SigningCredential,
    error::{AlloyRpcErrorToEngineError, EngineError, RpcErrorKind},
    execution_options::{WebhookOptions, aa::Erc4337ExecutionOptions},
    transaction::InnerTransaction,
    userop::UserOpSigner,
};
use serde::{Deserialize, Serialize};
use serde_json;
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
    confirm::{UserOpConfirmationHandler, UserOpConfirmationJobData},
    deployment::{RedisDeploymentCache, RedisDeploymentLock},
};

// --- Job Payload ---
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExternalBundlerSendJobData {
    pub transaction_id: String,
    pub chain_id: u64,
    pub transactions: Vec<InnerTransaction>,
    pub execution_options: Erc4337ExecutionOptions,
    pub signing_credential: SigningCredential,

    pub webhook_options: Option<Vec<WebhookOptions>>,

    pub rpc_credentials: RpcCredentials,

    /// Pregenerated nonce for vault signed tokens
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pregenerated_nonce: Option<U256>,
}

impl HasWebhookOptions for ExternalBundlerSendJobData {
    fn webhook_options(&self) -> Option<Vec<WebhookOptions>> {
        self.webhook_options.clone()
    }
}

// --- Success Result ---
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExternalBundlerSendResult {
    pub account_address: Address,
    pub nonce: U256,
    pub user_op_hash: Bytes,
    pub user_operation_sent: VersionedUserOp,
    pub deployment_lock_acquired: bool,
}

// --- Policy Error Structure ---
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PaymasterPolicyResponse {
    pub policy_id: String,
    pub reason: String,
}

// --- Error Types ---
#[derive(Serialize, Deserialize, Debug, Clone, thiserror::Error)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "errorCode")]
pub enum ExternalBundlerSendError {
    #[error("Chain service error for chainId {chain_id}: {message}")]
    ChainServiceError { chain_id: u64, message: String },

    #[error("Invalid account salt format: {message}")]
    InvalidAccountSalt { message: String },

    #[error("Account determination failed for signer {signer_address}: {message}")]
    AccountDeterminationFailed {
        signer_address: Address,
        factory_address: Address,
        account_salt: String,
        message: String,
        inner_error: Option<EngineError>,
    },

    #[error("Deployment lock for account {account_address} could not be acquired: {message}")]
    DeploymentLocked {
        account_address: Address,
        message: String,
    },

    #[error("UserOperation building failed at stage {stage}: {message}")]
    UserOpBuildFailed {
        account_address: Address,
        nonce_used: U256,
        had_deployment_lock: bool,
        stage: String,
        message: String,
        inner_error: Option<EngineError>,
    },

    #[error("Failed to send UserOperation to bundler: {message}")]
    BundlerSendFailed {
        account_address: Address,
        nonce_used: U256,
        had_deployment_lock: bool,
        user_op: VersionedUserOp,
        message: String,
        inner_error: Option<EngineError>,
    },

    #[error("Policy restriction error: {reason} (Policy ID: {policy_id})")]
    PolicyRestriction { policy_id: String, reason: String },

    #[error("Invalid RPC Credentials: {message}")]
    InvalidRpcCredentials { message: String },

    #[error("Internal error: {message}")]
    InternalError { message: String },

    #[error("Transaction cancelled by user")]
    UserCancelled,
}

impl From<TwmqError> for ExternalBundlerSendError {
    fn from(error: TwmqError) -> Self {
        ExternalBundlerSendError::InternalError {
            message: format!("Deserialization error for job data: {}", error),
        }
    }
}

impl UserCancellable for ExternalBundlerSendError {
    fn user_cancelled() -> Self {
        ExternalBundlerSendError::UserCancelled
    }
}

impl ExternalBundlerSendError {
    /// Returns the account address if a lock was acquired while processing
    /// Otherwise returns None
    pub fn did_acquire_lock(&self) -> Option<Address> {
        match self {
            ExternalBundlerSendError::BundlerSendFailed {
                had_deployment_lock,
                account_address,
                ..
            } => {
                if *had_deployment_lock {
                    Some(*account_address)
                } else {
                    None
                }
            }
            ExternalBundlerSendError::UserOpBuildFailed {
                had_deployment_lock,
                account_address,
                ..
            } => {
                if *had_deployment_lock {
                    Some(*account_address)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

// --- Handler ---
pub struct ExternalBundlerSendHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    pub chain_service: Arc<CS>,
    pub userop_signer: Arc<UserOpSigner>,
    pub deployment_cache: RedisDeploymentCache,
    pub deployment_lock: RedisDeploymentLock,
    pub webhook_queue: Arc<Queue<WebhookJobHandler>>,
    pub confirm_queue: Arc<Queue<UserOpConfirmationHandler<CS>>>,
    pub transaction_registry: Arc<TransactionRegistry>,
}

impl<CS> ExternalBundlerSendHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    pub fn deployment_manager(
        &self,
    ) -> DeploymentManager<RedisDeploymentCache, RedisDeploymentLock> {
        DeploymentManager::new(
            self.deployment_cache.clone(),
            self.deployment_lock.clone(),
            Duration::from_secs(60),
        )
    }
}

impl<CS> ExecutorStage for ExternalBundlerSendHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    fn executor_name() -> &'static str {
        "erc4337"
    }

    fn stage_name() -> &'static str {
        "prepare_and_send"
    }
}

impl<CS> WebhookCapable for ExternalBundlerSendHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    fn webhook_queue(&self) -> &Arc<Queue<WebhookJobHandler>> {
        &self.webhook_queue
    }
}

impl<CS> twmq::DurableExecution for ExternalBundlerSendHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    type Output = ExternalBundlerSendResult;
    type ErrorData = ExternalBundlerSendError;
    type JobData = ExternalBundlerSendJobData;

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
            .map_err(|e| ExternalBundlerSendError::ChainServiceError {
                chain_id: job_data.chain_id,
                message: format!("Failed to get chain instance: {}", e),
            })
            .map_err_fail()?;

        let chain_auth_headers = job
            .job
            .data
            .rpc_credentials
            .to_header_map()
            .map_err(|e| ExternalBundlerSendError::InvalidRpcCredentials {
                message: e.to_string(),
            })
            .map_err_fail()?;

        let chain = chain.with_new_default_headers(chain_auth_headers);

        // 2. Parse Account Salt using the helper method
        let salt_data = job_data
            .execution_options
            .get_salt_data()
            .map_err(|e| ExternalBundlerSendError::InvalidAccountSalt {
                message: e.to_string(),
            })
            .map_err_fail()?;

        // 3. Determine Smart Account
        let smart_account = match job_data.execution_options.smart_account_address {
            Some(address) => DeterminedSmartAccount { address },
            None => SmartAccountFromSalt {
                admin_address: job_data.execution_options.signer_address,
                chain: &chain,
                factory_address: job_data
                    .execution_options
                    .entrypoint_details
                    .factory_address,
                salt_data: &salt_data,
            }
            .to_determined_smart_account()
            .await
            .map_err(|e| ExternalBundlerSendError::AccountDeterminationFailed {
                signer_address: job_data.execution_options.signer_address,
                factory_address: job_data
                    .execution_options
                    .entrypoint_details
                    .factory_address,
                account_salt: job_data.execution_options.account_salt.clone(),
                message: e.to_string(),
                inner_error: Some(e),
            })
            .map_err_fail()?,
        };

        tracing::debug!(account_address = ?smart_account.address, "Smart account determined");

        // 4. Handle Deployment Management
        let is_deployed_check = smart_account.is_deployed(&chain);

        let deployment_status = self
            .deployment_manager()
            .check_deployment_status(job_data.chain_id, &smart_account.address, is_deployed_check)
            .await
            // we could have redis errors, or RPC errors
            // in case of RPC errors we'd want to see if the RPC error is retryable
            .map_err(|e| {
                tracing::error!(error = ?e, "Error in deployment manager");
                match &e {
                    EngineError::RpcError { kind: k, .. } => {
                        let mapped_error = ExternalBundlerSendError::ChainServiceError {
                            chain_id: chain.chain_id(),
                            message: format!("Deployment manager error: {}", e),
                        };
                        if is_retryable_rpc_error(k) {
                            mapped_error.nack(Some(Duration::from_secs(10)), RequeuePosition::Last)
                        } else {
                            mapped_error.fail()
                        }
                    }
                    _ => ExternalBundlerSendError::InternalError {
                        message: format!("Deployment manager error: {}", e),
                    }
                    .nack(Some(Duration::from_secs(10)), RequeuePosition::Last),
                }
            })?;

        let needs_init_code = match deployment_status {
            DeploymentStatus::Deployed => false,
            DeploymentStatus::BeingDeployed { stale, lock_id } => {
                return Err(ExternalBundlerSendError::DeploymentLocked {
                    account_address: smart_account.address,
                    message: format!(
                        "Deployment in progress (stale: {}, lock_id: {})",
                        stale, lock_id
                    ),
                })
                .map_err_nack(
                    Some(Duration::from_secs(if stale { 5 } else { 30 })),
                    RequeuePosition::Last,
                );
            }
            DeploymentStatus::NotDeployed => {
                match self
                    .deployment_lock
                    .acquire_lock(job_data.chain_id, &smart_account.address)
                    .await
                    .map_err(|e| ExternalBundlerSendError::DeploymentLocked {
                        account_address: smart_account.address,
                        message: format!("Failed to acquire deployment lock: {}", e),
                    })
                    .map_err_nack(Some(Duration::from_secs(15)), RequeuePosition::Last)?
                {
                    AcquireLockResult::Acquired => {
                        // We got the lock! Continue with deployment
                        true
                    }
                    AcquireLockResult::AlreadyLocked(lock_id) => {
                        // Someone else has the lock, NACK and retry later
                        return Err(ExternalBundlerSendError::DeploymentLocked {
                            account_address: smart_account.address,
                            message: format!("Lock held by another process: {}", lock_id),
                        })
                        .map_err_nack(Some(Duration::from_secs(15)), RequeuePosition::Last);
                    }
                }
            }
        };

        tracing::debug!(lock_acquired = ?needs_init_code);

        // 5. Get Nonce - use pregenerated nonce if available, otherwise generate random
        let nonce = job_data.pregenerated_nonce.unwrap_or_else(|| {
            use rand::Rng;
            let mut rng = rand::rng();
            let limb1: u64 = rng.random();
            let limb2: u64 = rng.random();
            let limb3: u64 = rng.random();
            U256::from_limbs([0, limb1, limb2, limb3])
        });

        // 6. Prepare Init Call Data
        let init_call_data = get_account_factory(
            &chain,
            job_data
                .execution_options
                .entrypoint_details
                .factory_address,
            None,
        )
        .init_calldata(job_data.execution_options.signer_address, salt_data);

        // 7. Encode Call Data
        let call_data = if job_data.transactions.len() == 1 {
            smart_account.encode_execute(&job_data.transactions[0])
        } else {
            smart_account.encode_execute_batch(&job_data.transactions)
        };

        // 8. Build User Operation
        let builder_config = UserOpBuilderConfig {
            account_address: smart_account.address,
            signer_address: job_data.execution_options.signer_address,
            entrypoint_and_factory: job_data.execution_options.entrypoint_details.clone(),
            call_data,
            init_call_data,
            is_deployed: !needs_init_code,
            nonce,
            credential: job_data.signing_credential.clone(),
            chain: &chain,
            signer: self.userop_signer.clone(),
        };

        let signed_user_op = UserOpBuilder::new(builder_config)
            .build()
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, "Error building user operation");
                let mapped_error =
                    map_build_error(&e, smart_account.address, nonce, needs_init_code);
                if is_external_bundler_error_retryable(&mapped_error) {
                    mapped_error.nack(Some(Duration::from_secs(10)), RequeuePosition::Last)
                } else {
                    mapped_error.fail()
                }
            })?;

        tracing::debug!("User operation built");

        // 9. Send to Bundler
        let user_op_hash = chain
            .bundler_client()
            .send_user_op(
                &signed_user_op,
                job_data
                    .execution_options
                    .entrypoint_details
                    .entrypoint_address,
            )
            .await
            .map_err(|e| {
                let mapped_error = map_bundler_error(
                    e,
                    &smart_account,
                    nonce,
                    &signed_user_op,
                    &chain,
                    needs_init_code,
                );

                tracing::warn!(
                    error = serde_json::to_string(&mapped_error).unwrap(),
                    "error"
                );

                if is_bundler_error_retryable(&mapped_error.to_string()) {
                    if job.job.attempts < 100 {
                        mapped_error.nack(Some(Duration::from_secs(10)), RequeuePosition::Last)
                    } else {
                        mapped_error.fail()
                    }
                } else {
                    mapped_error.fail()
                }
            })?;

        tracing::debug!(userop_hash = ?user_op_hash, "User operation sent to bundler");

        Ok(ExternalBundlerSendResult {
            account_address: smart_account.address,
            nonce,
            user_op_hash,
            user_operation_sent: signed_user_op,
            deployment_lock_acquired: needs_init_code,
        })
    }

    async fn on_success(
        &self,
        job: &BorrowedJob<ExternalBundlerSendJobData>,
        success_data: SuccessHookData<'_, ExternalBundlerSendResult>,
        tx: &mut TransactionContext<'_>,
    ) {
        // Update transaction registry: move from send queue to confirm queue
        self.transaction_registry.add_set_command(
            tx.pipeline(),
            &job.job.data.transaction_id,
            "userop_confirm",
        );

        let confirmation_job = self
            .confirm_queue
            .clone()
            .job(UserOpConfirmationJobData {
                account_address: success_data.result.account_address,
                chain_id: job.job.data.chain_id,
                nonce: success_data.result.nonce,
                user_op_hash: success_data.result.user_op_hash.clone(),
                transaction_id: job.job.data.transaction_id.clone(),
                webhook_options: job.job.data.webhook_options().clone(),
                rpc_credentials: job.job.data.rpc_credentials.clone(),
                deployment_lock_acquired: success_data.result.deployment_lock_acquired,
            })
            .with_id(job.job.transaction_id())
            .with_delay(DelayOptions {
                delay: Duration::from_secs(3),
                position: RequeuePosition::Last,
            });

        if let Err(e) = tx.queue_job(confirmation_job) {
            tracing::error!(
                transaction_id = %job.job.data.transaction_id,
                error = %e,
                "Failed to queue confirmation job"
            );
        }

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
        job: &BorrowedJob<ExternalBundlerSendJobData>,
        nack_data: NackHookData<'_, ExternalBundlerSendError>,
        tx: &mut TransactionContext<'_>,
    ) {
        if let Some(account_address) = nack_data.error.did_acquire_lock() {
            self.deployment_lock.release_lock_with_pipeline(
                tx.pipeline(),
                job.job.data.chain_id,
                &account_address,
            );
        }

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
        job: &BorrowedJob<ExternalBundlerSendJobData>,
        fail_data: FailHookData<'_, ExternalBundlerSendError>,
        tx: &mut TransactionContext<'_>,
    ) {
        // Remove transaction from registry since it failed permanently
        self.transaction_registry
            .add_remove_command(tx.pipeline(), &job.job.data.transaction_id);

        if let Some(account_address) = fail_data.error.did_acquire_lock() {
            self.deployment_lock.release_lock_with_pipeline(
                tx.pipeline(),
                job.job.data.chain_id,
                &account_address,
            );
        }

        if let Err(e) = self.queue_fail_webhook(job, fail_data, tx) {
            tracing::error!(
                transaction_id = %job.job.data.transaction_id,
                error = %e,
                "Failed to queue fail webhook"
            );
        }
    }
}

// --- Error Mapping Helpers ---

/// Attempts to parse a policy error from an error message/body
fn try_parse_policy_error(error_body: &str) -> Option<PaymasterPolicyResponse> {
    // Try to parse the error body as JSON containing policy error response
    serde_json::from_str::<PaymasterPolicyResponse>(error_body).ok()
}

fn map_build_error(
    engine_error: &EngineError,
    account_address: Address,
    nonce: U256,
    had_lock: bool,
) -> ExternalBundlerSendError {
    // First check if this is a paymaster policy error
    if let EngineError::PaymasterError { kind, .. } = engine_error {
        match kind {
            RpcErrorKind::TransportHttpError { body, .. } => {
                if let Some(policy_response) = try_parse_policy_error(body) {
                    return ExternalBundlerSendError::PolicyRestriction {
                        policy_id: policy_response.policy_id,
                        reason: policy_response.reason,
                    };
                }
            }
            RpcErrorKind::DeserError { text, .. } => {
                if let Some(policy_error) = try_parse_policy_error(text) {
                    return ExternalBundlerSendError::PolicyRestriction {
                        policy_id: policy_error.policy_id,
                        reason: policy_error.reason,
                    };
                }
            }
            _ => {}
        }
    }

    let stage = match engine_error {
        EngineError::RpcError { .. } | EngineError::PaymasterError { .. } => "BUILDING".to_string(),
        EngineError::BundlerError { .. } => "BUNDLING".to_string(),
        EngineError::VaultError { .. } => "SIGNING".to_string(),
        _ => "UNKNOWN".to_string(),
    };

    ExternalBundlerSendError::UserOpBuildFailed {
        account_address,
        nonce_used: nonce,
        stage,
        message: engine_error.to_string(),
        inner_error: Some(engine_error.clone()),
        had_deployment_lock: had_lock,
    }
}

fn map_bundler_error(
    bundler_error: impl AlloyRpcErrorToEngineError,
    smart_account: &DeterminedSmartAccount,
    nonce: U256,
    signed_user_op: &VersionedUserOp,
    chain: &impl Chain,
    had_lock: bool,
) -> ExternalBundlerSendError {
    let engine_error = bundler_error.to_engine_bundler_error(chain);

    ExternalBundlerSendError::BundlerSendFailed {
        account_address: smart_account.address,
        nonce_used: nonce,
        user_op: signed_user_op.clone(),
        message: engine_error.to_string(),
        inner_error: Some(engine_error),
        had_deployment_lock: had_lock,
    }
}

// fn is_http_error_code(kind: &engine_core::error::RpcErrorKind, error_code: u16) -> bool {
//     matches!(kind, engine_core::error::RpcErrorKind::TransportHttpError { status, .. } if *status == error_code)
// }

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

/// Determines if a bundler error should be retried based on its content
fn is_bundler_error_retryable(error_msg: &str) -> bool {
    // Check for specific AA error codes that should not be retried
    if error_msg.contains("AA10") || // sender already constructed
        error_msg.contains("AA13") || // initCode failed or OOG
        error_msg.contains("AA14") || // initCode must return sender
        error_msg.contains("AA15") || // initCode must create sender
        error_msg.contains("AA21") || // didn't pay prefund
        error_msg.contains("AA22") || // expired or not due
        error_msg.contains("AA23") || // reverted (or OOG)
        error_msg.contains("AA24") || // signature error
        error_msg.contains("AA25") || // invalid account nonce
        error_msg.contains("AA31") || // paymaster deposit too low
        error_msg.contains("AA32") || // paymaster stake too low
        error_msg.contains("AA33") || // reverted (or OOG)
        error_msg.contains("AA34") || // signature error
        error_msg.contains("AA40") || // over verificationGasLimit
        error_msg.contains("AA41") || // too little verificationGas
        error_msg.contains("AA50") || // postOp reverted
        error_msg.contains("AA51")
    // prefund below actualGasCost
    {
        return false;
    }

    // Check for revert-related messages that indicate permanent failures
    if error_msg.contains("execution reverted")
        || error_msg.contains("UserOperation reverted")
        || error_msg.contains("reverted during simulation")
        || error_msg.contains("invalid signature")
        || error_msg.contains("signature error")
        || error_msg.contains("nonce too low")
        || error_msg.contains("nonce too high")
        || error_msg.contains("insufficient funds")
    {
        return false;
    }

    // Check for HTTP status codes that shouldn't be retried (4xx client errors)
    if error_msg.contains("status: 400")
        || error_msg.contains("status: 401")
        || error_msg.contains("status: 403")
        || error_msg.contains("status: 404")
        || error_msg.contains("status: 422")
        || error_msg.contains("status: 429")
    // rate limit - could be retried but often permanent
    {
        return false;
    }

    // Retry everything else (network issues, 5xx errors, timeouts, etc.)
    true
}

/// Determines if an ExternalBundlerSendError should be retried
fn is_external_bundler_error_retryable(e: &ExternalBundlerSendError) -> bool {
    match e {
        // Policy restrictions are never retryable
        ExternalBundlerSendError::PolicyRestriction { .. } => false,

        // For other errors, check their inner EngineError if present
        ExternalBundlerSendError::UserOpBuildFailed {
            inner_error: Some(inner),
            ..
        } => is_build_error_retryable(inner),
        ExternalBundlerSendError::BundlerSendFailed {
            inner_error: Some(inner),
            ..
        } => is_build_error_retryable(inner),

        // User cancellations are not retryable
        ExternalBundlerSendError::UserCancelled => false,

        // Account determination failures are generally not retryable (validation errors)
        ExternalBundlerSendError::AccountDeterminationFailed { .. } => false,

        // Invalid account salt is not retryable (validation error)
        ExternalBundlerSendError::InvalidAccountSalt { .. } => false,

        // Invalid RPC credentials are not retryable (auth error)
        ExternalBundlerSendError::InvalidRpcCredentials { .. } => false,

        // Deployment locked and chain service errors can be retried
        ExternalBundlerSendError::DeploymentLocked { .. } => true,
        ExternalBundlerSendError::ChainServiceError { .. } => true,

        // Internal errors can be retried
        ExternalBundlerSendError::InternalError { .. } => true,

        // Default to not retryable for safety
        _ => false,
    }
}
