use alloy::{
    hex::FromHex,
    primitives::{Address, Bytes, U256},
};
use engine_aa_core::{
    account_factory::{AccountFactory, get_account_factory},
    smart_account::{DeterminedSmartAccount, SmartAccount, SmartAccountFromSalt},
    userop::{
        builder::{UserOpBuilder, UserOpBuilderConfig},
        deployment::{AcquireLockResult, DeploymentLock, DeploymentManager, DeploymentStatus},
    },
};
use engine_core::{
    chain::{Chain, ChainService, RpcCredentials},
    credentials::SigningCredential,
    error::{AlloyRpcErrorToEngineError, EngineError},
    execution_options::{WebhookOptions, aa::Erc4337ExecutionOptions},
    transaction::InnerTransaction,
    userop::{UserOpSigner, UserOpVersion},
};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use twmq::{
    FailHookData, NackHookData, Queue, SuccessHookData,
    error::TwmqError,
    hooks::TransactionContext,
    job::{DelayOptions, Job, JobResult, RequeuePosition, ToJobError, ToJobResult},
};

use crate::webhook::{
    WebhookJobHandler,
    envelope::{ExecutorStage, HasTransactionMetadata, HasWebhookOptions, WebhookCapable},
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

    pub webhook_options: Option<WebhookOptions>,

    pub rpc_credentials: RpcCredentials,
}

impl HasWebhookOptions for ExternalBundlerSendJobData {
    fn webhook_url(&self) -> Option<String> {
        self.webhook_options.as_ref().map(|opts| opts.url.clone())
    }
}

// --- Success Result ---
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExternalBundlerSendResult {
    pub account_address: Address,
    pub nonce: U256,
    pub user_op_hash: Bytes,
    pub user_operation_sent: UserOpVersion,
    pub deployment_lock_acquired: bool,
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
        #[serde(skip_serializing_if = "Option::is_none")]
        inner_error: Option<serde_json::Value>,
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
        #[serde(skip_serializing_if = "Option::is_none")]
        inner_error: Option<serde_json::Value>,
    },

    #[error("Failed to send UserOperation to bundler: {message}")]
    BundlerSendFailed {
        account_address: Address,
        nonce_used: U256,
        had_deployment_lock: bool,
        user_op: UserOpVersion,
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        inner_error: Option<serde_json::Value>,
    },

    #[error("Invalid RPC Credentials: {message}")]
    InvalidRpcCredentials { message: String },

    #[error("Internal error: {message}")]
    InternalError { message: String },
}

impl From<TwmqError> for ExternalBundlerSendError {
    fn from(error: TwmqError) -> Self {
        ExternalBundlerSendError::InternalError {
            message: format!("Deserialization error for job data: {}", error.to_string()),
        }
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

    #[tracing::instrument(skip(self, job), fields(transaction_id = job.id, stage = Self::stage_name(), executor = Self::executor_name()))]
    async fn process(
        &self,
        job: &twmq::job::Job<Self::JobData>,
    ) -> JobResult<Self::Output, Self::ErrorData> {
        let job_data = &job.data;

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
            .data
            .rpc_credentials
            .to_header_map()
            .map_err(|e| ExternalBundlerSendError::InvalidRpcCredentials {
                message: e.to_string(),
            })
            .map_err_fail()?;

        let chain = chain.with_new_default_headers(chain_auth_headers);

        // 2. Parse Account Salt
        let salt_data = if job_data.execution_options.account_salt.starts_with("0x") {
            Bytes::from_hex(job_data.execution_options.account_salt.clone())
                .map_err(|e| ExternalBundlerSendError::InvalidAccountSalt {
                    message: format!("Failed to parse hex salt: {}", e),
                })
                .map_err_fail()?
        } else {
            let hex_string = hex::encode(job_data.execution_options.account_salt.clone());
            Bytes::from_hex(hex_string)
                .map_err(|e| ExternalBundlerSendError::InvalidAccountSalt {
                    message: format!("Failed to encode salt as hex: {}", e),
                })
                .map_err_fail()?
        };

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
                inner_error: serde_json::to_value(&e).ok(),
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
            .map_err(|e| ExternalBundlerSendError::InternalError {
                message: format!("Deployment manager error: {}", e),
            })
            .map_err_nack(Some(Duration::from_secs(10)), RequeuePosition::Last)?;

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

        // 5. Get Nonce
        let nonce = {
            use rand::Rng;
            let mut rng = rand::rng();
            let limb1: u64 = rng.random();
            let limb2: u64 = rng.random();
            let limb3: u64 = rng.random();
            U256::from_limbs([0, limb1, limb2, limb3])
        };

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
                let mapped_error =
                    map_build_error(&e, smart_account.address, nonce, needs_init_code);
                if is_build_error_retryable(&e) {
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

                // if is_bundler_error_retryable(&e) {
                if job.attempts < 100 {
                    mapped_error.nack(Some(Duration::from_secs(10)), RequeuePosition::Last)
                } else {
                    mapped_error.fail()
                }

                // } else {
                //     mapped_error.fail()
                // }
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
        job: &Job<ExternalBundlerSendJobData>,
        success_data: SuccessHookData<'_, ExternalBundlerSendResult>,
        tx: &mut TransactionContext<'_>,
    ) {
        let confirmation_job = self
            .confirm_queue
            .clone()
            .job(UserOpConfirmationJobData {
                account_address: success_data.result.account_address,
                chain_id: job.data.chain_id,
                nonce: success_data.result.nonce,
                user_op_hash: success_data.result.user_op_hash.clone(),
                transaction_id: job.data.transaction_id.clone(),
                webhook_options: job.data.webhook_url().map(|url| WebhookOptions { url }),
                rpc_credentials: job.data.rpc_credentials.clone(),
                deployment_lock_acquired: success_data.result.deployment_lock_acquired,
            })
            .with_id(job.transaction_id())
            .with_delay(DelayOptions {
                delay: Duration::from_secs(3),
                position: RequeuePosition::Last,
            });

        if let Err(e) = tx.queue_job(confirmation_job) {
            tracing::error!(
                transaction_id = %job.data.transaction_id,
                error = %e,
                "Failed to queue confirmation job"
            );
        }

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
        job: &Job<ExternalBundlerSendJobData>,
        nack_data: NackHookData<'_, ExternalBundlerSendError>,
        tx: &mut TransactionContext<'_>,
    ) {
        if let Some(account_address) = nack_data.error.did_acquire_lock() {
            self.deployment_lock.release_lock_with_pipeline(
                tx.pipeline(),
                job.data.chain_id,
                &account_address,
            );
        }

        if let Err(e) = self.queue_nack_webhook(job, nack_data, tx) {
            tracing::error!(
                transaction_id = %job.data.transaction_id,
                error = %e,
                "Failed to queue nack webhook"
            );
        }
    }

    async fn on_fail(
        &self,
        job: &Job<ExternalBundlerSendJobData>,
        fail_data: FailHookData<'_, ExternalBundlerSendError>,
        tx: &mut TransactionContext<'_>,
    ) {
        if let Some(account_address) = fail_data.error.did_acquire_lock() {
            self.deployment_lock.release_lock_with_pipeline(
                tx.pipeline(),
                job.data.chain_id,
                &account_address,
            );
        }

        if let Err(e) = self.queue_fail_webhook(job, fail_data, tx) {
            tracing::error!(
                transaction_id = %job.data.transaction_id,
                error = %e,
                "Failed to queue fail webhook"
            );
        }
    }
}

// --- Error Mapping Helpers ---
fn map_build_error(
    engine_error: &EngineError,
    account_address: Address,
    nonce: U256,
    had_lock: bool,
) -> ExternalBundlerSendError {
    let stage = match engine_error {
        EngineError::RpcError { kind, .. }
        | EngineError::PaymasterError { kind, .. }
        | EngineError::BundlerError { kind, .. } => format!("{:?}", kind),
        EngineError::VaultError { .. } => "Signing".to_string(),
        _ => "UnknownBuildStep".to_string(),
    };

    ExternalBundlerSendError::UserOpBuildFailed {
        account_address,
        nonce_used: nonce,
        stage,
        message: engine_error.to_string(),
        inner_error: serde_json::to_value(&engine_error).ok(),
        had_deployment_lock: had_lock,
    }
}

fn map_bundler_error(
    bundler_error: impl AlloyRpcErrorToEngineError,
    smart_account: &DeterminedSmartAccount,
    nonce: U256,
    signed_user_op: &UserOpVersion,
    chain: &impl Chain,
    had_lock: bool,
) -> ExternalBundlerSendError {
    let engine_error = bundler_error.to_engine_bundler_error(chain);

    ExternalBundlerSendError::BundlerSendFailed {
        account_address: smart_account.address,
        nonce_used: nonce,
        user_op: signed_user_op.clone(),
        message: engine_error.to_string(),
        inner_error: serde_json::to_value(&engine_error).ok(),
        had_deployment_lock: had_lock,
    }
}

fn is_http_error_code(kind: &engine_core::error::RpcErrorKind, error_code: u16) -> bool {
    matches!(kind, engine_core::error::RpcErrorKind::TransportHttpError { status, .. } if *status == error_code)
}

fn is_build_error_retryable(e: &EngineError) -> bool {
    match e {
        EngineError::RpcError { kind, .. } => {
            !(is_http_error_code(kind, 400) || is_http_error_code(kind, 401))
        }
        EngineError::PaymasterError { kind, .. } | EngineError::BundlerError { kind, .. } => {
            !is_http_error_code(kind, 400)
                && !matches!(
                    kind,
                    engine_core::error::RpcErrorKind::ErrorResp(r)
                    if r.code == -32000 || r.code == -32603
                )
        }
        EngineError::VaultError { .. } => false,
        _ => false,
    }
}
