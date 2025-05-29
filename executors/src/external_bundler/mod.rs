use alloy::{
    hex::FromHex,
    primitives::{Address, Bytes, U256},
};
use engine_aa_core::{
    account_factory::{AccountFactory, get_account_factory},
    smart_account::{DeterminedSmartAccount, SmartAccount, SmartAccountFromSalt},
    userop::{
        builder::{UserOpBuilder, UserOpBuilderConfig},
        deployment::{DeploymentCache, DeploymentLock, DeploymentManager, DeploymentStatus},
    },
};
use engine_core::{
    chain::{Chain, ChainService},
    credentials::SigningCredential,
    error::{AlloyRpcErrorToEngineError, EngineError},
    execution_options::aa::Erc4337ExecutionOptions,
    transaction::InnerTransaction,
    userop::{UserOpSigner, UserOpVersion},
};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use twmq::{
    FailHookData, NackHookData, Queue, SuccessHookData,
    hooks::TransactionContext,
    job::{Job, JobResult, RequeuePosition, ToJobError, ToJobResult},
};

use crate::webhook::{
    WebhookJobHandler,
    envelope::{ExecutorStage, HasWebhookOptions, WebhookCapable},
};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WebhookOptions {
    pub webhook_url: String,
}

// --- Job Payload ---
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExternalBundlerSendJobData {
    pub original_request_id: String,
    pub chain_id: u64,
    pub transactions: Vec<InnerTransaction>,
    pub execution_options: Erc4337ExecutionOptions,
    pub signing_credential: SigningCredential,

    pub webhook_options: Option<WebhookOptions>,

    pub thirdweb_client_id: Option<String>,
    pub thirdweb_service_key: Option<String>,
}

impl HasWebhookOptions for ExternalBundlerSendJobData {
    fn webhook_url(&self) -> Option<String> {
        self.webhook_options
            .as_ref()
            .map(|opts| opts.webhook_url.clone())
    }

    fn transaction_id(&self) -> String {
        self.original_request_id.clone()
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
}

// --- Error Types ---
#[derive(Serialize, Deserialize, Debug, Clone, thiserror::Error)]
#[serde(rename_all = "camelCase", tag = "errorCode")]
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
        technical_details: Option<String>,
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
        stage: String,
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        technical_details: Option<String>,
    },

    #[error("Failed to send UserOperation to bundler: {message}")]
    BundlerSendFailed {
        account_address: Address,
        nonce_used: U256,
        user_op: UserOpVersion,
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        technical_details: Option<String>,
    },

    #[error("Internal error: {message}")]
    InternalError { message: String },
}

// --- Handler ---
pub struct ExternalBundlerSendHandler<CS, DCache, DLock>
where
    CS: ChainService + Send + Sync + 'static,
    DCache: DeploymentCache + Send + Sync + 'static,
    DLock: DeploymentLock + Send + Sync + 'static,
{
    pub chain_service: Arc<CS>,
    pub userop_signer: Arc<UserOpSigner>,
    pub deployment_manager: Arc<DeploymentManager<DCache, DLock>>,
    pub webhook_queue: Arc<Queue<WebhookJobHandler>>,
}

impl<CS, DCache, DLock> ExecutorStage for ExternalBundlerSendHandler<CS, DCache, DLock>
where
    CS: ChainService + Send + Sync + 'static,
    DCache: DeploymentCache + Send + Sync + 'static,
    DLock: DeploymentLock + Send + Sync + 'static,
{
    fn executor_name() -> &'static str {
        "erc4337"
    }

    fn stage_name() -> &'static str {
        "prepare_and_send"
    }
}

impl<CS, DCache, DLock> WebhookCapable for ExternalBundlerSendHandler<CS, DCache, DLock>
where
    CS: ChainService + Send + Sync + 'static,
    DCache: DeploymentCache + Send + Sync + 'static,
    DLock: DeploymentLock + Send + Sync + 'static,
{
    fn webhook_queue(&self) -> &Arc<Queue<WebhookJobHandler>> {
        &self.webhook_queue
    }
}

impl<CS, DCache, DLock> twmq::DurableExecution for ExternalBundlerSendHandler<CS, DCache, DLock>
where
    CS: ChainService + Send + Sync + 'static,
    DCache: DeploymentCache + Send + Sync + 'static,
    DLock: DeploymentLock + Send + Sync + 'static,
{
    type Output = ExternalBundlerSendResult;
    type ErrorData = ExternalBundlerSendError;
    type JobData = ExternalBundlerSendJobData;

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
            .fail_err()?;

        // 2. Parse Account Salt
        let salt_data = if job_data.execution_options.account_salt.starts_with("0x") {
            Bytes::from_hex(job_data.execution_options.account_salt.clone())
                .map_err(|e| ExternalBundlerSendError::InvalidAccountSalt {
                    message: format!("Failed to parse hex salt: {}", e),
                })
                .fail_err()?
        } else {
            let hex_string = hex::encode(job_data.execution_options.account_salt.clone());
            Bytes::from_hex(hex_string)
                .map_err(|e| ExternalBundlerSendError::InvalidAccountSalt {
                    message: format!("Failed to encode salt as hex: {}", e),
                })
                .fail_err()?
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
                technical_details: Some(serde_json::to_string(&e).unwrap_or_default()),
            })
            .fail_err()?,
        };

        // 4. Handle Deployment Management
        let is_deployed_check = smart_account.is_deployed(&chain);

        let deployment_status = self
            .deployment_manager
            .check_deployment_status(job_data.chain_id, &smart_account.address, is_deployed_check)
            .await
            .map_err(|e| ExternalBundlerSendError::InternalError {
                message: format!("Deployment manager error: {}", e),
            })
            .nack_err(Some(Duration::from_secs(10)), RequeuePosition::Last)?;

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
                .nack_err(
                    Some(Duration::from_secs(if stale { 5 } else { 30 })),
                    RequeuePosition::Last,
                );
            }
            DeploymentStatus::NotDeployed => {
                self.deployment_manager
                    .lock
                    .acquire_lock(job_data.chain_id, &smart_account.address)
                    .await
                    .map_err(|e| ExternalBundlerSendError::DeploymentLocked {
                        account_address: smart_account.address,
                        message: format!("Failed to acquire deployment lock: {}", e),
                    })
                    .nack_err(Some(Duration::from_secs(15)), RequeuePosition::Last)?;

                true
            }
        };

        // 5. Get Nonce - TODO: Replace with actual EntryPoint.getNonce() call
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
                let mapped_error = map_build_error(&e, smart_account.address, nonce);
                if is_build_error_retryable(&e) {
                    mapped_error.nack(Some(Duration::from_secs(10)), RequeuePosition::Last)
                } else {
                    mapped_error.fail()
                }
            })?;

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
                let mapped_error =
                    map_bundler_error(e, &smart_account, nonce, &signed_user_op, &chain);

                // if is_bundler_error_retryable(&e) {
                mapped_error.nack(Some(Duration::from_secs(10)), RequeuePosition::Last)
                // } else {
                //     mapped_error.fail()
                // }
            })?;

        Ok(ExternalBundlerSendResult {
            account_address: smart_account.address,
            nonce,
            user_op_hash,
            user_operation_sent: signed_user_op,
        })
    }

    async fn on_success(
        &self,
        job: &Job<ExternalBundlerSendJobData>,
        success_data: SuccessHookData<'_, ExternalBundlerSendResult>,
        tx: &mut TransactionContext<'_>,
    ) {
        if let Err(e) = self.queue_success_webhook(job, success_data, tx) {
            tracing::error!(
                transaction_id = %job.data.original_request_id,
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
        if let Err(e) = self.queue_nack_webhook(job, nack_data, tx) {
            tracing::error!(
                transaction_id = %job.data.original_request_id,
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
        if let Err(e) = self.queue_fail_webhook(job, fail_data, tx) {
            tracing::error!(
                transaction_id = %job.data.original_request_id,
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
        technical_details: Some(serde_json::to_string(engine_error).unwrap_or_default()),
    }
}

fn map_bundler_error(
    bundler_error: impl AlloyRpcErrorToEngineError,
    smart_account: &DeterminedSmartAccount,
    nonce: U256,
    signed_user_op: &UserOpVersion,
    chain: &impl Chain,
) -> ExternalBundlerSendError {
    let engine_error = bundler_error.to_engine_bundler_error(chain);

    ExternalBundlerSendError::BundlerSendFailed {
        account_address: smart_account.address,
        nonce_used: nonce,
        user_op: signed_user_op.clone(),
        message: engine_error.to_string(),
        technical_details: Some(serde_json::to_string(&engine_error).unwrap_or_default()),
    }
}

fn is_build_error_retryable(e: &EngineError) -> bool {
    match e {
        EngineError::RpcError { kind, .. }
        | EngineError::PaymasterError { kind, .. }
        | EngineError::BundlerError { kind, .. } => !matches!(
            kind,
            engine_core::error::RpcErrorKind::ErrorResp(r)
            if r.code == -32000 || r.code == -32603
        ),
        EngineError::VaultError { .. } => false,
        _ => false,
    }
}
