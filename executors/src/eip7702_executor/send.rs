use alloy::{
    dyn_abi::{DynSolType, DynSolValue},
    primitives::{Address, Bytes, U256, ChainId, FixedBytes},
    rpc::types::Authorization,
    dyn_abi::TypedData,
};
use engine_core::{
    chain::{Chain, ChainService, RpcCredentials},
    credentials::SigningCredential,
    error::EngineError,
    execution_options::WebhookOptions,
    signer::{AccountSigner, EoaSigner, EoaSigningOptions},
    transaction::InnerTransaction,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc};
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

use super::confirm::{Eip7702ConfirmationHandler, Eip7702ConfirmationJobData};

const MINIMAL_ACCOUNT_IMPLEMENTATION_ADDRESS: &str = "0xD6999651Fc0964B9c6B444307a0ab20534a66560";

// --- Job Payload ---
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Eip7702SendJobData {
    pub transaction_id: String,
    pub chain_id: u64,
    pub transactions: Vec<InnerTransaction>,
    pub eoa_address: Address,
    pub signing_credential: SigningCredential,
    pub webhook_options: Option<Vec<WebhookOptions>>,
    pub rpc_credentials: RpcCredentials,
    pub nonce: Option<U256>,
}

impl HasWebhookOptions for Eip7702SendJobData {
    fn webhook_options(&self) -> Option<Vec<WebhookOptions>> {
        self.webhook_options.clone()
    }
}

impl HasTransactionMetadata for Eip7702SendJobData {
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
pub struct Eip7702SendResult {
    pub eoa_address: Address,
    pub transaction_id: String,
    pub wrapped_calls: Value,
    pub signature: String,
    pub authorization: Option<Authorization>,
}

// --- Error Types ---
#[derive(Serialize, Deserialize, Debug, Clone, thiserror::Error)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "errorCode")]
pub enum Eip7702SendError {
    #[error("Chain service error for chainId {chain_id}: {message}")]
    ChainServiceError { chain_id: u64, message: String },

    #[error("Failed to sign typed data: {message}")]
    SigningError { message: String },

    #[error("Failed to sign authorization: {message}")]
    AuthorizationError { message: String },

    #[error("Failed to check 7702 delegation: {message}")]
    DelegationCheckError { message: String },

    #[error("Failed to call bundler: {message}")]
    BundlerCallError { message: String },

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
            message: format!("Deserialization error for job data: {}", error),
        }
    }
}

impl UserCancellable for Eip7702SendError {
    fn user_cancelled() -> Self {
        Eip7702SendError::UserCancelled
    }
}

// --- Wrapped Calls Structure ---
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Call {
    pub target: Address,
    pub value: U256,
    pub data: Bytes,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WrappedCalls {
    pub calls: Vec<Call>,
    pub uid: FixedBytes<32>,
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
        "prepare_and_send"
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
                message: format!("Failed to get chain instance: {}", e),
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

        // 2. Create wrapped calls with random UID
        let wrapped_calls = WrappedCalls {
            calls: job_data
                .transactions
                .iter()
                .map(|tx| Call {
                    target: tx.to.unwrap_or_default(),
                    value: tx.value.unwrap_or_default(),
                    data: tx.data.clone().unwrap_or_default(),
                })
                .collect(),
            uid: FixedBytes::random(),
        };

        // 3. Sign typed data for wrapped calls
        let typed_data = create_wrapped_calls_typed_data(
            job_data.chain_id,
            job_data.eoa_address,
            &wrapped_calls,
        );

        let signing_options = EoaSigningOptions {
            from: job_data.eoa_address,
            chain_id: Some(ChainId::from(job_data.chain_id)),
        };

        let signature = self
            .eoa_signer
            .sign_typed_data(signing_options.clone(), &typed_data, job_data.signing_credential.clone())
            .await
            .map_err(|e| Eip7702SendError::SigningError {
                message: e.to_string(),
            })
            .map_err_fail()?;

        // 4. Check if wallet has 7702 delegation set
        let is_minimal_account = check_is_7702_minimal_account(&chain, job_data.eoa_address)
            .await
            .map_err(|e| Eip7702SendError::DelegationCheckError {
                message: e.to_string(),
            })
            .map_err_fail()?;

        // 5. Sign authorization if needed
        let authorization = if !is_minimal_account {
            let nonce = job_data.nonce.unwrap_or_default();
            
            let auth = sign_authorization(
                &*self.eoa_signer,
                signing_options,
                job_data.signing_credential.clone(),
                job_data.chain_id,
                nonce,
            )
            .await
            .map_err(|e| Eip7702SendError::AuthorizationError {
                message: e.to_string(),
            })
            .map_err_fail()?;

            Some(auth)
        } else {
            None
        };

        // 6. Call bundler
        let transaction_id = call_bundler(
            &chain,
            job_data.eoa_address,
            &wrapped_calls,
            &signature,
            authorization.as_ref(),
        )
        .await
        .map_err(|e| Eip7702SendError::BundlerCallError {
            message: e.to_string(),
        })
        .map_err_fail()?;

        tracing::debug!(transaction_id = ?transaction_id, "EIP-7702 transaction sent to bundler");

        Ok(Eip7702SendResult {
            eoa_address: job_data.eoa_address,
            transaction_id,
            wrapped_calls: serde_json::to_value(&wrapped_calls).unwrap(),
            signature,
            authorization,
        })
    }

    async fn on_success(
        &self,
        job: &BorrowedJob<Eip7702SendJobData>,
        success_data: SuccessHookData<'_, Eip7702SendResult>,
        tx: &mut TransactionContext<'_>,
    ) {
        // Send confirmation job
        let confirmation_job = Eip7702ConfirmationJobData {
            transaction_id: job.job.data.transaction_id.clone(),
            chain_id: job.job.data.chain_id,
            bundler_transaction_id: success_data.result.transaction_id.clone(),
            eoa_address: success_data.result.eoa_address,
            rpc_credentials: job.job.data.rpc_credentials.clone(),
            webhook_options: job.job.data.webhook_options.clone(),
        };

        if let Err(e) = self.confirm_queue.enqueue(confirmation_job, tx).await {
            tracing::error!(
                transaction_id = job.job.data.transaction_id,
                error = ?e,
                "Failed to enqueue confirmation job"
            );
        }

        // Send webhook
        self.send_webhook_on_success(job, success_data, tx).await;
    }

    async fn on_nack(
        &self,
        job: &BorrowedJob<Eip7702SendJobData>,
        nack_data: NackHookData<'_, Eip7702SendError>,
        tx: &mut TransactionContext<'_>,
    ) {
        self.send_webhook_on_nack(job, nack_data, tx).await;
    }

    async fn on_fail(
        &self,
        job: &BorrowedJob<Eip7702SendJobData>,
        fail_data: FailHookData<'_, Eip7702SendError>,
        tx: &mut TransactionContext<'_>,
    ) {
        self.send_webhook_on_fail(job, fail_data, tx).await;
    }
}

// --- Helper Functions ---

fn create_wrapped_calls_typed_data(
    chain_id: u64,
    verifying_contract: Address,
    wrapped_calls: &WrappedCalls,
) -> TypedData {
    let domain = json!({
        "chainId": chain_id,
        "name": "MinimalAccount",
        "verifyingContract": verifying_contract,
        "version": "1"
    });

    let types = json!({
        "Call": [
            {"name": "target", "type": "address"},
            {"name": "value", "type": "uint256"},
            {"name": "data", "type": "bytes"}
        ],
        "WrappedCalls": [
            {"name": "calls", "type": "Call[]"},
            {"name": "uid", "type": "bytes32"}
        ]
    });

    let message = json!({
        "calls": wrapped_calls.calls,
        "uid": wrapped_calls.uid
    });

    TypedData {
        domain: Some(domain),
        types,
        primary_type: "WrappedCalls".to_string(),
        message,
    }
}

async fn check_is_7702_minimal_account(
    chain: &impl Chain,
    eoa_address: Address,
) -> Result<bool, EngineError> {
    // TODO: Implement actual 7702 delegation check
    // This should check if the EOA has a delegation set to the minimal account implementation
    // For now, returning false to always require authorization
    Ok(false)
}

async fn sign_authorization(
    signer: &EoaSigner,
    signing_options: EoaSigningOptions,
    credentials: SigningCredential,
    chain_id: u64,
    nonce: U256,
) -> Result<Authorization, EngineError> {
    // Create authorization typed data
    let domain = json!({
        "chainId": chain_id,
    });

    let types = json!({
        "Authorization": [
            {"name": "chainId", "type": "uint256"},
            {"name": "address", "type": "address"},
            {"name": "nonce", "type": "uint256"}
        ]
    });

    let message = json!({
        "chainId": chain_id,
        "address": MINIMAL_ACCOUNT_IMPLEMENTATION_ADDRESS,
        "nonce": nonce
    });

    let typed_data = TypedData {
        domain: Some(domain),
        types,
        primary_type: "Authorization".to_string(),
        message,
    };

    let signature = signer
        .sign_typed_data(signing_options, &typed_data, credentials)
        .await?;

    // Parse signature into r, s, v components
    let sig_bytes = Bytes::from_hex(&signature).map_err(|e| EngineError::ValidationError {
        message: format!("Invalid signature format: {}", e),
    })?;

    if sig_bytes.len() != 65 {
        return Err(EngineError::ValidationError {
            message: "Invalid signature length".to_string(),
        });
    }

    let r = FixedBytes::from_slice(&sig_bytes[0..32]);
    let s = FixedBytes::from_slice(&sig_bytes[32..64]);
    let v = sig_bytes[64];

    let authorization = Authorization {
        chain_id: ChainId::from(chain_id),
        address: MINIMAL_ACCOUNT_IMPLEMENTATION_ADDRESS.parse().unwrap(),
        nonce: nonce.into(),
        r,
        s,
        y_parity: v != 27,
    };

    Ok(authorization)
}

async fn call_bundler(
    chain: &impl Chain,
    eoa_address: Address,
    wrapped_calls: &WrappedCalls,
    signature: &str,
    authorization: Option<&Authorization>,
) -> Result<String, EngineError> {
    let bundler_client = chain.bundler_client();
    
    let params = json!([
        eoa_address,
        wrapped_calls,
        signature,
        authorization
    ]);

    let request = json!({
        "jsonrpc": "2.0",
        "method": "tw_execute",
        "params": params,
        "id": 1
    });

    let response: Value = bundler_client
        .inner
        .request("tw_execute", params)
        .await
        .map_err(|e| EngineError::RpcError {
            message: format!("Bundler call failed: {}", e),
            kind: crate::error::RpcErrorKind::Unknown,
        })?;

    let transaction_id = response
        .as_str()
        .ok_or_else(|| EngineError::RpcError {
            message: "Invalid response from bundler: expected transaction ID string".to_string(),
            kind: crate::error::RpcErrorKind::Unknown,
        })?;

    Ok(transaction_id.to_string())
}