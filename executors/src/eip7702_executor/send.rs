use alloy::{
    dyn_abi::TypedData,
    eips::eip7702::Authorization,
    primitives::{Address, Bytes, ChainId, FixedBytes, U256},
    providers::Provider,
    sol_types::eip712_domain,
};
use engine_core::{
    chain::{Chain, ChainService, RpcCredentials},
    credentials::SigningCredential,
    error::{EngineError, RpcErrorKind},
    execution_options::WebhookOptions,
    signer::{AccountSigner, EoaSigner, EoaSigningOptions},
    transaction::InnerTransaction,
};
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use twmq::{
    FailHookData, NackHookData, Queue, SuccessHookData, UserCancellable,
    error::TwmqError,
    hooks::TransactionContext,
    job::{BorrowedJob, JobResult, ToJobResult},
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
    fn transaction_id(&self) -> String {
        self.transaction_id.clone()
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
                    value: tx.value,
                    data: tx.data.clone(),
                })
                .collect(),
            uid: {
                let mut rng = rand::rng();
                let mut bytes = [0u8; 32];
                rng.fill(&mut bytes);
                FixedBytes::from(bytes)
            },
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
            .sign_typed_data(
                signing_options.clone(),
                &typed_data,
                job_data.signing_credential.clone(),
            )
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
            let minimal_account_address: Address = MINIMAL_ACCOUNT_IMPLEMENTATION_ADDRESS
                .parse()
                .map_err(|e| Eip7702SendError::AuthorizationError {
                    message: format!("Invalid minimal account implementation address: {}", e),
                })
                .map_err_fail()?;

            let auth = self
                .eoa_signer
                .sign_authorization(
                    signing_options.clone(),
                    job_data.chain_id,
                    minimal_account_address,
                    nonce,
                    job_data.signing_credential.clone(),
                )
                .await
                .map_err(|e| Eip7702SendError::AuthorizationError {
                    message: e.to_string(),
                })
                .map_err_fail()?;

            Some(auth.clone())
        } else {
            None
        };

        // 6. Call bundler
        let transaction_id = chain
            .bundler_client()
            .tw_execute(
                job_data.eoa_address,
                &serde_json::to_value(&wrapped_calls)
                    .map_err(|e| Eip7702SendError::InternalError {
                        message: format!("Failed to serialize wrapped calls: {}", e),
                    })
                    .map_err_fail()?,
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
            wrapped_calls: serde_json::to_value(&wrapped_calls)
                .map_err(|e| Eip7702SendError::InternalError {
                    message: format!("Failed to serialize wrapped calls: {}", e),
                })
                .map_err_fail()?,
            signature,
            authorization: authorization.map(|f| f.inner().clone()),
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
                eoa_address: success_data.result.eoa_address,
                rpc_credentials: job.job.data.rpc_credentials.clone(),
                webhook_options: job.job.data.webhook_options.clone(),
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

// --- Helper Functions ---

fn create_wrapped_calls_typed_data(
    chain_id: u64,
    verifying_contract: Address,
    wrapped_calls: &WrappedCalls,
) -> TypedData {
    let domain = eip712_domain! {
        name: "MinimalAccount",
        version: "1",
        chain_id: chain_id,
        verifying_contract: verifying_contract,
    };

    let types_json = json!({
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

    // Parse the JSON into Eip712Types and create resolver
    let eip712_types: alloy::dyn_abi::eip712::Eip712Types =
        serde_json::from_value(types_json).expect("Failed to parse EIP712 types");

    TypedData {
        domain,
        resolver: eip712_types.into(),
        primary_type: "WrappedCalls".to_string(),
        message,
    }
}

async fn check_is_7702_minimal_account(
    chain: &impl Chain,
    eoa_address: Address,
) -> Result<bool, EngineError> {
    // Get the bytecode at the EOA address using eth_getCode
    let code = chain
        .provider()
        .get_code_at(eoa_address)
        .await
        .map_err(|e| EngineError::RpcError {
            chain_id: chain.chain_id(),
            rpc_url: chain.rpc_url().to_string(),
            message: format!("Failed to get code at address {}: {}", eoa_address, e),
            kind: RpcErrorKind::InternalError {
                message: e.to_string(),
            },
        })?;

    tracing::debug!(
        eoa_address = ?eoa_address,
        code_length = code.len(),
        code_hex = ?alloy::hex::encode(&code),
        "Checking EIP-7702 delegation"
    );

    // Check if code exists and starts with EIP-7702 delegation prefix "0xef0100"
    if code.len() < 23 || !code.starts_with(&[0xef, 0x01, 0x00]) {
        tracing::debug!(
            eoa_address = ?eoa_address,
            has_delegation = false,
            reason = "Code too short or doesn't start with EIP-7702 prefix",
            "EIP-7702 delegation check result"
        );
        return Ok(false);
    }

    // Extract the target address from bytes 3-23 (20 bytes for address)
    // EIP-7702 format: 0xef0100 + 20 bytes address
    // JS equivalent: code.slice(8, 48) extracts 40 hex chars = 20 bytes
    // In hex string: "0xef0100" + address, so address starts at position 8
    // In byte array: [0xef, 0x01, 0x00, address_bytes...]
    // The address starts at byte 3 and is 20 bytes long (bytes 3-22)
    let target_bytes = &code[3..23];
    let target_address = Address::from_slice(target_bytes);

    // Compare with the minimal account implementation address
    let minimal_account_address: Address =
        MINIMAL_ACCOUNT_IMPLEMENTATION_ADDRESS
            .parse()
            .map_err(|e| EngineError::ValidationError {
                message: format!("Invalid minimal account implementation address: {}", e),
            })?;

    let is_delegated = target_address == minimal_account_address;

    tracing::debug!(
        eoa_address = ?eoa_address,
        target_address = ?target_address,
        minimal_account_address = ?minimal_account_address,
        has_delegation = is_delegated,
        "EIP-7702 delegation check result"
    );

    Ok(is_delegated)
}
