use alloy::primitives::{ChainId, U256};
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
};
use engine_core::{
    defs::U256Def,
    error::EngineError,
    execution_options::{
        ExecutionOptions, QueuedTransactionsResponse, SendTransactionRequest, WebhookOptions,
    },
    transaction::InnerTransaction,
};
use futures::future::join_all;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thirdweb_core::auth::ThirdwebAuth;

use crate::http::{
    dyn_contract::{ContractCall, ContractOperationResult, PreparedContractCall},
    error::ApiEngineError,
    extractors::{RpcCredentialsExtractor, SigningCredentialsExtractor},
    server::EngineServerState,
    types::SuccessResponse,
};

// ===== REQUEST/RESPONSE TYPES =====

/// A contract function call with optional ETH value to send
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ContractWrite {
    /// The contract function call details
    #[serde(flatten)]
    pub contract_call: ContractCall,
    /// Amount of ETH to send with the transaction (in wei)
    ///
    /// If omitted, no ETH will be sent (value = 0).
    /// Uses U256 to handle large numbers precisely.
    #[schemars(with = "Option<String>")]
    #[schema(value_type = Option<U256Def>)]
    pub value: Option<U256>,
}

/// Request to execute write transactions to smart contracts
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WriteContractRequest {
    /// Execution configuration including chain, account, and transaction options
    pub execution_options: ExecutionOptions,

    /// List of contract function calls to execute
    ///
    /// All calls will be executed in a single transaction if possible,
    /// or as separate transactions if atomic batching is not supported
    pub params: Vec<ContractWrite>,

    pub webhook_options: Option<Vec<WebhookOptions>>,
}

// ===== CONVENIENCE METHODS =====

impl ContractWrite {
    /// Get the value to send with the transaction, defaulting to 0
    pub fn get_value(&self) -> U256 {
        self.value.unwrap_or(U256::ZERO)
    }

    /// Convert to InnerTransaction using the prepared call data
    pub fn to_inner_transaction(&self, prepared: &PreparedContractCall) -> InnerTransaction {
        self.contract_call
            .to_inner_transaction(prepared, self.get_value())
    }
}

// ===== ROUTE HANDLER =====
#[utoipa::path(
    post,
    operation_id = "writeContract",
    path = "/write/contract",
    tag = "Write",
    request_body(content = WriteContractRequest, description = "Write contract request", content_type = "application/json"),
    responses(
        (status = 202, description = "Transaction(s) queued successfully", body = SuccessResponse<QueuedTransactionsResponse>, content_type = "application/json",
            example = json!({"transactions": [{"id": "1", "batchIndex": 0, "executionParams": {"chainId": 1, "idempotencyKey": "123", "executorType": "ERC4337"}, "transactionParams": [{"to": "0x123", "data": "0x123", "value": "0x123"}]}]})
        ),
    ),
    params(
        ("x-thirdweb-client-id" = Option<String>, Header, description = "Thirdweb client ID, passed along with the service key"),
        ("x-thirdweb-service-key" = Option<String>, Header, description = "Thirdweb service key, passed when using the client ID"),
        ("x-thirdweb-secret-key" = Option<String>, Header, description = "Thirdweb secret key, passed standalone"),

        ("x-vault-access-token" = Option<String>, Header, description = "Vault access token"),
    )
)]
/// Write Contract
///
/// Call a contract function with a transaction
pub async fn write_contract(
    State(state): State<EngineServerState>,
    RpcCredentialsExtractor(rpc_credentials): RpcCredentialsExtractor,
    SigningCredentialsExtractor(signing_credential): SigningCredentialsExtractor,
    Json(request): Json<WriteContractRequest>,
) -> Result<impl IntoResponse, ApiEngineError> {
    let auth: Option<ThirdwebAuth> = match &rpc_credentials {
        engine_core::chain::RpcCredentials::Thirdweb(auth) => Some(auth.clone()),
    };

    let chain_id: ChainId = request.execution_options.chain_id();
    let transaction_id = request.execution_options.transaction_id().to_string();
    let executor_type = request.execution_options.executor_type();

    tracing::info!(
        transaction_id = %transaction_id,
        executor_type = ?executor_type,
        chain_id = %chain_id,
        "Processing contract write request"
    );

    // Prepare all contract calls in parallel
    let prepare_futures = request.params.iter().map(|contract_write| {
        contract_write
            .contract_call
            .prepare_call_with_error_tracking(&state.abi_service, chain_id, auth.clone())
    });

    let preparation_results: Vec<ContractOperationResult<PreparedContractCall>> =
        join_all(prepare_futures).await;

    // Convert successful preparations to InnerTransactions
    let mut inner_transactions = Vec::new();
    let mut preparation_errors = Vec::new();

    for (index, (contract_write, prep_result)) in request
        .params
        .iter()
        .zip(preparation_results.iter())
        .enumerate()
    {
        match prep_result {
            ContractOperationResult::Success(prepared_call) => {
                let inner_tx = contract_write.to_inner_transaction(prepared_call);
                inner_transactions.push(inner_tx);
            }
            ContractOperationResult::Failure(error) => {
                preparation_errors.push((index, error.to_string()));
            }
        }
    }

    // If any preparations failed, return error with details
    if !preparation_errors.is_empty() {
        let error_details: Vec<String> = preparation_errors
            .iter()
            .map(|(index, error)| format!("Parameter {}: {}", index, error))
            .collect();

        return Err(ApiEngineError(EngineError::ValidationError {
            message: format!(
                "Failed to prepare contract calls: {}",
                error_details.join("; ")
            ),
        }));
    }

    if inner_transactions.is_empty() {
        return Err(ApiEngineError(EngineError::ValidationError {
            message: "No valid contract calls to execute".to_string(),
        }));
    }

    // Create the transaction request using engine_core types
    let transaction_request = SendTransactionRequest {
        execution_options: request.execution_options,
        params: inner_transactions,
        webhook_options: request.webhook_options, // Could be added to WriteContractRequest if needed
    };

    // Execute the transaction
    let queued_transactions = state
        .execution_router
        .execute(transaction_request, rpc_credentials, signing_credential)
        .await
        .map_err(ApiEngineError)?;

    tracing::info!(
        transaction_id = %transaction_id,
        executor_type = ?executor_type,
        "Contract write transaction queued successfully"
    );

    Ok((
        StatusCode::ACCEPTED,
        Json(SuccessResponse::new(QueuedTransactionsResponse {
            transactions: queued_transactions,
        })),
    ))
}
