// 8:12 PM - COLOCATION: Contract Write Operations

use aide::{axum::IntoApiResponse, transform::TransformOperation};
use alloy::primitives::{ChainId, U256};
use axum::{extract::State, http::StatusCode, response::Json};
use engine_core::{
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
    types::{ErrorResponse, SuccessResponse},
};

// ===== REQUEST/RESPONSE TYPES =====

/// A contract function call with optional ETH value to send
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
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
    pub value: Option<U256>,
}

/// Request to execute write transactions to smart contracts
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
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

/// Execute write transactions to smart contracts
pub async fn write_contract(
    State(state): State<EngineServerState>,
    RpcCredentialsExtractor(rpc_credentials): RpcCredentialsExtractor,
    SigningCredentialsExtractor(signing_credential): SigningCredentialsExtractor,
    Json(request): Json<WriteContractRequest>,
) -> Result<impl IntoApiResponse, ApiEngineError> {
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
        .map_err(|e| ApiEngineError(e))?;

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

// ===== DOCUMENTATION =====

pub fn write_contract_docs(op: TransformOperation) -> TransformOperation {
    op.id("writeContract")
        .description(
            "Execute write transactions to smart contracts.\n\n\
            This endpoint executes state-changing contract function calls as blockchain \
            transactions. The transactions are queued for execution using the specified \
            execution options (e.g., ERC-4337 account abstraction).\n\n\
            ## Features\n\
            - Execute multiple contract calls in one request\n\
            - Support for sending ETH value with calls\n\
            - Automatic ABI resolution and parameter encoding\n\
            - Integration with account abstraction and signing systems\n\
            - Transaction queuing with status tracking\n\n\
            ## Authentication\n\
            - Required: RPC credentials for transaction submission\n\
            - Required: Vault access token for transaction signing\n\n\
            ## Execution Options\n\
            - Chain ID and transaction configuration\n\
            - Account abstraction settings (ERC-4337)\n\
            - Gas and fee configurations\n\
            - Idempotency key for duplicate prevention",
        )
        .summary("Execute contract write functions")
        .response_with::<202, Json<SuccessResponse<QueuedTransactionsResponse>>, _>(|res| {
            res.description("Transaction queued successfully")
        })
        .response_with::<400, Json<ErrorResponse>, _>(|res| {
            res.description("Bad request - invalid parameters or missing credentials")
        })
        .response_with::<500, Json<ErrorResponse>, _>(|res| {
            res.description("Internal server error")
        })
        .tag("Contract Operations")
}
