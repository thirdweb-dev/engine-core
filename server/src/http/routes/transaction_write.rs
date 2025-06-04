// 8:12 PM - COLOCATION: Transaction Write Operations

use aide::{axum::IntoApiResponse, transform::TransformOperation};
use axum::{debug_handler, extract::State, http::StatusCode, response::Json};
use engine_core::execution_options::{QueuedTransactionsResponse, SendTransactionRequest};

use crate::http::{
    error::ApiEngineError,
    extractors::{EngineJson, RpcCredentialsExtractor, SigningCredentialsExtractor},
    server::EngineServerState,
    types::{ErrorResponse, SuccessResponse},
};

// ===== ROUTE HANDLER =====

/// Execute raw transactions
#[debug_handler]
pub async fn write_transaction(
    State(state): State<EngineServerState>,
    RpcCredentialsExtractor(rpc_credentials): RpcCredentialsExtractor,
    SigningCredentialsExtractor(signing_credential): SigningCredentialsExtractor,
    EngineJson(request): EngineJson<SendTransactionRequest>,
) -> Result<impl IntoApiResponse, ApiEngineError> {
    let transaction_id = request.execution_options.transaction_id().to_string();
    let executor_type = request.execution_options.executor_type();

    tracing::info!(
        transaction_id = %transaction_id,
        executor_type = ?executor_type,
        chain_id = %request.execution_options.chain_id(),
        "Processing transaction request"
    );

    let queued_transactions = state
        .execution_router
        .execute(request, rpc_credentials, signing_credential)
        .await
        .map_err(|e| ApiEngineError(e))?;

    tracing::info!(
        transaction_id = %transaction_id,
        executor_type = ?executor_type,
        "Transaction queued successfully"
    );

    Ok((
        StatusCode::ACCEPTED,
        Json(SuccessResponse::new(QueuedTransactionsResponse {
            transactions: queued_transactions,
        })),
    ))
}

// ===== DOCUMENTATION =====

pub fn write_transaction_docs(op: TransformOperation) -> TransformOperation {
    op.id("writeTransaction")
        .description(
            "Execute raw transactions.\n\n\
            This endpoint executes pre-prepared transactions without additional processing. \
            Use this for raw transaction data that has already been encoded.\n\n\
            ## Features\n\
            - Execute multiple raw transactions\n\
            - Support for any transaction type\n\
            - Integration with account abstraction\n\
            - Transaction queuing and status tracking\n\n\
            ## Authentication\n\
            - Required: RPC credentials and vault access token\n\n\
            ## Request Format\n\
            Uses the standard `SendTransactionRequest` from engine_core which includes:\n\
            - Execution options (chain, account abstraction, etc.)\n\
            - Array of `InnerTransaction` objects with `to`, `data`, and `value`\n\
            - Optional webhook configuration",
        )
        .summary("Execute prepared transactions")
        .response_with::<202, Json<SuccessResponse<QueuedTransactionsResponse>>, _>(|res| {
            res.description("Transaction queued successfully")
        })
        .response_with::<400, Json<ErrorResponse>, _>(|res| {
            res.description("Bad request - invalid parameters or missing credentials")
        })
        .response_with::<500, Json<ErrorResponse>, _>(|res| {
            res.description("Internal server error")
        })
        .tag("Transaction Operations")
}
