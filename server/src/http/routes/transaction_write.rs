// 8:12 PM - COLOCATION: Transaction Write Operations

use axum::{
    debug_handler,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
};
use engine_core::execution_options::{QueuedTransactionsResponse, SendTransactionRequest};

use crate::http::{
    error::ApiEngineError,
    extractors::{EngineJson, RpcCredentialsExtractor, SigningCredentialsExtractor},
    server::EngineServerState,
    types::{ErrorResponse, SuccessResponse},
};

// ===== ROUTE HANDLER =====

#[utoipa::path(
    post,
    operation_id = "writeTransaction",
    path = "/write/transaction",
    tag = "Write",
    request_body(content = SendTransactionRequest, description = "Transaction request", content_type = "application/json"),
    responses(
        (status = 202, description = "Transaction queued successfully", body = SuccessResponse<QueuedTransactionsResponse>, content_type = "application/json"),
    ),
    params(
        ("x-thirdweb-client-id" = Option<String>, Header, description = "Thirdweb client ID, passed along with the service key"),
        ("x-thirdweb-service-key" = Option<String>, Header, description = "Thirdweb service key, passed when using the client ID"),
        ("x-thirdweb-secret-key" = Option<String>, Header, description = "Thirdweb secret key, passed standalone"),
        ("x-vault-access-token" = Option<String>, Header, description = "Vault access token"),
    )
)]
/// Write Transaction
///
/// Execute raw transactions
#[debug_handler]
pub async fn write_transaction(
    State(state): State<EngineServerState>,
    RpcCredentialsExtractor(rpc_credentials): RpcCredentialsExtractor,
    SigningCredentialsExtractor(signing_credential): SigningCredentialsExtractor,
    EngineJson(request): EngineJson<SendTransactionRequest>,
) -> Result<impl IntoResponse, ApiEngineError> {
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
