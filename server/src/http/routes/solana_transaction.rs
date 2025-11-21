use axum::{
    debug_handler,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
};
use engine_core::execution_options::solana::{
    QueuedSolanaTransactionResponse, SendSolanaTransactionRequest,
};

use crate::http::{
    error::ApiEngineError,
    extractors::{EngineJson, SigningCredentialsExtractor},
    server::EngineServerState,
    types::SuccessResponse,
};

#[utoipa::path(
    post,
    operation_id = "sendSolanaTransaction",
    path = "/solana/transaction",
    tag = "Solana",
    request_body(content = SendSolanaTransactionRequest, description = "Solana transaction request", content_type = "application/json"),
    responses(
        (status = 202, description = "Solana transaction queued successfully", body = SuccessResponse<QueuedSolanaTransactionResponse>, content_type = "application/json"),
    ),
    params(
        ("x-vault-access-token" = Option<String>, Header, description = "Vault access token"),
    )
)]
/// Send Solana Transaction
///
/// Execute a Solana transaction with custom instructions
#[debug_handler]
pub async fn send_solana_transaction(
    State(state): State<EngineServerState>,
    SigningCredentialsExtractor(signing_credential): SigningCredentialsExtractor,
    EngineJson(request): EngineJson<SendSolanaTransactionRequest>,
) -> Result<impl IntoResponse, ApiEngineError> {
    let transaction_id = request.idempotency_key.clone();
    let chain_id = request.execution_options.chain_id;
    let signer_address = request.execution_options.signer_address;

    tracing::info!(
        transaction_id = %transaction_id,
        chain_id = %chain_id.as_str(),
        signer = %signer_address,
        "Processing Solana transaction request"
    );

    let response = state
        .execution_router
        .execute_solana(request, signing_credential)
        .await
        .map_err(ApiEngineError)?;

    tracing::info!(
        transaction_id = %transaction_id,
        chain_id = %chain_id.as_str(),
        "Solana transaction queued successfully"
    );

    Ok((StatusCode::ACCEPTED, Json(SuccessResponse::new(response))))
}
