use axum::{
    debug_handler,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
};
use engine_core::execution_options::solana::{
    QueuedSolanaTransactionResponse, SendSolanaTransactionRequest, SolanaExecutionOptions,
};
use engine_solana_core::ProgramCall;
use futures::future::join_all;
use serde::{Deserialize, Serialize};

use crate::http::{
    error::ApiEngineError,
    extractors::{EngineJson, SigningCredentialsExtractor},
    server::EngineServerState,
    types::SuccessResponse,
};

/// Request to execute Solana program instructions
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(title = "SendSolanaProgramRequest")]
pub struct SendSolanaProgramRequest {
    /// Idempotency key for this transaction (defaults to random UUID)
    #[serde(default)]
    pub idempotency_key: String,

    /// Solana execution options
    #[schema(value_type = Object)]
    pub execution_options: SolanaExecutionOptions,

    /// List of program calls to execute (will be batched in a single transaction)
    pub instructions: Vec<ProgramCall>,

    /// Webhook options for transaction status notifications
    #[serde(default)]
    pub webhook_options: Vec<engine_core::execution_options::WebhookOptions>,
}

#[utoipa::path(
    post,
    operation_id = "sendSolanaProgram",
    path = "/solana/program",
    tag = "Solana",
    request_body(content = SendSolanaProgramRequest, description = "Solana program interaction request", content_type = "application/json"),
    responses(
        (status = 202, description = "Solana program transaction queued successfully", body = SuccessResponse<QueuedSolanaTransactionResponse>, content_type = "application/json"),
    ),
    params(
        ("x-vault-access-token" = Option<String>, Header, description = "Vault access token"),
    )
)]
/// Send Solana Program Instruction
///
/// Execute high-level Solana program instructions with automatic account resolution
/// and instruction encoding from IDL.
#[debug_handler]
pub async fn send_solana_program(
    State(state): State<EngineServerState>,
    SigningCredentialsExtractor(signing_credential): SigningCredentialsExtractor,
    EngineJson(request): EngineJson<SendSolanaProgramRequest>,
) -> Result<impl IntoResponse, ApiEngineError> {
    let transaction_id = request.idempotency_key.clone();
    let chain_id = request.execution_options.chain_id;
    let signer_address = request.execution_options.signer_address;

    tracing::info!(
        transaction_id = %transaction_id,
        chain_id = %chain_id.as_str(),
        signer = %signer_address,
        num_instructions = request.instructions.len(),
        "Processing Solana program request"
    );

    // Prepare all program calls in parallel (like contract_write does)
    let prepare_futures = request.instructions.iter().map(|program_call| {
        let idl_cache = state.idl_cache.clone();
        let signer_pubkey = signer_address; // Already a Pubkey
        async move {
            program_call
                .prepare(signer_pubkey, &idl_cache)
                .await
                .map_err(|e| (program_call.instruction.clone(), e))
        }
    });

    let preparation_results = join_all(prepare_futures).await;

    // Collect successful preparations and errors
    let mut prepared_instructions = Vec::new();
    let mut preparation_errors = Vec::new();

    for (index, prep_result) in preparation_results.into_iter().enumerate() {
        match prep_result {
            Ok(prepared) => {
                tracing::debug!(
                    instruction = %prepared.instruction_name,
                    program = %prepared.program_id,
                    num_accounts = prepared.resolved_accounts.len(),
                    "Program call prepared successfully"
                );
                prepared_instructions.push(prepared.instruction);
            }
            Err((instruction_name, error)) => {
                tracing::warn!(
                    instruction = %instruction_name,
                    error = %error,
                    "Failed to prepare program call"
                );
                preparation_errors.push((index, instruction_name, error.to_string()));
            }
        }
    }

    // If any preparations failed, return error with details
    if !preparation_errors.is_empty() {
        let error_details: Vec<String> = preparation_errors
            .iter()
            .map(|(index, name, error)| {
                format!("Instruction {} ({}): {}", index, name, error)
            })
            .collect();

        return Err(ApiEngineError(engine_core::error::EngineError::ValidationError {
            message: format!(
                "Failed to prepare {} program call(s): {}",
                preparation_errors.len(),
                error_details.join("; ")
            ),
        }));
    }

    if prepared_instructions.is_empty() {
        return Err(ApiEngineError(engine_core::error::EngineError::ValidationError {
            message: "No valid program calls to execute".to_string(),
        }));
    }

    // Create the transaction request using existing Solana pipeline
    let transaction_request = SendSolanaTransactionRequest {
        idempotency_key: request.idempotency_key,
        instructions: prepared_instructions,
        execution_options: request.execution_options,
        webhook_options: request.webhook_options,
    };

    // Execute via the existing Solana executor
    let response = state
        .execution_router
        .execute_solana(transaction_request, signing_credential)
        .await
        .map_err(ApiEngineError)?;

    tracing::info!(
        transaction_id = %transaction_id,
        chain_id = %chain_id.as_str(),
        "Solana program transaction queued successfully"
    );

    Ok((
        StatusCode::ACCEPTED,
        Json(SuccessResponse::new(response)),
    ))
}

