use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
};
use base64::{Engine, engine::general_purpose::STANDARD as Base64Engine};
use bincode::config::standard as bincode_standard;
use engine_core::error::EngineError;
use engine_solana_core::transaction::SolanaTransaction;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::http::{
    error::ApiEngineError,
    extractors::{EngineJson, SigningCredentialsExtractor},
    server::EngineServerState,
    types::SuccessResponse,
};

// ===== REQUEST/RESPONSE TYPES =====

/// Request to sign a Solana transaction
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SignSolanaTransactionRequest {
    /// Transaction input (instructions or serialized transaction)
    #[serde(flatten)]
    pub input: engine_solana_core::transaction::SolanaTransactionInput,

    /// Solana execution options
    pub execution_options: engine_core::execution_options::solana::SolanaExecutionOptions,
}

/// Data returned from successful signing
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SignSolanaTransactionResponse {
    /// The signature (base58-encoded)
    pub signature: String,
    /// The signed serialized transaction (base64-encoded)
    pub signed_transaction: String,
}

// ===== ROUTE HANDLER =====

#[utoipa::path(
    post,
    operation_id = "signSolanaTransaction",
    path = "/solana/sign/transaction",
    tag = "Solana",
    request_body(content = SignSolanaTransactionRequest, description = "Sign Solana transaction request", content_type = "application/json"),
    responses(
        (status = 200, description = "Successfully signed Solana transaction", body = SuccessResponse<SignSolanaTransactionResponse>, content_type = "application/json"),
    ),
    params(
        ("x-vault-access-token" = Option<String>, Header, description = "Vault access token"),
    )
)]
/// Sign Solana Transaction
///
/// Sign a Solana transaction without broadcasting it
pub async fn sign_solana_transaction(
    State(state): State<EngineServerState>,
    SigningCredentialsExtractor(signing_credential): SigningCredentialsExtractor,
    EngineJson(request): EngineJson<SignSolanaTransactionRequest>,
) -> Result<impl IntoResponse, ApiEngineError> {
    let chain_id = request.execution_options.chain_id;
    let signer_address = request.execution_options.signer_address;

    tracing::info!(
        chain_id = %chain_id.as_str(),
        signer = %signer_address,
        "Processing Solana transaction signing request"
    );

    // Get RPC client from cache (same as executor)
    let rpc_client = state.solana_rpc_cache.get_or_create(chain_id).await;

    // Get recent blockhash
    let recent_blockhash = rpc_client
        .get_latest_blockhash()
        .await
        .map_err(|e| {
            ApiEngineError(EngineError::ValidationError {
                message: format!("Failed to get recent blockhash: {}", e),
            })
        })?;

    // Build the transaction
    let solana_tx = SolanaTransaction {
        input: request.input,
        compute_unit_limit: request.execution_options.compute_unit_limit,
        compute_unit_price: None, // Will be set if priority fee is configured
    };

    // Convert to versioned transaction
    let versioned_tx = solana_tx
        .to_versioned_transaction(signer_address, recent_blockhash)
        .map_err(|e| {
            ApiEngineError(EngineError::ValidationError {
                message: format!("Failed to build transaction: {}", e),
            })
        })?;

    // Sign the transaction
    let signed_tx = state
        .solana_signer
        .sign_transaction(versioned_tx, signer_address, &signing_credential)
        .await
        .map_err(ApiEngineError)?;

    // Get the signature (first signature in the transaction)
    let signature = signed_tx.signatures[0];

    // Serialize the signed transaction to base64
    let signed_tx_bytes = bincode::serde::encode_to_vec(&signed_tx, bincode_standard()).map_err(
        |e| {
            ApiEngineError(EngineError::ValidationError {
                message: format!("Failed to serialize signed transaction: {}", e),
            })
        },
    )?;
    let signed_tx_base64 = Base64Engine.encode(&signed_tx_bytes);

    let response = SignSolanaTransactionResponse {
        signature: signature.to_string(),
        signed_transaction: signed_tx_base64,
    };

    tracing::info!(
        chain_id = %chain_id.as_str(),
        signature = %signature,
        "Solana transaction signed successfully"
    );

    Ok((StatusCode::OK, Json(SuccessResponse::new(response))))
}
