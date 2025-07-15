use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
};
use engine_aa_core::signer::SmartAccountSignerBuilder;
use engine_core::{
    chain::ChainService,
    credentials::SigningCredential,
    error::EngineError,
    signer::{AccountSigner, SigningOptions},
};
use futures::future::join_all;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use vault_types::enclave::encrypted::eoa::MessageFormat;

use crate::http::{
    error::ApiEngineError,
    extractors::{EngineJson, SigningCredentialsExtractor},
    server::EngineServerState,
    types::{BatchResultItem, BatchResults},
};

// ===== REQUEST/RESPONSE TYPES =====
/// Individual message to sign
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MessageInput {
    /// The message to sign
    pub message: String,
    /// Message format (text or hex)
    #[serde(default = "default_message_format")]
    #[schema(value_type = MessageFormatDef)]
    pub format: MessageFormat,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum MessageFormatDef {
    Text,
    Hex,
}

/// Request to sign messages
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SignMessageRequest {
    /// Configuration options for signing
    pub signing_options: SigningOptions,
    /// List of messages to sign
    pub params: Vec<MessageInput>,
}

/// Data returned from successful signing
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SignResultData {
    /// The resulting signature
    pub signature: String,
    /// The data that was signed (original message)
    pub signed_data: String,
}

// ===== ROUTE HANDLER =====

#[utoipa::path(
    post,
    operation_id = "signMessage",
    path = "/sign/message",
    tag = "Signature",
    request_body(content = SignMessageRequest, description = "Sign message request", content_type = "application/json"),
    responses(
        (status = 200, description = "Successfully signed messages", body = BatchResults<SignResultData>, content_type = "application/json"),
    ),
    params(
        ("x-thirdweb-client-id" = Option<String>, Header, description = "Thirdweb client ID, passed along with the service key"),
        ("x-thirdweb-service-key" = Option<String>, Header, description = "Thirdweb service key, passed when using the client ID"),
        ("x-thirdweb-secret-key" = Option<String>, Header, description = "Thirdweb secret key, passed standalone"),
        ("x-vault-access-token" = Option<String>, Header, description = "Vault access token"),
    )
)]
/// Sign Message
///
/// Sign messages using either EOA or Smart Account
pub async fn sign_message(
    State(state): State<EngineServerState>,
    SigningCredentialsExtractor(signing_credential): SigningCredentialsExtractor,
    EngineJson(request): EngineJson<SignMessageRequest>,
) -> Result<impl IntoResponse, ApiEngineError> {
    // Process all messages in parallel
    let sign_futures = request.params.iter().map(|message_input| {
        sign_single_message(
            &state,
            &signing_credential,
            &request.signing_options,
            message_input,
        )
    });

    let results: Vec<BatchResultItem<SignResultData>> = join_all(sign_futures).await;

    Ok((StatusCode::OK, Json(BatchResults { result: results })))
}

// ===== HELPER FUNCTIONS =====

async fn sign_single_message(
    state: &EngineServerState,
    signing_credential: &SigningCredential,
    signing_options: &SigningOptions,
    message_input: &MessageInput,
) -> BatchResultItem<SignResultData> {
    let result = match signing_options {
        SigningOptions::Eoa(eoa_options) => {
            // Direct EOA signing
            state
                .eoa_signer
                .sign_message(
                    eoa_options.clone(),
                    &message_input.message,
                    message_input.format,
                    signing_credential,
                )
                .await
        }
        SigningOptions::ERC4337(smart_account_options) => {
            // Smart account signing via builder
            match state.chains.get_chain(smart_account_options.chain_id) {
                Ok(chain) => {
                    match SmartAccountSignerBuilder::new(
                        state.eoa_signer.clone(),
                        signing_credential.clone(),
                        smart_account_options.clone(),
                        chain,
                    )
                    .build()
                    .await
                    {
                        Ok(smart_signer) => {
                            smart_signer
                                .sign_message(&message_input.message, message_input.format)
                                .await
                        }
                        Err(e) => Err(e),
                    }
                }
                Err(e) => Err(EngineError::ValidationError {
                    message: format!(
                        "Failed to get chain {}: {}",
                        smart_account_options.chain_id, e
                    ),
                }),
            }
        }
    };

    match result {
        Ok(signature) => BatchResultItem::success(SignResultData {
            signature,
            signed_data: message_input.message.clone(),
        }),
        Err(e) => BatchResultItem::failure(e),
    }
}

fn default_message_format() -> MessageFormat {
    MessageFormat::Text
}
