// Sign Typed Data Operations

use alloy::{dyn_abi::TypedData, primitives::Address};
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
};
use engine_core::{
    error::EngineError, 
    signer::{EoaSigner, SigningOptions, SmartAccountSigningOptions},
    credentials::SigningCredential,
};
use engine_aa_core::signer::{SmartAccountSigner, SmartAccountSignerBuilder};
use futures::future::join_all;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thirdweb_core::auth::ThirdwebAuth;

use crate::http::{
    error::ApiEngineError,
    extractors::{EngineJson, SigningCredentialsExtractor},
    server::EngineServerState,
    types::ErrorResponse,
};

// ===== REQUEST/RESPONSE TYPES =====

/// Options for signing typed data
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SignOptions {
    /// Configuration options for signing
    #[serde(flatten)]
    pub signing_options: SigningOptions,
}

/// Request to sign typed data
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SignTypedDataRequest {
    /// Configuration options for signing
    pub sign_options: SignOptions,
    /// List of typed data to sign
    pub params: Vec<TypedData>,
}

/// Result of a single typed data signing operation
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
#[serde(untagged)]
pub enum SignResultItem {
    Success(SignResultSuccessItem),
    Failure(SignResultFailureItem),
}

/// Successful result from a typed data signing operation
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SignResultSuccessItem {
    /// Always true for successful operations
    #[schemars(with = "bool")]
    #[schema(value_type = bool)]
    pub success: serde_bool::True,
    /// The signing result data
    pub result: SignResultData,
}

/// Data returned from successful signing
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SignResultData {
    /// The resulting signature
    pub signature: String,
    /// The data that was signed (stringified typed data)
    pub signed_data: String,
}

/// Failed result from a typed data signing operation
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
pub struct SignResultFailureItem {
    /// Always false for failed operations
    #[schemars(with = "bool")]
    #[schema(value_type = bool)]
    pub success: serde_bool::False,
    /// Detailed error information describing what went wrong
    pub error: EngineError,
}

/// Collection of results from multiple typed data signing operations
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
pub struct SignResults {
    /// Array of results, one for each input typed data
    pub results: Vec<SignResultItem>,
}

/// Response from the sign typed data endpoint
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
pub struct SignTypedDataResponse {
    /// Container for all typed data signing results
    pub result: SignResults,
}

// ===== CONVENIENCE CONSTRUCTORS =====

impl SignResultSuccessItem {
    /// Create a new successful sign result
    pub fn new(signature: String, signed_data: String) -> Self {
        Self {
            success: serde_bool::True,
            result: SignResultData {
                signature,
                signed_data,
            },
        }
    }
}

impl SignResultFailureItem {
    /// Create a new failed sign result
    pub fn new(error: EngineError) -> Self {
        Self {
            success: serde_bool::False,
            error,
        }
    }
}

impl SignResultItem {
    /// Create a successful sign result item
    pub fn success(signature: String, signed_data: String) -> Self {
        SignResultItem::Success(SignResultSuccessItem::new(signature, signed_data))
    }

    /// Create a failed sign result item
    pub fn failure(error: EngineError) -> Self {
        SignResultItem::Failure(SignResultFailureItem::new(error))
    }
}

// ===== ROUTE HANDLER =====

#[utoipa::path(
    post,
    operation_id = "signTypedData",
    path = "/sign/typed-data",
    tag = "Signature",
    request_body(content = SignTypedDataRequest, description = "Sign typed data request", content_type = "application/json"),
    responses(
        (status = 200, description = "Successfully signed typed data", body = SignTypedDataResponse, content_type = "application/json"),
    ),
    params(
        ("x-thirdweb-client-id" = Option<String>, Header, description = "Thirdweb client ID, passed along with the service key"),
        ("x-thirdweb-service-key" = Option<String>, Header, description = "Thirdweb service key, passed when using the client ID"),
        ("x-thirdweb-secret-key" = Option<String>, Header, description = "Thirdweb secret key, passed standalone"),
        ("x-vault-access-token" = Option<String>, Header, description = "Vault access token"),
    )
)]
/// Sign Typed Data
///
/// Sign EIP-712 typed data using either EOA or Smart Account
pub async fn sign_typed_data(
    State(state): State<EngineServerState>,
    SigningCredentialsExtractor(signing_credential): SigningCredentialsExtractor,
    EngineJson(request): EngineJson<SignTypedDataRequest>,
) -> Result<impl IntoResponse, ApiEngineError> {
    // Process all typed data in parallel
    let sign_futures = request.params.iter().map(|typed_data| {
        sign_single_typed_data(&state.userop_signer, &signing_credential, &request.sign_options.signing_options, typed_data)
    });

    let results: Vec<SignResultItem> = join_all(sign_futures).await;

    Ok((
        StatusCode::OK,
        Json(SignTypedDataResponse {
            result: SignResults { results },
        }),
    ))
}

// ===== HELPER FUNCTIONS =====

async fn sign_single_typed_data(
    signer: &Signer,
    signing_credential: &SigningCredential,
    signing_options: &SigningOptions,
    typed_data: &TypedData,
) -> SignResultItem {
    let params = TypedDataSignerParams {
        credentials: signing_credential.clone(),
        typed_data: typed_data.clone(),
        signing_options: signing_options.clone(),
    };

    let result = signer.sign_typed_data(params).await;

    match result {
        Ok(signature) => {
            // Convert typed data to JSON string for signed_data field
            let signed_data = serde_json::to_string(typed_data)
                .unwrap_or_else(|_| "Failed to serialize typed data".to_string());
            SignResultItem::success(signature, signed_data)
        },
        Err(e) => SignResultItem::failure(e),
    }
}