// Sign Typed Data Operations

use alloy::dyn_abi::TypedData;
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
};
use engine_aa_core::signer::SmartAccountSignerBuilder;
use engine_core::{
    chain::ChainService,
    credentials::SigningCredential,
    defs::{AddressDef, U256Def},
    error::EngineError,
    signer::{AccountSigner, SigningOptions},
};
use futures::future::join_all;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::http::{
    error::ApiEngineError,
    extractors::{EngineJson, SigningCredentialsExtractor},
    server::EngineServerState,
    types::{BatchResultItem, BatchResults},
};

// ===== REQUEST/RESPONSE TYPES =====
/// Request to sign typed data
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SignTypedDataRequest {
    /// Configuration options for signing
    pub signing_options: SigningOptions,
    /// List of typed data to sign
    #[schema(value_type = Vec<TypedDataDef>)]
    pub params: Vec<TypedData>,
}

#[derive(utoipa::ToSchema, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TypedDataDef {
    /// Signing domain metadata. The signing domain is the intended context for
    /// the signature (e.g. the dapp, protocol, etc. that it's intended for).
    /// This data is used to construct the domain separator of the message.
    pub domain: TypedDataDomainDef,

    /// The custom types used by this message.
    #[schema(rename = "types")]
    pub resolver: Value,

    /// The type of the message.
    #[schema(rename = "primaryType")]
    pub primary_type: String,

    /// The message to be signed.
    pub message: serde_json::Value,
}

#[derive(utoipa::ToSchema, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TypedDataDomainDef {
    pub name: Option<String>,

    /// The current major version of the signing domain. Signatures from
    /// different versions are not compatible.
    pub version: Option<String>,

    /// The EIP-155 chain ID. The user-agent should refuse signing if it does
    /// not match the currently active chain.
    pub chain_id: Option<U256Def>,

    /// The address of the contract that will verify the signature.
    pub verifying_contract: Option<AddressDef>,

    /// A disambiguating salt for the protocol. This can be used as a domain
    /// separator of last resort.
    pub salt: Option<String>,
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

// ===== ROUTE HANDLER =====

#[utoipa::path(
    post,
    operation_id = "signTypedData",
    path = "/sign/typed-data",
    tag = "Signature",
    request_body(content = SignTypedDataRequest, description = "Sign typed data request", content_type = "application/json"),
    responses(
        (status = 200, description = "Successfully signed typed data", body = BatchResults<SignResultData>, content_type = "application/json"),
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
        sign_single_typed_data(
            &state,
            &signing_credential,
            &request.signing_options,
            typed_data,
        )
    });

    let results: Vec<BatchResultItem<SignResultData>> = join_all(sign_futures).await;

    Ok((StatusCode::OK, Json(BatchResults { result: results })))
}

// ===== HELPER FUNCTIONS =====

async fn sign_single_typed_data(
    state: &EngineServerState,
    signing_credential: &SigningCredential,
    signing_options: &SigningOptions,
    typed_data: &TypedData,
) -> BatchResultItem<SignResultData> {
    let result = match signing_options {
        SigningOptions::Eoa(eoa_options) => {
            // Direct EOA signing
            state
                .eoa_signer
                .sign_typed_data(eoa_options.clone(), typed_data, signing_credential.clone())
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
                        Ok(smart_signer) => smart_signer.sign_typed_data(typed_data).await,
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
        Ok(signature) => {
            // Convert typed data to JSON string for signed_data field
            match serde_json::to_string(typed_data) {
                Ok(signed_data) => BatchResultItem::success(SignResultData {
                    signature,
                    signed_data,
                }),
                Err(e) => BatchResultItem::failure(EngineError::ValidationError {
                    message: format!("Failed to serialize typed data: {}", e),
                }),
            }
        }
        Err(e) => BatchResultItem::failure(e),
    }
}
