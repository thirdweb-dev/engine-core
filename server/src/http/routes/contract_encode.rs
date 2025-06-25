// 8:12 PM - COLOCATION: Contract Encode Operations

use alloy::{hex, primitives::ChainId};
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
};
use engine_core::error::EngineError;
use futures::future::join_all;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thirdweb_core::auth::ThirdwebAuth;

use crate::http::{
    dyn_contract::{ContractCall, ContractOperationResult, PreparedContractCall},
    error::ApiEngineError,
    extractors::{EngineJson, OptionalRpcCredentialsExtractor},
    server::EngineServerState,
    types::{BatchResultItem, BatchResults, SuccessResponse},
};

// ===== REQUEST/RESPONSE TYPES =====

/// Options for encoding contract function calls
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct EncodeOptions {
    /// The blockchain network ID to encode for
    ///
    /// This is used to fetch the correct ABI if not provided inline
    pub chain_id: String,
}

/// Request to encode contract function calls
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct EncodeRequest {
    /// Configuration options for encoding
    pub encode_options: EncodeOptions,
    /// List of contract function calls to encode
    ///
    /// Each call will be encoded to its raw transaction data
    pub params: Vec<ContractCall>,
}

/// Successful result from a contract encode operation
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct EncodeResultSuccessItem {
    /// The contract address that would be called
    pub target: String,
    /// The encoded function call data
    ///
    /// This includes the function selector and encoded parameters,
    /// ready to be used in a transaction
    pub call_data: String,
    /// The 4-byte function selector (first 4 bytes of call_data)
    pub function_selector: String,
    /// The name of the function being called
    pub function_name: String,
}

// ===== CONVENIENCE CONSTRUCTORS =====

impl EncodeResultSuccessItem {
    /// Create a new successful encode result
    pub fn new(
        target: String,
        call_data: String,
        function_selector: String,
        function_name: String,
    ) -> Self {
        Self {
            target,
            call_data,
            function_selector,
            function_name,
        }
    }
}

// ===== ROUTE HANDLER =====

#[utoipa::path(
    post,
    operation_id = "encodeContract",
    path = "/encode/contract",
    tag = "Read",
    request_body(content = EncodeRequest, description = "Encode contract request", content_type = "application/json"),
    responses(
        (status = 200, description = "Successfully encoded contract calls", body = SuccessResponse<BatchResults<EncodeResultSuccessItem>>, content_type = "application/json"),
    ),
    params(
        ("x-thirdweb-client-id" = Option<String>, Header, description = "Thirdweb client ID, passed along with the service key"),
        ("x-thirdweb-service-key" = Option<String>, Header, description = "Thirdweb service key, passed when using the client ID"),
        ("x-thirdweb-secret-key" = Option<String>, Header, description = "Thirdweb secret key, passed standalone"),
    )
)]
/// Encode Contract
///
/// Encode contract function calls without execution
pub async fn encode_contract(
    State(state): State<EngineServerState>,
    OptionalRpcCredentialsExtractor(rpc_credentials): OptionalRpcCredentialsExtractor,
    EngineJson(request): EngineJson<EncodeRequest>,
) -> Result<impl IntoResponse, ApiEngineError> {
    let auth: Option<ThirdwebAuth> = rpc_credentials.map(|creds| match creds {
        engine_core::chain::RpcCredentials::Thirdweb(auth) => auth,
    });

    let chain_id: ChainId = request.encode_options.chain_id.parse().map_err(|_| {
        ApiEngineError(EngineError::ValidationError {
            message: "Invalid chain ID".to_string(),
        })
    })?;

    // Prepare all contract calls in parallel
    let prepare_futures = request.params.iter().map(|contract_call| {
        contract_call.prepare_call_with_error_tracking(&state.abi_service, chain_id, auth.clone())
    });

    let preparation_results: Vec<ContractOperationResult<PreparedContractCall>> =
        join_all(prepare_futures).await;

    // Convert all results to encode result items
    let results: Vec<BatchResultItem<EncodeResultSuccessItem>> = preparation_results
        .iter()
        .map(|prep_result| match prep_result {
            ContractOperationResult::Success(prepared_call) => {
                let selector = if prepared_call.call_data.len() >= 4 {
                    format!("0x{}", hex::encode(&prepared_call.call_data[..4]))
                } else {
                    "0x".to_string()
                };

                BatchResultItem::success(EncodeResultSuccessItem::new(
                    format!("{:#x}", prepared_call.target),
                    format!("0x{}", hex::encode(&prepared_call.call_data)),
                    selector,
                    prepared_call.function.name.clone(),
                ))
            }
            ContractOperationResult::Failure(error) => BatchResultItem::failure(error.clone()),
        })
        .collect();

    Ok((
        StatusCode::OK,
        Json(SuccessResponse::new(BatchResults { results })),
    ))
}
