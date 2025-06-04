// 8:12 PM - COLOCATION: Contract Encode Operations

use aide::{axum::IntoApiResponse, transform::TransformOperation};
use alloy::{hex, primitives::ChainId};
use axum::{extract::State, http::StatusCode, response::Json};
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
    types::ErrorResponse,
};

// ===== REQUEST/RESPONSE TYPES =====

/// Options for encoding contract function calls
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EncodeOptions {
    /// The blockchain network ID to encode for
    ///
    /// This is used to fetch the correct ABI if not provided inline
    pub chain_id: String,
}

/// Request to encode contract function calls
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EncodeRequest {
    /// Configuration options for encoding
    pub encode_options: EncodeOptions,
    /// List of contract function calls to encode
    ///
    /// Each call will be encoded to its raw transaction data
    pub params: Vec<ContractCall>,
}

/// Result of a single contract encode operation
///
/// Each result can either be successful (containing the encoded transaction data)
/// or failed (containing detailed error information).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum EncodeResultItem {
    Success(EncodeResultSuccessItem),
    Failure(EncodeResultFailureItem),
}

/// Successful result from a contract encode operation
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EncodeResultSuccessItem {
    /// Always true for successful operations
    #[schemars(with = "bool")]
    pub success: serde_bool::True,
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

/// Failed result from a contract encode operation
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EncodeResultFailureItem {
    /// Always false for failed operations
    #[schemars(with = "bool")]
    pub success: serde_bool::False,
    /// Detailed error information describing what went wrong
    pub error: EngineError,
}

/// Collection of results from multiple contract encode operations
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EncodeResults {
    /// Array of results, one for each input contract call
    pub results: Vec<EncodeResultItem>,
}

/// Response from the contract encode endpoint
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EncodeResponse {
    /// Container for all encode operation results
    pub result: EncodeResults,
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
            success: serde_bool::True,
            target,
            call_data,
            function_selector,
            function_name,
        }
    }
}

impl EncodeResultFailureItem {
    /// Create a new failed encode result
    pub fn new(error: EngineError) -> Self {
        Self {
            success: serde_bool::False,
            error,
        }
    }
}

impl EncodeResultItem {
    /// Create a successful encode result item
    pub fn success(
        target: String,
        call_data: String,
        function_selector: String,
        function_name: String,
    ) -> Self {
        EncodeResultItem::Success(EncodeResultSuccessItem::new(
            target,
            call_data,
            function_selector,
            function_name,
        ))
    }

    /// Create a failed encode result item
    pub fn failure(error: EngineError) -> Self {
        EncodeResultItem::Failure(EncodeResultFailureItem::new(error))
    }
}

// ===== ROUTE HANDLER =====

/// Encode contract function calls without execution
pub async fn encode_contract(
    State(state): State<EngineServerState>,
    OptionalRpcCredentialsExtractor(rpc_credentials): OptionalRpcCredentialsExtractor,
    EngineJson(request): EngineJson<EncodeRequest>,
) -> Result<impl IntoApiResponse, ApiEngineError> {
    let auth: Option<ThirdwebAuth> = rpc_credentials.and_then(|creds| match creds {
        engine_core::chain::RpcCredentials::Thirdweb(auth) => Some(auth),
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
    let results: Vec<EncodeResultItem> = preparation_results
        .iter()
        .map(|prep_result| match prep_result {
            ContractOperationResult::Success(prepared_call) => {
                let selector = if prepared_call.call_data.len() >= 4 {
                    format!("0x{}", hex::encode(&prepared_call.call_data[..4]))
                } else {
                    "0x".to_string()
                };

                EncodeResultItem::success(
                    format!("{:#x}", prepared_call.target),
                    format!("0x{}", hex::encode(&prepared_call.call_data)),
                    selector,
                    prepared_call.function.name.clone(),
                )
            }
            ContractOperationResult::Failure(error) => EncodeResultItem::failure(error.clone()),
        })
        .collect();

    Ok((
        StatusCode::OK,
        Json(EncodeResponse {
            result: EncodeResults { results },
        }),
    ))
}

// ===== DOCUMENTATION =====

pub fn encode_contract_docs(op: TransformOperation) -> TransformOperation {
    op.id("encodeContract")
        .description(
            "Encode contract function calls without execution.\n\n\
            This endpoint prepares contract function calls and returns the encoded \
            transaction data without executing them. This is useful for:\n\
            - Preparing transaction data for later execution\n\
            - Debugging contract interactions\n\
            - Building complex transaction batches\n\
            - Integration with external signing systems\n\n\
            ## Features\n\
            - Encode multiple contract calls in one request\n\
            - Automatic ABI resolution or use provided ABIs\n\
            - Returns function selector, call data, and metadata\n\
            - Detailed error information for each failed encoding\n\n\
            ## Authentication\n\
            - Optional: Same as read endpoint for ABI resolution",
        )
        .summary("Encode contract call data")
        .response_with::<200, Json<EncodeResponse>, _>(|res| {
            res.description("Successfully encoded contract calls")
        })
        .response_with::<400, Json<ErrorResponse>, _>(|res| {
            res.description("Bad request - invalid parameters")
        })
        .tag("Contract Operations")
}
