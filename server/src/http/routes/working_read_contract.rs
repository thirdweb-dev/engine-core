use aide::{
    OperationIo,
    axum::{ApiRouter, IntoApiResponse, routing::post_with},
};
use alloy::providers::RootProvider;
use alloy::{
    dyn_abi::FunctionExt,
    primitives::{Address, ChainId, address},
};
use alloy::{
    providers::Provider, rpc::types::eth::TransactionRequest as AlloyTransactionRequest, sol,
    sol_types::SolCall,
};
use axum::{debug_handler, extract::State, http::StatusCode, response::Json};
use engine_core::{
    chain::{Chain, ChainService},
    defs::AddressDef,
    error::EngineError,
};
use futures::future::join_all;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thirdweb_core::auth::ThirdwebAuth;

use crate::http::{
    dyn_contract::{
        ContractCall, ContractOperationResult, PreparedContractCall, dyn_sol_value_to_json,
    },
    error::ApiEngineError,
    extractors::OptionalRpcCredentialsExtractor,
    server::EngineServerState,
    types::ErrorResponse,
};

// Multicall3 contract ABI for aggregate3 function
sol! {
    struct Call3 {
        address target;
        bool allowFailure;
        bytes callData;
    }

    struct Result3 {
        bool success;
        bytes returnData;
    }

    function aggregate3(Call3[] calls) external payable returns (Result3[] returnData);
}

const MULTICALL3_DEFAULT_ADDRESS: Address = address!("0xcA11bde05977b3631167028862bE2a173976CA11");

// ===== REQUEST/RESPONSE TYPES =====

/// Options for reading from smart contracts
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReadOptions {
    /// The blockchain network ID to read from
    pub chain_id: String,
    /// Address of the Multicall3 contract to use for batching calls
    ///
    /// Defaults to the standard Multicall3 address: 0xcA11bde05977b3631167028862bE2a173976CA11
    /// which is deployed on most networks
    #[serde(default = "default_multicall_address")]
    #[schemars(with = "AddressDef")]
    pub multicall_address: Address,
    /// Optional address to use as the caller for view functions
    ///
    /// This can be useful for functions that return different values
    /// based on the caller's address or permissions
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<AddressDef>")]
    pub from: Option<Address>,
}

fn default_multicall_address() -> Address {
    MULTICALL3_DEFAULT_ADDRESS
}

/// Request to read from multiple smart contracts
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReadRequest {
    /// Configuration options for the read operation
    pub read_options: ReadOptions,
    /// List of contract function calls to execute
    ///
    /// All calls will be batched together using Multicall3 for efficiency
    pub params: Vec<ContractCall>,
}

/// Result of a single contract read operation
///
/// Each result can either be successful (containing the function return value)
/// or failed (containing detailed error information). The `success` field
/// indicates which case applies.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum ReadResultItem {
    Success(ReadResultSuccessItem),
    Failure(ReadResultFailureItem),
}

/// Successful result from a contract read operation
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ReadResultSuccessItem {
    /// Always true for successful operations
    #[schemars(with = "bool")]
    pub success: serde_bool::True,
    /// The decoded return value from the contract function
    ///
    /// For functions returning a single value, this will be that value.
    /// For functions returning multiple values, this will be an array.
    pub result: JsonValue,
}

/// Failed result from a contract read operation
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ReadResultFailureItem {
    /// Always false for failed operations
    #[schemars(with = "bool")]
    pub success: serde_bool::False,
    /// Detailed error information describing what went wrong
    ///
    /// This includes the error type, chain information, and specific
    /// failure details to help with debugging
    pub error: EngineError,
}

/// Collection of results from multiple contract read operations
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ReadResults {
    /// Array of results, one for each input contract call
    ///
    /// Results are returned in the same order as the input parameters
    pub results: Vec<ReadResultItem>,
}

/// Response from the contract read endpoint
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ReadResponse {
    /// Container for all read operation results
    pub result: ReadResults,
}

// ===== CONVENIENCE CONSTRUCTORS =====

impl ReadResultSuccessItem {
    /// Create a new successful read result
    pub fn new(result: JsonValue) -> Self {
        Self {
            success: serde_bool::True,
            result,
        }
    }
}

impl ReadResultFailureItem {
    /// Create a new failed read result
    pub fn new(error: EngineError) -> Self {
        Self {
            success: serde_bool::False,
            error,
        }
    }
}

impl ReadResultItem {
    /// Create a successful read result item
    pub fn success(result: JsonValue) -> Self {
        ReadResultItem::Success(ReadResultSuccessItem::new(result))
    }

    /// Create a failed read result item
    pub fn failure(error: EngineError) -> Self {
        ReadResultItem::Failure(ReadResultFailureItem::new(error))
    }
}

// ===== ROUTE HANDLER =====

/// Read from multiple smart contracts using multicall
///
/// This endpoint allows you to efficiently call multiple read-only contract functions
/// in a single request. All calls are batched using the Multicall3 contract for
/// optimal gas usage and performance.
///
/// ## Features
/// - Batch multiple contract calls in one request
/// - Automatic ABI resolution or use provided ABIs
/// - Support for function names, signatures, or full function declarations
/// - Detailed error information for each failed call
/// - Preserves original parameter order in results
///
/// ## Authentication
/// - Optional: Provide `x-thirdweb-secret-key` or `x-thirdweb-client-id` + `x-thirdweb-service-key`
/// - If provided, will be used for ABI resolution from verified contracts
///
/// ## Error Handling
/// - Individual call failures don't affect other calls
/// - Each result includes success status and detailed error information
/// - Preparation errors (ABI resolution, parameter encoding) are preserved
/// - Execution errors (multicall failures) are clearly identified
#[debug_handler]
async fn read_contract_handler(
    State(state): State<EngineServerState>,
    OptionalRpcCredentialsExtractor(rpc_credentials): OptionalRpcCredentialsExtractor,
    Json(request): Json<ReadRequest>,
) -> Result<impl IntoApiResponse, ApiEngineError> {
    let auth: Option<ThirdwebAuth> = rpc_credentials.and_then(|creds| match creds {
        engine_core::chain::RpcCredentials::Thirdweb(auth) => Some(auth),
    });

    let chain_id: ChainId = request.read_options.chain_id.parse().map_err(|_| {
        ApiEngineError(EngineError::ValidationError {
            message: "Invalid chain ID".to_string(),
        })
    })?;

    // Prepare all contract calls in parallel
    let prepare_futures = request.params.iter().map(|contract_read| {
        contract_read.prepare_call_with_error_tracking(&state.abi_service, chain_id, auth.clone())
    });

    let preparation_results: Vec<ContractOperationResult<PreparedContractCall>> =
        join_all(prepare_futures).await;

    // Separate successful calls for multicall while preserving original order and errors
    let (multicall_calls, call_indices): (Vec<Call3>, Vec<usize>) = preparation_results
        .iter()
        .enumerate()
        .filter_map(|(index, result)| match result {
            ContractOperationResult::Success(prepared) => {
                let call = Call3 {
                    target: prepared.target,
                    allowFailure: true,
                    callData: prepared.call_data.clone(),
                };
                Some((call, index))
            }
            ContractOperationResult::Failure(_) => None,
        })
        .unzip();

    // Execute multicall if we have any valid calls
    let multicall_results = if !multicall_calls.is_empty() {
        let chain = state.chains.get_chain(chain_id).map_err(ApiEngineError)?;
        match execute_multicall(
            &request.read_options.multicall_address,
            multicall_calls,
            chain.provider(),
            chain_id,
        )
        .await
        {
            Ok(results) => Some(results),
            Err(e) => {
                tracing::error!("Multicall failed: {}", e);
                None
            }
        }
    } else {
        None
    };

    // Map results back to original order, preserving all errors
    let results = map_results_to_original_order(
        &preparation_results,
        multicall_results.as_deref(),
        &call_indices,
    );

    Ok((
        StatusCode::OK,
        Json(ReadResponse {
            result: ReadResults { results },
        }),
    ))
}

// ===== HELPER FUNCTIONS =====

/// Execute the multicall and return results
async fn execute_multicall(
    multicall_address: &Address,
    multicall_calls: Vec<Call3>,
    provider: &RootProvider,
    chain_id: ChainId,
) -> Result<Vec<Result3>, EngineError> {
    let multicall_call = aggregate3Call {
        calls: multicall_calls,
    };

    let call_request = AlloyTransactionRequest::default()
        .to(*multicall_address)
        .input(multicall_call.abi_encode().into());

    let result = provider.call(call_request).await.map_err(|e| {
        EngineError::contract_multicall_error(chain_id, format!("Multicall failed: {}", e))
    })?;

    let decoded = aggregate3Call::abi_decode_returns(&result).map_err(|e| {
        EngineError::contract_multicall_error(
            chain_id,
            format!("Failed to decode multicall result: {}", e),
        )
    })?;

    Ok(decoded)
}

/// Map multicall results back to the original parameter order, preserving all errors
fn map_results_to_original_order(
    preparation_results: &[ContractOperationResult<PreparedContractCall>],
    multicall_results: Option<&[Result3]>,
    call_indices: &[usize],
) -> Vec<ReadResultItem> {
    let mut multicall_iter = multicall_results.unwrap_or(&[]).iter();

    preparation_results
        .iter()
        .enumerate()
        .map(|(original_index, prep_result)| {
            match prep_result {
                ContractOperationResult::Success(prepared_call) => {
                    if call_indices.contains(&original_index) {
                        if let Some(multicall_result) = multicall_iter.next() {
                            process_multicall_result(multicall_result, prepared_call)
                        } else {
                            ReadResultItem::failure(EngineError::contract_multicall_error(
                                0, // Chain ID not available here
                                "Multicall execution failed".to_string(),
                            ))
                        }
                    } else {
                        ReadResultItem::failure(EngineError::InternalError(
                            "Internal error: prepared call not found in multicall".to_string(),
                        ))
                    }
                }
                ContractOperationResult::Failure(error) => ReadResultItem::failure(error.clone()),
            }
        })
        .collect()
}

/// Process a single multicall result with the prepared call
fn process_multicall_result(
    multicall_result: &Result3,
    prepared_call: &PreparedContractCall,
) -> ReadResultItem {
    if !multicall_result.success {
        return ReadResultItem::failure(EngineError::contract_multicall_error(
            0, // Chain ID not available here
            "Multicall execution failed".to_string(),
        ));
    }

    match prepared_call
        .function
        .abi_decode_output(&multicall_result.returnData)
    {
        Ok(decoded_values) => {
            let result_json = match decoded_values.len() {
                1 => dyn_sol_value_to_json(&decoded_values[0]),
                _ => JsonValue::Array(decoded_values.iter().map(dyn_sol_value_to_json).collect()),
            };
            ReadResultItem::success(result_json)
        }
        Err(e) => {
            ReadResultItem::failure(EngineError::contract_decoding_error(
                Some(prepared_call.target),
                0, // Chain ID not available here
                format!("Failed to decode result: {}", e),
            ))
        }
    }
}

// ===== ROUTER =====

pub fn read_routes() -> ApiRouter<EngineServerState> {
    ApiRouter::new().api_route(
        "/read/contract",
        post_with(read_contract_handler, |op| {
            op.id("readContract")
                .description("Read from multiple smart contracts using multicall")
                .summary("Batch read contract functions")
                .response_with::<200, Json<ReadResponse>, _>(|res| {
                    res.description("Successfully read contract data")
                        .example(ReadResponse {
                            result: ReadResults { results: vec![] },
                        })
                })
                .response_with::<400, Json<ErrorResponse>, _>(|res| {
                    res.description("Bad request - invalid parameters or chain ID")
                })
                .response_with::<500, Json<ErrorResponse>, _>(|res| {
                    res.description("Internal server error")
                })
                .tag("Contract Operations")
        }),
    )
}
