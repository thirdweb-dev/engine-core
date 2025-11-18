// 8:12 PM - COLOCATION: Contract Read Operations

use alloy::dyn_abi::FunctionExt;
use alloy::eips::BlockNumberOrTag;
use alloy::primitives::{Address, ChainId, address};

use alloy::{
    providers::{Provider, RootProvider},
    rpc::types::eth::TransactionRequest as AlloyTransactionRequest,
    sol,
    sol_types::SolCall,
};
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
};
use engine_core::{
    chain::{Chain, ChainService},
    defs::AddressDef,
    error::{AlloyRpcErrorToEngineError, EngineError},
};
use futures::future::join_all;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value as JsonValue;
use serde_with::{DisplayFromStr, PickFirst, serde_as};
use thirdweb_core::auth::ThirdwebAuth;
use utoipa::ToSchema;

use crate::http::error::EngineResult;
use crate::http::extractors::EngineJson;
use crate::http::types::{BatchResultItem, BatchResults};
use crate::http::{
    dyn_contract::{
        ContractCall, ContractOperationResult, PreparedContractCall, dyn_sol_value_to_json,
    },
    error::ApiEngineError,
    extractors::OptionalRpcCredentialsExtractor,
    server::EngineServerState,
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

/// Multicall configuration
#[derive(Debug, Clone, Serialize, ToSchema, Default)]
#[serde(untagged)]
pub enum MulticallConfig {
    /// Enable multicall with default Multicall3 address
    #[schema(example = true)]
    #[default]
    Enabled,
    /// Disable multicall - make direct RPC calls instead
    #[schema(example = false)]
    Disabled,
    /// Enable multicall with custom address
    #[schema(value_type = AddressDef)]
    CustomAddress(Address),
}

impl<'de> Deserialize<'de> for MulticallConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;
        use serde_json::Value;

        let value = Value::deserialize(deserializer)?;

        match value {
            // Handle boolean values
            Value::Bool(true) => Ok(MulticallConfig::Enabled),
            Value::Bool(false) => Ok(MulticallConfig::Disabled),

            // Handle null as disabled
            Value::Null => Ok(MulticallConfig::Disabled),

            // Handle string addresses
            Value::String(addr_str) => {
                // Try to parse as Address
                match addr_str.parse::<Address>() {
                    Ok(address) => Ok(MulticallConfig::CustomAddress(address)),
                    Err(_) => Err(D::Error::custom(format!(
                        "Invalid address format: {addr_str}. Expected valid Ethereum address or boolean value"
                    ))),
                }
            }

            // Reject other types
            _ => Err(D::Error::custom(
                "Expected boolean (true/false), null, or address string",
            )),
        }
    }
}

impl MulticallConfig {
    /// Returns true if multicall is enabled
    pub fn is_enabled(&self) -> bool {
        matches!(
            self,
            MulticallConfig::Enabled | MulticallConfig::CustomAddress(_)
        )
    }

    /// Returns the multicall address to use
    pub fn address(&self) -> Option<Address> {
        match self {
            MulticallConfig::Enabled => Some(default_multicall_address()),
            MulticallConfig::Disabled => None, // Won't be used
            MulticallConfig::CustomAddress(addr) => Some(*addr),
        }
    }
}

fn default_multicall_address() -> Address {
    MULTICALL3_DEFAULT_ADDRESS
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum MulticallDocType {
    #[schema(example = true)]
    Boolean(bool),
    #[schema(value_type = AddressDef)]
    Address(Address),
}

/// Options for reading from smart contracts
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReadOptions {
    /// The blockchain network ID to read from
    #[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
    pub chain_id: u64,
    /// Multicall configuration
    ///
    /// Can be:
    /// - `null` or omitted (default): Use multicall with default Multicall3 address and fallback to direct calls if multicall fails
    /// - `true`: Explicitly enable multicall with default Multicall3 address (no fallback)
    /// - `false`: Disable multicall and make direct RPC calls
    /// - Address string: Use multicall with custom address (no fallback)
    #[serde(default)]
    #[schema(value_type = Option<MulticallDocType>)]
    pub multicall: Option<MulticallConfig>,
    /// Optional address to use as the caller for view functions
    ///
    /// This can be useful for functions that return different values
    /// based on the caller's address or permissions
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<AddressDef>)]
    pub from: Option<Address>,
}

/// Request to read from multiple smart contracts
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReadRequest {
    /// Configuration options for the read operation
    pub read_options: ReadOptions,
    /// List of contract function calls to execute
    ///
    /// All calls will be batched together using Multicall3 for efficiency
    pub params: Vec<ContractCall>,
}

/// Successful result from a contract read operation
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(transparent)]
pub struct ReadResultSuccessItem(
    /// The decoded return value from the contract function
    ///
    /// For functions returning a single value, this will be that value.
    /// For functions returning multiple values, this will be an array.
    JsonValue,
);

// ===== ROUTE HANDLER =====

#[utoipa::path(
    post,
    operation_id = "readContract",
    path = "/read/contract",
    tag = "Read",
    request_body(content = ReadRequest, description = "Read contract request", content_type = "application/json"),
    responses(
        (status = 200, description = "Successfully read contract data", body = BatchResults<ReadResultSuccessItem>, content_type = "application/json"),
    ),
    params(
        ("x-thirdweb-client-id" = Option<String>, Header, description = "Thirdweb client ID, passed along with the service key"),
        ("x-thirdweb-service-key" = Option<String>, Header, description = "Thirdweb service key, passed when using the client ID"),
        ("x-thirdweb-secret-key" = Option<String>, Header, description = "Thirdweb secret key, passed standalone"),
    )
)]
/// Read Contract
///
/// Read from smart contracts with intelligent execution strategy:
/// - Single calls: Always executed directly for efficiency
/// - Multiple calls: Uses multicall by default, or direct calls if disabled
/// - Failed preparations: Returns preparation errors directly
///
/// If multicall is not specified, it will be used by default. In case of multicall related errors, engine will fallback to direct calls.
/// Only in the case where multicall is explicitly enabled, engine will not fallback to direct calls.
pub async fn read_contract(
    State(state): State<EngineServerState>,
    OptionalRpcCredentialsExtractor(rpc_credentials): OptionalRpcCredentialsExtractor,
    EngineJson(request): EngineJson<ReadRequest>,
) -> Result<impl IntoResponse, ApiEngineError> {
    let auth: Option<ThirdwebAuth> = rpc_credentials.map(|creds| match creds {
        engine_core::chain::RpcCredentials::Thirdweb(auth) => auth,
    });

    let chain_id = request.read_options.chain_id;

    let chain = state.chains.get_chain(chain_id).api_error()?;

    // Prepare all contract calls in parallel
    let prepare_futures = request.params.iter().map(|contract_read| {
        contract_read.prepare_call_with_error_tracking(
            &state.abi_service,
            chain.chain_id(),
            auth.clone(),
        )
    });

    let preparation_results: Vec<ContractOperationResult<PreparedContractCall>> =
        join_all(prepare_futures).await;

    // Determine execution strategy and execute
    let results =
        match determine_execution_strategy(&request.read_options.multicall, &preparation_results) {
            ExecutionStrategy::Direct => {
                execute_direct_contract_calls(
                    &preparation_results,
                    chain.provider(),
                    &request.read_options.from,
                    &chain,
                )
                .await
            }
            ExecutionStrategy::Multicall(multicall_address) => {
                execute_with_multicall_and_fallback(
                    &preparation_results,
                    multicall_address,
                    chain.provider(),
                    chain_id,
                    &chain,
                    &request.read_options.from,
                    allows_fallback(&request.read_options.multicall),
                )
                .await
            }
            ExecutionStrategy::AllFailed => {
                // All calls failed during preparation
                preparation_results
                    .iter()
                    .map(|result| match result {
                        ContractOperationResult::Success(_) => {
                            BatchResultItem::failure(EngineError::InternalError {
                                message: "Internal error: no valid calls found".to_string(),
                            })
                        }
                        ContractOperationResult::Failure(error) => {
                            BatchResultItem::failure(error.clone())
                        }
                    })
                    .collect()
            }
        };

    Ok((StatusCode::OK, Json(BatchResults { result: results })))
}

// ===== HELPER FUNCTIONS =====

/// Execution strategy for contract calls
#[derive(Debug)]
enum ExecutionStrategy {
    /// Execute calls directly without multicall
    Direct,
    /// Use multicall with the specified address
    Multicall(Address),
    /// All calls failed during preparation
    AllFailed,
}

/// Returns the multicall address if enabled
fn get_multicall_address(multicall_config: &Option<MulticallConfig>) -> Option<Address> {
    match multicall_config {
        None => Some(default_multicall_address()), // Default behavior
        Some(config) => config.address(),
    }
}

/// Returns true if fallback to direct calls is allowed when multicall fails
fn allows_fallback(multicall_config: &Option<MulticallConfig>) -> bool {
    multicall_config.is_none() // Only allow fallback for default behavior (None)
}

/// Detect if an error is likely due to multicall contract issues that warrant fallback
fn is_multicall_fallback_error(error: &EngineError) -> bool {
    match error {
        EngineError::ContractInteractionError { message, .. } => {
            // Check for ABI decoding errors that suggest multicall contract doesn't exist or is incompatible
            message.contains("ABI decoding failed")
                || message.contains("buffer overrun")
                || message.contains("Failed to decode multicall result")
                || message.contains("execution reverted")
                || message.contains("contract not deployed")
        }
        _ => false,
    }
}

/// Determine the best execution strategy based on configuration and prepared calls
fn determine_execution_strategy(
    multicall_config: &Option<MulticallConfig>,
    preparation_results: &[ContractOperationResult<PreparedContractCall>],
) -> ExecutionStrategy {
    // Count successful preparations
    let successful_calls = preparation_results
        .iter()
        .filter(|result| matches!(result, ContractOperationResult::Success(_)))
        .count();

    match successful_calls {
        0 => ExecutionStrategy::AllFailed,
        1 => ExecutionStrategy::Direct, // Single call is always direct for efficiency
        _ => {
            // Multiple calls - check configuration
            if let Some(multicall_address) = get_multicall_address(multicall_config) {
                ExecutionStrategy::Multicall(multicall_address)
            } else {
                ExecutionStrategy::Direct
            }
        }
    }
}

/// Execute contract calls using multicall with optional fallback to direct calls
async fn execute_with_multicall_and_fallback<C: engine_core::chain::Chain>(
    preparation_results: &[ContractOperationResult<PreparedContractCall>],
    multicall_address: Address,
    provider: &RootProvider,
    chain_id: ChainId,
    chain: &C,
    from: &Option<Address>,
    allow_fallback: bool,
) -> Vec<BatchResultItem<ReadResultSuccessItem>> {
    // First try multicall
    let multicall_result =
        execute_with_multicall(preparation_results, multicall_address, provider, chain_id).await;

    // Check if we should fallback on error
    if allow_fallback {
        // Check if any results contain fallback-worthy errors
        let has_fallback_error = multicall_result.iter().any(|result| match result {
            BatchResultItem::Failure { error } => is_multicall_fallback_error(error),
            _ => false,
        });

        if has_fallback_error {
            tracing::warn!(
                "Multicall failed with fallback-worthy error on chain {}, falling back to direct calls",
                chain_id
            );

            // Fallback to direct calls
            return execute_direct_contract_calls(preparation_results, provider, from, chain).await;
        }
    }

    multicall_result
}

/// Execute contract calls using multicall
async fn execute_with_multicall(
    preparation_results: &[ContractOperationResult<PreparedContractCall>],
    multicall_address: Address,
    provider: &RootProvider,
    chain_id: ChainId,
) -> Vec<BatchResultItem<ReadResultSuccessItem>> {
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

    let multicall_results =
        execute_multicall(&multicall_address, multicall_calls, provider, chain_id).await;

    match multicall_results {
        Ok(results) => {
            map_multicall_results_to_original_order(preparation_results, &results, &call_indices)
        }
        Err(e) => {
            tracing::error!("Multicall failed: {}", e);
            // Return error for all calls
            preparation_results
                .iter()
                .map(|_| BatchResultItem::failure(e.clone()))
                .collect()
        }
    }
}

/// Execute contract calls directly without multicall
async fn execute_direct_contract_calls<C: engine_core::chain::Chain>(
    preparation_results: &[ContractOperationResult<PreparedContractCall>],
    provider: &RootProvider,
    from: &Option<Address>,
    chain: &C,
) -> Vec<BatchResultItem<ReadResultSuccessItem>> {
    let mut results = Vec::new();

    for prep_result in preparation_results {
        match prep_result {
            ContractOperationResult::Success(prepared_call) => {
                // Execute the single call directly
                let mut call_request = AlloyTransactionRequest::default()
                    .to(prepared_call.target)
                    .input(prepared_call.call_data.clone().into());

                if let Some(from_address) = from {
                    call_request = call_request.from(*from_address);
                }

                match provider
                    .call(call_request)
                    .block(BlockNumberOrTag::Latest.into())
                    .await
                {
                    Ok(result) => {
                        // Decode the result
                        match prepared_call.function.abi_decode_output(&result) {
                            Ok(decoded_values) => {
                                let result_json = match decoded_values.len() {
                                    1 => dyn_sol_value_to_json(&decoded_values[0]),
                                    _ => JsonValue::Array(
                                        decoded_values.iter().map(dyn_sol_value_to_json).collect(),
                                    ),
                                };
                                results.push(BatchResultItem::success(ReadResultSuccessItem(
                                    result_json,
                                )));
                            }
                            Err(e) => {
                                results.push(BatchResultItem::failure(
                                    EngineError::contract_decoding_error(
                                        Some(prepared_call.target),
                                        chain.chain_id(),
                                        format!("Failed to decode result: {e}"),
                                    ),
                                ));
                            }
                        }
                    }
                    Err(e) => {
                        // Convert alloy error to engine error
                        let engine_error = e.to_engine_error(chain);
                        results.push(BatchResultItem::failure(engine_error));
                    }
                }
            }
            ContractOperationResult::Failure(error) => {
                results.push(BatchResultItem::failure(error.clone()));
            }
        }
    }

    results
}

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

    let result = provider
        .call(call_request)
        .block(BlockNumberOrTag::Latest.into())
        .await
        .map_err(|e| {
            EngineError::contract_multicall_error(chain_id, format!("Multicall failed: {e}"))
        })?;

    let decoded = aggregate3Call::abi_decode_returns(&result).map_err(|e| {
        EngineError::contract_multicall_error(
            chain_id,
            format!("Failed to decode multicall result: {e}"),
        )
    })?;

    Ok(decoded)
}

/// Map multicall results back to the original parameter order, preserving all errors
fn map_multicall_results_to_original_order(
    preparation_results: &[ContractOperationResult<PreparedContractCall>],
    multicall_results: &[Result3],
    call_indices: &[usize],
) -> Vec<BatchResultItem<ReadResultSuccessItem>> {
    let mut multicall_iter = multicall_results.iter();

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
                            BatchResultItem::failure(EngineError::contract_multicall_error(
                                0, // Chain ID not available here
                                "Multicall execution failed".to_string(),
                            ))
                        }
                    } else {
                        BatchResultItem::failure(EngineError::InternalError {
                            message: "Internal error: prepared call not found in multicall"
                                .to_string(),
                        })
                    }
                }
                ContractOperationResult::Failure(error) => BatchResultItem::failure(error.clone()),
            }
        })
        .collect()
}

/// Process a single multicall result with the prepared call
fn process_multicall_result(
    multicall_result: &Result3,
    prepared_call: &PreparedContractCall,
) -> BatchResultItem<ReadResultSuccessItem> {
    if !multicall_result.success {
        return BatchResultItem::failure(EngineError::contract_multicall_error(
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
            BatchResultItem::success(ReadResultSuccessItem(result_json))
        }
        Err(e) => {
            BatchResultItem::failure(EngineError::contract_decoding_error(
                Some(prepared_call.target),
                0, // Chain ID not available here
                format!("Failed to decode result: {e}"),
            ))
        }
    }
}
