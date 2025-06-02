use alloy::dyn_abi::FunctionExt;
use alloy::hex;
use alloy::primitives::address;
use alloy::providers::RootProvider;
use alloy::{
    dyn_abi::{DynSolType, DynSolValue, JsonAbiExt},
    json_abi::{Function, JsonAbi},
    primitives::{Address, Bytes, ChainId},
    providers::{Provider, ProviderBuilder},
    rpc::types::eth::TransactionRequest as AlloyTransactionRequest,
    sol,
    sol_types::SolCall,
};
use axum::{
    Json as AxumJson,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Json},
};
use engine_core::chain::{Chain, ChainService, RpcCredentials};
use engine_core::error::EngineError;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thirdweb_core::{
    abi::{ThirdwebAbiService, ThirdwebContractOptions},
    auth::ThirdwebAuth,
};

use crate::http::{error::ApiEngineError, server::EngineServerState};

use super::transaction::extract_rpc_credentials;

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

// Default Multicall3 address (same across most chains)
const MULTICALL3_DEFAULT_ADDRESS: Address = address!("0xcA11bde05977b3631167028862bE2a173976CA11");

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContractRead {
    pub contract_address: Address,
    pub method: String,
    pub params: Vec<JsonValue>,
    pub abi: Option<JsonAbi>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadOptions {
    pub chain_id: String,
    #[serde(default = "default_multicall_address")]
    pub multicall_address: Address,
    pub from: Option<Address>,
}

fn default_multicall_address() -> Address {
    MULTICALL3_DEFAULT_ADDRESS
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadRequest {
    pub read_options: ReadOptions,
    pub params: Vec<ContractRead>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadResultItem {
    pub success: bool,
    pub result: Option<JsonValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadResults {
    pub results: Vec<ReadResultItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadResponse {
    pub result: ReadResults,
}

#[derive(Debug, Clone)]
struct PreparedCall {
    pub target: Address,
    pub call_data: Bytes,
    pub function: Function,
}

impl ContractRead {
    /// Determines if the method string is definitely just a function name (no signature)
    fn is_definitely_function_name_only(method: &str) -> bool {
        let trimmed = method.trim();
        // Definitely just a name if it doesn't contain parentheses and doesn't start with "function"
        !trimmed.contains('(') && !trimmed.starts_with("function ")
    }

    /// Tries to parse as a function signature using alloy's dyn abi
    fn try_parse_as_signature(method: &str) -> Option<Function> {
        Function::parse(method).ok()
    }

    /// Resolves the function using the optimal strategy
    async fn resolve_function(
        &self,
        abi_service: &ThirdwebAbiService,
        chain_id: ChainId,
        auth: Option<ThirdwebAuth>,
    ) -> Result<Function, ApiEngineError> {
        // Strategy 1: If we're certain it's just a name, skip signature parsing
        if Self::is_definitely_function_name_only(&self.method) {
            return self.resolve_from_abi(abi_service, chain_id, auth).await;
        }

        // Strategy 2: Try parsing as signature first (most efficient if it works)
        if let Some(function) = Self::try_parse_as_signature(&self.method) {
            // Validate parameter count matches
            if function.inputs.len() == self.params.len() {
                return Ok(function);
            } else {
                return Err(ApiEngineError::validation_error(&format!(
                    "Parameter count mismatch: signature expects {}, got {}",
                    function.inputs.len(),
                    self.params.len()
                )));
            }
        }

        // Strategy 3: Fall back to ABI resolution
        self.resolve_from_abi(abi_service, chain_id, auth).await
    }

    /// Resolves function from ABI with proper overload handling
    async fn resolve_from_abi(
        &self,
        abi_service: &ThirdwebAbiService,
        chain_id: ChainId,
        auth: Option<ThirdwebAuth>,
    ) -> Result<Function, ApiEngineError> {
        let abi = self.get_abi(abi_service, chain_id, auth).await?;
        let function_name = self.extract_function_name(&self.method)?;

        // Get all functions with this name
        let functions: Vec<&Function> = abi
            .functions()
            .filter(|f| f.name == function_name)
            .collect();

        if functions.is_empty() {
            return Err(ApiEngineError::validation_error(&format!(
                "Function '{}' not found in contract ABI",
                function_name
            )));
        }

        // Filter by parameter count
        let matching_functions: Vec<&Function> = functions
            .into_iter()
            .filter(|f| f.inputs.len() == self.params.len())
            .collect();

        match matching_functions.len() {
            0 => Err(ApiEngineError::validation_error(&format!(
                "No overload of function '{}' matches {} parameters",
                function_name,
                self.params.len()
            ))),
            1 => Ok(matching_functions[0].clone()),
            _ => {
                // Multiple candidates - try each until one works for parameter encoding
                self.find_working_overload(matching_functions)
            }
        }
    }

    /// Tries each function overload until one works for parameter encoding
    fn find_working_overload(
        &self,
        candidates: Vec<&Function>,
    ) -> Result<Function, ApiEngineError> {
        for function in candidates {
            if let Ok(_) = self.encode_parameters(function) {
                return Ok(function.clone());
            }
        }

        Err(ApiEngineError::validation_error(
            "No function overload could successfully encode the provided parameters",
        ))
    }

    /// Gets ABI either from provided or from service
    async fn get_abi(
        &self,
        abi_service: &ThirdwebAbiService,
        chain_id: ChainId,
        auth: Option<ThirdwebAuth>,
    ) -> Result<JsonAbi, ApiEngineError> {
        if let Some(ref provided_abi) = self.abi {
            return Ok(provided_abi.clone());
        }

        let contract_options = ThirdwebContractOptions {
            address: self.contract_address,
            chain_id,
        };

        let abi = if let Some(auth) = auth {
            abi_service.get_abi_with_auth(contract_options, auth).await
        } else {
            abi_service.get_abi(contract_options).await
        }
        .map_err(|e| {
            ApiEngineError::validation_error(&format!(
                "Failed to fetch ABI for contract {}: {}",
                self.contract_address, e
            ))
        })?;

        Ok(abi)
    }

    /// Extracts function name from method string (handles both signatures and plain names)
    fn extract_function_name(&self, method: &str) -> Result<String, ApiEngineError> {
        let trimmed = method.trim();

        if trimmed.starts_with("function ") {
            let after_function = &trimmed[9..];
            if let Some(paren_pos) = after_function.find('(') {
                return Ok(after_function[..paren_pos].trim().to_string());
            }
        } else if let Some(paren_pos) = trimmed.find('(') {
            return Ok(trimmed[..paren_pos].trim().to_string());
        } else {
            // Just a plain function name
            return Ok(trimmed.to_string());
        }

        Err(ApiEngineError::validation_error(&format!(
            "Invalid method format: {}",
            method
        )))
    }

    /// Encodes parameters using serde to directly deserialize into DynSolValue
    fn encode_parameters(&self, function: &Function) -> Result<Vec<u8>, ApiEngineError> {
        if self.params.len() != function.inputs.len() {
            return Err(ApiEngineError::validation_error(&format!(
                "Parameter count mismatch: expected {}, got {}",
                function.inputs.len(),
                self.params.len()
            )));
        }

        let mut parsed_params = Vec::new();

        for (param, input) in self.params.iter().zip(function.inputs.iter()) {
            // Parse the Solidity type for validation
            let sol_type: DynSolType = input.ty.parse().map_err(|e| {
                ApiEngineError::validation_error(&format!(
                    "Invalid Solidity type '{}': {}",
                    input.ty, e
                ))
            })?;

            // Directly deserialize JSON into DynSolValue using serde
            let parsed_value: DynSolValue = sol_type.coerce_json(param).map_err(|e| {
                ApiEngineError::validation_error(&format!(
                    "Failed to parse parameter as DynSolValue: {}",
                    e
                ))
            })?;

            // Validate that the parsed value matches the expected type
            if !parsed_value.matches(&sol_type) {
                return Err(ApiEngineError::validation_error(&format!(
                    "Parameter type mismatch: expected {}, got {:?}",
                    input.ty,
                    parsed_value.as_type()
                )));
            }

            parsed_params.push(parsed_value);
        }

        // Encode the function call
        function.abi_encode_input(&parsed_params).map_err(|e| {
            ApiEngineError::validation_error(&format!("Failed to encode function call: {}", e))
        })
    }

    /// Prepares the contract call by encoding the function call
    async fn prepare_call(
        &self,
        abi_service: &ThirdwebAbiService,
        chain_id: ChainId,
        auth: Option<ThirdwebAuth>,
    ) -> Result<PreparedCall, ApiEngineError> {
        let function = self.resolve_function(abi_service, chain_id, auth).await?;
        let call_data = self.encode_parameters(&function)?;

        Ok(PreparedCall {
            target: self.contract_address,
            call_data: call_data.into(),
            function,
        })
    }
}

/// Converts DynSolValue back to JSON for response
fn dyn_sol_value_to_json(value: &DynSolValue) -> JsonValue {
    match value {
        DynSolValue::Address(addr) => JsonValue::String(format!("{:#x}", addr)),
        DynSolValue::Uint(val, _) => JsonValue::String(val.to_string()),
        DynSolValue::Int(val, _) => JsonValue::String(val.to_string()),
        DynSolValue::Bool(b) => JsonValue::Bool(*b),
        DynSolValue::String(s) => JsonValue::String(s.clone()),
        DynSolValue::Bytes(b) => JsonValue::String(format!("0x{}", hex::encode(b))),
        DynSolValue::Array(arr) => {
            JsonValue::Array(arr.iter().map(dyn_sol_value_to_json).collect())
        }
        DynSolValue::Tuple(tuple) => {
            JsonValue::Array(tuple.iter().map(dyn_sol_value_to_json).collect())
        }
        _ => JsonValue::Null,
    }
}

/// Processes a single multicall result with the prepared call
fn process_multicall_result(
    multicall_result: &Result3,
    prepared_call: &PreparedCall,
) -> ReadResultItem {
    if !multicall_result.success {
        return failed_result();
    }

    prepared_call
        .function
        .abi_decode_output(&multicall_result.returnData)
        .ok()
        .map(|decoded_values| {
            let result_json = match decoded_values.len() {
                1 => dyn_sol_value_to_json(&decoded_values[0]),
                _ => JsonValue::Array(decoded_values.iter().map(dyn_sol_value_to_json).collect()),
            };
            ReadResultItem {
                success: true,
                result: Some(result_json),
            }
        })
        .unwrap_or_else(failed_result)
}

/// Creates a failed result item
fn failed_result() -> ReadResultItem {
    ReadResultItem {
        success: false,
        result: None,
    }
}

/// Main read endpoint handler
#[axum::debug_handler]
pub async fn read_contract(
    State(state): State<EngineServerState>,
    headers: HeaderMap,
    AxumJson(request): AxumJson<ReadRequest>,
) -> Result<impl IntoResponse, ApiEngineError> {
    let rpc_credentials = extract_rpc_credentials(&headers).ok();
    let auth: Option<ThirdwebAuth> = rpc_credentials.map(|RpcCredentials::Thirdweb(auth)| auth);

    let chain_id: ChainId = request
        .read_options
        .chain_id
        .parse()
        .map_err(|_| ApiEngineError::validation_error("Invalid chain ID"))?;

    // Execute all prepare_call futures in parallel
    let prepare_futures = request.params.iter().map(|contract_read| {
        contract_read.prepare_call(&state.abi_service, chain_id, auth.clone())
    });

    let preparation_results: Vec<Result<PreparedCall, ApiEngineError>> =
        join_all(prepare_futures).await;

    // Separate successful calls for multicall while preserving original order
    let (multicall_calls, call_indices): (Vec<Call3>, Vec<usize>) = preparation_results
        .iter()
        .enumerate()
        .filter_map(|(index, result)| {
            result.as_ref().ok().map(|prepared| {
                let call = Call3 {
                    target: prepared.target,
                    allowFailure: true,
                    callData: prepared.call_data.clone(),
                };
                (call, index)
            })
        })
        .unzip();

    if multicall_calls.is_empty() {
        return Err(ApiEngineError::validation_error(
            "No valid calls to execute",
        ));
    }

    let chain = state.chains.get_chain(chain_id)?;

    // Execute multicall
    let multicall_results = execute_multicall(
        &request.read_options.multicall_address,
        multicall_calls,
        chain.provider(),
    )
    .await?;

    // Map multicall results back to original order
    let results =
        map_results_to_original_order(&preparation_results, &multicall_results, &call_indices);

    Ok((
        StatusCode::OK,
        Json(ReadResponse {
            result: ReadResults { results },
        }),
    ))
}

/// Execute the multicall and return results
async fn execute_multicall(
    multicall_address: &Address,
    multicall_calls: Vec<Call3>,
    provider: &RootProvider,
) -> Result<Vec<Result3>, ApiEngineError> {
    let multicall_call = aggregate3Call {
        calls: multicall_calls,
    };

    let call_request = AlloyTransactionRequest::default()
        .to(*multicall_address)
        .input(multicall_call.abi_encode().into());

    let result = provider
        .call(call_request)
        .await
        .map_err(|e| ApiEngineError::validation_error(&format!("Multicall failed: {}", e)))?;

    let decoded = aggregate3Call::abi_decode_returns(&result).map_err(|e| {
        ApiEngineError::validation_error(&format!("Failed to decode multicall result: {}", e))
    })?;

    Ok(decoded)
}

/// Map multicall results back to the original parameter order
fn map_results_to_original_order(
    preparation_results: &[Result<PreparedCall, ApiEngineError>],
    multicall_results: &[Result3],
    call_indices: &[usize],
) -> Vec<ReadResultItem> {
    let mut multicall_iter = multicall_results.iter();

    preparation_results
        .iter()
        .enumerate()
        .map(|(original_index, prep_result)| {
            match prep_result {
                Ok(prepared_call) => {
                    // This was a successful preparation, check if we have a multicall result
                    if call_indices.contains(&original_index) {
                        if let Some(multicall_result) = multicall_iter.next() {
                            process_multicall_result(multicall_result, prepared_call)
                        } else {
                            failed_result()
                        }
                    } else {
                        failed_result()
                    }
                }
                Err(_) => failed_result(), // Preparation failed
            }
        })
        .collect()
}

// You'll also need to add this to your ApiEngineError implementation
impl ApiEngineError {
    pub fn validation_error(message: &str) -> Self {
        EngineError::ValidationError {
            message: message.to_string(),
        }
        .into()
    }
}
