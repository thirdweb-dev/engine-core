// 8:12 PM - COLOCATION: Contract Read Operations

use alloy::dyn_abi::FunctionExt;
use alloy::primitives::{Address, ChainId, address};
use alloy::providers::RootProvider;
use alloy::{
    providers::Provider, rpc::types::eth::TransactionRequest as AlloyTransactionRequest, sol,
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
    error::EngineError,
};
use futures::future::join_all;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thirdweb_core::auth::ThirdwebAuth;

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

/// Options for reading from smart contracts
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
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
    #[schema(value_type = AddressDef)]
    pub multicall_address: Address,
    /// Optional address to use as the caller for view functions
    ///
    /// This can be useful for functions that return different values
    /// based on the caller's address or permissions
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<AddressDef>")]
    #[schema(value_type = Option<AddressDef>)]
    pub from: Option<Address>,
}

fn default_multicall_address() -> Address {
    MULTICALL3_DEFAULT_ADDRESS
}

/// Request to read from multiple smart contracts
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
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
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
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
/// Read from multiple smart contracts using multicall
pub async fn read_contract(
    State(state): State<EngineServerState>,
    OptionalRpcCredentialsExtractor(rpc_credentials): OptionalRpcCredentialsExtractor,
    EngineJson(request): EngineJson<ReadRequest>,
) -> Result<impl IntoResponse, ApiEngineError> {
    let auth: Option<ThirdwebAuth> = rpc_credentials.map(|creds| match creds {
        engine_core::chain::RpcCredentials::Thirdweb(auth) => auth,
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

    Ok((StatusCode::OK, Json(BatchResults { result: results })))
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
) -> Vec<BatchResultItem<ReadResultSuccessItem>> {
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
                format!("Failed to decode result: {}", e),
            ))
        }
    }
}
