use alloy::hex;
use alloy::json_abi::Param;
use alloy::{
    dyn_abi::{DynSolType, DynSolValue, JsonAbiExt},
    json_abi::{Function, JsonAbi},
    primitives::{Address, Bytes, ChainId, U256},
};
use engine_core::{defs::AddressDef, error::EngineError, transaction::InnerTransaction};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thirdweb_core::{
    abi::{ThirdwebAbiService, ThirdwebContractOptions},
    auth::ThirdwebAuth,
};

/// Represents a contract function call with parameters
///
/// This is the base type used by all contract interaction endpoints.
/// It supports both function names and full function signatures, with
/// automatic ABI resolution when needed.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ContractCall {
    /// The address of the smart contract to call
    #[schemars(with = "AddressDef")]
    #[schema(value_type = AddressDef)]
    pub contract_address: Address,
    /// The function to call - can be a name like "transfer" or full signature like "transfer(address,uint256)"
    pub method: String,
    /// Array of parameters to pass to the function
    pub params: Vec<JsonValue>,
    /// Optional ABI to use instead of fetching from contract verification services
    #[schemars(with = "Option<JsonValue>")]
    #[schema(value_type = Option<JsonValue>)]
    pub abi: Option<JsonAbi>,
}

/// Result of preparing a contract call
///
/// Contains the encoded call data and function metadata needed for execution
#[derive(Debug, Clone)]
pub struct PreparedContractCall {
    /// The contract address to call
    pub target: Address,
    /// The encoded function call data
    pub call_data: Bytes,
    /// The resolved function definition
    pub function: Function,
}

/// Result of a contract operation (success or failure with preserved error)
#[derive(Debug, Clone)]
pub enum ContractOperationResult<T> {
    Success(T),
    Failure(EngineError),
}

impl ContractCall {
    /// Determines if the method string is definitely just a function name (no signature)
    fn is_definitely_function_name_only(method: &str) -> bool {
        let trimmed = method.trim();
        !trimmed.contains('(') && !trimmed.starts_with("function ")
    }

    /// Tries to parse as a function signature using alloy's dyn abi
    fn try_parse_as_signature(method: &str) -> Option<Function> {
        Function::parse(method).ok()
    }

    /// Resolves the function using the optimal strategy
    pub async fn resolve_function(
        &self,
        abi_service: &ThirdwebAbiService,
        chain_id: ChainId,
        auth: Option<ThirdwebAuth>,
    ) -> Result<Function, EngineError> {
        if Self::is_definitely_function_name_only(&self.method) {
            return self.resolve_from_abi(abi_service, chain_id, auth).await;
        }

        if let Some(function) = Self::try_parse_as_signature(&self.method) {
            if function.inputs.len() == self.params.len() {
                return Ok(function);
            } else {
                return Err(EngineError::contract_preparation_error(
                    Some(self.contract_address),
                    chain_id,
                    format!(
                        "Parameter count mismatch: signature expects {}, got {}",
                        function.inputs.len(),
                        self.params.len()
                    ),
                ));
            }
        }

        self.resolve_from_abi(abi_service, chain_id, auth).await
    }

    /// Resolves function from ABI with proper overload handling
    async fn resolve_from_abi(
        &self,
        abi_service: &ThirdwebAbiService,
        chain_id: ChainId,
        auth: Option<ThirdwebAuth>,
    ) -> Result<Function, EngineError> {
        let abi = self.get_abi(abi_service, chain_id, auth).await?;
        let function_name = self.extract_function_name(&self.method)?;

        let functions: Vec<&Function> = abi
            .functions()
            .filter(|f| f.name == function_name)
            .collect();

        if functions.is_empty() {
            return Err(EngineError::contract_preparation_error(
                Some(self.contract_address),
                chain_id,
                format!("Function '{}' not found in contract ABI", function_name),
            ));
        }

        let matching_functions: Vec<&Function> = functions
            .into_iter()
            .filter(|f| f.inputs.len() == self.params.len())
            .collect();

        match matching_functions.len() {
            0 => Err(EngineError::contract_preparation_error(
                Some(self.contract_address),
                chain_id,
                format!(
                    "No overload of function '{}' matches {} parameters",
                    function_name,
                    self.params.len()
                ),
            )),
            1 => Ok(matching_functions[0].clone()),
            _ => self.find_working_overload(matching_functions, chain_id),
        }
    }

    /// Tries each function overload until one works for parameter encoding
    fn find_working_overload(
        &self,
        candidates: Vec<&Function>,
        chain_id: ChainId,
    ) -> Result<Function, EngineError> {
        for function in candidates {
            if self.encode_parameters(function, chain_id).is_ok() {
                return Ok(function.clone());
            }
        }

        Err(EngineError::contract_preparation_error(
            Some(self.contract_address),
            chain_id,
            "No function overload could successfully encode the provided parameters".to_string(),
        ))
    }

    /// Gets ABI either from provided or from service
    async fn get_abi(
        &self,
        abi_service: &ThirdwebAbiService,
        chain_id: ChainId,
        auth: Option<ThirdwebAuth>,
    ) -> Result<JsonAbi, EngineError> {
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
            EngineError::contract_preparation_error(
                Some(self.contract_address),
                chain_id,
                format!(
                    "Failed to fetch ABI for contract {}: {}",
                    self.contract_address, e
                ),
            )
        })?;

        Ok(abi)
    }

    /// Extracts function name from method string
    fn extract_function_name(&self, method: &str) -> Result<String, EngineError> {
        let trimmed = method.trim();

        if let Some(after_function) = trimmed.strip_prefix("function ") {
            if let Some(paren_pos) = after_function.find('(') {
                return Ok(after_function[..paren_pos].trim().to_string());
            }
        } else if let Some(paren_pos) = trimmed.find('(') {
            return Ok(trimmed[..paren_pos].trim().to_string());
        } else {
            return Ok(trimmed.to_string());
        }

        Err(EngineError::ValidationError {
            message: format!("Invalid method format: {}", method),
        })
    }

    fn json_to_sol(
        json_values: &[JsonValue],
        json_abi_params: &[Param],
    ) -> Result<Vec<DynSolValue>, String> {
        if json_values.len() != json_abi_params.len() {
            return Err(format!(
                "Parameter count mismatch: expected {}, got {}",
                json_abi_params.len(),
                json_values.len()
            ));
        }

        let mut parsed_params = Vec::new();

        for (json_value, json_abi_param) in json_values.iter().zip(json_abi_params.iter()) {
            if json_abi_param.is_complex_type() {
                let json_value = json_value
                    .as_array()
                    .ok_or_else(|| "Expected array for complex type".to_string())?;

                let dyn_sol_value = Self::json_to_sol(json_value, &json_abi_param.components)?;

                parsed_params.push(DynSolValue::Tuple(dyn_sol_value));
            } else {
                let sol_type: DynSolType = json_abi_param
                    .ty
                    .parse()
                    .map_err(|e| format!("Invalid Solidity type '{}': {}", json_abi_param.ty, e))?;

                let parsed_value: DynSolValue = sol_type
                    .coerce_json(json_value)
                    .map_err(|e| format!("Failed to parse parameter as DynSolValue: {}", e))?;

                if !parsed_value.matches(&sol_type) {
                    return Err(format!(
                        "Parameter type mismatch: expected {}, got {:?}",
                        json_abi_param.ty,
                        parsed_value.as_type()
                    ));
                }

                parsed_params.push(parsed_value);
            }
        }

        Ok(parsed_params)
    }

    /// Encodes parameters using serde to directly deserialize into DynSolValue
    pub fn encode_parameters(
        &self,
        function: &Function,
        chain_id: ChainId,
    ) -> Result<Vec<u8>, EngineError> {
        if self.params.len() != function.inputs.len() {
            return Err(EngineError::contract_preparation_error(
                Some(self.contract_address),
                chain_id,
                format!(
                    "Parameter count mismatch: expected {}, got {}",
                    function.inputs.len(),
                    self.params.len()
                ),
            ));
        }

        let parsed_params = Self::json_to_sol(&self.params, &function.inputs).map_err(|e| {
            EngineError::contract_preparation_error(Some(self.contract_address), chain_id, e)
        })?;

        function.abi_encode_input(&parsed_params).map_err(|e| {
            EngineError::contract_preparation_error(
                Some(self.contract_address),
                chain_id,
                format!("Failed to encode function call: {}", e),
            )
        })
    }

    /// Prepares the contract call by encoding the function call
    pub async fn prepare_call(
        &self,
        abi_service: &ThirdwebAbiService,
        chain_id: ChainId,
        auth: Option<ThirdwebAuth>,
    ) -> Result<PreparedContractCall, EngineError> {
        let function = self.resolve_function(abi_service, chain_id, auth).await?;
        let call_data = self.encode_parameters(&function, chain_id)?;

        Ok(PreparedContractCall {
            target: self.contract_address,
            call_data: call_data.into(),
            function,
        })
    }

    /// Prepares the contract call and returns the result with error tracking
    pub async fn prepare_call_with_error_tracking(
        &self,
        abi_service: &ThirdwebAbiService,
        chain_id: ChainId,
        auth: Option<ThirdwebAuth>,
    ) -> ContractOperationResult<PreparedContractCall> {
        match self.prepare_call(abi_service, chain_id, auth).await {
            Ok(prepared_call) => ContractOperationResult::Success(prepared_call),
            Err(e) => ContractOperationResult::Failure(e),
        }
    }

    /// Convert to InnerTransaction with optional value
    pub fn to_inner_transaction(
        &self,
        prepared: &PreparedContractCall,
        value: U256,
    ) -> InnerTransaction {
        InnerTransaction {
            to: Some(prepared.target),
            data: prepared.call_data.clone(),
            value,
            gas_limit: None,
            transaction_type_data: None,
        }
    }
}

/// Converts DynSolValue back to JSON for response
pub fn dyn_sol_value_to_json(value: &DynSolValue) -> JsonValue {
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
