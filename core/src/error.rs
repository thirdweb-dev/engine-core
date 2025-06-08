use crate::defs::AddressDef;
use alloy::{
    primitives::Address,
    transports::{
        RpcError as AlloyRpcError, TransportErrorKind, http::reqwest::header::InvalidHeaderValue,
    },
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thirdweb_core::error::ThirdwebError;
use thiserror::Error;
use twmq::error::TwmqError;

use crate::chain::Chain;

#[derive(Debug, Error, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
pub enum RpcErrorKind {
    /// Server returned an error response.
    #[error("server returned an error response: {0}")]
    ErrorResp(RpcErrorResponse),

    /// Server returned a null response when a non-null response was expected.
    #[error("server returned a null response when a non-null response was expected")]
    NullResp,

    /// Rpc server returned an unsupported feature.
    #[error("unsupported feature: {0}")]
    UnsupportedFeature(String),

    /// Returned when a local pre-processing step fails. This allows custom
    /// errors from local signers or request pre-processors.
    #[error("local usage error: {0}")]
    InternalError(String),

    /// JSON serialization error.
    #[error("serialization error: {message}")]
    SerError {
        /// The underlying serde_json error.
        // To avoid accidentally confusing ser and deser errors, we do not use
        // the `#[from]` tag.
        message: String, // sourced from serde_json::Error
    },
    /// JSON deserialization error.
    #[error("deserialization error: {message}, text: {text}")]
    DeserError {
        /// The underlying serde_json error.
        // To avoid accidentally confusing ser and deser errors, we do not use
        // the `#[from]` tag.
        message: String, // sourced from serde_json::Error
        /// For deser errors, the text that failed to deserialize.
        text: String,
    },

    #[error("HTTP error {status}")]
    TransportHttpError { status: u16, body: String },

    #[error("Other transport error: {0}")]
    OtherTransportError(String),
}

#[derive(Debug, Serialize, Deserialize, Clone, JsonSchema, utoipa::ToSchema)]
pub struct RpcErrorResponse {
    /// The error code.
    pub code: i64,
    /// The error message (if any).
    pub message: String,
    /// The error data (if any).
    pub data: Option<String>,
}

impl RpcErrorResponse {
    pub fn as_display(&self) -> String {
        format!(
            "code {}: {}{}",
            self.code,
            self.message,
            self.data
                .as_ref()
                .map(|data| format!(", data: {data}"))
                .unwrap_or_default()
        )
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct RpcErrorInfo {
    /// The chain ID where the error occurred
    pub chain_id: u64,

    /// The provider URL
    pub provider_url: String,

    /// Human-readable error message
    pub message: String,

    /// The error response payload as a SerdeValue
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response: Option<RpcErrorResponse>,
}

/// A serializable contract interaction error type
#[derive(Debug, Error, Serialize, Deserialize, Clone, JsonSchema, utoipa::ToSchema)]
pub enum ContractInteractionErrorKind {
    /// Unknown function referenced.
    #[error("unknown function: function {0} does not exist")]
    UnknownFunction(String),

    /// Unknown function selector referenced.
    #[error("unknown function: function with selector {0:?} does not exist")]
    UnknownSelector(String), // Serialize as string instead of Selector

    /// Called `deploy` with a transaction that is not a deployment transaction.
    #[error("transaction is not a deployment transaction")]
    NotADeploymentTransaction,

    /// `contractAddress` was not found in the deployment transaction's receipt.
    #[error("missing `contractAddress` from deployment transaction receipt")]
    ContractNotDeployed,

    /// The contract returned no data.
    #[error(
        "contract call to `{function}` returned no data (\"0x\"); the called address might not be a contract"
    )]
    ZeroData {
        function: String,
        message: String, // From AbiError
    },

    /// An error occurred ABI encoding or decoding.
    #[error("ABI error: {0}")]
    AbiError(String),

    /// An error occurred interacting with a contract over RPC.
    #[error("transport error: {0}")]
    TransportError(String),

    /// An error occured while waiting for a pending transaction.
    #[error("pending transaction error: {0}")]
    PendingTransactionError(String),

    /// Error during contract function preparation (ABI resolution, parameter encoding)
    #[error("contract preparation failed: {0}")]
    PreparationFailed(String),

    /// Error during multicall execution
    #[error("multicall execution failed: {0}")]
    MulticallExecutionFailed(String),

    /// Error during result decoding
    #[error("result decoding failed: {0}")]
    ResultDecodingFailed(String),

    /// Parameter validation error
    #[error("parameter validation failed: {0}")]
    ParameterValidationFailed(String),

    /// Function resolution error
    #[error("function resolution failed: {0}")]
    FunctionResolutionFailed(String),
}

#[derive(Error, Debug, Serialize, Clone, Deserialize, JsonSchema, utoipa::ToSchema)]
pub enum EngineError {
    #[error("RPC error on chain {chain_id} at {rpc_url}: {message}")]
    RpcError {
        /// Detailed RPC error information
        chain_id: u64,
        rpc_url: String,
        message: String,
        kind: RpcErrorKind,
    },

    #[error("Paymaster error on chain {chain_id} at {rpc_url}: {message}")]
    PaymasterError {
        /// Detailed RPC error information
        chain_id: u64,
        rpc_url: String,
        message: String,
        kind: RpcErrorKind,
    },

    #[error("BundlerError error on chain {chain_id} at {rpc_url}: {message}")]
    BundlerError {
        /// Detailed RPC error information
        chain_id: u64,
        rpc_url: String,
        message: String,
        kind: RpcErrorKind,
    },

    #[error("Error interaction with vault: {message}")]
    VaultError { message: String },

    #[error("Bad RPC configuration: {message}")]
    RpcConfigError { message: String },

    #[error("Contract interaction error: {message}")]
    ContractInteractionError {
        /// Contract address
        #[schemars(with = "Option<AddressDef>")]
        #[schema(value_type = Option<AddressDef>)]
        contract_address: Option<Address>,
        /// Chain ID
        chain_id: u64,
        /// Human-readable error message
        message: String,
        /// Specific error kind
        kind: ContractInteractionErrorKind,
    },

    #[error("Validation error: {message}")]
    ValidationError { message: String },

    #[error("Thirdweb error: {0}")]
    ThirdwebError(#[from] ThirdwebError),

    #[error("Internal error: {0}")]
    InternalError(String),
}

impl From<InvalidHeaderValue> for EngineError {
    fn from(err: InvalidHeaderValue) -> Self {
        EngineError::ValidationError {
            message: err.to_string(),
        }
    }
}

impl EngineError {
    pub fn contract_preparation_error(
        contract_address: Option<Address>,
        chain_id: u64,
        message: String,
    ) -> Self {
        EngineError::ContractInteractionError {
            contract_address,
            chain_id,
            message: message.clone(),
            kind: ContractInteractionErrorKind::PreparationFailed(message),
        }
    }

    pub fn contract_multicall_error(chain_id: u64, message: String) -> Self {
        EngineError::ContractInteractionError {
            contract_address: None,
            chain_id,
            message: message.clone(),
            kind: ContractInteractionErrorKind::MulticallExecutionFailed(message),
        }
    }

    pub fn contract_decoding_error(
        contract_address: Option<Address>,
        chain_id: u64,
        message: String,
    ) -> Self {
        EngineError::ContractInteractionError {
            contract_address,
            chain_id,
            message: message.clone(),
            kind: ContractInteractionErrorKind::ResultDecodingFailed(message),
        }
    }
}

pub trait AlloyRpcErrorToEngineError {
    fn to_engine_error(&self, chain: &impl Chain) -> EngineError;
    fn to_engine_bundler_error(&self, chain: &impl Chain) -> EngineError;
    fn to_engine_paymaster_error(&self, chain: &impl Chain) -> EngineError;
}

fn to_engine_rpc_error_kind(err: &AlloyRpcError<TransportErrorKind>) -> RpcErrorKind {
    match err {
        AlloyRpcError::ErrorResp(err) => RpcErrorKind::ErrorResp(RpcErrorResponse {
            code: err.code,
            message: err.message.to_string(),
            data: err.data.as_ref().map(|data| data.to_string()),
        }),
        AlloyRpcError::NullResp => RpcErrorKind::NullResp,
        AlloyRpcError::UnsupportedFeature(feature) => {
            RpcErrorKind::UnsupportedFeature(feature.to_string())
        }
        AlloyRpcError::LocalUsageError(err) => RpcErrorKind::InternalError(err.to_string()),
        AlloyRpcError::SerError(err) => RpcErrorKind::SerError {
            message: err.to_string(),
        },
        AlloyRpcError::DeserError { err, text } => RpcErrorKind::DeserError {
            message: err.to_string(),
            text: text.to_string(),
        },
        AlloyRpcError::Transport(err) => match err {
            TransportErrorKind::HttpError(err) => RpcErrorKind::TransportHttpError {
                status: err.status,
                body: err.body.to_string(),
            },
            TransportErrorKind::Custom(err) => RpcErrorKind::OtherTransportError(err.to_string()),
            _ => RpcErrorKind::OtherTransportError(err.to_string()),
        },
    }
}

impl AlloyRpcErrorToEngineError for AlloyRpcError<TransportErrorKind> {
    fn to_engine_error(&self, chain: &impl Chain) -> EngineError {
        EngineError::RpcError {
            chain_id: chain.chain_id(),
            rpc_url: chain.rpc_url().to_string(),
            message: self.to_string(),
            kind: to_engine_rpc_error_kind(self),
        }
    }

    fn to_engine_bundler_error(&self, chain: &impl Chain) -> EngineError {
        EngineError::BundlerError {
            chain_id: chain.chain_id(),
            rpc_url: chain.bundler_url().to_string(),
            message: self.to_string(),
            kind: to_engine_rpc_error_kind(self),
        }
    }
    fn to_engine_paymaster_error(&self, chain: &impl Chain) -> EngineError {
        EngineError::PaymasterError {
            chain_id: chain.chain_id(),
            rpc_url: chain.paymaster_url().to_string(),
            message: self.to_string(),
            kind: to_engine_rpc_error_kind(self),
        }
    }
}

// Conversion trait for the original error type
pub trait ContractErrorToEngineError {
    fn to_engine_error(self, chain_id: u64, contract_address: Option<Address>) -> EngineError;
}

// Implementation for the original Error type
impl ContractErrorToEngineError for alloy::contract::Error {
    fn to_engine_error(self, chain_id: u64, contract_address: Option<Address>) -> EngineError {
        let (message, kind) = match self {
            alloy::contract::Error::UnknownFunction(name) => (
                format!("Unknown function: {}", name),
                ContractInteractionErrorKind::UnknownFunction(name),
            ),
            alloy::contract::Error::UnknownSelector(selector) => (
                format!("Unknown selector: {:?}", selector),
                ContractInteractionErrorKind::UnknownSelector(format!("{:?}", selector)),
            ),
            alloy::contract::Error::NotADeploymentTransaction => (
                "Transaction is not a deployment transaction".to_string(),
                ContractInteractionErrorKind::NotADeploymentTransaction,
            ),
            alloy::contract::Error::ContractNotDeployed => (
                "Contract not deployed - missing contractAddress in receipt".to_string(),
                ContractInteractionErrorKind::ContractNotDeployed,
            ),
            alloy::contract::Error::ZeroData(function, err) => (
                format!("Zero data returned from contract call to {}", function),
                ContractInteractionErrorKind::ZeroData {
                    function,
                    message: err.to_string(),
                },
            ),
            alloy::contract::Error::AbiError(err) => (
                format!("ABI error: {}", err),
                ContractInteractionErrorKind::AbiError(err.to_string()),
            ),
            alloy::contract::Error::TransportError(err) => (
                format!("Transport error: {}", err),
                ContractInteractionErrorKind::TransportError(err.to_string()),
            ),
            alloy::contract::Error::PendingTransactionError(err) => (
                format!("Pending transaction error: {}", err),
                ContractInteractionErrorKind::PendingTransactionError(err.to_string()),
            ),
        };

        EngineError::ContractInteractionError {
            contract_address,
            chain_id,
            message,
            kind,
        }
    }
}

impl From<twmq::redis::RedisError> for EngineError {
    fn from(error: twmq::redis::RedisError) -> Self {
        EngineError::InternalError(error.to_string())
    }
}

impl From<TwmqError> for EngineError {
    fn from(error: TwmqError) -> Self {
        EngineError::InternalError(error.to_string())
    }
}
