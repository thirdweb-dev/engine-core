use std::fmt::Debug;

use crate::defs::AddressDef;
use alloy::{
    primitives::Address,
    transports::{
        RpcError as AlloyRpcError, TransportErrorKind, http::reqwest::header::InvalidHeaderValue,
    },
};
use alloy_signer_aws::AwsSignerError;
use aws_sdk_kms::error::SdkError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thirdweb_core::error::ThirdwebError;

use thiserror::Error;
use twmq::error::TwmqError;

use crate::chain::Chain;

/// Serializable version of Solana's RpcResponseErrorData
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SolanaRpcResponseErrorData {
    /// Empty error data
    Empty,
    /// Transaction simulation/preflight failed
    SendTransactionPreflightFailure {
        /// Error message from simulation
        #[serde(skip_serializing_if = "Option::is_none")]
        err: Option<String>,
        /// Logs from simulation
        #[serde(skip_serializing_if = "Option::is_none")]
        logs: Option<Vec<String>>,
        /// Accounts used in simulation
        #[serde(skip_serializing_if = "Option::is_none")]
        accounts: Option<serde_json::Value>,
        /// Units consumed during simulation
        #[serde(skip_serializing_if = "Option::is_none")]
        units_consumed: Option<u64>,
        /// Return data from the transaction
        #[serde(skip_serializing_if = "Option::is_none")]
        return_data: Option<serde_json::Value>,
    },
    /// Node is unhealthy
    NodeUnhealthy {
        #[serde(skip_serializing_if = "Option::is_none")]
        num_slots_behind: Option<u64>,
    },
}

/// Solana-specific RPC error types
#[derive(Debug, Error, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SolanaRpcErrorKind {
    /// IO error (network issues, connection problems)
    #[error("IO error: {message}")]
    Io {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        kind: Option<String>,
    },

    /// Reqwest HTTP error
    #[error("HTTP error: {message}")]
    Reqwest {
        message: String,
        #[serde(skip)]
        url: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        status: Option<u16>,
    },

    /// RPC protocol error (JSON-RPC error response)
    #[error("RPC protocol error (code {code}): {message}")]
    RpcError {
        code: i64,
        message: String,
        /// Structured error data from Solana RPC
        data: SolanaRpcResponseErrorData,
    },

    /// JSON serialization/deserialization error
    #[error("JSON error: {message}")]
    SerdeJson {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        line: Option<usize>,
        #[serde(skip_serializing_if = "Option::is_none")]
        column: Option<usize>,
    },

    /// Transaction error from Solana (e.g., insufficient funds, invalid signature)
    #[error("Transaction error: {error_type}")]
    TransactionError { error_type: String, message: String },

    /// Signing error
    #[error("Signing error: {message}")]
    SigningError { message: String },

    /// Custom error from middleware or validators
    #[error("Custom error: {message}")]
    Custom { message: String },

    /// Unknown/other error
    #[error("Unknown error: {message}")]
    Unknown { message: String },
}

#[derive(Debug, Error, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcErrorKind {
    /// Server returned an error response.
    #[error("server returned an error response: {0}")]
    ErrorResp(RpcErrorResponse),

    /// Server returned a null response when a non-null response was expected.
    #[error("server returned a null response when a non-null response was expected")]
    NullResp,

    /// Rpc server returned an unsupported feature.
    #[error("unsupported feature: {message}")]
    UnsupportedFeature { message: String },

    /// Returned when a local pre-processing step fails. This allows custom
    /// errors from local signers or request pre-processors.
    #[error("local usage error: {message}")]
    InternalError { message: String },

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

    #[error("Other transport error: {message}")]
    OtherTransportError { message: String },
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
#[serde(tag = "type")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ContractInteractionErrorKind {
    /// Unknown function referenced.
    #[error("unknown function: function {function_name} does not exist")]
    UnknownFunction {
        #[serde(rename = "functionName")]
        function_name: String,
    },

    /// Unknown function selector referenced.
    #[error("unknown function: function with selector {function_selector:?} does not exist")]
    UnknownSelector {
        #[serde(rename = "functionSelector")]
        function_selector: String,
    }, // Serialize as string instead of Selector

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
    #[error("ABI error: {message}")]
    AbiError { message: String },

    /// An error occurred interacting with a contract over RPC.
    #[error("transport error: {message}")]
    TransportError { message: String },

    /// An error occured while waiting for a pending transaction.
    #[error("pending transaction error: {message}")]
    PendingTransactionError { message: String },

    /// Error during contract function preparation (ABI resolution, parameter encoding)
    #[error("contract preparation failed: {message}")]
    PreparationFailed { message: String },

    /// Error during multicall execution
    #[error("multicall execution failed: {message}")]
    MulticallExecutionFailed { message: String },

    /// Error during result decoding
    #[error("result decoding failed: {message}")]
    ResultDecodingFailed { message: String },

    /// Parameter validation error
    #[error("parameter validation failed: {message}")]
    ParameterValidationFailed { message: String },

    /// Function resolution error
    #[error("function resolution failed: {message}")]
    FunctionResolutionFailed { message: String },
}

#[derive(Error, Debug, Serialize, Clone, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "type")]
pub enum EngineError {
    #[schema(title = "EVM RPC Error")]
    #[error("RPC error on chain {chain_id} at {rpc_url}: {message}")]
    RpcError {
        /// Detailed RPC error information
        chain_id: u64,
        rpc_url: String,
        message: String,
        kind: RpcErrorKind,
    },

    #[schema(title = "ERC4337 Paymster Error")]
    #[error("Paymaster error on chain {chain_id} at {rpc_url}: {message}")]
    PaymasterError {
        /// Detailed RPC error information
        chain_id: u64,
        rpc_url: String,
        message: String,
        kind: RpcErrorKind,
    },

    #[schema(title = "ERC4337 Bundler Error")]
    #[error("BundlerError error on chain {chain_id} at {rpc_url}: {message}")]
    BundlerError {
        /// Detailed RPC error information
        chain_id: u64,
        rpc_url: String,
        message: String,
        kind: RpcErrorKind,
    },

    #[schema(title = "Solana RPC Error")]
    #[error("Solana RPC error on {chain_id}: {message}")]
    SolanaRpcError {
        /// Solana chain identifier
        chain_id: String,
        /// Human-readable error message
        message: String,
        /// Structured error kind
        kind: SolanaRpcErrorKind,
    },

    #[schema(title = "Engine Vault KMS Error")]
    #[error("Error interaction with vault: {message}")]
    #[serde(rename_all = "camelCase")]
    VaultError { message: String },

    #[schema(title = "Engine IAW Service Error")]
    #[error("Error interaction with IAW service: {error}")]
    #[serde(rename_all = "camelCase")]
    IawError {
        #[from]
        error: thirdweb_core::iaw::IAWError,
    },

    #[schema(title = "RPC Configuration Error")]
    #[error("Bad RPC configuration: {message}")]
    RpcConfigError { message: String },

    #[schema(title = "EVM Contract Interaction Error")]
    #[error("Contract interaction error: {message}")]
    #[serde(rename_all = "camelCase")]
    ContractInteractionError {
        /// Contract address
        #[schema(value_type = Option<AddressDef>)]
        contract_address: Option<Address>,
        /// Chain ID
        chain_id: u64,
        /// Human-readable error message
        message: String,
        /// Specific error kind
        kind: ContractInteractionErrorKind,
    },

    #[schema(title = "Validation Error")]
    #[error("Validation error: {message}")]
    ValidationError { message: String },

    #[schema(title = "Thirdweb Services Error")]
    #[error("Thirdweb error: {message}")]
    ThirdwebError { message: String },

    #[schema(title = "AWS KMS Error")]
    #[error(transparent)]
    #[serde(rename_all = "camelCase")]
    AwsKmsSignerError {
        #[serde(flatten)]
        error: SerialisableAwsSignerError,
    },

    #[schema(title = "Engine Internal Error")]
    #[error("Internal error: {message}")]
    InternalError { message: String },
}

#[derive(thiserror::Error, Debug, Serialize, Clone, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "type")]
pub enum SerialisableAwsSdkError {
    /// The request failed during construction. It was not dispatched over the network.
    #[error("Construction failure: {message}")]
    ConstructionFailure { message: String },

    /// The request failed due to a timeout. The request MAY have been sent and received.
    #[error("Timeout error: {message}")]
    TimeoutError { message: String },

    /// The request failed during dispatch. An HTTP response was not received. The request MAY
    /// have been sent.
    #[error("Dispatch failure: {message}")]
    DispatchFailure { message: String },

    /// A response was received but it was not parseable according the the protocol (for example
    /// the server hung up without sending a complete response)
    #[error("Response error: {message}")]
    ResponseError { message: String },

    /// An error response was received from the service
    #[error("Service error: {message}")]
    ServiceError { message: String },

    #[error("Other error: {message}")]
    Other { message: String },
}

#[derive(Error, Debug, Serialize, Clone, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "type")]
pub enum SerialisableAwsSignerError {
    /// Thrown when the AWS KMS API returns a signing error.
    #[error(transparent)]
    Sign {
        aws_sdk_error: SerialisableAwsSdkError,
    },

    /// Thrown when the AWS KMS API returns an error.
    #[error(transparent)]
    GetPublicKey {
        aws_sdk_error: SerialisableAwsSdkError,
    },

    /// [`ecdsa`] error.
    #[error("ECDSA error: {message}")]
    K256 { message: String },

    /// [`spki`] error.
    #[error("SPKI error: {message}")]
    Spki { message: String },

    /// [`hex`](mod@hex) error.
    #[error("Hex error: {message}")]
    Hex { message: String },

    /// Thrown when the AWS KMS API returns a response without a signature.
    #[error("signature not found in response")]
    SignatureNotFound,

    /// Thrown when the AWS KMS API returns a response without a public key.
    #[error("public key not found in response")]
    PublicKeyNotFound,

    #[error("Unknown error: {message}")]
    Unknown { message: String },

    #[error("Signature recovery failed")]
    SignatureRecoveryFailed,
}

impl<T: Debug> From<SdkError<T>> for SerialisableAwsSdkError {
    fn from(err: SdkError<T>) -> Self {
        match err {
            SdkError::ConstructionFailure(err) => SerialisableAwsSdkError::ConstructionFailure {
                message: format!("{err:?}"),
            },
            SdkError::TimeoutError(err) => SerialisableAwsSdkError::TimeoutError {
                message: format!("{err:?}"),
            },
            SdkError::DispatchFailure(err) => SerialisableAwsSdkError::DispatchFailure {
                message: format!("{err:?}"),
            },
            SdkError::ResponseError(err) => SerialisableAwsSdkError::ResponseError {
                message: format!("{err:?}"),
            },
            SdkError::ServiceError(err) => SerialisableAwsSdkError::ServiceError {
                message: format!("{err:?}"),
            },
            _ => SerialisableAwsSdkError::Other {
                message: format!("{err:?}"),
            },
        }
    }
}

impl From<AwsSignerError> for EngineError {
    fn from(err: AwsSignerError) -> Self {
        match err {
            AwsSignerError::Sign(err) => EngineError::AwsKmsSignerError {
                error: SerialisableAwsSignerError::Sign {
                    aws_sdk_error: err.into(),
                },
            },
            AwsSignerError::GetPublicKey(err) => EngineError::AwsKmsSignerError {
                error: SerialisableAwsSignerError::GetPublicKey {
                    aws_sdk_error: err.into(),
                },
            },
            AwsSignerError::K256(err) => EngineError::AwsKmsSignerError {
                error: SerialisableAwsSignerError::K256 {
                    message: err.to_string(),
                },
            },
            AwsSignerError::Spki(err) => EngineError::AwsKmsSignerError {
                error: SerialisableAwsSignerError::Spki {
                    message: err.to_string(),
                },
            },
            AwsSignerError::Hex(err) => EngineError::AwsKmsSignerError {
                error: SerialisableAwsSignerError::Hex {
                    message: err.to_string(),
                },
            },
            AwsSignerError::SignatureNotFound => EngineError::AwsKmsSignerError {
                error: SerialisableAwsSignerError::SignatureNotFound,
            },
            AwsSignerError::PublicKeyNotFound => EngineError::AwsKmsSignerError {
                error: SerialisableAwsSignerError::PublicKeyNotFound,
            },
            AwsSignerError::SignatureRecoveryFailed => EngineError::AwsKmsSignerError {
                error: SerialisableAwsSignerError::SignatureRecoveryFailed,
            },
        }
    }
}

impl From<vault_sdk::error::VaultError> for EngineError {
    fn from(err: vault_sdk::error::VaultError) -> Self {
        let message = match &err {
            vault_sdk::error::VaultError::EnclaveError {
                code,
                message,
                details,
            } => match details {
                Some(details) => format!("Enclave error: {code} - {message} - details: {details}"),
                None => format!("Enclave error: {code} - {message}"),
            },
            _ => err.to_string(),
        };

        EngineError::VaultError { message }
    }
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
            kind: ContractInteractionErrorKind::PreparationFailed { message },
        }
    }

    pub fn contract_multicall_error(chain_id: u64, message: String) -> Self {
        EngineError::ContractInteractionError {
            contract_address: None,
            chain_id,
            message: message.clone(),
            kind: ContractInteractionErrorKind::MulticallExecutionFailed { message },
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
            kind: ContractInteractionErrorKind::ResultDecodingFailed { message },
        }
    }
}

pub trait AlloyRpcErrorToEngineError {
    fn to_engine_error(&self, chain: &impl Chain) -> EngineError;
    fn to_engine_bundler_error(&self, chain: &impl Chain) -> EngineError;
    fn to_engine_paymaster_error(&self, chain: &impl Chain) -> EngineError;
}

/// Trait for converting Solana RPC errors to EngineError
pub trait SolanaRpcErrorToEngineError {
    fn to_engine_solana_error(&self, chain_id: &str) -> EngineError;
}

// Implementation for Solana client errors
impl SolanaRpcErrorToEngineError for solana_client::client_error::ClientError {
    fn to_engine_solana_error(&self, chain_id: &str) -> EngineError {
        use solana_client::client_error::ClientErrorKind;

        let kind = match self.kind() {
            ClientErrorKind::Io(err) => SolanaRpcErrorKind::Io {
                message: err.to_string(),
                kind: Some(format!("{:?}", err.kind())),
            },
            ClientErrorKind::Reqwest(err) => {
                let status = err.status().map(|s| s.as_u16());
                let url = err.url().map(|u| u.to_string());
                SolanaRpcErrorKind::Reqwest {
                    message: err.to_string(),
                    url,
                    status,
                }
            }
            ClientErrorKind::RpcError(rpc_err) => {
                use solana_client::rpc_request::{RpcError, RpcResponseErrorData};
                match rpc_err {
                    RpcError::RpcResponseError {
                        code,
                        message,
                        data,
                    } => {
                        let structured_data = match data {
                            RpcResponseErrorData::Empty => SolanaRpcResponseErrorData::Empty,
                            RpcResponseErrorData::SendTransactionPreflightFailure(result) => {
                                SolanaRpcResponseErrorData::SendTransactionPreflightFailure {
                                    err: result.err.as_ref().map(|e| format!("{:?}", e)),
                                    logs: result.logs.clone(),
                                    accounts: result.accounts.as_ref().map(|a| {
                                        serde_json::to_value(a).unwrap_or(serde_json::Value::Null)
                                    }),
                                    units_consumed: result.units_consumed,
                                    return_data: result.return_data.as_ref().map(|d| {
                                        serde_json::to_value(d).unwrap_or(serde_json::Value::Null)
                                    }),
                                }
                            }
                            RpcResponseErrorData::NodeUnhealthy { num_slots_behind } => {
                                SolanaRpcResponseErrorData::NodeUnhealthy {
                                    num_slots_behind: *num_slots_behind,
                                }
                            }
                        };
                        SolanaRpcErrorKind::RpcError {
                            code: *code,
                            message: message.clone(),
                            data: structured_data,
                        }
                    }
                    RpcError::RpcRequestError(msg) => SolanaRpcErrorKind::RpcError {
                        code: -32600,
                        message: msg.clone(),
                        data: SolanaRpcResponseErrorData::Empty,
                    },
                    RpcError::ParseError(msg) => SolanaRpcErrorKind::SerdeJson {
                        message: msg.clone(),
                        line: None,
                        column: None,
                    },
                    RpcError::ForUser(msg) => SolanaRpcErrorKind::Custom {
                        message: msg.clone(),
                    },
                }
            }
            ClientErrorKind::SerdeJson(err) => {
                let line = err.line();
                let column = err.column();
                SolanaRpcErrorKind::SerdeJson {
                    message: err.to_string(),
                    line: Some(line),
                    column: Some(column),
                }
            }
            ClientErrorKind::SigningError(err) => SolanaRpcErrorKind::SigningError {
                message: err.to_string(),
            },
            ClientErrorKind::TransactionError(err) => {
                // Extract structured transaction error information
                let error_type = format!("{:?}", err)
                    .split('(')
                    .next()
                    .unwrap_or("Unknown")
                    .to_string();
                SolanaRpcErrorKind::TransactionError {
                    error_type,
                    message: err.to_string(),
                }
            }
            ClientErrorKind::Custom(msg) => SolanaRpcErrorKind::Custom {
                message: msg.clone(),
            },
            _ => SolanaRpcErrorKind::Unknown {
                message: self.to_string(),
            },
        };

        EngineError::SolanaRpcError {
            chain_id: chain_id.to_string(),
            message: self.to_string(),
            kind,
        }
    }
}

fn to_engine_rpc_error_kind(err: &AlloyRpcError<TransportErrorKind>) -> RpcErrorKind {
    match err {
        AlloyRpcError::ErrorResp(err) => RpcErrorKind::ErrorResp(RpcErrorResponse {
            code: err.code,
            message: err.message.to_string(),
            data: err.data.as_ref().map(|data| data.to_string()),
        }),
        AlloyRpcError::NullResp => RpcErrorKind::NullResp,
        AlloyRpcError::UnsupportedFeature(feature) => RpcErrorKind::UnsupportedFeature {
            message: feature.to_string(),
        },
        AlloyRpcError::LocalUsageError(err) => RpcErrorKind::InternalError {
            message: err.to_string(),
        },
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
            TransportErrorKind::Custom(err) => RpcErrorKind::OtherTransportError {
                message: err.to_string(),
            },
            _ => RpcErrorKind::OtherTransportError {
                message: err.to_string(),
            },
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
                format!("Unknown function: {name}"),
                ContractInteractionErrorKind::UnknownFunction {
                    function_name: name,
                },
            ),
            alloy::contract::Error::UnknownSelector(selector) => (
                format!("Unknown selector: {selector:?}"),
                ContractInteractionErrorKind::UnknownSelector {
                    function_selector: format!("{selector:?}"),
                },
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
                format!("Zero data returned from contract call to {function}"),
                ContractInteractionErrorKind::ZeroData {
                    function,
                    message: err.to_string(),
                },
            ),
            alloy::contract::Error::AbiError(err) => (
                format!("ABI error: {err}"),
                ContractInteractionErrorKind::AbiError {
                    message: err.to_string(),
                },
            ),
            alloy::contract::Error::TransportError(err) => (
                format!("Transport error: {err}"),
                ContractInteractionErrorKind::TransportError {
                    message: err.to_string(),
                },
            ),
            alloy::contract::Error::PendingTransactionError(err) => (
                format!("Pending transaction error: {err}"),
                ContractInteractionErrorKind::PendingTransactionError {
                    message: err.to_string(),
                },
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

impl From<ThirdwebError> for EngineError {
    fn from(error: ThirdwebError) -> Self {
        EngineError::ThirdwebError {
            message: error.to_string(),
        }
    }
}

impl From<twmq::redis::RedisError> for EngineError {
    fn from(error: twmq::redis::RedisError) -> Self {
        EngineError::InternalError {
            message: error.to_string(),
        }
    }
}

impl From<TwmqError> for EngineError {
    fn from(error: TwmqError) -> Self {
        EngineError::InternalError {
            message: error.to_string(),
        }
    }
}
