use crate::defs::AddressDef;
use alloy::eips::eip7702::SignedAuthorization;
use alloy::primitives::{Address, U256};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// ### EOA Execution Options
/// This struct configures EOA (Externally Owned Account) direct execution.
///
/// EOA execution sends transactions directly from an EOA address without
/// smart contract abstraction. This is the most basic form of transaction
/// execution and is suitable for simple transfers and contract interactions.
///
/// ### Use Cases
/// - Direct ETH transfers
/// - Simple contract interactions
/// - Gas-efficient transactions
/// - When smart account features are not needed
///
/// ### Features
/// - Direct transaction execution from EOA
/// - Automatic nonce management
/// - Gas price optimization
/// - Transaction confirmation tracking
/// - Retry and recovery mechanisms
/// - Support for EIP-1559, EIP-2930, and Legacy transactions
/// - Support for EIP-7702 delegated transactions
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct EoaExecutionOptions {
    /// The EOA address to send transactions from
    /// This account must have sufficient balance to pay for gas and transaction value
    #[schemars(with = "AddressDef")]
    #[schema(value_type = AddressDef)]
    pub from: Address,

    /// The gas limit to use for the transaction
    /// If not provided, the system will auto-detect the best gas limit
    #[schemars(with = "Option<u64>")]
    #[schema(value_type = Option<u64>)]
    pub gas_limit: Option<u64>,

    // /// Maximum number of in-flight transactions for this EOA
    // /// Controls how many transactions can be pending confirmation at once
    // /// Defaults to 100 if not specified
    // #[serde(default = "default_max_inflight")]
    // pub max_inflight: u64,

    // /// Maximum number of recycled nonces to keep
    // /// When transactions fail, their nonces are recycled for reuse
    // /// Defaults to 50 if not specified
    // #[serde(default = "default_max_recycled_nonces")]
    // pub max_recycled_nonces: u64,
    /// Transaction type-specific data for gas configuration
    /// If not provided, the system will auto-detect the best transaction type
    #[serde(flatten)]
    pub transaction_type_data: Option<EoaTransactionTypeData>,
}

/// EOA Transaction type-specific data for different EIP standards
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, utoipa::ToSchema)]
#[serde(untagged)]
pub enum EoaTransactionTypeData {
    /// EIP-7702 transaction with authorization list and EIP-1559 gas pricing
    Eip7702(EoaSend7702JobData),
    /// EIP-1559 transaction with priority fee and max fee per gas
    Eip1559(EoaSend1559JobData),
    /// Legacy transaction with simple gas price
    Legacy(EoaSendLegacyJobData),
}

/// EIP-7702 transaction configuration
/// Allows delegation of EOA to smart contract logic temporarily
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct EoaSend7702JobData {
    /// List of signed authorizations for contract delegation
    /// Each authorization allows the EOA to temporarily delegate to a smart contract
    #[schemars(with = "Option<Vec<SignedAuthorizationSchema>>")]
    #[schema(value_type = Option<Vec<SignedAuthorizationSchema>>)]
    pub authorization_list: Option<Vec<SignedAuthorization>>,

    /// Maximum fee per gas willing to pay (in wei)
    /// This is the total fee cap including base fee and priority fee
    pub max_fee_per_gas: Option<u128>,

    /// Maximum priority fee per gas willing to pay (in wei)
    /// This is the tip paid to validators for transaction inclusion
    pub max_priority_fee_per_gas: Option<u128>,
}

/// EIP-1559 transaction configuration
/// Uses base fee + priority fee model for more predictable gas pricing
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct EoaSend1559JobData {
    /// Maximum fee per gas willing to pay (in wei)
    /// This is the total fee cap including base fee and priority fee
    pub max_fee_per_gas: Option<u128>,

    /// Maximum priority fee per gas willing to pay (in wei)
    /// This is the tip paid to validators for transaction inclusion
    pub max_priority_fee_per_gas: Option<u128>,
}

/// Legacy transaction configuration
/// Uses simple gas price model (pre-EIP-1559)
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct EoaSendLegacyJobData {
    /// Gas price willing to pay (in wei)
    /// This is the total price per unit of gas for legacy transactions
    pub gas_price: Option<u128>,
}

/// EIP-7702 Authorization structure for OpenAPI schema
/// Represents an unsigned authorization that allows an EOA to delegate to a smart contract
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AuthorizationSchema {
    /// The chain ID of the authorization
    /// Must match the chain where the transaction will be executed
    #[schemars(with = "String")]
    #[schema(value_type = String, example = "1")]
    pub chain_id: U256,

    /// The smart contract address to delegate to
    /// This contract will be able to execute logic on behalf of the EOA
    #[schemars(with = "AddressDef")]
    #[schema(value_type = AddressDef)]
    pub address: Address,

    /// The nonce for the authorization
    /// Must be the current nonce of the authorizing account
    #[schema(example = 42)]
    pub nonce: u64,
}

/// EIP-7702 Signed Authorization structure for OpenAPI schema
/// Contains an authorization plus the cryptographic signature
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SignedAuthorizationSchema {
    /// The chain ID of the authorization
    /// Must match the chain where the transaction will be executed
    #[schemars(with = "String")]
    #[schema(value_type = String, example = "1")]
    pub chain_id: U256,

    /// The smart contract address to delegate to
    /// This contract will be able to execute logic on behalf of the EOA
    #[schemars(with = "AddressDef")]
    #[schema(value_type = AddressDef)]
    pub address: Address,

    /// The nonce for the authorization
    /// Must be the current nonce of the authorizing account
    #[schema(example = 42)]
    pub nonce: u64,

    /// Signature parity value (0 or 1)
    /// Used for ECDSA signature recovery
    #[serde(rename = "yParity", alias = "v")]
    #[schema(example = 0)]
    pub y_parity: u8,

    /// Signature r value
    /// First component of the ECDSA signature
    #[schemars(with = "String")]
    #[schema(value_type = String, example = "0x1234567890abcdef...")]
    pub r: U256,

    /// Signature s value
    /// Second component of the ECDSA signature
    #[schemars(with = "String")]
    #[schema(value_type = String, example = "0xfedcba0987654321...")]
    pub s: U256,
}

fn default_max_inflight() -> u64 {
    100
}

fn default_max_recycled_nonces() -> u64 {
    50
}
