use alloy::primitives::{Address, U256};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(JsonSchema, Serialize, Deserialize, Clone, utoipa::ToSchema)]
#[schema(title = "EVM Address")]
/// Used to represent an EVM address. This is a string of length 42 with a `0x` prefix. Non-checksummed addresses are also supported, but will be converted to checksummed.
pub struct AddressDef(pub String);

#[derive(JsonSchema, Serialize, Deserialize, Clone, utoipa::ToSchema)]
#[schema(title = "Bytes")]
/// Used to represent "bytes". This is a 0x prefixed hex string.
pub struct BytesDef(pub String);

#[derive(JsonSchema, Serialize, Deserialize, Clone, utoipa::ToSchema)]
#[schema(title = "U256")]
/// Used to represent a 256-bit unsigned integer. Engine can parse these from any valid encoding of the Ethereum "quantity" format.
pub struct U256Def(pub String);

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
#[schema(title = "EIP-7702 Signed Authorization")]
pub struct SignedAuthorizationSchema {
    /// The chain ID of the authorization
    /// Must match the chain where the transaction will be executed
    #[schemars(with = "String")]
    #[schema(value_type = U256Def, example = "1")]
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
    #[schema(value_type = U256Def, example = "0x1234567890abcdef...")]
    pub r: U256,

    /// Signature s value
    /// Second component of the ECDSA signature
    #[schemars(with = "String")]
    #[schema(value_type = U256Def, example = "0xfedcba0987654321...")]
    pub s: U256,
}
