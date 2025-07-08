use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(JsonSchema, Serialize, Deserialize, Clone, utoipa::ToSchema)]
#[schema(title = "EVM Address")]
/// ### Address
/// Used to represent an EVM address. This is a string of length 42 with a `0x` prefix. Non-checksummed addresses are also supported, but will be converted to checksummed.
pub struct AddressDef(pub String);

#[derive(JsonSchema, Serialize, Deserialize, Clone, utoipa::ToSchema)]
#[schema(title = "Bytes")]
/// # Bytes
/// Used to represent "bytes". This is a 0x prefixed hex string.
pub struct BytesDef(pub String);

#[derive(JsonSchema, Serialize, Deserialize, Clone, utoipa::ToSchema)]
#[schema(title = "U256")]
/// # U256
/// Used to represent a 256-bit unsigned integer. Engine can parse these from any valid encoding of the Ethereum "quantity" format.
pub struct U256Def(pub String);
