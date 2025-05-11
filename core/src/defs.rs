use alloy::primitives::Address;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(JsonSchema, Serialize, Deserialize, Clone)]
#[serde(remote = "Address", transparent)]
/// # Address
/// Used to represent an EVM address. This is a string of length 42 with a `0x` prefix. Non-checksummed addresses are also supported, but will be converted to checksummed.
pub struct AddressDef(pub String);
