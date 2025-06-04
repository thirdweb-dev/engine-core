use crate::defs::{AddressDef, BytesDef, U256Def};
use alloy::primitives::{Address, Bytes, U256};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// # InnerTransaction
/// This is the actual encoded inner transaction data that will be sent to the blockchain.
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
pub struct InnerTransaction {
    #[schemars(with = "AddressDef")]
    pub to: Address,
    #[schemars(with = "BytesDef")]
    pub data: Bytes,
    #[schemars(with = "U256Def")]
    pub value: U256,
}
