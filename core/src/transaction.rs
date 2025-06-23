use crate::defs::{AddressDef, BytesDef, U256Def};
use alloy::primitives::{Address, Bytes, U256};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// # InnerTransaction
/// This is the actual encoded inner transaction data that will be sent to the blockchain.
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema, utoipa::ToSchema)]
pub struct InnerTransaction {
    #[schemars(with = "AddressDef")]
    #[schema(value_type = AddressDef)]
    pub to: Address,

    #[schemars(with = "BytesDef")]
    #[schema(value_type = BytesDef)]
    pub data: Bytes,

    #[schemars(with = "U256Def")]
    #[schema(value_type = U256Def)]
    pub value: U256,
}
