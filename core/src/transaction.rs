use crate::defs::{AddressDef, BytesDef, U256Def};
use alloy::primitives::{Address, Bytes, U256};
use serde::{Deserialize, Serialize};

/// # InnerTransaction
/// This is the actual encoded inner transaction data that will be sent to the blockchain.
#[derive(Deserialize, Serialize, Debug, Clone, utoipa::ToSchema)]
pub struct InnerTransaction {
    #[schema(value_type = Option<AddressDef>)]
    pub to: Option<Address>,

    #[schema(value_type = BytesDef)]
    #[serde(default)]
    pub data: Bytes,

    #[schema(value_type = U256Def)]
    #[serde(default)]
    pub value: U256,
}
