use alloy::primitives::{Address, Bytes, U256};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct InnerTransaction {
    pub to: Address,
    pub data: Bytes,
    pub value: U256,
}
