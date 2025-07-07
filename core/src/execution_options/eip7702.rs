use alloy::primitives::Address;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::defs::AddressDef;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
#[schema(title = "EIP-7702 Execution Options")]
#[serde(rename_all = "camelCase")]
pub struct Eip7702ExecutionOptions {
    /// The EOA address that will sign the EIP-7702 transaction
    #[schemars(with = "AddressDef")]
    #[schema(value_type = AddressDef)]
    pub from: Address,
}
