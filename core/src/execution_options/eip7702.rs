use alloy::primitives::Address;
use serde::{Deserialize, Serialize};

use crate::defs::AddressDef;

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[schema(title = "EIP-7702 Execution Options")]
#[serde(rename_all = "camelCase", untagged)]
pub enum Eip7702ExecutionOptions {
    /// Execute the transaction as the owner of the account
    Owner(Eip7702OwnerExecution),
    /// Execute a transaction on a different delegated account (`account_address`), which has granted a session key to the `session_key_address`
    /// `session_key_address` is the signer for this transaction
    SessionKey(Eip7702SessionKeyExecution),
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[schema(title = "EIP-7702 Owner Execution")]
#[serde(rename_all = "camelCase")]
pub struct Eip7702OwnerExecution {
    #[schema(value_type = AddressDef)]
    /// The delegated EOA address
    pub from: Address,
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[schema(title = "EIP-7702 Session Key Execution")]
#[serde(rename_all = "camelCase")]
pub struct Eip7702SessionKeyExecution {
    #[schema(value_type = AddressDef)]
    /// The session key address is your server wallet, which has been granted a session key to the `account_address`
    pub session_key_address: Address,
    #[schema(value_type = AddressDef)]
    /// The account address is the address of a delegated account you want to execute the transaction on. This account has granted a session key to the `session_key_address`
    pub account_address: Address,
}
