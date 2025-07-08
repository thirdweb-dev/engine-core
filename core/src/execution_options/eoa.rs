use crate::defs::AddressDef;
use alloy::primitives::Address;
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
}
