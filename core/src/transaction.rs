use crate::defs::{AddressDef, BytesDef, SignedAuthorizationSchema, U256Def};
use alloy::{
    eips::eip7702::SignedAuthorization,
    primitives::{Address, Bytes, U256},
};
use serde::{Deserialize, Serialize};

/// ### InnerTransaction
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

    /// Gas limit for the transaction
    /// If not provided, engine will estimate the gas limit
    #[schema(value_type = Option<u64>)]
    #[serde(default, rename = "gasLimit")]
    pub gas_limit: Option<u64>,

    /// Transaction type-specific data for different EIP standards
    ///
    /// This is the actual encoded inner transaction data that will be sent to the blockchain.
    ///
    /// Depending on the execution mode chosen, these might be ignored:
    ///
    /// - For ERC4337 execution, all gas fee related fields are ignored. Sending signed authorizations is also not supported.
    #[serde(flatten)]
    pub transaction_type_data: Option<TransactionTypeData>,
}

#[derive(Serialize, Deserialize, Debug, Clone, utoipa::ToSchema)]
#[serde(untagged)]
#[schema(title = "Transaction Type Specific Data")]
pub enum TransactionTypeData {
    /// EIP-7702 transaction with authorization list and EIP-1559 gas pricing
    Eip7702(Transaction7702Data),
    /// EIP-1559 transaction with priority fee and max fee per gas
    Eip1559(Transaction1559Data),
    /// Legacy transaction with simple gas price
    Legacy(TransactionLegacyData),
}

/// EIP-7702 transaction configuration
/// Allows delegation of EOA to smart contract logic temporarily
#[derive(Serialize, Deserialize, Debug, Clone, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(title = "EIP-7702 Specific Transaction Data")]
pub struct Transaction7702Data {
    /// List of signed authorizations for contract delegation
    /// Each authorization allows the EOA to temporarily delegate to a smart contract
    #[schema(value_type = Option<Vec<SignedAuthorizationSchema>>)]
    pub authorization_list: Option<Vec<SignedAuthorization>>,

    /// Maximum fee per gas willing to pay (in wei)
    /// This is the total fee cap including base fee and priority fee
    pub max_fee_per_gas: Option<u128>,

    /// Maximum priority fee per gas willing to pay (in wei)
    /// This is the tip paid to validators for transaction inclusion
    pub max_priority_fee_per_gas: Option<u128>,
}

/// EIP-1559 transaction configuration
/// Uses base fee + priority fee model for more predictable gas pricing
#[derive(Serialize, Deserialize, Debug, Clone, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(title = "EIP-1559 Specific Transaction Data")]
pub struct Transaction1559Data {
    /// Maximum fee per gas willing to pay (in wei)
    /// This is the total fee cap including base fee and priority fee
    pub max_fee_per_gas: Option<u128>,

    /// Maximum priority fee per gas willing to pay (in wei)
    /// This is the tip paid to validators for transaction inclusion
    pub max_priority_fee_per_gas: Option<u128>,
}

/// Legacy transaction configuration
/// Uses simple gas price model (pre-EIP-1559)
#[derive(Serialize, Deserialize, Debug, Clone, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(title = "Legacy Specific Transaction Data")]
pub struct TransactionLegacyData {
    /// Gas price willing to pay (in wei)
    /// This is the total price per unit of gas for legacy transactions
    pub gas_price: Option<u128>,
}
