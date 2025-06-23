use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
#[schema(title = "Auto Determine Execution")]
#[serde(rename_all = "camelCase")]
/// This is the default execution option.
/// If you do not specify an execution type, and only specify a "from" string,
/// engine will automatically determine the most optimal options for you.
/// If you would like to specify granular options about execution strategy
/// choose one of the other executionOptions type and provide them.
pub struct AutoExecutionOptions {
    /// The identifier of the entity to send the transaction from.
    /// Automatically picks best execution strategy based on the identifier.
    /// - If EOA address, execution uses EIP7702 based smart-wallet execution
    /// - If 7702 not supported on chain, falls back to smart-contract wallet (ERC4337) with default smart account for this EOA (v0.7)
    /// - UNLESS this is a zk-chain, in which case, zk-sync native-aa is used
    pub from: String,
}
