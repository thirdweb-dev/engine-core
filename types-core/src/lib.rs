use alloy::rpc::types::{PackedUserOperation, UserOperation};
use serde::{Deserialize, Serialize};

/// UserOp version enum
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum UserOpVersion {
    V0_6(UserOperation),
    V0_7(PackedUserOperation),
} 