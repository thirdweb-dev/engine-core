use serde::{Deserialize, Serialize};
pub mod aa;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BaseExecutionOptions {
    chain_id: u64,
    #[serde(default = "default_idempotency_key")]
    idempotency_key: String,
}

fn default_idempotency_key() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SpecificExecutionOptions {
    ERC4337(aa::Erc4337ExecutionOptions),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionOptions {
    #[serde(flatten)]
    pub base: BaseExecutionOptions,
    #[serde(flatten)]
    pub specific: SpecificExecutionOptions,
}
