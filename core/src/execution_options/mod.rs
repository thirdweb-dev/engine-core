use serde::{Deserialize, Serialize};

use crate::transaction::InnerTransaction;
pub mod aa;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BaseExecutionOptions {
    pub chain_id: u64,
    #[serde(default = "default_idempotency_key")]
    pub idempotency_key: String,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionRequest {
    #[serde(flatten)]
    pub execution_options: ExecutionOptions,
    pub params: Vec<InnerTransaction>,
    pub webhook_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionResponse {
    pub transaction_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueuedNotification {
    pub transaction_id: String,
    pub executor_type: ExecutorType,
    pub execution_options: ExecutionOptions,
    pub timestamp: u64,
    pub queue_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorType {
    Erc4337,
}

impl ExecutionOptions {
    pub fn executor_type(&self) -> ExecutorType {
        match &self.specific {
            SpecificExecutionOptions::ERC4337(_) => ExecutorType::Erc4337,
        }
    }

    pub fn chain_id(&self) -> u64 {
        self.base.chain_id
    }

    pub fn transaction_id(&self) -> &str {
        &self.base.idempotency_key
    }
}
