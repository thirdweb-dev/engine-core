use alloy::primitives::{Address, Bytes, U256};
use engine_core::rpc_clients::UserOperationReceipt;
use serde::Serialize;

/// Trait for sending transaction events to external messaging systems
#[async_trait::async_trait]
pub trait TransactionEventSender: Send + Sync {
    async fn send_transaction_sent(&self, event: TransactionSentEvent);
    async fn send_transaction_confirmed(&self, event: TransactionConfirmedEvent);
}

/// No-op implementation for when messaging is disabled
pub struct NoOpEventSender;

#[async_trait::async_trait]
impl TransactionEventSender for NoOpEventSender {
    async fn send_transaction_sent(&self, _event: TransactionSentEvent) {
        // Do nothing
    }

    async fn send_transaction_confirmed(&self, _event: TransactionConfirmedEvent) {
        // Do nothing
    }
}

pub type SharedEventSender = std::sync::Arc<dyn TransactionEventSender>;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct TransactionSentEvent {
    pub transaction_id: String,
    pub chain_id: String,
    pub account_address: Address,
    pub user_op_hash: Bytes,
    pub nonce: U256,
    pub deployment_lock_acquired: bool,
    pub timestamp: u64,
    pub team_id: String,
    pub project_id: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct TransactionConfirmedEvent {
    pub transaction_id: String,
    pub user_op_hash: Bytes,
    pub receipt: UserOperationReceipt,
    pub deployment_lock_released: bool,
    pub timestamp: u64,
    pub team_id: String,
    pub project_id: String,
}

