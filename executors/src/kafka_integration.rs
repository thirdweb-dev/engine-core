use alloy::primitives::{Address, Bytes, U256};
use engine_core::rpc_clients::UserOperationReceipt;
use serde::Serialize;
use std::sync::Arc;

/// Trait for sending transaction events to external messaging systems
#[async_trait::async_trait]
pub trait TransactionEventSender: Send + Sync {
    async fn send_transaction_sent(&self, message: TransactionSentEvent);
    async fn send_transaction_confirmed(&self, message: TransactionConfirmedEvent);
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionSentEvent {
    pub transaction_id: String,
    pub chain_id: u64,
    pub account_address: Address,
    pub user_op_hash: Bytes,
    pub nonce: U256,
    pub deployment_lock_acquired: bool,
    pub timestamp: u64,
    pub team_id: String,
    pub project_id: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionConfirmedEvent {
    pub transaction_id: String,
    pub user_op_hash: Bytes,
    pub receipt: UserOperationReceipt,
    pub deployment_lock_released: bool,
    pub timestamp: u64,
    pub team_id: String,
    pub project_id: String,
}

/// No-op implementation for when messaging is disabled
pub struct NoOpEventSender;

#[async_trait::async_trait]
impl TransactionEventSender for NoOpEventSender {
    async fn send_transaction_sent(&self, _message: TransactionSentEvent) {
        // Do nothing
    }

    async fn send_transaction_confirmed(&self, _message: TransactionConfirmedEvent) {
        // Do nothing
    }
}

pub type SharedEventSender = Arc<dyn TransactionEventSender>;