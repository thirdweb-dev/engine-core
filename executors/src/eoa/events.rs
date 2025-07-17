use std::fmt::Display;

use alloy::primitives::Address;
use serde::{Deserialize, Serialize};
use twmq::job::RequeuePosition;

use crate::{
    eoa::{
        store::{ConfirmedTransaction, SubmittedTransactionDehydrated},
        worker::error::EoaExecutorWorkerError,
    },
    webhook::envelope::{
        BareWebhookNotificationEnvelope, SerializableFailData, SerializableNackData,
        SerializableSuccessData, StageEvent,
    },
};

pub struct EoaExecutorEvent {
    pub transaction_id: String,
    pub address: Address,
}

#[derive(Serialize, Deserialize, Debug, Clone, thiserror::Error)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "errorCode")]
pub enum EoaConfirmationError {
    #[error(
        "Previously submitted attempt for transaction replaced at nonce with different transaction"
    )]
    #[serde(rename_all = "camelCase")]
    TransactionReplaced { nonce: u64, hash: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EoaSendAttemptNackData {
    pub nonce: u64,
    pub error: EoaExecutorWorkerError,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EoaSendAttemptSuccessData {
    #[serde(flatten)]
    pub submitted_transaction: SubmittedTransactionDehydrated,
    pub address: Address,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EoaExecutorConfirmedTransaction {
    pub receipt: alloy::rpc::types::TransactionReceipt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EoaExecutorStage {
    Send,
    Confirmation,
}

impl Display for EoaExecutorStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EoaExecutorStage::Send => write!(f, "send"),
            EoaExecutorStage::Confirmation => write!(f, "confirm"),
        }
    }
}

const EXECUTOR_NAME: &str = "eoa";

impl EoaExecutorEvent {
    pub fn send_attempt_success_envelope(
        &self,
        submitted_transaction: SubmittedTransactionDehydrated,
    ) -> BareWebhookNotificationEnvelope<SerializableSuccessData<EoaSendAttemptSuccessData>> {
        BareWebhookNotificationEnvelope {
            transaction_id: self.transaction_id.clone(),
            executor_name: EXECUTOR_NAME.to_string(),
            stage_name: EoaExecutorStage::Send.to_string(),
            event_type: StageEvent::Success,
            payload: SerializableSuccessData {
                result: EoaSendAttemptSuccessData {
                    submitted_transaction: submitted_transaction.clone(),
                    address: self.address,
                },
            },
        }
    }

    pub fn send_attempt_nack_envelope(
        &self,
        nonce: u64,
        error: EoaExecutorWorkerError,
        attempt_number: u32,
    ) -> BareWebhookNotificationEnvelope<SerializableNackData<EoaSendAttemptNackData>> {
        BareWebhookNotificationEnvelope {
            transaction_id: self.transaction_id.clone(),
            executor_name: EXECUTOR_NAME.to_string(),
            stage_name: EoaExecutorStage::Send.to_string(),
            event_type: StageEvent::Nack,
            payload: SerializableNackData {
                error: EoaSendAttemptNackData {
                    nonce,
                    error: error.clone(),
                },
                delay_ms: None,
                position: RequeuePosition::Last,
                attempt_number,
                max_attempts: None,
                next_retry_at: None,
            },
        }
    }

    pub fn transaction_replaced_envelope(
        &self,
        replaced_transaction: SubmittedTransactionDehydrated,
    ) -> BareWebhookNotificationEnvelope<SerializableNackData<EoaConfirmationError>> {
        BareWebhookNotificationEnvelope {
            transaction_id: self.transaction_id.clone(),
            executor_name: EXECUTOR_NAME.to_string(),
            stage_name: EoaExecutorStage::Confirmation.to_string(),
            event_type: StageEvent::Nack,
            payload: SerializableNackData {
                error: EoaConfirmationError::TransactionReplaced {
                    nonce: replaced_transaction.nonce,
                    hash: replaced_transaction.transaction_hash,
                },
                delay_ms: None,
                position: RequeuePosition::Last,
                attempt_number: 0,
                max_attempts: None,
                next_retry_at: None,
            },
        }
    }

    pub fn transaction_confirmed_envelope(
        &self,
        confirmed_transaction: ConfirmedTransaction,
    ) -> BareWebhookNotificationEnvelope<SerializableSuccessData<EoaExecutorConfirmedTransaction>>
    {
        BareWebhookNotificationEnvelope {
            transaction_id: self.transaction_id.clone(),
            executor_name: EXECUTOR_NAME.to_string(),
            stage_name: EoaExecutorStage::Confirmation.to_string(),
            event_type: StageEvent::Success,
            payload: SerializableSuccessData {
                result: EoaExecutorConfirmedTransaction {
                    receipt: confirmed_transaction.receipt,
                },
            },
        }
    }

    pub fn transaction_failed_envelope(
        &self,
        error: EoaExecutorWorkerError,
        final_attempt_number: u32,
    ) -> BareWebhookNotificationEnvelope<SerializableFailData<EoaExecutorWorkerError>> {
        BareWebhookNotificationEnvelope {
            transaction_id: self.transaction_id.clone(),
            executor_name: EXECUTOR_NAME.to_string(),
            stage_name: EoaExecutorStage::Send.to_string(),
            event_type: StageEvent::Failure,
            payload: SerializableFailData {
                error: error.clone(),
                final_attempt_number,
            },
        }
    }
}
