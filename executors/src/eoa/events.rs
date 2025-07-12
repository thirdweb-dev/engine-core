use std::fmt::Display;

use serde::{Deserialize, Serialize};
use twmq::job::RequeuePosition;

use crate::{
    eoa::{
        store::{SubmittedTransaction, TransactionData},
        worker::{ConfirmedTransactionWithRichReceipt, EoaExecutorWorkerError},
    },
    webhook::envelope::{
        BareWebhookNotificationEnvelope, SerializableFailData, SerializableNackData,
        SerializableSuccessData, StageEvent,
    },
};

pub struct EoaExecutorEvent {
    pub transaction_data: TransactionData,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EoaSendAttemptNackData {
    pub nonce: u64,
    pub error: EoaExecutorWorkerError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EoaExecutorStage {
    SendAttempt,
    TransactionReplaced,
    TransactionConfirmed,
}

impl Display for EoaExecutorStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EoaExecutorStage::SendAttempt => write!(f, "send_attempt"),
            EoaExecutorStage::TransactionReplaced => write!(f, "transaction_replaced"),
            EoaExecutorStage::TransactionConfirmed => write!(f, "transaction_confirmed"),
        }
    }
}

const EXECUTOR_NAME: &str = "eoa";

impl EoaExecutorEvent {
    pub fn send_attempt_success_envelope(
        &self,
        submitted_transaction: SubmittedTransaction,
    ) -> BareWebhookNotificationEnvelope<SerializableSuccessData<SubmittedTransaction>> {
        BareWebhookNotificationEnvelope {
            transaction_id: self.transaction_data.transaction_id.clone(),
            executor_name: EXECUTOR_NAME.to_string(),
            stage_name: EoaExecutorStage::SendAttempt.to_string(),
            event_type: StageEvent::Success,
            payload: SerializableSuccessData {
                result: submitted_transaction.clone(),
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
            transaction_id: self.transaction_data.transaction_id.clone(),
            executor_name: EXECUTOR_NAME.to_string(),
            stage_name: EoaExecutorStage::SendAttempt.to_string(),
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
        replaced_transaction: SubmittedTransaction,
    ) -> BareWebhookNotificationEnvelope<SerializableSuccessData<SubmittedTransaction>> {
        BareWebhookNotificationEnvelope {
            transaction_id: self.transaction_data.transaction_id.clone(),
            executor_name: EXECUTOR_NAME.to_string(),
            stage_name: EoaExecutorStage::TransactionReplaced.to_string(),
            event_type: StageEvent::Success,
            payload: SerializableSuccessData {
                result: replaced_transaction.clone(),
            },
        }
    }

    pub fn transaction_confirmed_envelope(
        &self,
        confirmed_transaction: ConfirmedTransactionWithRichReceipt,
    ) -> BareWebhookNotificationEnvelope<SerializableSuccessData<ConfirmedTransactionWithRichReceipt>>
    {
        BareWebhookNotificationEnvelope {
            transaction_id: self.transaction_data.transaction_id.clone(),
            executor_name: EXECUTOR_NAME.to_string(),
            stage_name: EoaExecutorStage::TransactionConfirmed.to_string(),
            event_type: StageEvent::Success,
            payload: SerializableSuccessData {
                result: confirmed_transaction.clone(),
            },
        }
    }

    pub fn transaction_failed_envelope(
        &self,
        error: EoaExecutorWorkerError,
        final_attempt_number: u32,
    ) -> BareWebhookNotificationEnvelope<SerializableFailData<EoaExecutorWorkerError>> {
        BareWebhookNotificationEnvelope {
            transaction_id: self.transaction_data.transaction_id.clone(),
            executor_name: EXECUTOR_NAME.to_string(),
            stage_name: EoaExecutorStage::SendAttempt.to_string(),
            event_type: StageEvent::Failure,
            payload: SerializableFailData {
                error: error.clone(),
                final_attempt_number,
            },
        }
    }
}
