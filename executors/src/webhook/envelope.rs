use std::sync::Arc;

use engine_core::execution_options::WebhookOptions;
use serde::{Deserialize, Serialize};
use twmq::{
    DurableExecution, FailHookData, NackHookData, Queue, SuccessHookData,
    error::TwmqError,
    hooks::TransactionContext,
    job::{BorrowedJob, Job, RequeuePosition},
};
use uuid::Uuid;

use crate::webhook::WebhookJobPayload;

use super::WebhookJobHandler;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StageEvent {
    Success,
    Nack,
    Failure,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WebhookNotificationEnvelope<T> {
    // P is now StageOutcomePayload
    pub notification_id: String,
    pub transaction_id: String,

    pub timestamp: u64,

    pub executor_name: String,
    pub stage_name: String,
    pub event_type: StageEvent,

    pub payload: T,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery_target_url: Option<String>,
}

// --- Serializable Hook Data Wrappers ---
// These wrap the hook data to make them serializable (removing lifetimes)
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SerializableSuccessData<T> {
    pub result: T,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SerializableNackData<E> {
    pub error: E,
    pub delay_ms: Option<u64>,
    pub position: RequeuePosition, // "First" or "Last"
    pub attempt_number: u32,
    pub max_attempts: Option<u32>,
    pub next_retry_at: Option<u64>, // Unix timestamp
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SerializableFailData<E> {
    pub error: E,
    pub final_attempt_number: u32,
}

// --- Trait for Stages ---
pub trait ExecutorStage {
    fn executor_name() -> &'static str;
    fn stage_name() -> &'static str;
}

// --- Webhook Options Trait ---
pub trait HasWebhookOptions {
    fn webhook_options(&self) -> Option<Vec<WebhookOptions>>;
}

pub trait HasTransactionMetadata {
    fn transaction_id(&self) -> String;
}

impl<T: Clone> HasTransactionMetadata for Job<T> {
    fn transaction_id(&self) -> String {
        self.id.clone()
    }
}

// --- Main Webhook Capability Trait ---
pub trait WebhookCapable: DurableExecution + ExecutorStage {
    fn webhook_queue(&self) -> &Arc<Queue<WebhookJobHandler>>;

    fn queue_success_webhook(
        &self,
        job: &BorrowedJob<Self::JobData>,
        success_data: SuccessHookData<'_, Self::Output>,
        tx: &mut TransactionContext<'_>,
    ) -> Result<(), TwmqError>
    where
        Self::JobData: HasWebhookOptions,
        Self::Output: Serialize + Clone,
    {
        let webhook_options = match job.job.data.webhook_options() {
            Some(w) => w,
            None => return Ok(()), // No webhook configured, skip silently
        };

        for w in webhook_options {
            let envelope = WebhookNotificationEnvelope {
                notification_id: Uuid::new_v4().to_string(),
                transaction_id: job.job.transaction_id(),
                timestamp: chrono::Utc::now().timestamp().try_into().unwrap(),
                executor_name: Self::executor_name().to_string(),
                stage_name: Self::stage_name().to_string(),
                event_type: StageEvent::Success,
                payload: SerializableSuccessData {
                    result: success_data.result.clone(),
                },
                delivery_target_url: Some(w.url.clone()),
            };

            self.queue_webhook_envelope(envelope, w, job, tx)?
        }

        Ok(())
    }

    fn queue_nack_webhook(
        &self,
        job: &BorrowedJob<Self::JobData>,
        nack_data: NackHookData<'_, Self::ErrorData>,
        tx: &mut TransactionContext<'_>,
    ) -> Result<(), TwmqError>
    where
        Self::JobData: HasWebhookOptions,
        Self::ErrorData: Serialize + Clone,
    {
        let webhook_options = match job.job.data.webhook_options() {
            Some(w) => w,
            None => return Ok(()), // No webhook configured, skip silently
        };
        for w in webhook_options {
            let now: u64 = chrono::Utc::now().timestamp().try_into().unwrap();
            let next_retry_at = nack_data.delay.map(|delay| now + delay.as_secs());

            let envelope = WebhookNotificationEnvelope {
                notification_id: Uuid::new_v4().to_string(),
                transaction_id: job.job.transaction_id(),
                timestamp: chrono::Utc::now().timestamp().try_into().unwrap(),
                executor_name: Self::executor_name().to_string(),
                stage_name: Self::stage_name().to_string(),
                event_type: StageEvent::Nack,
                payload: SerializableNackData {
                    error: nack_data.error.clone(),
                    delay_ms: nack_data.delay.map(|d| d.as_millis() as u64),
                    position: nack_data.position,
                    attempt_number: job.job.attempts,
                    max_attempts: None, // TODO: Get from job config if available
                    next_retry_at,
                },
                delivery_target_url: Some(w.url.clone()),
            };

            self.queue_webhook_envelope(envelope, w, job, tx)?;
        }
        Ok(())
    }

    fn queue_fail_webhook(
        &self,
        job: &BorrowedJob<Self::JobData>,
        fail_data: FailHookData<'_, Self::ErrorData>,
        tx: &mut TransactionContext<'_>,
    ) -> Result<(), TwmqError>
    where
        Self::JobData: HasWebhookOptions,
        Self::ErrorData: Serialize + Clone,
    {
        let webhook_options = match job.job.data.webhook_options() {
            Some(w) => w,
            None => return Ok(()), // No webhook configured, skip silently
        };
        for w in webhook_options {
            let envelope = WebhookNotificationEnvelope {
                notification_id: Uuid::new_v4().to_string(),
                transaction_id: job.job.transaction_id(),
                timestamp: chrono::Utc::now().timestamp().try_into().unwrap(),
                executor_name: Self::executor_name().to_string(),
                stage_name: Self::stage_name().to_string(),
                event_type: StageEvent::Failure,
                payload: SerializableFailData {
                    error: fail_data.error.clone(),
                    final_attempt_number: job.job.attempts,
                },
                delivery_target_url: Some(w.url.clone()),
            };

            self.queue_webhook_envelope(envelope, w, job, tx)?;
        }
        Ok(())
    }

    // Private helper method
    fn queue_webhook_envelope<T: Serialize>(
        &self,
        envelope: WebhookNotificationEnvelope<T>,
        webhook_options: WebhookOptions,
        job: &BorrowedJob<Self::JobData>,
        tx: &mut TransactionContext<'_>,
    ) -> Result<(), TwmqError>
    where
        Self::JobData: HasWebhookOptions,
    {
        let webhook_payload = WebhookJobPayload {
            url: webhook_options.url,
            body: serde_json::to_string(&envelope)?,
            headers: Some(
                [
                    ("Content-Type".to_string(), "application/json".to_string()),
                    (
                        "User-Agent".to_string(),
                        format!("{}/{}", Self::executor_name(), Self::stage_name()),
                    ),
                ]
                .into_iter()
                .collect(),
            ),
            hmac_secret: webhook_options.secret, // TODO: Add HMAC support if needed
            http_method: Some("POST".to_string()),
        };

        let mut webhook_job = self.webhook_queue().clone().job(webhook_payload);
        webhook_job.options.id = format!(
            "{}_{}_webhook",
            job.job.transaction_id(),
            envelope.notification_id
        );

        tx.queue_job(webhook_job)?;

        tracing::info!(
            transaction_id = %job.job.transaction_id(),
            executor = %Self::executor_name(),
            stage = %Self::stage_name(),
            event = ?envelope.event_type,
            notification_id = %envelope.notification_id,
            "Queued webhook notification"
        );

        Ok(())
    }
}
