use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::{Address, Bytes, U256};
use engine_aa_types::VersionedUserOp;
use engine_core::rpc_clients::UserOperationReceipt;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use serde::{Deserialize, Serialize};

use crate::config::{Environment, KafkaConfig};

/// Kafka event envelope structure that wraps all transaction data
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaEventEnvelope<T> {
    pub id: String,
    pub team_id: String,
    pub project_id: String,
    pub created_at: String,
    pub data: T,
}

impl<T> KafkaEventEnvelope<T> {
    pub fn new(team_id: String, project_id: String, data: T) -> Self {
        Self {
            id: format!("evt_{}", cuid::cuid1().expect("Failed to generate CUID")),
            team_id,
            project_id,
            created_at: chrono::Utc::now().to_rfc3339(),
            data,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TransactionTopic {
    #[serde(rename = "engine.transaction.sent")]
    Sent,
    #[serde(rename = "engine.transaction.confirmed")]
    Confirmed,
}

impl TransactionTopic {
    pub fn as_str(&self) -> &'static str {
        match self {
            TransactionTopic::Sent => "engine.transaction.sent",
            TransactionTopic::Confirmed => "engine.transaction.confirmed",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionSentMessage {
    pub transaction_id: String,
    pub chain_id: u64,
    pub account_address: Address,
    pub user_op_hash: Bytes,
    pub nonce: U256,
    pub deployment_lock_acquired: bool,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionConfirmedMessage {
    pub transaction_id: String,
    pub user_op_hash: Bytes,
    pub receipt: UserOperationReceipt,
    pub deployment_lock_released: bool,
    pub timestamp: u64,
}

/// Unified transaction webhook event structure with explicit null fields
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionWebhookEvent {
    // Core transaction data (always present)
    pub transaction_id: String,
    pub chain_id: u64,
    pub account_address: Address,
    pub user_op_hash: Bytes,
    pub nonce: U256,
    pub timestamp: u64,
    
    // Send-specific fields (null for confirm events)
    pub user_operation_sent: Option<VersionedUserOp>,
    pub deployment_lock_acquired: Option<bool>,
    
    // Confirm-specific fields (null for send events)
    pub receipt: Option<UserOperationReceipt>,
    pub deployment_lock_released: Option<bool>,
    
    // Transaction status info (null for send events)
    pub success: Option<bool>,
    pub actual_gas_cost: Option<U256>,
    pub actual_gas_used: Option<U256>,
}

pub struct KafkaProducer {
    producer: Option<FutureProducer>,
    config: KafkaConfig,
}

impl KafkaProducer {
    pub fn new(kafka_config: KafkaConfig, environment: &Environment) -> Result<Self, String> {
        let group_id = format!("engine-core-{}", environment.as_str());
        
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &kafka_config.url)
            .set("security.protocol", "SASL_SSL")
            .set("sasl.mechanism", "PLAIN")
            .set("sasl.username", &kafka_config.username)
            .set("sasl.password", &kafka_config.password)
            .set("group.id", &group_id)
            // Hardcoded settings as specified
            .set("acks", "1")
            .set("compression.type", "lz4")
            .set("enable.idempotence", "true")
            .set("linger.ms", "10")
            // Configurable settings
            .set("batch.size", kafka_config.batch_size.to_string())
            .set("buffer.memory", (kafka_config.buffer_memory_kb * 1024).to_string())
            .set("request.timeout.ms", kafka_config.request_timeout_ms.to_string())
            .set("retries", kafka_config.max_retries.to_string());

        let producer = client_config
            .create()
            .map_err(|e| format!("Failed to create Kafka producer: {}", e))?;

        Ok(KafkaProducer {
            producer: Some(producer),
            config: kafka_config,
        })
    }

    pub fn disabled() -> Self {
        KafkaProducer {
            producer: None,
            config: KafkaConfig {
                url: String::new(),
                username: String::new(),
                password: String::new(),
                batch_size: 1000,
                buffer_memory_kb: 32768,
                request_timeout_ms: 5000,
                max_retries: 3,
            },
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.producer.is_some()
    }

    pub async fn send_transaction_sent(&self, message: TransactionSentMessage, team_id: String, project_id: String) {
        if !self.is_enabled() {
            tracing::debug!("Kafka disabled, skipping transaction sent message");
            return;
        }

        let envelope = KafkaEventEnvelope::new(team_id, project_id, message);
        self.send_message(TransactionTopic::Sent, &envelope.data.transaction_id, &envelope)
            .await;
    }

    pub async fn send_transaction_confirmed(&self, message: TransactionConfirmedMessage, team_id: String, project_id: String) {
        if !self.is_enabled() {
            tracing::debug!("Kafka disabled, skipping transaction confirmed message");
            return;
        }

        let envelope = KafkaEventEnvelope::new(team_id, project_id, message);
        self.send_message(TransactionTopic::Confirmed, &envelope.data.transaction_id, &envelope)
            .await;
    }

    /// Helper function to send webhook event with unified structure
    pub async fn send_webhook_event(&self, topic: &str, event: TransactionWebhookEvent, team_id: String, project_id: String) {
        if !self.is_enabled() {
            tracing::debug!("Kafka disabled, skipping webhook event");
            return;
        }

        let envelope = KafkaEventEnvelope::new(team_id, project_id, event);
        let payload_json = match serde_json::to_string(&envelope) {
            Ok(json) => json,
            Err(e) => {
                tracing::error!(
                    topic = %topic,
                    transaction_id = %envelope.data.transaction_id,
                    error = %e,
                    "Failed to serialize webhook event payload"
                );
                return;
            }
        };

        let Some(ref producer) = self.producer else {
            tracing::debug!("Kafka producer not available, skipping webhook event");
            return;
        };

        let record = FutureRecord::to(topic)
            .key(&envelope.data.transaction_id)
            .payload(&payload_json);

        let timeout = Timeout::After(Duration::from_millis(self.config.request_timeout_ms as u64));

        match producer.send(record, timeout).await {
            Ok((partition, offset)) => {
                tracing::debug!(
                    topic = %topic,
                    transaction_id = %envelope.data.transaction_id,
                    partition = %partition,
                    offset = %offset,
                    "Successfully sent webhook event to Kafka"
                );
            }
            Err((e, _original_record)) => {
                tracing::error!(
                    topic = %topic,
                    transaction_id = %envelope.data.transaction_id,
                    error = %e,
                    "Failed to send webhook event to Kafka"
                );
            }
        }
    }

    async fn send_message<T: Serialize>(&self, topic: TransactionTopic, key: &str, payload: &T) {
        let Some(ref producer) = self.producer else {
            tracing::debug!("Kafka producer not available, skipping message send");
            return;
        };

        let payload_json = match serde_json::to_string(payload) {
            Ok(json) => json,
            Err(e) => {
                tracing::error!(
                    topic = %topic.as_str(),
                    key = %key,
                    error = %e,
                    "Failed to serialize Kafka message payload"
                );
                return;
            }
        };

        let record = FutureRecord::to(topic.as_str())
            .key(key)
            .payload(&payload_json);

        let timeout = Timeout::After(Duration::from_millis(self.config.request_timeout_ms as u64));

        match producer.send(record, timeout).await {
            Ok((partition, offset)) => {
                tracing::debug!(
                    topic = %topic.as_str(),
                    key = %key,
                    partition = %partition,
                    offset = %offset,
                    "Successfully sent message to Kafka"
                );
            }
            Err((e, _original_record)) => {
                tracing::error!(
                    topic = %topic.as_str(),
                    key = %key,
                    error = %e,
                    "Failed to send message to Kafka"
                );
            }
        }
    }

    pub async fn flush(&self) -> Result<(), String> {
        let Some(ref producer) = self.producer else {
            return Ok(());
        };

        let timeout = Duration::from_millis(self.config.request_timeout_ms as u64);
        producer
            .flush(timeout)
            .map_err(|e| format!("Failed to flush Kafka producer: {}", e))
    }
}

impl Drop for KafkaProducer {
    fn drop(&mut self) {
        if self.is_enabled() {
            tracing::debug!("Dropping Kafka producer");
        }
    }
}

pub type SharedKafkaProducer = Arc<KafkaProducer>;

// Helper functions for creating webhook events
impl TransactionWebhookEvent {
    /// Create a transaction sent event
    pub fn transaction_sent(
        transaction_id: String,
        chain_id: u64,
        account_address: Address,
        user_op_hash: Bytes,
        nonce: U256,
        user_operation_sent: VersionedUserOp,
        deployment_lock_acquired: bool,
        timestamp: u64,
    ) -> Self {
        Self {
            transaction_id,
            chain_id,
            account_address,
            user_op_hash,
            nonce,
            timestamp,
            user_operation_sent: Some(user_operation_sent),
            deployment_lock_acquired: Some(deployment_lock_acquired),
            receipt: None,
            deployment_lock_released: None,
            success: None,
            actual_gas_cost: None,
            actual_gas_used: None,
        }
    }

    /// Create a transaction confirmed event
    pub fn transaction_confirmed(
        transaction_id: String,
        chain_id: u64,
        account_address: Address,
        user_op_hash: Bytes,
        nonce: U256,
        receipt: UserOperationReceipt,
        deployment_lock_released: bool,
        timestamp: u64,
    ) -> Self {
        let success = Some(receipt.success);
        let actual_gas_cost = Some(receipt.actual_gas_cost);
        let actual_gas_used = Some(receipt.actual_gas_used);
        
        Self {
            transaction_id,
            chain_id,
            account_address,
            user_op_hash,
            nonce,
            timestamp,
            user_operation_sent: None,
            deployment_lock_acquired: None,
            receipt: Some(receipt),
            deployment_lock_released: Some(deployment_lock_released),
            success,
            actual_gas_cost,
            actual_gas_used,
        }
    }
}

/// Standalone helper function to send webhook events
pub async fn send_webhook_event(
    kafka_producer: &KafkaProducer,
    topic: &str,
    event: TransactionWebhookEvent,
    team_id: String,
    project_id: String,
) {
    kafka_producer.send_webhook_event(topic, event, team_id, project_id).await;
}

// Adapter to implement TransactionEventSender trait for KafkaProducer
use engine_executors::kafka_integration::{
    TransactionEventSender, TransactionSentEvent, TransactionConfirmedEvent, 
    SharedEventSender, NoOpEventSender
};

#[async_trait::async_trait]
impl TransactionEventSender for KafkaProducer {
    async fn send_transaction_sent(&self, message: TransactionSentEvent) {
        let kafka_message = TransactionSentMessage {
            transaction_id: message.transaction_id,
            chain_id: message.chain_id,
            account_address: message.account_address,
            user_op_hash: message.user_op_hash,
            nonce: message.nonce,
            deployment_lock_acquired: message.deployment_lock_acquired,
            timestamp: message.timestamp,
        };
        self.send_transaction_sent(kafka_message, message.team_id, message.project_id).await;
    }

    async fn send_transaction_confirmed(&self, message: TransactionConfirmedEvent) {
        let kafka_message = TransactionConfirmedMessage {
            transaction_id: message.transaction_id,
            user_op_hash: message.user_op_hash,
            receipt: message.receipt,
            deployment_lock_released: message.deployment_lock_released,
            timestamp: message.timestamp,
        };
        self.send_transaction_confirmed(kafka_message, message.team_id, message.project_id).await;
    }
}

// Helper function to create KafkaProducer from config
pub fn create_kafka_producer(
    kafka_config: Option<KafkaConfig>,
    environment: &Environment,
) -> Result<SharedKafkaProducer, String> {
    match kafka_config {
        Some(config) => {
            tracing::info!("Initializing Kafka producer with WarpStream");
            let producer = KafkaProducer::new(config, environment)?;
            Ok(Arc::new(producer))
        }
        None => {
            tracing::info!("Kafka configuration not provided, creating disabled producer");
            Ok(Arc::new(KafkaProducer::disabled()))
        }
    }
}

// Helper function to create event sender from Kafka producer
pub fn create_event_sender(kafka_producer: &SharedKafkaProducer) -> SharedEventSender {
    if kafka_producer.is_enabled() {
        kafka_producer.clone() as SharedEventSender
    } else {
        Arc::new(NoOpEventSender) as SharedEventSender
    }
}