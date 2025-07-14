use std::sync::Arc;
use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use serde::Serialize;

use crate::config::{Environment, KafkaConfig};
use engine_executors::kafka_integration::{TransactionSentEvent, TransactionConfirmedEvent, TransactionEventSender};

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

pub enum TransactionTopic {
    Sent,
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

    pub async fn send_transaction_sent(&self, event: TransactionSentEvent) {
        if !self.is_enabled() {
            tracing::debug!("Kafka disabled, skipping transaction sent message");
            return;
        }

        let envelope = KafkaEventEnvelope::new(event.team_id.clone(), event.project_id.clone(), event);
        self.send_message(TransactionTopic::Sent, &envelope.data.transaction_id, &envelope)
            .await;
    }

    pub async fn send_transaction_confirmed(&self, event: TransactionConfirmedEvent) {
        if !self.is_enabled() {
            tracing::debug!("Kafka disabled, skipping transaction confirmed message");
            return;
        }

        let envelope = KafkaEventEnvelope::new(event.team_id.clone(), event.project_id.clone(), event);
        self.send_message(TransactionTopic::Confirmed, &envelope.data.transaction_id, &envelope)
            .await;
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

// Implement TransactionEventSender trait for KafkaProducer
#[async_trait::async_trait]
impl TransactionEventSender for KafkaProducer {
    async fn send_transaction_sent(&self, event: TransactionSentEvent) {
        self.send_transaction_sent(event).await;
    }

    async fn send_transaction_confirmed(&self, event: TransactionConfirmedEvent) {
        self.send_transaction_confirmed(event).await;
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
pub fn create_event_sender(kafka_producer: &SharedKafkaProducer) -> engine_executors::kafka_integration::SharedEventSender {
    if kafka_producer.is_enabled() {
        kafka_producer.clone() as engine_executors::kafka_integration::SharedEventSender
    } else {
        Arc::new(engine_executors::kafka_integration::NoOpEventSender) as engine_executors::kafka_integration::SharedEventSender
    }
}