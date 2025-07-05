use std::sync::Arc;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;
use twmq::job::{Job, BorrowedJob, RequeuePosition};
use twmq::{Queue, hooks::TransactionContext};
use engine_core::execution_options::WebhookOptions;
use engine_executors::webhook::{
    WebhookJobHandler, WebhookJobPayload, WebhookRetryConfig,
    envelope::{
        WebhookNotificationEnvelope, StageEvent, SerializableSuccessData, SerializableNackData,
        SerializableFailData, ExecutorStage, HasWebhookOptions, HasTransactionMetadata,
        WebhookCapable,
    },
};

// Mock job data for testing
#[derive(Serialize, Deserialize, Debug, Clone)]
struct MockJobData {
    pub id: String,
    pub webhook_options: Option<Vec<WebhookOptions>>,
}

impl HasWebhookOptions for MockJobData {
    fn webhook_options(&self) -> Option<Vec<WebhookOptions>> {
        self.webhook_options.clone()
    }
}

// Mock output for testing
#[derive(Serialize, Deserialize, Debug, Clone)]
struct MockOutput {
    pub success: bool,
    pub data: String,
}

// Mock error for testing
#[derive(Serialize, Deserialize, Debug, Clone)]
struct MockError {
    pub code: i32,
    pub message: String,
}

// Mock executor for testing
struct MockExecutor {
    pub webhook_queue: Arc<Queue<WebhookJobHandler>>,
}

impl ExecutorStage for MockExecutor {
    fn executor_name() -> &'static str {
        "mock_executor"
    }

    fn stage_name() -> &'static str {
        "mock_stage"
    }
}

impl WebhookCapable for MockExecutor {
    fn webhook_queue(&self) -> &Arc<Queue<WebhookJobHandler>> {
        &self.webhook_queue
    }
}

#[test]
fn test_stage_event_serialization() {
    let success_event = StageEvent::Success;
    let serialized = serde_json::to_string(&success_event).unwrap();
    assert_eq!(serialized, "\"SUCCESS\"");
    
    let nack_event = StageEvent::Nack;
    let serialized = serde_json::to_string(&nack_event).unwrap();
    assert_eq!(serialized, "\"NACK\"");
    
    let failure_event = StageEvent::Failure;
    let serialized = serde_json::to_string(&failure_event).unwrap();
    assert_eq!(serialized, "\"FAILURE\"");
}

#[test]
fn test_webhook_notification_envelope_serialization() {
    let envelope = WebhookNotificationEnvelope {
        notification_id: "notif_123".to_string(),
        transaction_id: "tx_456".to_string(),
        timestamp: 1234567890,
        executor_name: "test_executor".to_string(),
        stage_name: "test_stage".to_string(),
        event_type: StageEvent::Success,
        payload: SerializableSuccessData {
            result: MockOutput {
                success: true,
                data: "test_data".to_string(),
            },
        },
        delivery_target_url: Some("https://example.com/webhook".to_string()),
    };
    
    let serialized = serde_json::to_string(&envelope).unwrap();
    let deserialized: WebhookNotificationEnvelope<SerializableSuccessData<MockOutput>> = 
        serde_json::from_str(&serialized).unwrap();
    
    assert_eq!(deserialized.notification_id, "notif_123");
    assert_eq!(deserialized.transaction_id, "tx_456");
    assert_eq!(deserialized.timestamp, 1234567890);
    assert_eq!(deserialized.executor_name, "test_executor");
    assert_eq!(deserialized.stage_name, "test_stage");
    assert!(matches!(deserialized.event_type, StageEvent::Success));
    assert_eq!(deserialized.payload.result.success, true);
    assert_eq!(deserialized.payload.result.data, "test_data");
    assert_eq!(deserialized.delivery_target_url, Some("https://example.com/webhook".to_string()));
}

#[test]
fn test_serializable_success_data() {
    let success_data = SerializableSuccessData {
        result: MockOutput {
            success: true,
            data: "success_data".to_string(),
        },
    };
    
    let serialized = serde_json::to_string(&success_data).unwrap();
    let deserialized: SerializableSuccessData<MockOutput> = serde_json::from_str(&serialized).unwrap();
    
    assert_eq!(deserialized.result.success, true);
    assert_eq!(deserialized.result.data, "success_data");
}

#[test]
fn test_serializable_nack_data() {
    let nack_data = SerializableNackData {
        error: MockError {
            code: 500,
            message: "Server Error".to_string(),
        },
        delay_ms: Some(1000),
        position: RequeuePosition::Last,
        attempt_number: 3,
        max_attempts: Some(5),
        next_retry_at: Some(1234567890),
    };
    
    let serialized = serde_json::to_string(&nack_data).unwrap();
    let deserialized: SerializableNackData<MockError> = serde_json::from_str(&serialized).unwrap();
    
    assert_eq!(deserialized.error.code, 500);
    assert_eq!(deserialized.error.message, "Server Error");
    assert_eq!(deserialized.delay_ms, Some(1000));
    assert_eq!(deserialized.position, RequeuePosition::Last);
    assert_eq!(deserialized.attempt_number, 3);
    assert_eq!(deserialized.max_attempts, Some(5));
    assert_eq!(deserialized.next_retry_at, Some(1234567890));
}

#[test]
fn test_serializable_fail_data() {
    let fail_data = SerializableFailData {
        error: MockError {
            code: 400,
            message: "Bad Request".to_string(),
        },
        final_attempt_number: 5,
    };
    
    let serialized = serde_json::to_string(&fail_data).unwrap();
    let deserialized: SerializableFailData<MockError> = serde_json::from_str(&serialized).unwrap();
    
    assert_eq!(deserialized.error.code, 400);
    assert_eq!(deserialized.error.message, "Bad Request");
    assert_eq!(deserialized.final_attempt_number, 5);
}

#[test]
fn test_has_webhook_options_trait() {
    let webhook_options = vec![
        WebhookOptions {
            url: "https://example.com/webhook1".to_string(),
            secret: Some("secret1".to_string()),
        },
        WebhookOptions {
            url: "https://example.com/webhook2".to_string(),
            secret: None,
        },
    ];
    
    let job_data = MockJobData {
        id: "test_id".to_string(),
        webhook_options: Some(webhook_options.clone()),
    };
    
    let result = job_data.webhook_options();
    assert!(result.is_some());
    
    let options = result.unwrap();
    assert_eq!(options.len(), 2);
    assert_eq!(options[0].url, "https://example.com/webhook1");
    assert_eq!(options[0].secret, Some("secret1".to_string()));
    assert_eq!(options[1].url, "https://example.com/webhook2");
    assert_eq!(options[1].secret, None);
}

#[test]
fn test_has_webhook_options_none() {
    let job_data = MockJobData {
        id: "test_id".to_string(),
        webhook_options: None,
    };
    
    let result = job_data.webhook_options();
    assert!(result.is_none());
}

#[test]
fn test_has_transaction_metadata_trait() {
    let job_data = MockJobData {
        id: "test_id".to_string(),
        webhook_options: None,
    };
    
    let job = Job::new(job_data);
    let transaction_id = job.transaction_id();
    
    assert_eq!(transaction_id, job.id);
}

#[test]
fn test_webhook_notification_envelope_without_delivery_url() {
    let envelope = WebhookNotificationEnvelope {
        notification_id: "notif_123".to_string(),
        transaction_id: "tx_456".to_string(),
        timestamp: 1234567890,
        executor_name: "test_executor".to_string(),
        stage_name: "test_stage".to_string(),
        event_type: StageEvent::Failure,
        payload: SerializableFailData {
            error: MockError {
                code: 500,
                message: "Internal Error".to_string(),
            },
            final_attempt_number: 3,
        },
        delivery_target_url: None,
    };
    
    let serialized = serde_json::to_string(&envelope).unwrap();
    let deserialized: WebhookNotificationEnvelope<SerializableFailData<MockError>> = 
        serde_json::from_str(&serialized).unwrap();
    
    assert_eq!(deserialized.delivery_target_url, None);
    assert!(matches!(deserialized.event_type, StageEvent::Failure));
    assert_eq!(deserialized.payload.error.code, 500);
    assert_eq!(deserialized.payload.final_attempt_number, 3);
}

#[test]
fn test_envelope_with_nack_event() {
    let envelope = WebhookNotificationEnvelope {
        notification_id: "notif_nack".to_string(),
        transaction_id: "tx_nack".to_string(),
        timestamp: 1234567890,
        executor_name: "retry_executor".to_string(),
        stage_name: "retry_stage".to_string(),
        event_type: StageEvent::Nack,
        payload: SerializableNackData {
            error: MockError {
                code: 503,
                message: "Service Unavailable".to_string(),
            },
            delay_ms: Some(5000),
            position: RequeuePosition::First,
            attempt_number: 2,
            max_attempts: Some(10),
            next_retry_at: Some(1234567895),
        },
        delivery_target_url: Some("https://retry.example.com/webhook".to_string()),
    };
    
    let serialized = serde_json::to_string(&envelope).unwrap();
    let deserialized: WebhookNotificationEnvelope<SerializableNackData<MockError>> = 
        serde_json::from_str(&serialized).unwrap();
    
    assert_eq!(deserialized.notification_id, "notif_nack");
    assert_eq!(deserialized.transaction_id, "tx_nack");
    assert_eq!(deserialized.executor_name, "retry_executor");
    assert_eq!(deserialized.stage_name, "retry_stage");
    assert!(matches!(deserialized.event_type, StageEvent::Nack));
    assert_eq!(deserialized.payload.error.code, 503);
    assert_eq!(deserialized.payload.delay_ms, Some(5000));
    assert_eq!(deserialized.payload.position, RequeuePosition::First);
}

#[test]
fn test_executor_stage_trait() {
    assert_eq!(MockExecutor::executor_name(), "mock_executor");
    assert_eq!(MockExecutor::stage_name(), "mock_stage");
}

#[test]
fn test_envelope_timestamp_generation() {
    let envelope1 = WebhookNotificationEnvelope {
        notification_id: "notif_1".to_string(),
        transaction_id: "tx_1".to_string(),
        timestamp: chrono::Utc::now().timestamp() as u64,
        executor_name: "test".to_string(),
        stage_name: "test".to_string(),
        event_type: StageEvent::Success,
        payload: SerializableSuccessData {
            result: MockOutput {
                success: true,
                data: "test".to_string(),
            },
        },
        delivery_target_url: None,
    };
    
    std::thread::sleep(std::time::Duration::from_millis(100));
    
    let envelope2 = WebhookNotificationEnvelope {
        notification_id: "notif_2".to_string(),
        transaction_id: "tx_2".to_string(),
        timestamp: chrono::Utc::now().timestamp() as u64,
        executor_name: "test".to_string(),
        stage_name: "test".to_string(),
        event_type: StageEvent::Success,
        payload: SerializableSuccessData {
            result: MockOutput {
                success: true,
                data: "test".to_string(),
            },
        },
        delivery_target_url: None,
    };
    
    // Second envelope should have a timestamp >= first envelope
    assert!(envelope2.timestamp >= envelope1.timestamp);
}

#[test]
fn test_notification_id_uniqueness() {
    let mut ids = std::collections::HashSet::new();
    
    for _ in 0..100 {
        let id = Uuid::new_v4().to_string();
        assert!(ids.insert(id), "Duplicate UUID generated");
    }
}

#[test]
fn test_complex_nested_payload() {
    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct ComplexPayload {
        pub nested: HashMap<String, serde_json::Value>,
        pub array: Vec<String>,
        pub number: f64,
    }
    
    let mut nested_data = HashMap::new();
    nested_data.insert("key1".to_string(), json!("value1"));
    nested_data.insert("key2".to_string(), json!(42));
    nested_data.insert("key3".to_string(), json!({"nested": "object"}));
    
    let complex_payload = ComplexPayload {
        nested: nested_data,
        array: vec!["item1".to_string(), "item2".to_string()],
        number: 3.14159,
    };
    
    let envelope = WebhookNotificationEnvelope {
        notification_id: "complex_notif".to_string(),
        transaction_id: "complex_tx".to_string(),
        timestamp: 1234567890,
        executor_name: "complex_executor".to_string(),
        stage_name: "complex_stage".to_string(),
        event_type: StageEvent::Success,
        payload: SerializableSuccessData {
            result: complex_payload,
        },
        delivery_target_url: Some("https://complex.example.com/webhook".to_string()),
    };
    
    let serialized = serde_json::to_string(&envelope).unwrap();
    let deserialized: WebhookNotificationEnvelope<SerializableSuccessData<ComplexPayload>> = 
        serde_json::from_str(&serialized).unwrap();
    
    assert_eq!(deserialized.payload.result.array.len(), 2);
    assert_eq!(deserialized.payload.result.number, 3.14159);
    assert_eq!(deserialized.payload.result.nested.len(), 3);
    assert_eq!(deserialized.payload.result.nested["key1"], json!("value1"));
    assert_eq!(deserialized.payload.result.nested["key2"], json!(42));
}

#[test]
fn test_requeue_position_serialization() {
    let first_pos = RequeuePosition::First;
    let last_pos = RequeuePosition::Last;
    
    let first_serialized = serde_json::to_string(&first_pos).unwrap();
    let last_serialized = serde_json::to_string(&last_pos).unwrap();
    
    assert_eq!(first_serialized, "\"First\"");
    assert_eq!(last_serialized, "\"Last\"");
    
    let first_deserialized: RequeuePosition = serde_json::from_str(&first_serialized).unwrap();
    let last_deserialized: RequeuePosition = serde_json::from_str(&last_serialized).unwrap();
    
    assert_eq!(first_deserialized, RequeuePosition::First);
    assert_eq!(last_deserialized, RequeuePosition::Last);
}