use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;

use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use twmq::redis::aio::ConnectionManager;
use twmq::error::TwmqError;
use twmq::hooks::TransactionContext;
use twmq::job::{BorrowedJob, JobResult};
use twmq::{DurableExecution, SuccessHookData, UserCancellable};

use engine_core::execution_options::WebhookOptions;
use engine_executors::webhook::{WebhookJobHandler, WebhookJobPayload, WebhookRetryConfig};

// Redis connection URL for tests
pub const REDIS_URL: &str = "redis://127.0.0.1:6379/";

// Helper to clean up Redis keys for a given queue name pattern
pub async fn cleanup_redis_keys(conn_manager: &ConnectionManager, queue_name: &str) {
    let mut conn = conn_manager.clone();
    let keys_pattern = format!("twmq:{}:*", queue_name);

    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(&keys_pattern)
        .query_async(&mut conn)
        .await
        .unwrap_or_default();
    if !keys.is_empty() {
        redis::cmd("DEL")
            .arg(keys)
            .query_async::<()>(&mut conn)
            .await
            .unwrap_or_default();
    }
    tracing::info!("Cleaned up keys for pattern: {}", keys_pattern);
}

// Setup tracing for tests
pub fn setup_tracing() {
    use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};
    
    let _ = tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            "engine_executors=debug,twmq=debug".into()
        }))
        .with(tracing_subscriber::fmt::layer())
        .try_init();
}

// --- Test Job Definition for basic testing ---

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TestJobPayload {
    pub message: String,
    pub id_to_check: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct TestJobOutput {
    pub reply: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TestJobErrorData {
    pub reason: String,
}

impl From<TwmqError> for TestJobErrorData {
    fn from(error: TwmqError) -> Self {
        TestJobErrorData {
            reason: error.to_string(),
        }
    }
}

impl UserCancellable for TestJobErrorData {
    fn user_cancelled() -> Self {
        TestJobErrorData {
            reason: "Transaction cancelled by user".to_string(),
        }
    }
}

// Use a static AtomicBool to signal from the job process to the test
pub static TEST_JOB_PROCESSED_SUCCESSFULLY: AtomicBool = AtomicBool::new(false);

pub struct TestJobHandler;

impl DurableExecution for TestJobHandler {
    type Output = TestJobOutput;
    type ErrorData = TestJobErrorData;
    type JobData = TestJobPayload;

    async fn process(&self, job: &BorrowedJob<Self::JobData>) -> JobResult<Self::Output, Self::ErrorData> {
        tracing::info!(
            "TEST_JOB: Processing job with id_to_check: {}",
            job.job.data.id_to_check
        );
        // Simulate some work
        tokio::time::sleep(Duration::from_millis(50)).await;
        TEST_JOB_PROCESSED_SUCCESSFULLY.store(true, Ordering::SeqCst);
        Ok(TestJobOutput {
            reply: format!("Successfully processed '{}'", job.job.data.message),
        })
    }

    async fn on_success(
        &self,
        job: &BorrowedJob<Self::JobData>,
        _d: SuccessHookData<'_, Self::Output>,
        _tx: &mut TransactionContext<'_>,
    ) {
        tracing::info!(
            "TEST_JOB: on_success hook for id_to_check: {}",
            job.job.data.id_to_check
        );
    }
}

// --- Mock Job Data for Webhook Testing ---

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MockJobData {
    pub id: String,
    pub webhook_options: Option<Vec<WebhookOptions>>,
}

impl engine_executors::webhook::envelope::HasWebhookOptions for MockJobData {
    fn webhook_options(&self) -> Option<Vec<WebhookOptions>> {
        self.webhook_options.clone()
    }
}

// --- Mock Output for Testing ---

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MockOutput {
    pub success: bool,
    pub data: String,
}

// --- Mock Error for Testing ---

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MockError {
    pub code: i32,
    pub message: String,
}

impl From<TwmqError> for MockError {
    fn from(error: TwmqError) -> Self {
        MockError {
            code: 500,
            message: error.to_string(),
        }
    }
}

impl UserCancellable for MockError {
    fn user_cancelled() -> Self {
        MockError {
            code: 499,
            message: "Transaction cancelled by user".to_string(),
        }
    }
}

// --- Mock Executor for Testing Webhook Envelopes ---

pub struct MockExecutor {
    pub webhook_queue: Arc<twmq::Queue<WebhookJobHandler>>,
}

impl engine_executors::webhook::envelope::ExecutorStage for MockExecutor {
    fn executor_name() -> &'static str {
        "mock_executor"
    }

    fn stage_name() -> &'static str {
        "mock_stage"
    }
}

impl engine_executors::webhook::envelope::WebhookCapable for MockExecutor {
    fn webhook_queue(&self) -> &Arc<twmq::Queue<WebhookJobHandler>> {
        &self.webhook_queue
    }
}

// --- Test Data Creators ---

pub fn create_test_webhook_options() -> Vec<WebhookOptions> {
    vec![
        WebhookOptions {
            url: "https://example.com/webhook1".to_string(),
            secret: Some("secret1".to_string()),
        },
        WebhookOptions {
            url: "https://example.com/webhook2".to_string(),
            secret: None,
        },
    ]
}

pub fn create_test_webhook_payload(url: String) -> WebhookJobPayload {
    WebhookJobPayload {
        url,
        body: serde_json::json!({"message": "test"}).to_string(),
        headers: None,
        hmac_secret: None,
        http_method: Some("POST".to_string()),
    }
}

pub fn create_webhook_handler() -> WebhookJobHandler {
    WebhookJobHandler {
        http_client: reqwest::Client::new(),
        retry_config: Arc::new(WebhookRetryConfig::default()),
    }
}