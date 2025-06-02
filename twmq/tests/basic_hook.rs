// Add this to tests/basic.rs
mod fixtures;
use fixtures::{TestJobErrorData, TestJobOutput};
use redis::aio::ConnectionManager;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use serde::{Deserialize, Serialize};
use twmq::{
    DurableExecution, Queue, SuccessHookData,
    hooks::TransactionContext,
    job::{Job, JobResult, JobStatus},
    queue::QueueOptions,
};

// Helper to clean up Redis keys for a given queue name pattern
async fn cleanup_redis_keys(conn_manager: &ConnectionManager, queue_name: &str) {
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
    println!("Cleaned up keys for pattern: {}", keys_pattern);
}

// Define webhook job types
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WebhookJobPayload {
    pub url: String,
    pub payload: String,
    pub parent_job_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct WebhookJobOutput {
    pub status_code: u16,
    pub response: String,
}

// Main job that queues webhook jobs
#[derive(Serialize, Deserialize, Clone)]
pub struct MainJobPayload {
    pub message: String,
    pub id_to_check: String,
    // Pass webhook queue reference as part of job data
}

pub static MAIN_JOB_PROCESSED: AtomicBool = AtomicBool::new(false);
pub static WEBHOOK_JOB_PROCESSED: AtomicBool = AtomicBool::new(false);

const REDIS_URL: &str = "redis://127.0.0.1:6379/";

type WebhookQueue = Queue<WebhookJobHandler>;
type MainJobQueue = Queue<MainJobHandler>;

struct MainJobHandler {
    pub webhook_queue: Arc<WebhookQueue>,
}

struct WebhookJobHandler;

impl DurableExecution for MainJobHandler {
    type Output = TestJobOutput;
    type ErrorData = TestJobErrorData;
    type JobData = MainJobPayload;

    async fn process(&self, job: &Job<Self::JobData>) -> JobResult<Self::Output, Self::ErrorData> {
        println!("MAIN_JOB: Processing job with id: {}", job.id);
        tokio::time::sleep(Duration::from_millis(50)).await;

        MAIN_JOB_PROCESSED.store(true, Ordering::SeqCst);

        Ok(TestJobOutput {
            reply: format!("Main job processed: {}", job.data.message),
        })
    }

    async fn on_success(
        &self,
        job: &Job<Self::JobData>,
        d: SuccessHookData<'_, Self::Output>,
        tx: &mut TransactionContext<'_>,
    ) {
        println!("MAIN_JOB: on_success hook - queuing webhook job");

        let webhook_job = WebhookJobPayload {
            url: "https://api.example.com/webhook".to_string(),
            payload: serde_json::to_string(d.result).unwrap(),
            parent_job_id: job.data.id_to_check.clone(),
        };

        // Use the type-safe API!
        let mut webhook_builder = self.webhook_queue.clone().job(webhook_job);

        webhook_builder.options.id = format!("{}_webhook", job.data.id_to_check);

        if let Err(e) = tx.queue_job(webhook_builder) {
            tracing::error!("Failed to queue webhook job: {:?}", e);
        } else {
            tracing::info!("Successfully queued webhook job!");
        }
    }
}

impl DurableExecution for WebhookJobHandler {
    type Output = WebhookJobOutput;
    type ErrorData = TestJobErrorData;
    type JobData = WebhookJobPayload;

    async fn process(&self, job: &Job<Self::JobData>) -> JobResult<Self::Output, Self::ErrorData> {
        println!("WEBHOOK_JOB: Sending webhook to: {}", job.data.url);
        println!("WEBHOOK_JOB: Payload: {}", job.data.payload);
        tokio::time::sleep(Duration::from_millis(25)).await;

        WEBHOOK_JOB_PROCESSED.store(true, Ordering::SeqCst);

        // Simulate successful webhook call
        Ok(WebhookJobOutput {
            status_code: 200,
            response: "Webhook delivered successfully".to_string(),
        })
    }

    async fn on_success(
        &self,
        job: &Job<Self::JobData>,
        _d: SuccessHookData<'_, Self::Output>,
        _tx: &mut TransactionContext<'_>,
    ) {
        tracing::info!(
            "WEBHOOK_JOB: Webhook delivered successfully for parent: {}",
            job.data.parent_job_id
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cross_queue_job_scheduling() {
    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "thirdweb_engine=debug,tower_http=debug,axum=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let main_queue_name = format!("test_main_{}", nanoid::nanoid!(6));
    let webhook_queue_name = format!("test_webhook_{}", nanoid::nanoid!(6));

    // Reset flags
    MAIN_JOB_PROCESSED.store(false, Ordering::SeqCst);
    WEBHOOK_JOB_PROCESSED.store(false, Ordering::SeqCst);

    println!("Creating main queue: {}", main_queue_name);
    println!("Creating webhook queue: {}", webhook_queue_name);

    let mut queue_options = QueueOptions::default();
    queue_options.local_concurrency = 1;

    let webhook_handler = WebhookJobHandler;

    // Create webhook queue
    let webhook_queue = Arc::new(
        WebhookQueue::new(
            REDIS_URL,
            &webhook_queue_name,
            Some(queue_options.clone()),
            webhook_handler,
        )
        .await
        .expect("Failed to create webhook queue"),
    );

    let main_handler = MainJobHandler {
        webhook_queue: webhook_queue.clone(),
    };

    // Create main job queue
    let main_queue = Arc::new(
        MainJobQueue::new(
            REDIS_URL,
            &main_queue_name,
            Some(queue_options),
            main_handler,
        )
        .await
        .expect("Failed to create main queue"),
    );

    // Clean up before test
    cleanup_redis_keys(&main_queue.redis, &main_queue_name).await;
    cleanup_redis_keys(&webhook_queue.redis, &webhook_queue_name).await;

    // Create main job with access to webhook queue
    let main_job = MainJobPayload {
        message: "Process transaction #123".to_string(),
        id_to_check: "main_job_001".to_string(),
    };

    println!("Pushing main job");

    main_queue
        .clone()
        .job(main_job)
        .with_id("main_job_001")
        .push()
        .await
        .expect("Failed to push main job");

    // Start workers for both queues
    println!("Starting workers");
    let main_worker = main_queue.work();
    let webhook_worker = webhook_queue.work();

    // Wait for main job to complete
    let mut main_processed = false;
    for _ in 0..50 {
        if MAIN_JOB_PROCESSED.load(Ordering::SeqCst) {
            main_processed = true;
            println!("Main job processed!");
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(main_processed, "Main job should be processed");

    // Give a moment for the webhook job to be queued and processed
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Check that webhook job was queued and processed
    let webhook_pending = webhook_queue.count(JobStatus::Pending).await.unwrap();
    let webhook_success = webhook_queue.count(JobStatus::Success).await.unwrap();

    println!(
        "Webhook queue - Pending: {}, Success: {}",
        webhook_pending, webhook_success
    );

    // Either the webhook job is still pending or already succeeded
    assert!(
        webhook_pending > 0 || webhook_success > 0,
        "Webhook job should be queued"
    );

    // Wait for webhook job to complete if it's still pending
    if webhook_pending > 0 {
        let mut webhook_processed = false;
        for _ in 0..50 {
            if WEBHOOK_JOB_PROCESSED.load(Ordering::SeqCst) {
                webhook_processed = true;
                println!("Webhook job processed!");
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(webhook_processed, "Webhook job should be processed");
    }

    // Verify final state
    let main_success = main_queue.count(JobStatus::Success).await.unwrap();
    let webhook_final_success = webhook_queue.count(JobStatus::Success).await.unwrap();

    assert_eq!(main_success, 1, "Main job should be in success list");
    assert_eq!(
        webhook_final_success, 1,
        "Webhook job should be in success list"
    );

    println!("✅ Cross-queue job scheduling works!");
    println!("Main job triggered webhook job atomically via transaction hooks");

    // Cleanup
    main_worker.shutdown().await.unwrap();
    webhook_worker.shutdown().await.unwrap();
    cleanup_redis_keys(&main_queue.redis, &main_queue_name).await;
    cleanup_redis_keys(&webhook_queue.redis, &webhook_queue_name).await;
}
