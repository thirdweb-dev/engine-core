// tests/delay.rs

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

mod fixtures;
use fixtures::TestJobErrorData;

use twmq::{
    DurableExecution, Queue, SuccessHookData,
    hooks::TransactionContext,
    job::{DelayOptions, Job, JobResult, JobStatus, RequeuePosition},
    queue::QueueOptions,
    redis::aio::ConnectionManager,
};

const REDIS_URL: &str = "redis://127.0.0.1:6379/";

// Helper to clean up Redis keys
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
    tracing::info!("Cleaned up keys for pattern: {}", keys_pattern);
}

// Job that tests delay functionality
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DelayTestJobData {
    pub expected_delay_seconds: u64,
    pub test_id: String, // Unique per test to avoid conflicts
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct DelayTestOutput {
    pub actual_delay_seconds: u64,
    pub message: String,
    pub test_id: String,
}

struct DelayTestJobHandler;

impl DurableExecution for DelayTestJobHandler {
    type Output = DelayTestOutput;
    type ErrorData = TestJobErrorData;
    type JobData = DelayTestJobData;

    async fn process(&self, job: &Job<Self::JobData>) -> JobResult<Self::Output, Self::ErrorData> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let actual_delay = now - job.created_at;

        tracing::info!(
            "DELAY_JOB: Processing job {}, expected delay: {}s, actual delay: {}s",
            job.id,
            job.data.expected_delay_seconds,
            actual_delay
        );

        Ok(DelayTestOutput {
            actual_delay_seconds: actual_delay,
            message: format!("Job {} processed after {}s delay", job.id, actual_delay),
            test_id: job.data.test_id.clone(),
        })
    }

    async fn on_success(
        &self,
        job: &Job<Self::JobData>,
        d: SuccessHookData<'_, Self::Output>,
        tx: &mut TransactionContext<'_>,
    ) {
        tracing::info!("DELAY_JOB: on_success hook - {}", d.result.message);

        // Store processing order in Redis using test-specific key
        let order_key = format!("test:{}:processing_order", job.data.test_id);

        // Use pipeline to add to processing order list
        tx.pipeline().rpush(&order_key, &job.id);
        tx.pipeline().expire(&order_key, 300); // Expire after 5 minutes
    }
}

type DelayTestQueue = Queue<DelayTestJobHandler>;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_job_delay_basic() {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "twmq=debug".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let test_id = nanoid::nanoid!();
    let queue_name = format!("test_delay_{}", test_id);
    let job_id = "delay_job_001";
    let delay_duration = Duration::from_secs(2);

    tracing::info!(
        "Creating delay test queue: {} (test_id: {})",
        queue_name,
        test_id
    );
    tracing::info!("Job should be delayed by: {:?}", delay_duration);

    let queue_options = QueueOptions {
        local_concurrency: 1,
        polling_interval: Duration::from_millis(100),
        always_poll: true,
        ..Default::default()
    };

    // Create Redis connection for the execution context
    let redis_client = redis::Client::open(REDIS_URL).unwrap();
    let redis_conn = Arc::new(redis_client.get_connection_manager().await.unwrap());

    let handler = DelayTestJobHandler;

    let queue = Arc::new(
        DelayTestQueue::new(REDIS_URL, &queue_name, Some(queue_options), handler)
            .await
            .expect("Failed to create delay test queue"),
    );

    cleanup_redis_keys(&redis_conn, &queue_name).await;

    // Create job with delay
    let delay_job = DelayTestJobData {
        expected_delay_seconds: delay_duration.as_secs(),
        test_id: test_id.clone(),
    };

    tracing::info!("Pushing delayed job with ID: {}", job_id);

    let delay_options = DelayOptions {
        delay: delay_duration,
        position: RequeuePosition::Last,
    };

    queue
        .clone()
        .job(delay_job)
        .with_id(job_id)
        .with_delay(delay_options)
        .push()
        .await
        .expect("Failed to push delayed job");

    // Verify job starts in delayed queue, not pending
    let initial_pending = queue.count(JobStatus::Pending).await.unwrap();
    let initial_delayed = queue.count(JobStatus::Delayed).await.unwrap();
    let initial_active = queue.count(JobStatus::Active).await.unwrap();

    tracing::info!(
        "Initial state - Pending: {}, Delayed: {}, Active: {}",
        initial_pending,
        initial_delayed,
        initial_active
    );

    assert_eq!(
        initial_pending, 0,
        "Job should not be in pending queue initially"
    );
    assert_eq!(initial_delayed, 1, "Job should be in delayed queue");
    assert_eq!(initial_active, 0, "No jobs should be active initially");

    // Start worker
    tracing::info!("Starting worker");
    let worker = {
        let queue = queue.clone();
        tokio::spawn(async move {
            if let Err(e) = queue.work().await {
                tracing::error!("Delay worker failed: {:?}", e);
            }
        })
    };

    // Job should not be processed immediately (still delayed)
    tokio::time::sleep(Duration::from_millis(500)).await;

    let early_success_count = queue.count(JobStatus::Success).await.unwrap();
    assert_eq!(
        early_success_count, 0,
        "Job should not be processed before delay expires"
    );

    let mid_pending = queue.count(JobStatus::Pending).await.unwrap();
    let mid_delayed = queue.count(JobStatus::Delayed).await.unwrap();
    tracing::info!(
        "Mid-delay state - Pending: {}, Delayed: {}",
        mid_pending,
        mid_delayed
    );

    // Wait for delay to expire and job to be processed
    let total_wait = delay_duration + Duration::from_secs(2);
    tracing::info!(
        "Waiting {:?} for delay to expire and job to process...",
        total_wait
    );

    let mut job_processed = false;
    let start_waiting = SystemTime::now();

    while start_waiting.elapsed().unwrap() < total_wait {
        let success_count = queue.count(JobStatus::Success).await.unwrap();
        if success_count > 0 {
            job_processed = true;
            tracing::info!(
                "Job processed after waiting {:?}",
                start_waiting.elapsed().unwrap()
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    assert!(job_processed, "Job should be processed after delay expires");

    // Give a moment for final processing
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify final state
    let final_pending = queue.count(JobStatus::Pending).await.unwrap();
    let final_delayed = queue.count(JobStatus::Delayed).await.unwrap();
    let final_active = queue.count(JobStatus::Active).await.unwrap();
    let final_success = queue.count(JobStatus::Success).await.unwrap();

    tracing::info!(
        "Final state - Pending: {}, Delayed: {}, Active: {}, Success: {}",
        final_pending,
        final_delayed,
        final_active,
        final_success
    );

    assert_eq!(final_delayed, 0, "No jobs should remain in delayed queue");
    assert_eq!(final_active, 0, "No jobs should be active");
    assert_eq!(final_success, 1, "Job should be in success queue");

    // Verify the job result shows correct delay timing
    let mut redis_conn_direct = redis_conn.as_ref().clone();
    let result_json: Option<String> = redis_conn_direct
        .hget(queue.job_result_hash_name(), job_id)
        .await
        .expect("Failed to get job result");

    assert!(result_json.is_some(), "Job result should be stored");

    let job_output: DelayTestOutput =
        serde_json::from_str(&result_json.unwrap()).expect("Failed to deserialize job result");

    tracing::info!("Job output: {:?}", job_output);

    // Allow some tolerance for timing (±1 second)
    let expected_delay = delay_duration.as_secs();
    let actual_delay = job_output.actual_delay_seconds;

    assert!(
        actual_delay >= expected_delay && actual_delay <= expected_delay + 2,
        "Actual delay ({}) should be close to expected delay ({})",
        actual_delay,
        expected_delay
    );

    tracing::info!("✅ Basic delay mechanism works correctly!");

    worker.abort();
    cleanup_redis_keys(&redis_conn, &queue_name).await;

    // Clean up test-specific keys
    let _: () = redis_conn_direct
        .del(format!("test:{}:processing_order", test_id))
        .await
        .unwrap_or(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_delay_position_ordering() {
    // Test that delayed jobs respect RequeuePosition when they expire

    let test_id = nanoid::nanoid!();
    let queue_name = format!("test_delay_order_{}", test_id);

    tracing::info!(
        "\n=== Testing delay position ordering (test_id: {}) ===",
        test_id
    );

    let queue_options = QueueOptions {
        local_concurrency: 1, // Process one job at a time to see clear ordering
        polling_interval: Duration::from_millis(50),
        always_poll: true,
        ..Default::default()
    };

    // Create Redis connection for the execution context
    let redis_client = redis::Client::open(REDIS_URL).unwrap();
    let redis_conn = Arc::new(redis_client.get_connection_manager().await.unwrap());

    let handler = DelayTestJobHandler;

    let queue = Arc::new(
        DelayTestQueue::new(REDIS_URL, &queue_name, Some(queue_options), handler)
            .await
            .expect("Failed to create delay order queue"),
    );

    cleanup_redis_keys(&redis_conn, &queue_name).await;

    let short_delay = Duration::from_secs(1);

    // Push jobs in specific order:
    // 1. Normal job (no delay) - should process first
    // 2. Delayed job with "First" position - should process second when delay expires
    // 3. Delayed job with "Last" position - should process third when delay expires

    // Job 1: Normal job (immediate)
    let immediate_job = DelayTestJobData {
        expected_delay_seconds: 0,
        test_id: test_id.clone(),
    };

    queue
        .clone()
        .job(immediate_job)
        .with_id("immediate")
        .push()
        .await
        .unwrap();

    // Job 2: Delayed job with First position
    let delayed_first_job = DelayTestJobData {
        expected_delay_seconds: short_delay.as_secs(),
        test_id: test_id.clone(),
    };

    let first_delay_options = DelayOptions {
        delay: short_delay,
        position: RequeuePosition::First,
    };

    queue
        .clone()
        .job(delayed_first_job)
        .with_id("delayed_first")
        .with_delay(first_delay_options)
        .push()
        .await
        .unwrap();

    // Job 3: Delayed job with Last position
    let delayed_last_job = DelayTestJobData {
        expected_delay_seconds: short_delay.as_secs(),
        test_id: test_id.clone(),
    };

    let last_delay_options = DelayOptions {
        delay: short_delay,
        position: RequeuePosition::Last,
    };

    queue
        .clone()
        .job(delayed_last_job)
        .with_id("delayed_last")
        .with_delay(last_delay_options)
        .push()
        .await
        .unwrap();

    // Verify initial state
    let pending_count = queue.count(JobStatus::Pending).await.unwrap();
    let delayed_count = queue.count(JobStatus::Delayed).await.unwrap();

    tracing::info!(
        "Initial state - Pending: {}, Delayed: {}",
        pending_count,
        delayed_count
    );
    assert_eq!(pending_count, 1, "Should have 1 immediate job pending");
    assert_eq!(delayed_count, 2, "Should have 2 delayed jobs");

    // Start worker
    let worker = {
        let queue = queue.clone();
        tokio::spawn(async move {
            if let Err(e) = queue.work().await {
                tracing::error!("Delay order worker failed: {:?}", e);
            }
        })
    };

    // Wait for all jobs to complete
    let max_wait = short_delay + Duration::from_secs(3);
    let start_time = SystemTime::now();

    while start_time.elapsed().unwrap() < max_wait {
        let success_count = queue.count(JobStatus::Success).await.unwrap();
        if success_count >= 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Check processing order from Redis
    let order_key = format!("test:{}:processing_order", test_id);
    let mut redis_conn_direct = redis_conn.as_ref().clone();
    let processing_order: Vec<String> = redis_conn_direct
        .lrange(&order_key, 0, -1)
        .await
        .unwrap_or_default();

    tracing::info!("Processing order: {:?}", processing_order);

    assert_eq!(processing_order.len(), 3, "All 3 jobs should be processed");
    assert_eq!(
        processing_order[0], "immediate",
        "Immediate job should process first"
    );

    // The delayed jobs should process in position order: First comes before Last
    assert_eq!(
        processing_order[1], "delayed_first",
        "Delayed job with First position should process before Last position"
    );
    assert_eq!(
        processing_order[2], "delayed_last",
        "Delayed job with Last position should process last"
    );

    tracing::info!("✅ Delay position ordering works correctly!");

    worker.abort();
    cleanup_redis_keys(&redis_conn, &queue_name).await;

    // Clean up test-specific keys
    let _: () = redis_conn_direct.del(&order_key).await.unwrap_or(());
}
