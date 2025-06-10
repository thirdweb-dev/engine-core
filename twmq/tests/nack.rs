// tests/nack.rs

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use twmq::{
    DurableExecution, FailHookData, NackHookData, Queue, SuccessHookData,
    hooks::TransactionContext,
    job::{BorrowedJob, JobError, JobResult, JobStatus, RequeuePosition},
    queue::QueueOptions,
    redis::aio::ConnectionManager,
};

mod fixtures;
use fixtures::TestJobErrorData;

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

// Job that retries until reaching desired attempts
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RetryJobPayload {
    pub id_to_check: String,
    pub desired_attempts: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct RetryJobOutput {
    pub final_attempt: u32,
    pub message: String,
}
struct RetryJobHandler;

pub static RETRY_JOB_FINAL_SUCCESS: AtomicBool = AtomicBool::new(false);

impl DurableExecution for RetryJobHandler {
    type Output = RetryJobOutput;
    type ErrorData = TestJobErrorData;
    type JobData = RetryJobPayload;

    async fn process(
        &self,
        job: &BorrowedJob<Self::JobData>,
    ) -> JobResult<Self::Output, Self::ErrorData> {
        let current_attempt = job.job.attempts;

        tracing::info!(
            "RETRY_JOB: Processing job {}, attempt {}/{}",
            job.job.id,
            current_attempt,
            job.job.data.desired_attempts
        );

        if current_attempt < job.job.data.desired_attempts {
            // Not enough attempts yet, nack it
            tracing::info!(
                "RETRY_JOB: Nacking job {} (attempt {}/{})",
                job.job.id,
                current_attempt,
                job.job.data.desired_attempts
            );

            Err(JobError::Nack {
                error: TestJobErrorData {
                    reason: format!(
                        "Need {} attempts, only at {}",
                        job.job.data.desired_attempts, current_attempt
                    ),
                },
                delay: None,
                position: RequeuePosition::Last,
            })
        } else {
            // Reached desired attempts, succeed!
            tracing::info!(
                "RETRY_JOB: Success on attempt {}/{}",
                current_attempt,
                job.job.data.desired_attempts
            );

            RETRY_JOB_FINAL_SUCCESS.store(true, Ordering::SeqCst);

            Ok(RetryJobOutput {
                final_attempt: current_attempt,
                message: format!("Succeeded after {} attempts", current_attempt),
            })
        }
    }

    async fn on_success(
        &self,
        _job: &BorrowedJob<Self::JobData>,
        d: SuccessHookData<'_, Self::Output>,
        _tx: &mut TransactionContext<'_>,
    ) {
        tracing::info!(
            "RETRY_JOB: on_success hook - final attempt was {}",
            d.result.final_attempt
        );
    }

    async fn on_nack(
        &self,
        job: &BorrowedJob<Self::JobData>,
        d: NackHookData<'_, Self::ErrorData>,
        _tx: &mut TransactionContext<'_>,
    ) {
        tracing::info!(
            "RETRY_JOB: on_nack hook - attempt {} failed: {}",
            job.job.attempts,
            d.error.reason
        );
        if let Some(delay_duration) = d.delay {
            tracing::info!("Will retry after {:?}", delay_duration);
        }
    }

    async fn on_fail(
        &self,
        job: &BorrowedJob<Self::JobData>,
        _d: FailHookData<'_, Self::ErrorData>,
        _tx: &mut TransactionContext<'_>,
    ) {
        tracing::error!(
            "RETRY_JOB: on_fail hook - permanently failed at attempt {}",
            job.job.attempts
        );
    }

    async fn on_timeout(&self, _tx: &mut TransactionContext<'_>) {
        tracing::info!("RETRY_JOB: on_timeout hook");
    }
}

type RetryJobQueue = Queue<RetryJobHandler>;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_job_retry_attempts() {
    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "thirdweb_engine=debug,tower_http=debug,axum=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let queue_name = format!("test_retry_{}", nanoid::nanoid!(6));
    let job_id = "retry_job_001";
    let desired_attempts = 4u32; // Job should nack 3 times, then succeed on 4th attempt

    // Reset counters
    RETRY_JOB_FINAL_SUCCESS.store(false, Ordering::SeqCst);

    tracing::info!("Creating retry queue: {}", queue_name);
    tracing::info!("Job should succeed after {} attempts", desired_attempts);

    let mut queue_options = QueueOptions::default();
    queue_options.local_concurrency = 1;

    let handler = RetryJobHandler;

    let queue = Arc::new(
        RetryJobQueue::new(REDIS_URL, &queue_name, Some(queue_options), handler)
            .await
            .expect("Failed to create retry queue"),
    );

    // Clean up before test
    cleanup_redis_keys(&queue.redis, &queue_name).await;

    // Create job that requires multiple attempts
    let retry_job = RetryJobPayload {
        id_to_check: job_id.to_string(),
        desired_attempts,
    };

    tracing::info!("Pushing retry job with ID: {}", job_id);
    queue
        .clone()
        .job(retry_job)
        .with_id(job_id)
        .push()
        .await
        .expect("Failed to push retry job");

    // Start worker
    tracing::info!("Starting worker");
    let worker = queue.work();

    // Wait for job to complete (with retries)
    let mut final_success = false;
    for i in 0..100 {
        // Give it plenty of time for retries
        if RETRY_JOB_FINAL_SUCCESS.load(Ordering::SeqCst) {
            final_success = true;
            tracing::info!(
                "Retry job finally succeeded after {} polling attempts",
                i + 1
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    assert!(final_success, "Retry job should eventually succeed");

    // Give a moment for final processing
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify job is in success list
    let success_count = queue.count(JobStatus::Success).await.unwrap();
    assert_eq!(success_count, 1, "Job should be in success list");

    // Fetch the completed job and verify attempt count
    let completed_job = queue
        .get_job(job_id)
        .await
        .expect("Failed to fetch job")
        .expect("Job should exist");

    tracing::info!("Final job state:");
    tracing::info!("  Job ID: {}", completed_job.id);
    tracing::info!("  Attempts: {}", completed_job.attempts);
    tracing::info!("  Created at: {}", completed_job.created_at);
    tracing::info!("  Processed at: {:?}", completed_job.processed_at);
    tracing::info!("  Finished at: {:?}", completed_job.finished_at);

    // Verify the job took exactly the desired number of attempts
    assert_eq!(
        completed_job.attempts, desired_attempts,
        "Job should have exactly {} attempts, but had {}",
        desired_attempts, completed_job.attempts
    );

    // Verify the job result contains the correct attempt count
    let mut redis_conn = queue.redis.clone();
    let result_json: Option<String> = redis_conn
        .hget(queue.job_result_hash_name(), job_id)
        .await
        .expect("Failed to get job result");

    assert!(result_json.is_some(), "Job result should be stored");

    let job_output: RetryJobOutput =
        serde_json::from_str(&result_json.unwrap()).expect("Failed to deserialize job result");

    assert_eq!(
        job_output.final_attempt, desired_attempts,
        "Job result should show final attempt as {}",
        desired_attempts
    );

    tracing::info!("✅ Retry mechanism works correctly!");
    tracing::info!(
        "Job succeeded on attempt {} as expected",
        job_output.final_attempt
    );
    tracing::info!("Result message: {}", job_output.message);

    // Cleanup
    worker.shutdown().await.unwrap();
    cleanup_redis_keys(&queue.redis, &queue_name).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_different_retry_counts() {
    // Test multiple different retry counts to ensure it works consistently
    let test_cases = vec![1, 2, 3, 5, 7];

    for desired_attempts in test_cases {
        tracing::info!("\n=== Testing {} attempts ===", desired_attempts);

        let queue_name = format!("test_retry_{}_{}", desired_attempts, nanoid::nanoid!(4));
        let job_id = format!("retry_job_{}", desired_attempts);

        // Reset counters
        RETRY_JOB_FINAL_SUCCESS.store(false, Ordering::SeqCst);

        let mut queue_options = QueueOptions::default();
        queue_options.local_concurrency = 1;

        let handler = RetryJobHandler;

        let queue = Arc::new(
            RetryJobQueue::new(REDIS_URL, &queue_name, Some(queue_options), handler)
                .await
                .expect("Failed to create retry queue"),
        );

        cleanup_redis_keys(&queue.redis, &queue_name).await;

        let retry_job = RetryJobPayload {
            id_to_check: job_id.clone(),
            desired_attempts,
        };

        queue
            .clone()
            .job(retry_job)
            .with_id(&job_id)
            .push()
            .await
            .expect("Failed to push retry job");
        let worker = queue.work();

        // Wait for completion
        for _ in 0..50 {
            if RETRY_JOB_FINAL_SUCCESS.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        tokio::time::sleep(Duration::from_millis(200)).await;

        let completed_job = queue.get_job(&job_id).await.unwrap().unwrap();
        assert_eq!(completed_job.attempts, desired_attempts);

        worker.shutdown().await.unwrap();
        cleanup_redis_keys(&queue.redis, &queue_name).await;

        tracing::info!("✅ {} attempts test passed", desired_attempts);
    }

    tracing::info!("\n✅ All retry count tests passed!");
}
