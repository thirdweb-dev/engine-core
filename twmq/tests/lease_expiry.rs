// tests/lease_expiry.rs

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use twmq::{
    DurableExecution, FailHookData, NackHookData, Queue, SuccessHookData,
    hooks::TransactionContext,
    job::{Job, JobResult, JobStatus},
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

// Job that sleeps forever to test lease expiry
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SleepForeverJob {
    pub id_to_check: String,
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct SleepJobOutput {
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SleepJobErrorData {
    pub reason: String,
}

pub static JOB_STARTED_PROCESSING: AtomicBool = AtomicBool::new(false);
pub static JOB_SHOULD_CONTINUE_SLEEPING: AtomicBool = AtomicBool::new(true);

pub static MULTI_JOB_STARTED_PROCESSING: AtomicBool = AtomicBool::new(false);
pub static MULTI_JOB_SHOULD_CONTINUE_SLEEPING: AtomicBool = AtomicBool::new(true);

impl DurableExecution for SleepForeverJob {
    type Output = SleepJobOutput;
    type ErrorData = SleepJobErrorData;
    type ExecutionContext = ();

    async fn process(
        job: &Job<Self>,
        _: &Self::ExecutionContext,
    ) -> JobResult<Self::Output, Self::ErrorData> {
        tracing::info!(
            "SLEEP_JOB: Starting to process job {}, attempt {}",
            job.id,
            job.attempts
        );

        // Signal that we started processing
        JOB_STARTED_PROCESSING.store(true, Ordering::SeqCst);

        // Sleep forever (or until test tells us to stop)
        while JOB_SHOULD_CONTINUE_SLEEPING.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        tracing::info!("SLEEP_JOB: Job {} woke up, finishing", job.id);

        JobResult::Success(SleepJobOutput {
            message: format!("Job {} completed after sleeping", job.id),
        })
    }

    async fn on_success(
        &self,
        _job: &Job<Self>,
        d: SuccessHookData<'_, Self::Output>,
        _tx: &mut TransactionContext<'_>,
        _ec: &Self::ExecutionContext,
    ) {
        tracing::info!("SLEEP_JOB: on_success hook - {}", d.result.message);
    }

    async fn on_nack(
        &self,
        _job: &Job<Self>,
        d: NackHookData<'_, Self::ErrorData>,
        _tx: &mut TransactionContext<'_>,
        _ec: &Self::ExecutionContext,
    ) {
        tracing::info!("SLEEP_JOB: on_nack hook - {}", d.error.reason);
        if let Some(delay_duration) = d.delay {
            tracing::info!("Will retry after {:?}", delay_duration);
        }
    }

    async fn on_fail(
        &self,
        _job: &Job<Self>,
        d: FailHookData<'_, Self::ErrorData>,
        _tx: &mut TransactionContext<'_>,
        _ec: &Self::ExecutionContext,
    ) {
        tracing::error!("SLEEP_JOB: on_fail hook - {}", d.error.reason);
    }

    async fn on_timeout(&self, _tx: &mut TransactionContext<'_>) {
        tracing::info!("SLEEP_JOB: on_timeout hook - job lease expired");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_job_lease_expiry() {
    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "thirdweb_engine=debug,tower_http=debug,axum=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let queue_name = format!("test_lease_{}", nanoid::nanoid!(6));
    let job_id = "sleep_job_001";
    let lease_duration = Duration::from_secs(3); // Short lease for testing

    // Reset flags
    JOB_STARTED_PROCESSING.store(false, Ordering::SeqCst);
    JOB_SHOULD_CONTINUE_SLEEPING.store(true, Ordering::SeqCst);

    tracing::info!("Creating lease expiry queue: {}", queue_name);
    tracing::info!("Lease duration: {:?}", lease_duration);

    // Create queue with short lease duration
    let queue_options = QueueOptions {
        max_success: 1000,
        max_failed: 1000,
        lease_duration,
        polling_interval: Duration::from_millis(100),
        local_concurrency: 1,
        always_poll: true,
    };

    let queue = Arc::new(
        Queue::<SleepForeverJob, SleepJobOutput, SleepJobErrorData, ()>::new(
            REDIS_URL,
            &queue_name,
            Some(queue_options),
            (),
        )
        .await
        .expect("Failed to create lease expiry queue"),
    );

    // Clean up before test
    cleanup_redis_keys(&queue.redis, &queue_name).await;

    // Create job that will sleep forever
    let sleep_job = SleepForeverJob {
        id_to_check: job_id.to_string(),
        message: "I will sleep until my lease expires".to_string(),
    };

    tracing::info!("Pushing sleep job with ID: {}", job_id);
    queue
        .clone()
        .job(sleep_job)
        .with_id(job_id)
        .push()
        .await
        .expect("Failed to push sleep job");

    // Verify job is initially pending
    let initial_pending = queue.count(JobStatus::Pending).await.unwrap();
    let initial_active = queue.count(JobStatus::Active).await.unwrap();
    tracing::info!(
        "Initial state - Pending: {}, Active: {}",
        initial_pending,
        initial_active
    );
    assert_eq!(initial_pending, 1, "Job should start in pending");
    assert_eq!(initial_active, 0, "No jobs should be active initially");

    // Start worker with concurrency 1 (so job won't get picked up again immediately after expiry)
    tracing::info!("Starting worker with concurrency 1");
    let worker = {
        let queue = queue.clone();
        tokio::spawn(async move {
            if let Err(e) = queue.work().await {
                tracing::error!("Lease worker failed: {:?}", e);
            }
        })
    };

    // Wait for job to start processing
    let mut job_started = false;
    for i in 0..50 {
        if JOB_STARTED_PROCESSING.load(Ordering::SeqCst) {
            job_started = true;
            tracing::info!("Job started processing after {} polling attempts", i + 1);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(job_started, "Job should have started processing");

    // Job should now be active
    tokio::time::sleep(Duration::from_millis(200)).await;
    let active_count = queue.count(JobStatus::Active).await.unwrap();
    let pending_count = queue.count(JobStatus::Pending).await.unwrap();
    tracing::info!(
        "After job start - Pending: {}, Active: {}",
        pending_count,
        active_count
    );
    assert_eq!(active_count, 1, "Job should be active while processing");
    assert_eq!(
        pending_count, 0,
        "No jobs should be pending while one is active"
    );

    // Wait for lease to expire (lease duration + some buffer)
    let wait_time = lease_duration + Duration::from_secs(2);
    tracing::info!("Waiting {:?} for lease to expire...", wait_time);
    tokio::time::sleep(wait_time).await;

    // Job should have moved back to pending due to lease expiry
    let expired_active = queue.count(JobStatus::Active).await.unwrap();
    let expired_pending = queue.count(JobStatus::Pending).await.unwrap();
    tracing::info!(
        "After lease expiry - Pending: {}, Active: {}",
        expired_pending,
        expired_active
    );

    assert_eq!(
        expired_active, 0,
        "Job should no longer be active after lease expiry"
    );
    assert_eq!(
        expired_pending, 1,
        "Job should be back in pending after lease expiry"
    );

    // Verify the job metadata shows increased attempts
    let job_after_expiry = queue
        .get_job(job_id)
        .await
        .expect("Failed to fetch job")
        .expect("Job should exist");

    tracing::info!("Job state after lease expiry:");
    tracing::info!("  Job ID: {}", job_after_expiry.id);
    tracing::info!("  Attempts: {}", job_after_expiry.attempts);
    tracing::info!("  Created at: {}", job_after_expiry.created_at);
    tracing::info!("  Processed at: {:?}", job_after_expiry.processed_at);

    // Job should have at least 2 attempts (original + after lease expiry)
    assert!(
        job_after_expiry.attempts >= 2,
        "Job should have at least 2 attempts after lease expiry, but had {}",
        job_after_expiry.attempts
    );

    tracing::info!("✅ Lease expiry mechanism works correctly!");
    tracing::info!("Job moved from active back to pending after lease expired");

    // Stop the sleeping job and cleanup
    JOB_SHOULD_CONTINUE_SLEEPING.store(false, Ordering::SeqCst);
    worker.abort();
    cleanup_redis_keys(&queue.redis, &queue_name).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multiple_job_lease_expiry() {
    // Test that multiple jobs can have their leases expire independently

    let queue_name = format!("test_multi_lease_{}", nanoid::nanoid!(6));
    let lease_duration = Duration::from_secs(2);

    // Reset flags
    MULTI_JOB_STARTED_PROCESSING.store(false, Ordering::SeqCst);
    MULTI_JOB_SHOULD_CONTINUE_SLEEPING.store(true, Ordering::SeqCst);

    tracing::info!("\n=== Testing multiple job lease expiry ===");

    let queue_options = QueueOptions {
        max_success: 1000,
        max_failed: 1000,
        local_concurrency: 3,
        lease_duration,
        polling_interval: Duration::from_millis(100),
        always_poll: true,
    };

    let queue = Arc::new(
        Queue::<SleepForeverJob, SleepJobOutput, SleepJobErrorData, ()>::new(
            REDIS_URL,
            &queue_name,
            Some(queue_options),
            (),
        )
        .await
        .expect("Failed to create multi-lease queue"),
    );

    cleanup_redis_keys(&queue.redis, &queue_name).await;

    // Push multiple jobs
    let job_ids = vec!["multi_job_1", "multi_job_2", "multi_job_3"];
    for job_id in job_ids {
        let sleep_job = SleepForeverJob {
            id_to_check: job_id.to_string(),
            message: format!("Multi-job test: {}", job_id),
        };

        queue
            .clone()
            .job(sleep_job)
            .with_id(job_id)
            .push()
            .await
            .expect("Failed to push multi-job");
    }

    // Start worker with higher concurrency to process multiple jobs
    let worker = {
        let queue = queue.clone();
        tokio::spawn(async move {
            if let Err(e) = queue.work().await {
                tracing::error!("Multi-lease worker failed: {:?}", e);
            }
        })
    };

    // Wait a bit for jobs to start
    tokio::time::sleep(Duration::from_millis(500)).await;

    // All jobs should be active
    let active_count = queue.count(JobStatus::Active).await.unwrap();
    let pending_count = queue.count(JobStatus::Pending).await.unwrap();
    tracing::info!(
        "During processing - Pending: {}, Active: {}",
        pending_count,
        active_count
    );

    // At least some jobs should be active (maybe not all 3 due to timing)
    assert!(active_count > 0, "Some jobs should be active");
    assert_eq!(pending_count + active_count, 3, "Total jobs should be 3");

    // Wait for leases to expire
    let wait_time = lease_duration + Duration::from_secs(1);
    tracing::info!("Waiting {:?} for leases to expire...", wait_time);
    tokio::time::sleep(wait_time).await;

    // All jobs should be back to pending
    let final_active = queue.count(JobStatus::Active).await.unwrap();
    let final_pending = queue.count(JobStatus::Pending).await.unwrap();
    tracing::info!(
        "After lease expiry - Pending: {}, Active: {}",
        final_pending,
        final_active
    );

    assert_eq!(
        final_active, 0,
        "No jobs should be active after lease expiry"
    );
    assert_eq!(final_pending, 3, "All jobs should be back in pending");

    tracing::info!("✅ Multiple job lease expiry works correctly!");

    // Cleanup
    MULTI_JOB_SHOULD_CONTINUE_SLEEPING.store(false, Ordering::SeqCst);
    worker.abort();
    cleanup_redis_keys(&queue.redis, &queue_name).await;
}
