// Test to reproduce the production race condition:
// When a job with static ID (e.g., eoa_chainId) gets nacked and succeeded repeatedly,
// and successful job pruning runs while an active job with the same ID is running,
// the active job loses its lock because pruning deletes the job metadata hash
// (which contains the lease_token).

mod fixtures;
use fixtures::TestJobErrorData;
use redis::aio::ConnectionManager;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    time::Duration,
};

use serde::{Deserialize, Serialize};
use twmq::{
    DurableExecution, IdempotencyMode, NackHookData, Queue, SuccessHookData,
    hooks::TransactionContext,
    job::{BorrowedJob, JobError, JobResult, RequeuePosition},
    queue::QueueOptions,
};

const REDIS_URL: &str = "redis://127.0.0.1:6379/";

// Shared state to control test flow
static SHOULD_NACK: AtomicBool = AtomicBool::new(true);
static SUCCESS_COUNT: AtomicU32 = AtomicU32::new(0);
static NACK_COUNT: AtomicU32 = AtomicU32::new(0);
static ACTIVE_WORKERS: AtomicU32 = AtomicU32::new(0);
static RACE_DETECTED: AtomicBool = AtomicBool::new(false);
static PROCESS_SLOWLY: AtomicBool = AtomicBool::new(false); // Control processing speed

// Job that simulates EOA executor behavior with static ID
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EoaSimulatorJobPayload {
    pub eoa: String,
    pub chain_id: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EoaSimulatorJobOutput {
    pub message: String,
    pub success_number: u32,
}

pub struct EoaSimulatorJobHandler;

impl DurableExecution for EoaSimulatorJobHandler {
    type Output = EoaSimulatorJobOutput;
    type ErrorData = TestJobErrorData;
    type JobData = EoaSimulatorJobPayload;

    async fn process(
        &self,
        job: &BorrowedJob<Self::JobData>,
    ) -> JobResult<Self::Output, Self::ErrorData> {
        let worker_count = ACTIVE_WORKERS.fetch_add(1, Ordering::SeqCst) + 1;

        tracing::warn!(
            job_id = ?job.job.id,
            lease_token = ?job.lease_token,
            worker_count = worker_count,
            "Job processing started"
        );

        // Detect duplicate workers (race condition indicator)
        if worker_count > 1 {
            tracing::error!(
                "🚨 RACE CONDITION DETECTED! {} workers processing the same job simultaneously!",
                worker_count
            );
            RACE_DETECTED.store(true, Ordering::SeqCst);
        }

        // Simulate work - process slowly if flag is set to keep job active longer
        let process_slowly = PROCESS_SLOWLY.load(Ordering::SeqCst);
        if process_slowly {
            tracing::warn!("Processing SLOWLY to keep job active...");
            tokio::time::sleep(Duration::from_secs(2)).await; // Long enough for pruning to happen
        } else {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let should_nack = SHOULD_NACK.load(Ordering::SeqCst);

        ACTIVE_WORKERS.fetch_sub(1, Ordering::SeqCst);

        if should_nack {
            let nack_num = NACK_COUNT.fetch_add(1, Ordering::SeqCst) + 1;
            tracing::info!(
                job_id = ?job.job.id,
                nack_count = nack_num,
                "Job nacking (simulating work remaining)"
            );

            Err(JobError::Nack {
                error: TestJobErrorData {
                    reason: format!("Work remaining (nack #{})", nack_num),
                },
                delay: Some(Duration::from_millis(50)),
                position: RequeuePosition::Last,
            })
        } else {
            let success_num = SUCCESS_COUNT.fetch_add(1, Ordering::SeqCst) + 1;
            tracing::info!(
                job_id = ?job.job.id,
                success_count = success_num,
                "Job succeeding"
            );

            Ok(EoaSimulatorJobOutput {
                message: format!("Success #{}", success_num),
                success_number: success_num,
            })
        }
    }

    async fn on_success(
        &self,
        job: &BorrowedJob<Self::JobData>,
        _d: SuccessHookData<'_, Self::Output>,
        _tx: &mut TransactionContext<'_>,
    ) {
        tracing::info!(
            job_id = ?job.job.id,
            "on_success hook called"
        );
    }

    async fn on_nack(
        &self,
        job: &BorrowedJob<Self::JobData>,
        _d: NackHookData<'_, Self::ErrorData>,
        _tx: &mut TransactionContext<'_>,
    ) {
        tracing::info!(
            job_id = ?job.job.id,
            "on_nack hook called"
        );
    }
}

// Helper to clean up Redis keys
async fn cleanup_redis_keys(conn_manager: &ConnectionManager, queue_name: &str) {
    let mut conn = conn_manager.clone();
    // twmq queue keys are hash-tagged for Redis Cluster compatibility
    let keys_pattern = format!("twmq:{{{queue_name}}}:*");

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
    tracing::info!("Cleaned up keys for pattern: {keys_pattern}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_prune_race_condition_two_workers() {
    // Initialize tracing
    let _ = tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "twmq=info,prune_race_condition=warn".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .try_init();

    let queue_name = format!("test_two_worker_race_{}", nanoid::nanoid!(6));

    // CRITICAL: Configure to only keep 1 successful job to trigger aggressive pruning
    // Use TWO workers and local_concurrency=2 to maximize race probability
    let queue_options = QueueOptions {
        local_concurrency: 2, // Allow 2 concurrent jobs per worker
        max_success: 1,       // Aggressive pruning - only keep 1 success
        max_failed: 10,
        lease_duration: Duration::from_secs(3),
        idempotency_mode: IdempotencyMode::Active, // CRITICAL: allows same ID after success
        ..Default::default()
    };

    tracing::warn!("=== TWO WORKER RACE TEST ===");
    tracing::warn!("Queue: {}", queue_name);
    tracing::warn!("Max success jobs: {}", queue_options.max_success);
    tracing::warn!(
        "This test reproduces the race where pruning deletes metadata while job is active"
    );
    tracing::warn!("Using 2 workers to enable the race condition (impossible with 1 worker)");

    // Reset test state
    SHOULD_NACK.store(true, Ordering::SeqCst);
    SUCCESS_COUNT.store(0, Ordering::SeqCst);
    NACK_COUNT.store(0, Ordering::SeqCst);
    ACTIVE_WORKERS.store(0, Ordering::SeqCst);
    RACE_DETECTED.store(false, Ordering::SeqCst);
    PROCESS_SLOWLY.store(false, Ordering::SeqCst);

    let handler = EoaSimulatorJobHandler;

    // Create queue
    let queue = Arc::new(
        Queue::new(REDIS_URL, &queue_name, Some(queue_options), handler)
            .await
            .expect("Failed to create queue"),
    );

    cleanup_redis_keys(&queue.redis, &queue_name).await;

    // Static job ID (simulating eoa_chainId pattern from production)
    let static_job_id = "eoa_0x1234_137";

    let job_payload = EoaSimulatorJobPayload {
        eoa: "0x1234".to_string(),
        chain_id: 137,
    };

    // Push initial job with static ID
    queue
        .clone()
        .job(job_payload.clone())
        .with_id(static_job_id)
        .push()
        .await
        .expect("Failed to push job");

    tracing::warn!("Initial job pushed with ID: {}", static_job_id);

    // Start TWO workers to enable the race condition
    let worker1 = queue.work();
    let worker2 = queue.work();

    tracing::warn!("Two workers started!");

    // PHASE 1: Create one successful job to fill success list
    tracing::warn!("=== PHASE 1: Creating initial success ===");
    SHOULD_NACK.store(false, Ordering::SeqCst);
    PROCESS_SLOWLY.store(false, Ordering::SeqCst);

    // Wait for first success
    for _ in 0..50 {
        if SUCCESS_COUNT.load(Ordering::SeqCst) > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let first_success = SUCCESS_COUNT.load(Ordering::SeqCst);
    tracing::warn!("First success completed: {}", first_success);
    assert!(first_success > 0, "Should have at least one success");

    // PHASE 2: Run thousands of iterations to catch the race
    // The race window is: job completes → moves to success → pruning runs
    // We need a new job with same ID to become active right in that window
    tracing::warn!("=== PHASE 2: Running iterations to catch the pruning race ===");

    let mut iteration = 0;
    let max_iterations = 10000;

    while iteration < max_iterations {
        iteration += 1;

        if iteration % 100 == 0 {
            tracing::info!(
                "Iteration {}/{}, race detected: {}",
                iteration,
                max_iterations,
                RACE_DETECTED.load(Ordering::SeqCst)
            );
        }

        // Queue a new job with same ID
        queue
            .clone()
            .job(job_payload.clone())
            .with_id(static_job_id)
            .push()
            .await
            .ok(); // May fail if already queued

        // Very short wait to let the job process
        tokio::time::sleep(Duration::from_millis(5)).await;

        // Check if we caught the race
        if RACE_DETECTED.load(Ordering::SeqCst) {
            tracing::error!("🎯 RACE CAUGHT AT ITERATION {}!", iteration);
            break;
        }
    }

    let race_detected = RACE_DETECTED.load(Ordering::SeqCst);

    let final_race_detected = RACE_DETECTED.load(Ordering::SeqCst);

    tracing::warn!("=== RESULTS ===");
    tracing::warn!("Race detected: {}", final_race_detected);
    tracing::warn!("Total successes: {}", SUCCESS_COUNT.load(Ordering::SeqCst));
    tracing::warn!("Total nacks: {}", NACK_COUNT.load(Ordering::SeqCst));

    worker1.shutdown().await.unwrap();
    worker2.shutdown().await.unwrap();
    cleanup_redis_keys(&queue.redis, &queue_name).await;

    if final_race_detected {
        tracing::error!(
            "⚠️  UNEXPECTED: Race condition detected after {} iterations!",
            iteration
        );
        tracing::error!("   This should NOT happen with the fix in place!");
        tracing::error!("   The fix should prevent pruning from deleting active job metadata.");
        panic!("Race condition detected - the fix is not working properly!");
    } else {
        tracing::info!(
            "✅ SUCCESS: No race detected after {} iterations",
            iteration
        );
        tracing::info!("   The fix is working correctly!");
        tracing::info!(
            "   Pruning now checks if a job is active/pending/delayed before deleting metadata."
        );
        tracing::info!(
            "   This prevents the race where Worker 1's pruning deletes Worker 2's active job metadata."
        );
    }
}
