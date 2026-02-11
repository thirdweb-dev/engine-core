// Test to verify pruning works correctly with randomly generated job IDs
// This tests the scenario where job IDs are unique (not reused), which should
// allow more aggressive pruning without race conditions.

mod fixtures;
use fixtures::TestJobErrorData;
use redis::{AsyncCommands, aio::ConnectionManager};
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

// Job that simulates EOA executor behavior with random IDs
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RandomJobPayload {
    pub eoa: String,
    pub chain_id: u64,
    pub unique_id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RandomJobOutput {
    pub message: String,
    pub success_number: u32,
}

pub struct RandomJobHandler;

impl DurableExecution for RandomJobHandler {
    type Output = RandomJobOutput;
    type ErrorData = TestJobErrorData;
    type JobData = RandomJobPayload;

    async fn process(
        &self,
        job: &BorrowedJob<Self::JobData>,
    ) -> JobResult<Self::Output, Self::ErrorData> {
        tracing::debug!(
            job_id = ?job.job.id,
            "Job processing started"
        );

        // Simulate work
        tokio::time::sleep(Duration::from_millis(50)).await;

        let should_nack = SHOULD_NACK.load(Ordering::SeqCst);

        if should_nack {
            let nack_num = NACK_COUNT.fetch_add(1, Ordering::SeqCst) + 1;
            tracing::debug!(
                job_id = ?job.job.id,
                nack_count = nack_num,
                "Job nacking"
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
            tracing::debug!(
                job_id = ?job.job.id,
                success_count = success_num,
                "Job succeeding"
            );

            Ok(RandomJobOutput {
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
        tracing::debug!(
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
        tracing::debug!(
            job_id = ?job.job.id,
            "on_nack hook called"
        );
    }
}

// Helper to clean up Redis keys
async fn cleanup_redis_keys(conn_manager: &ConnectionManager, queue_name: &str) {
    let mut conn = conn_manager.clone();
    let keys_pattern = format!("twmq:{queue_name}:*");

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
async fn test_prune_with_random_ids() {
    // Initialize tracing
    let _ = tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "twmq=debug,prune_race_random_ids=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .try_init();

    let queue_name = format!("test_random_ids_{}", nanoid::nanoid!(6));

    // Aggressive pruning settings - only keep 1 successful job
    let queue_options = QueueOptions {
        local_concurrency: 2,
        max_success: 1, // Aggressive pruning
        max_failed: 10,
        lease_duration: Duration::from_secs(3),
        idempotency_mode: IdempotencyMode::Active,
        ..Default::default()
    };

    tracing::info!("=== RANDOM ID PRUNING TEST ===");
    tracing::info!("Queue: {}", queue_name);
    tracing::info!("Max success jobs: {}", queue_options.max_success);
    tracing::info!("Testing pruning behavior with unique random job IDs");

    // Reset test state
    SHOULD_NACK.store(false, Ordering::SeqCst); // Start with successes
    SUCCESS_COUNT.store(0, Ordering::SeqCst);
    NACK_COUNT.store(0, Ordering::SeqCst);

    let handler = RandomJobHandler;

    // Create queue
    let queue = Arc::new(
        Queue::new(REDIS_URL, &queue_name, Some(queue_options), handler)
            .await
            .expect("Failed to create queue"),
    );

    cleanup_redis_keys(&queue.redis, &queue_name).await;

    // Start two workers
    let worker1 = queue.work();
    let worker2 = queue.work();

    tracing::info!("Two workers started!");

    // Push jobs with random IDs and let them succeed
    for i in 0..100 {
        let random_job_id = nanoid::nanoid!(16); // Random unique ID

        let job_payload = RandomJobPayload {
            eoa: format!("0x{}", nanoid::nanoid!(8)),
            chain_id: 137,
            unique_id: random_job_id.clone(),
        };

        queue
            .clone()
            .job(job_payload)
            .with_id(&random_job_id)
            .push()
            .await
            .expect("Failed to push job");

        if i % 10 == 0 {
            tracing::info!("Pushed {} jobs", i + 1);
        }

        // Small delay between pushes
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Wait for jobs to complete
    tracing::info!("Waiting for jobs to complete...");
    for _ in 0..100 {
        let success_count = SUCCESS_COUNT.load(Ordering::SeqCst);
        if success_count >= 100 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let final_success = SUCCESS_COUNT.load(Ordering::SeqCst);

    // Check Redis directly to see if pruning occurred
    let mut conn = queue.redis.clone();
    let success_list_len: usize = conn.llen(queue.success_list_name()).await.unwrap();
    let pending_list_len: usize = conn.llen(queue.pending_list_name()).await.unwrap();
    let delayed_zset_len: usize = conn.zcard(queue.delayed_zset_name()).await.unwrap();
    let active_hash_len: usize = conn.hlen(queue.active_hash_name()).await.unwrap();

    // Get actual job IDs in success list
    let success_job_ids: Vec<String> = conn.lrange(queue.success_list_name(), 0, -1).await.unwrap();

    // Count how many job metadata hashes still exist (should match success list length if pruning works)
    let meta_pattern = format!("twmq:{}:job:*:meta", queue.name());
    let meta_keys: Vec<String> = redis::cmd("KEYS")
        .arg(&meta_pattern)
        .query_async(&mut conn)
        .await
        .unwrap_or_default();
    let metadata_count = meta_keys.len();

    // Count job data entries
    let job_data_count: usize = conn.hlen(queue.job_data_hash_name()).await.unwrap();

    // Get what's in pending/delayed/active to understand why pruning might be blocked
    let pending_jobs: Vec<String> = conn
        .lrange(queue.pending_list_name(), 0, -1)
        .await
        .unwrap_or_default();
    let delayed_jobs: Vec<String> = redis::cmd("ZRANGE")
        .arg(queue.delayed_zset_name())
        .arg(0)
        .arg(-1)
        .query_async(&mut conn)
        .await
        .unwrap_or_default();
    let active_jobs: Vec<String> = conn
        .hkeys(queue.active_hash_name())
        .await
        .unwrap_or_default();

    // Get debug info from Lua script
    let debug_candidates: String = conn
        .get("debug:candidates_count")
        .await
        .unwrap_or_else(|_| "N/A".to_string());
    let debug_active_count: String = conn
        .get("debug:active_count")
        .await
        .unwrap_or_else(|_| "N/A".to_string());
    let debug_delayed_count: String = conn
        .get("debug:delayed_count")
        .await
        .unwrap_or_else(|_| "N/A".to_string());
    let debug_blocked_job: String = conn
        .get("debug:last_blocked_job_id")
        .await
        .unwrap_or_else(|_| "N/A".to_string());
    let debug_hexists: String = conn
        .get("debug:last_hexists_result")
        .await
        .unwrap_or_else(|_| "N/A".to_string());
    let debug_zscore: String = conn
        .get("debug:last_zscore_result")
        .await
        .unwrap_or_else(|_| "N/A".to_string());
    let debug_is_active: String = conn
        .get("debug:last_is_active")
        .await
        .unwrap_or_else(|_| "N/A".to_string());
    let debug_is_delayed: String = conn
        .get("debug:last_is_delayed")
        .await
        .unwrap_or_else(|_| "N/A".to_string());

    tracing::info!("=== RESULTS ===");
    tracing::info!("Total successes: {}", final_success);
    tracing::info!("Total nacks: {}", NACK_COUNT.load(Ordering::SeqCst));
    tracing::info!("");
    tracing::info!("=== Redis State ===");
    tracing::info!("Success list length: {}", success_list_len);
    tracing::info!("Pending list length: {}", pending_list_len);
    tracing::info!("Delayed zset length: {}", delayed_zset_len);
    tracing::info!("Active hash length: {}", active_hash_len);
    tracing::info!("Job metadata count: {}", metadata_count);
    tracing::info!("Job data hash entries: {}", job_data_count);
    tracing::info!("");
    tracing::info!("=== Job IDs (for leak investigation) ===");
    tracing::info!("Success list job IDs: {:?}", success_job_ids);
    tracing::info!("Pending list job IDs: {:?}", pending_jobs);
    tracing::info!("Delayed zset job IDs: {:?}", delayed_jobs);
    tracing::info!("Active hash job IDs: {:?}", active_jobs);
    tracing::info!("");
    tracing::info!("=== Lua Script Debug Info ===");
    tracing::info!("Candidates to delete (last run): {}", debug_candidates);
    tracing::info!("Active count (at check time): {}", debug_active_count);
    tracing::info!("Delayed count (at check time): {}", debug_delayed_count);
    tracing::info!("First blocked job ID: {}", debug_blocked_job);
    tracing::info!("  HEXISTS result: {}", debug_hexists);
    tracing::info!("  ZSCORE result: {}", debug_zscore);
    tracing::info!("  is_active (HEXISTS==1): {}", debug_is_active);
    tracing::info!("  is_delayed (ZSCORE~=nil): {}", debug_is_delayed);
    tracing::info!("");
    tracing::info!("Max success setting: {}", queue.options.max_success);

    if success_list_len <= queue.options.max_success {
        tracing::info!("✅ List pruning is working - success list is within max_success limit");
    } else {
        tracing::warn!(
            "⚠️  Success list ({}) exceeds max_success ({})",
            success_list_len,
            queue.options.max_success
        );
        tracing::warn!("   This might indicate list pruning is not working correctly");
    }

    if metadata_count == success_list_len {
        tracing::info!("✅ Metadata cleanup is working - metadata count matches list length");
    } else {
        tracing::warn!("⚠️  Metadata leak detected!");
        tracing::warn!(
            "   Job metadata hashes: {}, Success list length: {}",
            metadata_count,
            success_list_len
        );
        tracing::warn!(
            "   {} job metadata entries were not cleaned up",
            metadata_count.saturating_sub(success_list_len)
        );
    }

    worker1.shutdown().await.unwrap();
    worker2.shutdown().await.unwrap();
    cleanup_redis_keys(&queue.redis, &queue_name).await;
}
