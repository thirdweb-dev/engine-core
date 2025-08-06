use serde::{Deserialize, Serialize};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use twmq::{
    DurableExecution, Queue,
    job::{BorrowedJob, JobResult, JobStatus},
    queue::{IdempotencyMode, QueueOptions},
    redis::aio::ConnectionManager,
};

const REDIS_URL: &str = "redis://127.0.0.1:6379/";

#[derive(Serialize, Deserialize, Debug, Clone)]
struct TestJobData {
    message: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct TestJobOutput {
    processed: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct TestJobError {
    error_message: String,
}

impl From<twmq::error::TwmqError> for TestJobError {
    fn from(err: twmq::error::TwmqError) -> Self {
        TestJobError {
            error_message: err.to_string(),
        }
    }
}

impl twmq::UserCancellable for TestJobError {
    fn user_cancelled() -> Self {
        TestJobError {
            error_message: "User cancelled".to_string(),
        }
    }
}

struct TestJobHandler {
    processed_count: Arc<AtomicUsize>,
}

impl DurableExecution for TestJobHandler {
    type Output = TestJobOutput;
    type ErrorData = TestJobError;
    type JobData = TestJobData;

    async fn process(
        &self,
        job: &BorrowedJob<Self::JobData>,
    ) -> JobResult<Self::Output, Self::ErrorData> {
        self.processed_count.fetch_add(1, Ordering::SeqCst);

        Ok(TestJobOutput {
            processed: format!("Processed: {}", job.data().message),
        })
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
}

#[tokio::test]
async fn test_permanent_idempotency_mode() {
    let queue_name = format!("test_perm_{}", nanoid::nanoid!(6));
    let processed_count = Arc::new(AtomicUsize::new(0));

    let queue_options = QueueOptions {
        idempotency_mode: IdempotencyMode::Permanent,
        local_concurrency: 1,
        ..Default::default()
    };

    let handler = TestJobHandler {
        processed_count: processed_count.clone(),
    };

    let queue = Arc::new(
        Queue::new(REDIS_URL, &queue_name, Some(queue_options), handler)
            .await
            .expect("Failed to create queue"),
    );

    cleanup_redis_keys(&queue.redis, &queue_name).await;

    let job_data = TestJobData {
        message: "test message".to_string(),
    };

    // Push the same job twice with the same ID
    let job_id = "test_job_permanent";

    let _job1 = queue
        .clone()
        .job(job_data.clone())
        .with_id(job_id)
        .push()
        .await
        .unwrap();
    let _job2 = queue
        .clone()
        .job(job_data.clone())
        .with_id(job_id)
        .push()
        .await
        .unwrap();

    // Only one job should be in pending (deduplication should prevent the second)
    let pending_count = queue.count(JobStatus::Pending).await.unwrap();
    assert_eq!(
        pending_count, 1,
        "Only one job should be pending due to deduplication"
    );

    // Start worker and let it process
    let worker = queue.work();

    // Wait for processing
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Should have processed exactly one job
    assert_eq!(
        processed_count.load(Ordering::SeqCst),
        1,
        "Should have processed exactly one job"
    );

    // Try to add the same job again - should still be blocked by permanent idempotency
    let _job3 = queue
        .clone()
        .job(job_data.clone())
        .with_id(job_id)
        .push()
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Should still be only one processed job
    assert_eq!(
        processed_count.load(Ordering::SeqCst),
        1,
        "Should still have processed only one job"
    );

    worker.shutdown().await.unwrap();
    cleanup_redis_keys(&queue.redis, &queue_name).await;
}

#[tokio::test]
async fn test_active_idempotency_mode() {
    let queue_name = format!("test_active_{}", nanoid::nanoid!(6));
    let processed_count = Arc::new(AtomicUsize::new(0));

    let queue_options = QueueOptions {
        idempotency_mode: IdempotencyMode::Active,
        local_concurrency: 1,
        ..Default::default()
    };

    let handler = TestJobHandler {
        processed_count: processed_count.clone(),
    };

    let queue = Arc::new(
        Queue::new(REDIS_URL, &queue_name, Some(queue_options), handler)
            .await
            .expect("Failed to create queue"),
    );

    cleanup_redis_keys(&queue.redis, &queue_name).await;

    let job_data = TestJobData {
        message: "test message".to_string(),
    };

    // Push the same job twice with the same ID
    let job_id = "test_job_active";

    let _job1 = queue
        .clone()
        .job(job_data.clone())
        .with_id(job_id)
        .push()
        .await
        .unwrap();
    let _job2 = queue
        .clone()
        .job(job_data.clone())
        .with_id(job_id)
        .push()
        .await
        .unwrap();

    // Only one job should be in pending (deduplication should prevent the second)
    let pending_count = queue.count(JobStatus::Pending).await.unwrap();
    assert_eq!(
        pending_count, 1,
        "Only one job should be pending due to deduplication"
    );

    // Start worker and let it process
    let worker = queue.work();

    // Wait for processing
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Should have processed exactly one job
    assert_eq!(
        processed_count.load(Ordering::SeqCst),
        1,
        "Should have processed exactly one job"
    );

    // Try to add the same job again - should be allowed with active idempotency
    let _job3 = queue
        .clone()
        .job(job_data.clone())
        .with_id(job_id)
        .push()
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Should have processed two jobs now
    assert_eq!(
        processed_count.load(Ordering::SeqCst),
        2,
        "Should have processed two jobs with active idempotency"
    );

    worker.shutdown().await.unwrap();
    cleanup_redis_keys(&queue.redis, &queue_name).await;
}
