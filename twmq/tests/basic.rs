mod fixtures;
use fixtures::*;
use redis::AsyncCommands;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use twmq::Queue;

use std::sync::atomic::Ordering;
// Or use specific imports if they are in a different module.
use std::sync::Arc;
use std::time::Duration;
use twmq::job::{JobOptions, JobStatus}; // Assuming JobStatus is in twmq::job
use twmq::redis::aio::ConnectionManager; // For cleanup utility

const REDIS_URL: &str = "redis://127.0.0.1:6379/";

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_queue_push_and_process_job() {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            // Default to info level if RUST_LOG environment variable is not set
            "thirdweb_engine=debug,tower_http=debug,axum=debug".into()
        }))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let test_job_id = "test_job_001".to_string();
    let queue_name = format!("test_q_simple_{}", nanoid::nanoid!(6));

    // Reset the flag before each test run
    TEST_JOB_PROCESSED_SUCCESSFULLY.store(false, Ordering::SeqCst);

    println!("Creating queue: {}", queue_name);

    let basic_handler = TestJobHandler;

    let queue = Arc::new(
        Queue::<TestJobHandler>::new(
            REDIS_URL,
            &queue_name,
            None, // Default QueueOptions
            basic_handler,
        )
        .await
        .expect("Failed to create queue"),
    );

    // Cleanup Redis before starting, in case of previous failed test
    cleanup_redis_keys(&queue.redis.clone(), &queue_name).await;

    let job_payload = TestJobPayload {
        message: "hello from test".to_string(),
        id_to_check: test_job_id.clone(),
    };

    println!("Pushing job with ID: {}", test_job_id);
    let job_options = JobOptions {
        data: job_payload,
        id: test_job_id.clone(),
        delay: None,
    };

    let pushed_job_details = queue.push(job_options).await.expect("Failed to push job");
    assert_eq!(pushed_job_details.id, test_job_id);

    let pending_count = queue
        .count(JobStatus::Pending)
        .await
        .expect("Failed to count pending jobs");
    assert_eq!(
        pending_count, 1,
        "There should be 1 job in the pending list"
    );

    println!("Starting worker for queue: {}", queue_name);

    let worker_queue_ref = Arc::clone(&queue);
    let worker_handle = worker_queue_ref.work();

    // Wait for the job to be processed
    // Poll the flag, with a timeout
    let mut processed = false;
    for _ in 0..50 {
        // Max wait 5 seconds (50 * 100ms)
        if TEST_JOB_PROCESSED_SUCCESSFULLY.load(Ordering::SeqCst) {
            processed = true;
            println!("Job processed flag is true.");
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(processed, "Job was not processed by the worker in time");

    // Verify job moved to success list
    let success_count = queue
        .count(JobStatus::Success)
        .await
        .expect("Failed to count successful jobs");
    assert_eq!(
        success_count, 1,
        "There should be 1 job in the success list"
    );

    // Verify pending and active lists are empty
    let pending_after_processing = queue
        .count(JobStatus::Pending)
        .await
        .expect("Failed to count pending jobs after processing");
    assert_eq!(
        pending_after_processing, 0,
        "Pending list should be empty after processing"
    );

    let active_count = queue
        .count(JobStatus::Active)
        .await
        .expect("Failed to count active jobs");
    assert_eq!(active_count, 0, "Active list should be empty");

    // Verify the job result was stored
    let mut redis_conn = queue.redis.clone();
    let result_json_opt: Option<String> = redis_conn
        .hget(queue.job_result_hash_name(), &test_job_id)
        .await
        .expect("Redis HGET for result failed");

    assert!(
        result_json_opt.is_some(),
        "Job result should be stored in Redis"
    );
    let result_json = result_json_opt.unwrap();
    let job_output: TestJobOutput =
        serde_json::from_str(&result_json).expect("Failed to deserialize job output");
    assert_eq!(job_output.reply, "Successfully processed 'hello from test'");

    println!("Test completed for queue: {}", queue_name.clone());

    // The worker task runs in a loop. For a clean test exit,
    // you might want to abort it or implement a shutdown signal for the worker.
    // For this simple test, we'll let it be.
    worker_handle.shutdown().await.unwrap(); // Or a more graceful shutdown if implemented

    // Cleanup Redis keys after test
    cleanup_redis_keys(&queue.redis, &queue_name).await;
}
