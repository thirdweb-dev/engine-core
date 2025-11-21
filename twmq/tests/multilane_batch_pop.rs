use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::time::timeout;

use twmq::error::TwmqError;
use twmq::job::{BorrowedJob, JobResult};
use twmq::{DurableExecution, MultilaneQueue, UserCancellable};

const REDIS_URL: &str = "redis://127.0.0.1:6379/";

// Simple test job that just holds an ID
#[derive(Serialize, Deserialize, Debug, Clone)]
struct TestJob {
    id: u32,
    data: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct TestOutput {
    processed_id: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct TestError {
    message: String,
}

impl From<TwmqError> for TestError {
    fn from(error: TwmqError) -> Self {
        TestError {
            message: error.to_string(),
        }
    }
}

impl UserCancellable for TestError {
    fn user_cancelled() -> Self {
        TestError {
            message: "Cancelled".to_string(),
        }
    }
}

// Dummy handler - these tests focus on batch pop logic, not processing
struct DummyHandler;

impl DurableExecution for DummyHandler {
    type Output = TestOutput;
    type ErrorData = TestError;
    type JobData = TestJob;

    async fn process(
        &self,
        job: &BorrowedJob<Self::JobData>,
    ) -> JobResult<Self::Output, Self::ErrorData> {
        Ok(TestOutput {
            processed_id: job.job.data.id,
        })
    }
}

/// Test harness for multilane queue batch operations
struct MultilaneTestHarness {
    pub queue: Arc<MultilaneQueue<DummyHandler>>,
    pub queue_id: String,
}

impl MultilaneTestHarness {
    async fn new() -> Self {
        let queue_id = format!("test_multilane_{}", nanoid::nanoid!(8));
        let handler = DummyHandler;

        let queue = Arc::new(
            MultilaneQueue::new(REDIS_URL, &queue_id, None, handler)
                .await
                .expect("Failed to create multilane queue"),
        );

        // warm up redis connection
        let _ = queue.count(twmq::job::JobStatus::Active, None).await;

        let harness = Self { queue, queue_id };
        harness.cleanup().await;
        harness
    }

    /// Clean up all Redis keys for this test
    async fn cleanup(&self) {
        let mut conn = self.queue.redis.clone();
        let keys_pattern = format!("twmq_multilane:{}:*", self.queue_id);

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

    /// Add jobs to specific lanes
    async fn add_jobs_to_lanes(&self, jobs_per_lane: &HashMap<String, Vec<TestJob>>) {
        for (lane_id, jobs) in jobs_per_lane {
            for job in jobs {
                self.queue
                    .clone()
                    .job_for_lane(lane_id, job.clone())
                    .id(format!("job_{}_{}", lane_id, job.id))
                    .push()
                    .await
                    .expect("Failed to push job");
            }
        }
    }

    /// Batch pop jobs and return the results grouped by lane
    async fn batch_pop(&self, batch_size: usize) -> HashMap<String, Vec<u32>> {
        let jobs = self
            .queue
            .pop_batch_jobs(batch_size)
            .await
            .expect("Failed to pop batch jobs");

        let mut results = HashMap::new();
        for (lane_id, job) in jobs {
            results
                .entry(lane_id)
                .or_insert_with(Vec::new)
                .push(job.job.data.id);
        }
        results
    }

    /// Count total jobs across all lanes by status
    async fn count_total_jobs(&self, status: twmq::job::JobStatus) -> usize {
        self.queue
            .count(status, None)
            .await
            .expect("Failed to count jobs")
    }

    /// Count jobs in specific lane by status  
    async fn count_lane_jobs(&self, lane_id: &str, status: twmq::job::JobStatus) -> usize {
        self.queue
            .count(status, Some(lane_id))
            .await
            .expect("Failed to count lane jobs")
    }
}

impl Drop for MultilaneTestHarness {
    fn drop(&mut self) {
        // Cleanup in background since we can't await in drop
        let queue_id = self.queue_id.clone();
        let redis = self.queue.clone().redis.clone();

        tokio::spawn(async move {
            let mut conn = redis;
            let keys_pattern = format!("twmq_multilane:{queue_id}:*");
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
        });
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn multilane_test_batch_pop_single_lane_with_100k_empty_lanes() {
    // Test: 100,000 lanes, only 1 has 100 jobs, batch pop 100
    println!("🧪 Testing batch pop with 100,000 lanes, 1 active lane with 100 jobs");

    let harness = MultilaneTestHarness::new().await;

    // Create jobs for the single active lane
    let active_lane = "lane_active".to_string();
    let mut jobs_per_lane = HashMap::new();

    let mut jobs = Vec::new();
    for i in 0..100 {
        jobs.push(TestJob {
            id: i,
            data: format!("job_{i}"),
        });
    }
    jobs_per_lane.insert(active_lane.clone(), jobs);

    // Add 99,999 empty lanes by creating them in Redis lanes zset
    // We do this by adding empty lanes to the zset directly
    let mut conn = harness.queue.redis.clone();
    for i in 0..99_999 {
        let lane_id = format!("empty_lane_{i}");
        // Add lane to lanes zset with score 0 (never processed)
        redis::cmd("ZADD")
            .arg(harness.queue.lanes_zset_name())
            .arg("NX") // Only add if not exists
            .arg(0)
            .arg(&lane_id)
            .query_async::<()>(&mut conn)
            .await
            .expect("Failed to add empty lane");
    }

    // Add the actual jobs
    harness.add_jobs_to_lanes(&jobs_per_lane).await;

    // Verify setup
    let pending_count = harness
        .count_total_jobs(twmq::job::JobStatus::Pending)
        .await;
    assert_eq!(pending_count, 100, "Should have 100 pending jobs");

    let active_lane_count = harness
        .count_lane_jobs(&active_lane, twmq::job::JobStatus::Pending)
        .await;
    assert_eq!(
        active_lane_count, 100,
        "Active lane should have 100 pending jobs"
    );

    // Test batch pop with timeout to ensure it doesn't hang
    println!("⏱️  Executing batch pop (should complete quickly despite 100k lanes)...");
    let start = std::time::Instant::now();

    let result = timeout(Duration::from_secs(10), harness.batch_pop(100))
        .await
        .expect("Batch pop should complete within 10 seconds");

    let duration = start.elapsed();
    println!("✅ Batch pop completed in {duration:?}");

    // Verify results
    assert_eq!(result.len(), 1, "Should get jobs from exactly 1 lane");
    assert!(
        result.contains_key(&active_lane),
        "Should get jobs from active lane"
    );

    let jobs_from_active = &result[&active_lane];
    assert_eq!(jobs_from_active.len(), 100, "Should get all 100 jobs");

    // Verify all job IDs are present
    let mut expected_ids: Vec<u32> = (0..100).collect();
    let mut actual_ids = jobs_from_active.clone();
    expected_ids.sort();
    actual_ids.sort();
    assert_eq!(actual_ids, expected_ids, "Should get all expected job IDs");

    // Verify no jobs left pending
    let remaining_pending = harness
        .count_total_jobs(twmq::job::JobStatus::Pending)
        .await;
    assert_eq!(remaining_pending, 0, "Should have no pending jobs left");

    // Performance assertion - should complete in reasonable time even with 100k lanes
    assert!(
        duration < Duration::from_secs(5),
        "Should complete within 5 seconds even with 100k lanes"
    );

    println!("✅ Test passed: Single lane with 100k empty lanes");
}

#[tokio::test(flavor = "multi_thread")]
async fn multilane_test_batch_pop_distributed_jobs_across_100k_lanes() {
    // Test: 200 jobs distributed randomly across 100,000 lanes, batch pop 100 three times
    println!("🧪 Testing batch pop with 200 jobs distributed across 100,000 lanes");

    let harness = MultilaneTestHarness::new().await;

    // Create 200 jobs distributed across 200 different lanes (1 job per lane)
    let mut jobs_per_lane = HashMap::new();
    for i in 0..200 {
        let lane_id = format!("lane_{i}");
        let job = TestJob {
            id: i,
            data: format!("job_{i}"),
        };
        jobs_per_lane.insert(lane_id, vec![job]);
    }

    // Add 99,800 empty lanes to reach 100,000 total
    let mut conn = harness.queue.redis.clone();
    for i in 200..100_000 {
        let lane_id = format!("empty_lane_{i}");
        redis::cmd("ZADD")
            .arg(harness.queue.lanes_zset_name())
            .arg("NX")
            .arg(0)
            .arg(&lane_id)
            .query_async::<()>(&mut conn)
            .await
            .expect("Failed to add empty lane");
    }

    // Add the jobs
    harness.add_jobs_to_lanes(&jobs_per_lane).await;

    // Verify setup
    let pending_count = harness
        .count_total_jobs(twmq::job::JobStatus::Pending)
        .await;
    assert_eq!(pending_count, 200, "Should have 200 pending jobs");

    // First batch pop - should get 100 jobs
    println!("[200 jobs - 200/100k lanes] ⏱️  First batch pop (100 jobs)...");
    let start = std::time::Instant::now();
    let result1 = timeout(Duration::from_secs(10), harness.batch_pop(100))
        .await
        .expect("First batch pop should complete within 10 seconds");
    let duration1 = start.elapsed();
    println!("[200 jobs - 200/100k lanes] ✅ First batch pop completed in {duration1:?}");

    let new_lanes_count = harness.queue.lanes_count().await.unwrap();
    println!(
        "[200 jobs - 200/100k lanes] New lanes count after initial batch pop: {new_lanes_count}"
    );

    let total_jobs_1: usize = result1.values().map(|jobs| jobs.len()).sum();
    assert_eq!(total_jobs_1, 100, "First batch should return 100 jobs");

    // Verify remaining pending jobs
    let remaining_after_1 = harness
        .count_total_jobs(twmq::job::JobStatus::Pending)
        .await;
    assert_eq!(
        remaining_after_1, 100,
        "Should have 100 pending jobs after first batch"
    );

    // Second batch pop - should get 100 jobs
    println!("[200 jobs - 200/100k lanes] ⏱️  Second batch pop (100 jobs)...");
    let start = std::time::Instant::now();
    let result2 = timeout(Duration::from_secs(10), harness.batch_pop(100))
        .await
        .expect("Second batch pop should complete within 10 seconds");
    let duration2 = start.elapsed();
    println!("[200 jobs - 200/100k lanes] ✅ Second batch pop completed in {duration2:?}");

    let total_jobs_2: usize = result2.values().map(|jobs| jobs.len()).sum();
    assert_eq!(total_jobs_2, 100, "Second batch should return 100 jobs");

    // Verify no remaining pending jobs
    let remaining_after_2 = harness
        .count_total_jobs(twmq::job::JobStatus::Pending)
        .await;
    assert_eq!(
        remaining_after_2, 0,
        "Should have 0 pending jobs after second batch"
    );

    // Third batch pop - should get 0 jobs
    println!("⏱️  Third batch pop (should get 0 jobs)...");
    let start = std::time::Instant::now();
    let result3 = timeout(Duration::from_secs(10), harness.batch_pop(100))
        .await
        .expect("Third batch pop should complete within 10 seconds");
    let duration3 = start.elapsed();
    println!("✅ Third batch pop completed in {duration3:?}");

    let total_jobs_3: usize = result3.values().map(|jobs| jobs.len()).sum();
    assert_eq!(total_jobs_3, 0, "Third batch should return 0 jobs");

    // Verify all unique job IDs were returned across both batches
    let mut all_job_ids: Vec<u32> = Vec::new();
    for jobs in result1.values() {
        all_job_ids.extend(jobs);
    }
    for jobs in result2.values() {
        all_job_ids.extend(jobs);
    }

    all_job_ids.sort();
    let expected_ids: Vec<u32> = (0..200).collect();
    assert_eq!(
        all_job_ids, expected_ids,
        "Should get all 200 unique job IDs across two batches"
    );

    // Performance assertions
    assert!(
        duration1 < Duration::from_secs(5),
        "First batch should complete quickly"
    );
    assert!(
        duration2 < Duration::from_secs(5),
        "Second batch should complete quickly"
    );
    assert!(
        duration3 < Duration::from_secs(2),
        "Third batch should complete very quickly (no jobs)"
    );

    println!("✅ Test passed: Distributed jobs across 100k lanes");
}

#[tokio::test(flavor = "multi_thread")]
async fn multilane_test_batch_pop_fairness_across_lanes() {
    // Test fairness: ensure round-robin behavior across multiple lanes with jobs
    println!("🧪 Testing batch pop fairness across multiple active lanes");

    let harness = MultilaneTestHarness::new().await;

    // Create 10 lanes, each with 10 jobs (100 total)
    let mut jobs_per_lane = HashMap::new();
    for lane_num in 0..10 {
        let lane_id = format!("lane_{lane_num}");
        let mut jobs = Vec::new();
        for job_num in 0..10 {
            jobs.push(TestJob {
                id: lane_num * 10 + job_num,
                data: format!("job_{lane_num}_{job_num}"),
            });
        }
        jobs_per_lane.insert(lane_id, jobs);
    }

    harness.add_jobs_to_lanes(&jobs_per_lane).await;

    // Batch pop 10 jobs - should get 1 from each lane (fairness)
    let result = harness.batch_pop(10).await;

    assert_eq!(result.len(), 10, "Should get jobs from all 10 lanes");

    for lane_num in 0..10 {
        let lane_id = format!("lane_{lane_num}");
        assert!(
            result.contains_key(&lane_id),
            "Should have job from lane {lane_num}"
        );
        assert_eq!(
            result[&lane_id].len(),
            1,
            "Should get exactly 1 job from lane {lane_num}"
        );
    }

    // Verify remaining jobs
    let remaining = harness
        .count_total_jobs(twmq::job::JobStatus::Pending)
        .await;
    assert_eq!(remaining, 90, "Should have 90 jobs remaining");

    println!("✅ Test passed: Fairness across multiple lanes");
}

#[tokio::test(flavor = "multi_thread")]
async fn multilane_test_batch_pop_empty_queue() {
    // Edge case: batch pop from completely empty queue
    println!("🧪 Testing batch pop from empty queue");

    let harness = MultilaneTestHarness::new().await;

    // Don't add any jobs
    let result = harness.batch_pop(100).await;

    assert_eq!(result.len(), 0, "Should get no jobs from empty queue");

    let pending = harness
        .count_total_jobs(twmq::job::JobStatus::Pending)
        .await;
    assert_eq!(pending, 0, "Should have no pending jobs");

    println!("✅ Test passed: Empty queue handling");
}
