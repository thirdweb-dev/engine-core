use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::hint::black_box;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::runtime::Runtime;

use twmq::{
    DurableExecution, NackHookData, Queue, SuccessHookData,
    hooks::TransactionContext,
    job::{Job, JobResult, JobStatus, RequeuePosition},
    queue::QueueOptions,
};

const REDIS_URL: &str = "redis://127.0.0.1:6379/";

// Benchmark job that either succeeds immediately or nacks based on probability
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BenchmarkJob {
    pub job_id: String,
    pub nack_probability: f64, // For metrics
    pub created_at: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BenchmarkOutput {
    pub job_id: String,
    pub processed_at: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BenchmarkErrorData {
    pub job_id: String,
    pub attempt: u32,
}

// Shared metrics across all benchmark jobs
#[derive(Clone)]
pub struct BenchmarkMetrics {
    pub jobs_processed: Arc<AtomicU64>,
    pub jobs_nacked: Arc<AtomicU64>,
    pub jobs_succeeded: Arc<AtomicU64>,
    pub total_processing_time_ms: Arc<AtomicU64>,
}

impl BenchmarkMetrics {
    fn new() -> Self {
        Self {
            jobs_processed: Arc::new(AtomicU64::new(0)),
            jobs_nacked: Arc::new(AtomicU64::new(0)),
            jobs_succeeded: Arc::new(AtomicU64::new(0)),
            total_processing_time_ms: Arc::new(AtomicU64::new(0)),
        }
    }

    fn reset(&self) {
        self.jobs_processed.store(0, Ordering::SeqCst);
        self.jobs_nacked.store(0, Ordering::SeqCst);
        self.jobs_succeeded.store(0, Ordering::SeqCst);
        self.total_processing_time_ms.store(0, Ordering::SeqCst);
    }

    fn processed_count(&self) -> u64 {
        self.jobs_processed.load(Ordering::SeqCst)
    }

    fn success_rate(&self) -> f64 {
        let succeeded = self.jobs_succeeded.load(Ordering::SeqCst) as f64;
        let total = self.jobs_processed.load(Ordering::SeqCst) as f64;
        if total > 0.0 { succeeded / total } else { 0.0 }
    }

    fn avg_processing_time_ms(&self) -> f64 {
        let total_time = self.total_processing_time_ms.load(Ordering::SeqCst) as f64;
        let total_jobs = self.jobs_processed.load(Ordering::SeqCst) as f64;
        if total_jobs > 0.0 {
            total_time / total_jobs
        } else {
            0.0
        }
    }
}

impl DurableExecution for BenchmarkJob {
    type Output = BenchmarkOutput;
    type ErrorData = BenchmarkErrorData;
    type ExecutionContext = BenchmarkMetrics;

    async fn process(
        job: &Job<Self>,
        metrics: &Self::ExecutionContext,
    ) -> JobResult<Self::Output, Self::ErrorData> {
        let start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Simulate minimal work (just increment counter)
        metrics.jobs_processed.fetch_add(1, Ordering::SeqCst);

        let end_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let processing_time = end_time - start_time;
        metrics
            .total_processing_time_ms
            .fetch_add(processing_time, Ordering::SeqCst);

        // Fresh random decision each processing attempt
        if rand::thread_rng().gen_bool(job.data.nack_probability) {
            metrics.jobs_nacked.fetch_add(1, Ordering::SeqCst);

            // Random position for nacks as requested
            let position = if rand::thread_rng().gen_bool(0.5) {
                RequeuePosition::First
            } else {
                RequeuePosition::Last
            };

            JobResult::Nack {
                error: BenchmarkErrorData {
                    job_id: job.data.job_id.clone(),
                    attempt: job.attempts,
                },
                delay: None, // No delay as requested
                position,
            }
        } else {
            metrics.jobs_succeeded.fetch_add(1, Ordering::SeqCst);

            JobResult::Success(BenchmarkOutput {
                job_id: job.data.job_id.clone(),
                processed_at: end_time,
            })
        }
    }

    async fn on_success(
        &self,
        _job: &Job<Self>,
        _d: SuccessHookData<'_, Self::Output>,
        _tx: &mut TransactionContext<'_>,
        _metrics: &Self::ExecutionContext,
    ) {
        // Keep hooks minimal for max performance
    }

    async fn on_nack(
        &self,
        _job: &Job<Self>,
        _d: NackHookData<'_, Self::ErrorData>,
        _tx: &mut TransactionContext<'_>,
        _metrics: &Self::ExecutionContext,
    ) {
        // Keep hooks minimal for max performance
    }

    async fn on_timeout(&self, _tx: &mut TransactionContext<'_>) {
        // Minimal timeout handling
    }
}

// Load test that finds maximum sustainable throughput
async fn load_test_throughput(
    jobs_per_second: usize,
    duration_seconds: u64,
    nack_percentage: f64,
) -> (u64, f64, f64, bool) {
    let test_id = nanoid::nanoid!(8);
    let queue_name = format!("bench_queue_{}", test_id);

    let metrics = BenchmarkMetrics::new();

    // Optimize queue for high throughput
    let queue_options = QueueOptions {
        local_concurrency: 200,                      // High concurrency
        polling_interval: Duration::from_millis(10), // Fast polling
        always_poll: true,                           // Always poll for max responsiveness
        lease_duration: Duration::from_secs(30),     // Reasonable lease time
        max_success: 10000,                          // Large success queue
        max_failed: 1000,                            // Reasonable failed queue
    };

    let queue = Arc::new(
        Queue::<BenchmarkJob, BenchmarkOutput, BenchmarkErrorData, BenchmarkMetrics>::new(
            REDIS_URL,
            &queue_name,
            Some(queue_options),
            metrics.clone(),
        )
        .await
        .expect("Failed to create benchmark queue"),
    );

    // Clean up any existing data
    let mut redis_conn = queue.redis.clone();
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("{}:*", queue_name))
        .query_async(&mut redis_conn)
        .await
        .unwrap_or_default();
    if !keys.is_empty() {
        let _: () = redis::cmd("DEL")
            .arg(keys)
            .query_async(&mut redis_conn)
            .await
            .unwrap_or(());
    }

    // Start workers
    let worker_handle = {
        let queue = queue.clone();
        tokio::spawn(async move {
            let _ = queue.work().await;
        })
    };

    // Job producer task
    let producer_handle = {
        let queue = queue.clone();
        tokio::spawn(async move {
            let mut job_counter = 0u64;
            let interval_duration = Duration::from_secs_f64(1.0 / jobs_per_second as f64);
            let mut interval = tokio::time::interval(interval_duration);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            let start_time = SystemTime::now();

            loop {
                if start_time.elapsed().unwrap().as_secs() >= duration_seconds {
                    break;
                }

                interval.tick().await;

                let job = BenchmarkJob {
                    job_id: format!("job_{}", job_counter),
                    nack_probability: nack_percentage,
                    created_at: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                };

                if queue
                    .clone()
                    .job(job)
                    .with_id(&format!("job_{}", job_counter))
                    .push()
                    .await
                    .is_ok()
                {
                    job_counter += 1;
                }
            }

            job_counter
        })
    };

    // Monitor queue depth
    let mut max_queue_depth = 0usize;
    let monitor_handle = {
        let queue = queue.clone();
        tokio::spawn(async move {
            let start_time = SystemTime::now();
            while start_time.elapsed().unwrap().as_secs() < duration_seconds + 5 {
                let pending = queue.count(JobStatus::Pending).await.unwrap_or(0);
                let active = queue.count(JobStatus::Active).await.unwrap_or(0);
                let depth = pending + active;
                max_queue_depth = max_queue_depth.max(depth);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            max_queue_depth
        })
    };

    // Wait for test completion
    let jobs_pushed = producer_handle.await.unwrap();

    // Give some extra time for jobs to finish processing
    tokio::time::sleep(Duration::from_secs(2)).await;

    let max_depth = monitor_handle.await.unwrap();

    // Check if queue is sustainable (not building up)
    let final_pending = queue.count(JobStatus::Pending).await.unwrap_or(0);
    let final_active = queue.count(JobStatus::Active).await.unwrap_or(0);
    let is_sustainable = (final_pending + final_active) < jobs_per_second / 2; // Heuristic: backlog < 0.5 seconds of work

    let total_processed = metrics.processed_count();
    let success_rate = metrics.success_rate();
    let avg_processing_time = metrics.avg_processing_time_ms();

    println!(
        "Load Test Results - {}jobs/s for {}s:",
        jobs_per_second, duration_seconds
    );
    println!("  Jobs pushed: {}", jobs_pushed);
    println!("  Jobs processed: {}", total_processed);
    println!("  Success rate: {:.1}%", success_rate * 100.0);
    println!("  Avg processing time: {:.2}ms", avg_processing_time);
    println!("  Max queue depth: {}", max_depth);
    println!("  Final backlog: {}", final_pending + final_active);
    println!("  Sustainable: {}", is_sustainable);

    // Cleanup
    worker_handle.abort();
    let _: () = redis::cmd("DEL")
        .arg(format!("{}:*", queue_name))
        .query_async(&mut redis_conn)
        .await
        .unwrap_or(());

    (
        total_processed,
        success_rate,
        avg_processing_time,
        is_sustainable,
    )
}

// Benchmark that finds maximum sustainable throughput
fn find_max_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("twmq_max_throughput");
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(10);

    // Test different throughput levels to find the limit
    let throughput_levels = vec![50_000, 100_000, 120_000, 150_000, 200_000, 250_000];

    for &jobs_per_second in &throughput_levels {
        group.throughput(Throughput::Elements(jobs_per_second as u64));

        group.bench_with_input(
            BenchmarkId::new("sustainable_throughput", jobs_per_second),
            &jobs_per_second,
            |b, &jobs_per_second| {
                b.to_async(&rt).iter(|| async {
                    let (processed, success_rate, avg_time, sustainable) =
                        load_test_throughput(jobs_per_second, 10, 0.1).await;

                    // Return metrics for Criterion
                    black_box((processed, success_rate, avg_time, sustainable))
                });
            },
        );
    }

    group.finish();
}

// Benchmark different nack percentages
fn nack_percentage_impact(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("twmq_nack_impact");
    group.measurement_time(Duration::from_secs(20));

    let nack_percentages = vec![0.0, 0.05, 0.1, 0.2, 0.3, 0.5];

    for &nack_pct in &nack_percentages {
        group.bench_with_input(
            BenchmarkId::new("nack_percentage", (nack_pct * 100.0) as u32),
            &nack_pct,
            |b, &nack_pct| {
                b.to_async(&rt).iter(|| async move {
                    let (processed, success_rate, avg_time, sustainable) =
                        load_test_throughput(5000, 8, nack_pct).await;

                    black_box((processed, success_rate, avg_time, sustainable))
                });
            },
        );
    }

    group.finish();
}

// Quick smoke test benchmark
fn basic_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("twmq_basic_1k_jobs");
    group.sample_size(10); // ← Only 5 samples instead of 100

    group.bench_function("1k_jobs", |b| {
        b.to_async(&rt).iter(|| async {
            let (processed, success_rate, avg_time, sustainable) =
                load_test_throughput(1000, 5, 0.1).await;

            black_box((processed, success_rate, avg_time, sustainable))
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    basic_throughput,
    find_max_throughput,
    nack_percentage_impact
);
criterion_main!(benches);
