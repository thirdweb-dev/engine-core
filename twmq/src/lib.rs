pub mod error;
pub mod hooks;
pub mod job;
pub mod multilane;
pub mod queue;
pub mod shutdown;

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use error::TwmqError;
use hooks::TransactionContext;
pub use job::BorrowedJob;
use job::{
    DelayOptions, Job, JobError, JobErrorRecord, JobErrorType, JobOptions, JobResult, JobStatus,
    PushableJob, RequeuePosition,
};
pub use multilane::{MultilanePushableJob, MultilaneQueue};
use queue::QueueOptions;
use redis::Pipeline;
use redis::{AsyncCommands, RedisResult, aio::ConnectionManager};
use serde::{Serialize, de::DeserializeOwned};
use shutdown::WorkerHandle;
use tokio::sync::Semaphore;
use tokio::time::sleep;

pub use queue::IdempotencyMode;
pub use redis;
use tracing::Instrument;

// Trait for error types to implement user cancellation
pub trait UserCancellable {
    fn user_cancelled() -> Self;
}

#[derive(Debug)]
pub enum CancelResult {
    CancelledImmediately,
    CancellationPending,
    NotFound,
}

pub struct SuccessHookData<'a, O> {
    pub result: &'a O,
}

pub struct NackHookData<'a, E> {
    pub error: &'a E,
    pub delay: Option<Duration>,
    pub position: RequeuePosition,
}

pub struct FailHookData<'a, E> {
    pub error: &'a E,
}

pub struct QueueInternalErrorHookData<'a> {
    pub error: &'a TwmqError,
}

// Main DurableExecution trait
pub trait DurableExecution: Sized + Send + Sync + 'static {
    type Output: Serialize + DeserializeOwned + Send + Sync;
    type ErrorData: Serialize + DeserializeOwned + From<TwmqError> + UserCancellable + Send + Sync;
    type JobData: Serialize + DeserializeOwned + Clone + Send + Sync + 'static;

    // Required method to process a job
    fn process(
        &self,
        job: &BorrowedJob<Self::JobData>,
    ) -> impl Future<Output = JobResult<Self::Output, Self::ErrorData>> + Send;

    fn on_success(
        &self,
        _job: &BorrowedJob<Self::JobData>,
        _d: SuccessHookData<Self::Output>,
        _tx: &mut TransactionContext<'_>,
    ) -> impl Future<Output = ()> + Send {
        std::future::ready(())
    }

    fn on_nack(
        &self,
        _job: &BorrowedJob<Self::JobData>,
        _d: NackHookData<Self::ErrorData>,
        _tx: &mut TransactionContext<'_>,
    ) -> impl Future<Output = ()> + Send {
        std::future::ready(())
    }

    fn on_fail(
        &self,
        _job: &BorrowedJob<Self::JobData>,
        _d: FailHookData<Self::ErrorData>,
        _tx: &mut TransactionContext<'_>,
    ) -> impl Future<Output = ()> + Send {
        std::future::ready(())
    }

    fn on_timeout(
        &self,
        _tx: &mut TransactionContext<'_>,
    ) -> impl Future<Output = ()> + Send + Sync {
        std::future::ready(())
    }

    /// Data available to the `on_queue_error` hook. The failure might have been related to deserialization of the job data.
    /// So the job data might be `None`. This hook is called before the job is moved to the failed state.
    fn on_queue_error(
        &self,
        _job: &Job<Option<Self::JobData>>,
        _d: QueueInternalErrorHookData<'_>,
        _tx: &mut TransactionContext<'_>,
    ) -> impl Future<Output = ()> + Send {
        std::future::ready(())
    }
}

// Main Queue struct
pub struct Queue<H>
where
    H: DurableExecution,
{
    pub redis: ConnectionManager,
    handler: Arc<H>,
    options: QueueOptions,
    // concurrency: usize,
    name: String,
}

impl<H: DurableExecution> Queue<H> {
    pub async fn new(
        redis_url: &str,
        name: &str,
        // concurrency: usize,
        options: Option<QueueOptions>,
        handler: H,
    ) -> Result<Self, TwmqError> {
        let client = redis::Client::open(redis_url)?;
        let redis = client.get_connection_manager().await?;

        let queue = Self {
            redis,
            name: name.to_string(),
            // concurrency,
            options: options.unwrap_or_default(),
            handler: Arc::new(handler),
        };

        Ok(queue)
    }

    pub fn arc(self) -> Arc<Self> {
        Arc::new(self)
    }

    pub fn job(self: Arc<Self>, data: H::JobData) -> PushableJob<H> {
        PushableJob {
            options: JobOptions::new(data),
            queue: self,
        }
    }

    /// Create a TransactionContext from an existing Redis pipeline
    /// This allows queueing jobs atomically within an existing transaction
    pub fn transaction_context_from_pipeline<'a>(
        &self,
        pipeline: &'a mut redis::Pipeline,
    ) -> hooks::TransactionContext<'a> {
        hooks::TransactionContext::new(pipeline, self.name.clone())
    }

    // Get queue name
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn pending_list_name(&self) -> String {
        format!("twmq:{}:pending", self.name())
    }

    pub fn active_hash_name(&self) -> String {
        format!("twmq:{}:active", self.name)
    }

    pub fn delayed_zset_name(&self) -> String {
        format!("twmq:{}:delayed", self.name)
    }

    pub fn success_list_name(&self) -> String {
        format!("twmq:{}:success", self.name)
    }

    pub fn failed_list_name(&self) -> String {
        format!("twmq:{}:failed", self.name)
    }

    pub fn job_data_hash_name(&self) -> String {
        format!("twmq:{}:jobs:data", self.name)
    }

    pub fn job_meta_hash_name(&self, job_id: &str) -> String {
        format!("twmq:{}:job:{}:meta", self.name, job_id)
    }

    pub fn job_errors_list_name(&self, job_id: &str) -> String {
        format!("twmq:{}:job:{}:errors", self.name, job_id)
    }

    pub fn job_result_hash_name(&self) -> String {
        format!("twmq:{}:jobs:result", self.name)
    }

    pub fn dedupe_set_name(&self) -> String {
        format!("twmq:{}:dedup", self.name)
    }

    pub fn pending_cancellation_set_name(&self) -> String {
        format!("twmq:{}:pending_cancellations", self.name)
    }

    pub fn lease_key_name(&self, job_id: &str, lease_token: &str) -> String {
        format!("twmq:{}:job:{}:lease:{}", self.name, job_id, lease_token)
    }

    pub async fn push(
        &self,
        job_options: JobOptions<H::JobData>,
    ) -> Result<Job<H::JobData>, TwmqError> {
        // Check for duplicates and handle job creation with deduplication
        let script = redis::Script::new(
            r#"
            local job_id = ARGV[1]
            local job_data = ARGV[2]
            local now = ARGV[3]
            local delay = ARGV[4]
            local reentry_position = ARGV[5]  -- "first" or "last"

            local queue_id = KEYS[1]
            local delayed_zset_name = KEYS[2]
            local pending_list_name = KEYS[3]

            local job_data_hash_name = KEYS[4]
            local job_meta_hash_name = KEYS[5]

            local dedupe_set_name = KEYS[6]

            -- Check if job already exists in any queue
            if redis.call('SISMEMBER', dedupe_set_name, job_id) == 1 then
                -- Job with this ID already exists, skip
                return { 0, job_id }
            end

            -- Store job data
            redis.call('HSET', job_data_hash_name, job_id, job_data)

            -- Store job metadata as a hash
            redis.call('HSET', job_meta_hash_name, 'created_at', now)
            redis.call('HSET', job_meta_hash_name, 'attempts', 0)

            -- Add to deduplication set
            redis.call('SADD', dedupe_set_name, job_id)

            -- Add to appropriate queue based on delay
            if tonumber(delay) > 0 then
                local process_at = now + tonumber(delay)
                -- Store position information for this delayed job
                redis.call('HSET', job_meta_hash_name, 'reentry_position', reentry_position)
                redis.call('ZADD', delayed_zset_name, process_at, job_id)
            else
                -- Non-delayed job always goes to end of pending
                redis.call('RPUSH', pending_list_name, job_id)
            end

            return { 1, job_id }
            "#,
        );

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let job = Job {
            id: job_options.id.clone(),
            data: job_options.data,
            attempts: 0,
            created_at: now,
            processed_at: None,
            finished_at: None,
        };

        let job_data = serde_json::to_string(&job.data)?;

        let delay = job_options.delay.unwrap_or(DelayOptions {
            delay: Duration::ZERO,
            position: RequeuePosition::Last,
        });

        let delay_secs = delay.delay.as_secs();
        let position_string = delay.position.to_string();

        let _result: (i32, String) = script
            .key(&self.name)
            .key(self.delayed_zset_name())
            .key(self.pending_list_name())
            .key(self.job_data_hash_name())
            .key(self.job_meta_hash_name(&job.id))
            .key(self.dedupe_set_name())
            .arg(job_options.id)
            .arg(job_data)
            .arg(now)
            .arg(delay_secs)
            .arg(position_string)
            .invoke_async(&mut self.redis.clone())
            .await?;

        // Return job_id whether new or existing
        Ok(job)
    }

    pub async fn get_job(&self, job_id: &str) -> Result<Option<Job<H::JobData>>, TwmqError> {
        let mut conn = self.redis.clone();
        let job_data_t_json: Option<String> = conn.hget(self.job_data_hash_name(), job_id).await?;

        if let Some(data_json) = job_data_t_json {
            let data_t: H::JobData = serde_json::from_str(&data_json)?;

            // Fetch metadata
            let meta_map: std::collections::HashMap<String, String> =
                conn.hgetall(self.job_meta_hash_name(job_id)).await?;

            let attempts: u32 = meta_map
                .get("attempts")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let created_at: u64 = meta_map
                .get("created_at")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0); // Consider a more robust default or error
            let processed_at: Option<u64> =
                meta_map.get("processed_at").and_then(|s| s.parse().ok());
            let finished_at: Option<u64> = meta_map.get("finished_at").and_then(|s| s.parse().ok());
            // reentry_position is also in meta if needed for display

            Ok(Some(Job {
                id: job_id.to_string(),
                data: data_t,
                attempts,
                created_at,
                processed_at,
                finished_at,
            }))
        } else {
            Ok(None)
        }
    }

    pub async fn count(&self, status: JobStatus) -> Result<usize, TwmqError> {
        let mut conn = self.redis.clone();

        let count = match status {
            JobStatus::Pending => {
                let count: usize = conn.llen(self.pending_list_name()).await?;
                count
            }
            JobStatus::Active => {
                let count: usize = conn.hlen(self.active_hash_name()).await?;
                count
            }
            JobStatus::Delayed => {
                let count: usize = conn.zcard(self.delayed_zset_name()).await?;
                count
            }
            JobStatus::Success => {
                let count: usize = conn.llen(self.success_list_name()).await?;
                count
            }
            JobStatus::Failed => {
                let count: usize = conn.llen(self.failed_list_name()).await?;
                count
            }
        };

        Ok(count)
    }

    pub async fn cancel_job(&self, job_id: &str) -> Result<CancelResult, TwmqError> {
        let script = redis::Script::new(
            r#"
            local job_id = ARGV[1]
            
            local pending_list = KEYS[1]
            local delayed_zset = KEYS[2]
            local active_hash = KEYS[3]
            local failed_list = KEYS[4]
            local pending_cancellation_set = KEYS[5]
            local job_meta_hash = KEYS[6]
            
            -- Try to remove from pending queue
            if redis.call('LREM', pending_list, 0, job_id) > 0 then
                -- Move to failed state with cancellation
                redis.call('LPUSH', failed_list, job_id)
                redis.call('HSET', job_meta_hash, 'finished_at', ARGV[2])
                return "cancelled_immediately"
            end
            
            -- Try to remove from delayed queue  
            if redis.call('ZREM', delayed_zset, job_id) > 0 then
                -- Move to failed state with cancellation
                redis.call('LPUSH', failed_list, job_id)
                redis.call('HSET', job_meta_hash, 'finished_at', ARGV[2])
                return "cancelled_immediately"
            end
            
            -- Check if job is active
            if redis.call('HEXISTS', active_hash, job_id) == 1 then
                -- Add to pending cancellations set
                redis.call('SADD', pending_cancellation_set, job_id)
                return "cancellation_pending"
            end
            
            return "not_found"
            "#,
        );

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let result: String = script
            .key(self.pending_list_name())
            .key(self.delayed_zset_name())
            .key(self.active_hash_name())
            .key(self.failed_list_name())
            .key(self.pending_cancellation_set_name())
            .key(self.job_meta_hash_name(job_id))
            .arg(job_id)
            .arg(now)
            .invoke_async(&mut self.redis.clone())
            .await?;

        match result.as_str() {
            "cancelled_immediately" => {
                // Process the cancellation through hook system
                if let Err(e) = self.process_cancelled_job(job_id).await {
                    tracing::error!(
                        job_id = job_id,
                        error = ?e,
                        "Failed to process immediately cancelled job"
                    );
                }
                Ok(CancelResult::CancelledImmediately)
            }
            "cancellation_pending" => Ok(CancelResult::CancellationPending),
            "not_found" => Ok(CancelResult::NotFound),
            _ => Err(TwmqError::Runtime {
                message: format!("Unexpected cancel result: {}", result),
            }),
        }
    }

    pub fn work(self: &Arc<Self>) -> WorkerHandle<Queue<H>> {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        // Local semaphore to limit concurrency per instance
        let semaphore = Arc::new(Semaphore::new(self.options.local_concurrency));
        let handler = self.handler.clone();
        let outer_queue_clone = self.clone();
        // Start worker
        let join_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(outer_queue_clone.options.polling_interval);
            let handler_clone = handler.clone();
            let always_poll = outer_queue_clone.options.always_poll;

            tracing::info!("Worker started for queue: {}", outer_queue_clone.name());
            loop {
                tokio::select! {
                    // Check for shutdown signal
                    _ = &mut shutdown_rx => {
                        tracing::info!("Shutdown signal received for queue: {}", outer_queue_clone.name());
                        break;
                    }

                    // Normal polling tick
                    _ = interval.tick() => {
                        let queue_clone = outer_queue_clone.clone();
                        let queue_name = queue_clone.name();

                        // Check available permits for batch size
                        let available_permits = semaphore.available_permits();
                        if available_permits == 0 && !always_poll {
                            tracing::trace!("No permits available, waiting...");
                            continue;
                        }

                        tracing::trace!("Available permits: {}", available_permits);
                        // Try to get multiple jobs - as many as we have permits
                        match queue_clone.pop_batch_jobs(available_permits).await {
                            Ok(jobs) => {
                                tracing::trace!("Got {} jobs", jobs.len());
                                for job in jobs {
                                    let permit = semaphore.clone().acquire_owned().await.unwrap();
                                    let queue_clone = queue_clone.clone();
                                    let job_id = job.id().to_string();
                                    let handler_clone = handler_clone.clone();

                                    tokio::spawn(
                                        async move {
                                        // Process job - note we don't pass a context here
                                        let result = handler_clone.process(&job).await;

                                        // Complete job using unified method with hooks and retry logic
                                        if let Err(e) = queue_clone.complete_job(&job, result).await {
                                            tracing::error!(
                                                "Failed to complete job {} handling: {:?}",
                                                job.id(),
                                                e
                                            );
                                        }

                                        // Release permit when done
                                        drop(permit);
                                    }.instrument(tracing::info_span!("twmq_worker", job_id, queue_name)));
                                }
                            }
                            Err(e) => {
                                // No jobs found, we hit an error
                                tracing::error!("Failed to pop batch jobs: {:?}", e);
                                sleep(Duration::from_millis(1000)).await;
                            }
                        };

                    }
                }
            }

            // Graceful shutdown: wait for all active jobs to complete
            tracing::info!(
                "Waiting for {} active jobs to complete for queue: {}",
                semaphore
                    .available_permits()
                    .saturating_sub(outer_queue_clone.options.local_concurrency),
                outer_queue_clone.name()
            );

            // Acquire all permits to ensure no jobs are running
            let _permits: Vec<_> = (0..outer_queue_clone.options.local_concurrency)
                .map(|_| semaphore.clone().acquire_owned())
                .collect::<futures::future::JoinAll<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| TwmqError::Runtime {
                    message: format!("Failed to acquire permits during shutdown: {}", e),
                })?;

            tracing::info!(
                "All jobs completed, worker shutdown complete for queue: {}",
                outer_queue_clone.name()
            );
            Ok(())
        });

        WorkerHandle {
            join_handle,
            shutdown_tx,
            queue: self.clone(),
        }
    }

    // Improved batch job popping - gets multiple jobs at once
    async fn pop_batch_jobs(
        self: &Arc<Self>,
        batch_size: usize,
    ) -> RedisResult<Vec<BorrowedJob<H::JobData>>> {
        // Lua script that does:
        // 1. Clean up expired leases (with lease token validation)
        // 2. Process pending cancellations
        // 3. Process expired delayed jobs
        // 4. Pop up to batch_size jobs from pending (with new lease tokens)
        let script = redis::Script::new(
            r#"
            local now = tonumber(ARGV[1])
            local batch_size = tonumber(ARGV[2])
            local lease_seconds = tonumber(ARGV[3])

            local queue_id = KEYS[1]
            local delayed_zset_name = KEYS[2]
            local pending_list_name = KEYS[3]
            local active_hash_name = KEYS[4]
            local job_data_hash_name = KEYS[5]
            local pending_cancellation_set = KEYS[6]
            local failed_list_name = KEYS[7]
            local success_list_name = KEYS[8]

            local result_jobs = {}
            local timed_out_jobs = {}
            local cancelled_jobs = {}
            local completed_jobs = {}

            -- Step 1: Clean up expired leases by checking lease keys stored in job meta
            -- Get all active jobs (now just contains job_id -> attempts)
            local active_jobs = redis.call('HGETALL', active_hash_name)

            -- Process in pairs (job_id, attempts)
            for i = 1, #active_jobs, 2 do
                local job_id = active_jobs[i]
                local attempts = active_jobs[i + 1]
                local job_meta_hash_name = 'twmq:' .. queue_id .. ':job:' .. job_id .. ':meta'
                
                -- Get the current lease token from job metadata
                local current_lease_token = redis.call('HGET', job_meta_hash_name, 'lease_token')
                
                if current_lease_token then
                    -- Build the lease key and check if it exists (Redis auto-expires)
                    local lease_key = 'twmq:' .. queue_id .. ':job:' .. job_id .. ':lease:' .. current_lease_token
                    local lease_exists = redis.call('EXISTS', lease_key)
                    
                    -- If lease doesn't exist (expired), move job back to pending
                    if lease_exists == 0 then
                        redis.call('HINCRBY', job_meta_hash_name, 'attempts', 1)
                        redis.call('HDEL', job_meta_hash_name, 'lease_token')
                        
                        -- Move job back to pending
                        redis.call('HDEL', active_hash_name, job_id)
                        redis.call('LPUSH', pending_list_name, job_id)
                        
                        -- Add to list of timed out jobs
                        table.insert(timed_out_jobs, job_id)
                    end
                else
                    -- No lease token in meta, something's wrong - move back to pending
                    redis.call('HINCRBY', job_meta_hash_name, 'attempts', 1)
                    redis.call('HDEL', active_hash_name, job_id)
                    redis.call('LPUSH', pending_list_name, job_id)
                    table.insert(timed_out_jobs, job_id)
                end
            end

            -- Step 2: Process pending cancellations AFTER lease cleanup
            local cancel_requests = redis.call('SMEMBERS', pending_cancellation_set)
            
            for i, job_id in ipairs(cancel_requests) do
                -- Check if job is still active
                if redis.call('HEXISTS', active_hash_name, job_id) == 1 then
                    -- Still processing, keep in cancellation set
                else
                    -- Job finished processing, check outcome
                    if redis.call('LPOS', success_list_name, job_id) then
                        -- Job succeeded, just remove from cancellation set
                        table.insert(completed_jobs, job_id)
                    else
                        -- Job not successful, cancel it now
                        redis.call('LPUSH', failed_list_name, job_id)
                        -- Add cancellation timestamp
                        local job_meta_hash_name = 'twmq:' .. queue_id .. ':job:' .. job_id .. ':meta'
                        redis.call('HSET', job_meta_hash_name, 'finished_at', now)
                        table.insert(cancelled_jobs, job_id)
                    end
                    -- Remove from pending cancellations
                    redis.call('SREM', pending_cancellation_set, job_id)
                end
            end

            -- Step 3: Move expired delayed jobs to pending
            local delayed_jobs = redis.call('ZRANGEBYSCORE', delayed_zset_name, 0, now)
            for i, job_id in ipairs(delayed_jobs) do
                local job_meta_hash_name = 'twmq:' .. queue_id .. ':job:' .. job_id .. ':meta'
                local reentry_position = redis.call('HGET', job_meta_hash_name, 'reentry_position') or 'last'

                -- Remove from delayed
                redis.call('ZREM', delayed_zset_name, job_id)
                redis.call('HDEL', job_meta_hash_name, 'reentry_position')

                -- Add to pending based on position
                if reentry_position == 'first' then
                    redis.call('LPUSH', pending_list_name, job_id)
                else
                    redis.call('RPUSH', pending_list_name, job_id)
                end
            end

            -- Step 4: Pop jobs from pending and create lease keys (up to batch_size)
            local popped_job_ids = {}
            for i = 1, batch_size do
                local job_id = redis.call('LPOP', pending_list_name)
                if not job_id then
                    break
                end
                table.insert(popped_job_ids, job_id)
            end

            local result_jobs = {}

            -- Process popped jobs
            for _, job_id in ipairs(popped_job_ids) do
                -- Get job data
                local job_data = redis.call('HGET', job_data_hash_name, job_id)

                -- Only process if we have data
                if job_data then
                    -- Update metadata
                    local job_meta_hash_name = 'twmq:' .. queue_id .. ':job:' .. job_id .. ':meta'
                    redis.call('HSET', job_meta_hash_name, 'processed_at', now)
                    local created_at = redis.call('HGET', job_meta_hash_name, 'created_at') or now
                    local attempts = redis.call('HINCRBY', job_meta_hash_name, 'attempts', 1)

                    -- Generate unique lease token
                    local lease_token = now .. '_' .. job_id .. '_' .. attempts

                    -- Create separate lease key with TTL
                    local lease_key = 'twmq:' .. queue_id .. ':job:' .. job_id .. ':lease:' .. lease_token
                    redis.call('SET', lease_key, '1')
                    redis.call('EXPIRE', lease_key, lease_seconds)

                    -- Store lease token in job metadata
                    redis.call('HSET', job_meta_hash_name, 'lease_token', lease_token)

                    -- Add to active hash (just job_id -> attempts, no lease info)
                    redis.call('HSET', active_hash_name, job_id, attempts)

                    -- Add to result with job data and lease token
                    table.insert(result_jobs, {job_id, job_data, tostring(attempts), tostring(created_at), tostring(now), lease_token})
                end
            end

            return {result_jobs, cancelled_jobs, timed_out_jobs}
            "#,
        );

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let results_from_lua: (
            Vec<(String, String, String, String, String, String)>,
            Vec<String>,
            Vec<String>,
        ) = script
            .key(self.name())
            .key(self.delayed_zset_name())
            .key(self.pending_list_name())
            .key(self.active_hash_name())
            .key(self.job_data_hash_name())
            .key(self.pending_cancellation_set_name())
            .key(self.failed_list_name())
            .key(self.success_list_name())
            .arg(now)
            .arg(batch_size)
            .arg(self.options.lease_duration.as_secs())
            .invoke_async(&mut self.redis.clone())
            .await?;

        let (job_results, cancelled_jobs, timed_out_jobs) = results_from_lua;

        // Log individual lease timeouts and cancellations
        for job_id in &timed_out_jobs {
            tracing::warn!(job_id = job_id, "Job lease expired, moved back to pending");
        }
        for job_id in &cancelled_jobs {
            tracing::info!(job_id = job_id, "Job cancelled by user request");
        }

        let mut jobs = Vec::new();
        for (
            job_id_str,
            job_data_t_json,
            attempts_str,
            created_at_str,
            processed_at_str,
            lease_token,
        ) in job_results
        {
            match serde_json::from_str::<H::JobData>(&job_data_t_json) {
                Ok(data_t) => {
                    let attempts: u32 = attempts_str.parse().unwrap_or(1); // Default or handle error
                    let created_at: u64 = created_at_str.parse().unwrap_or(now); // Default or handle error
                    let processed_at: u64 = processed_at_str.parse().unwrap_or(now); // Default or handle error

                    let job = Job {
                        id: job_id_str,
                        data: data_t,
                        attempts,
                        created_at,
                        processed_at: Some(processed_at),
                        finished_at: None, // Not finished yet
                    };

                    jobs.push(BorrowedJob::new(job, lease_token));
                }
                Err(e) => {
                    // Log error: failed to deserialize job data T for job_id_str
                    tracing::error!(
                        job_id = job_id_str,
                        error = ?e,
                        "Failed to deserialize job data. Spawning task to move job to failed state.",
                    );

                    let queue_clone = self.clone();

                    tokio::spawn(async move {
                        // let's call the on_queue_error hook and move the job to the failed state
                        let mut pipeline = redis::pipe();
                        pipeline.atomic(); // Use MULTI/EXEC

                        let mut _tx_context =
                            TransactionContext::new(&mut pipeline, queue_clone.name().to_string());

                        let job: Job<Option<H::JobData>> = Job {
                            id: job_id_str.to_string(),
                            data: None,
                            attempts: attempts_str.parse().unwrap_or(1),
                            created_at: created_at_str.parse().unwrap_or(now),
                            processed_at: processed_at_str.parse().ok(),
                            finished_at: Some(now),
                        };

                        let twmq_error: TwmqError = e.into();

                        // Complete job using queue error method with lease token
                        if let Err(e) = queue_clone
                            .complete_job_queue_error(&job, &lease_token, &twmq_error.into())
                            .await
                        {
                            tracing::error!(
                                job_id = job.id,
                                error = ?e,
                                "Failed to complete job fail handling successfully",
                            );
                        }
                    });
                }
            }
        }

        // Process cancelled jobs through hook system
        for job_id in cancelled_jobs {
            let queue_clone = self.clone();
            tokio::spawn(async move {
                if let Err(e) = queue_clone.process_cancelled_job(&job_id).await {
                    tracing::error!(
                        job_id = job_id,
                        error = ?e,
                        "Failed to process cancelled job"
                    );
                }
            });
        }

        Ok(jobs)
    }

    /// Process a cancelled job through the hook system with user cancellation error
    async fn process_cancelled_job(&self, job_id: &str) -> Result<(), TwmqError> {
        // Get job data for the cancelled job
        match self.get_job(job_id).await? {
            Some(job) => {
                // Create cancellation error using the trait
                let cancellation_error = H::ErrorData::user_cancelled();

                // Create transaction pipeline for atomicity
                let mut pipeline = redis::pipe();
                pipeline.atomic();

                // Create transaction context with mutable access to pipeline
                let mut tx_context =
                    TransactionContext::new(&mut pipeline, self.name().to_string());

                let fail_hook_data = FailHookData {
                    error: &cancellation_error,
                };

                // Create a BorrowedJob with a dummy lease token since cancelled jobs don't have active leases
                let borrowed_job = BorrowedJob::new(job, "cancelled".to_string());

                // Call fail hook for user cancellation
                self.handler
                    .on_fail(&borrowed_job, fail_hook_data, &mut tx_context)
                    .await;

                // Execute the pipeline (just hook commands, job already moved to failed)
                pipeline.query_async::<()>(&mut self.redis.clone()).await?;

                tracing::info!(
                    job_id = job_id,
                    "Successfully processed job cancellation hooks"
                );

                Ok(())
            }
            None => {
                tracing::warn!(
                    job_id = job_id,
                    "Cancelled job not found when trying to process hooks"
                );
                Ok(())
            }
        }
    }

    fn add_success_operations(
        &self,
        job: &BorrowedJob<H::JobData>,
        result: &H::Output,
        pipeline: &mut Pipeline,
    ) -> Result<(), TwmqError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let lease_key = self.lease_key_name(&job.job.id, &job.lease_token);

        // Delete the lease key to consume it
        pipeline.del(&lease_key);

        // Add job completion operations
        pipeline
            .hdel(self.active_hash_name(), &job.job.id)
            .lpush(self.success_list_name(), &job.job.id)
            .hset(self.job_meta_hash_name(&job.job.id), "finished_at", now)
            .hdel(self.job_meta_hash_name(&job.job.id), "lease_token");

        let result_json = serde_json::to_string(result)?;
        pipeline.hset(self.job_result_hash_name(), &job.job.id, result_json);

        // For "active" idempotency mode, remove from deduplication set immediately
        if self.options.idempotency_mode == queue::IdempotencyMode::Active {
            pipeline.srem(self.dedupe_set_name(), &job.job.id);
        }

        Ok(())
    }

    async fn post_success_completion(&self) -> Result<(), TwmqError> {
        // Separate call for pruning with data deletion using Lua
        let trim_script = redis::Script::new(
            r#"
                local queue_id = KEYS[1]
                local list_name = KEYS[2]
                local job_data_hash = KEYS[3]
                local results_hash = KEYS[4] -- e.g., "myqueue:results"
                local dedupe_set_name = KEYS[5]

                local max_len = tonumber(ARGV[1])

                local job_ids_to_delete = redis.call('LRANGE', list_name, max_len, -1)

                if #job_ids_to_delete > 0 then
                    for _, j_id in ipairs(job_ids_to_delete) do
                        local job_meta_hash = 'twmq:' .. queue_id .. ':job:' .. j_id .. ':meta'
                        local errors_list_name = 'twmq:' .. queue_id .. ':job:' .. j_id .. ':errors'

                        redis.call('SREM', dedupe_set_name, j_id)
                        redis.call('HDEL', job_data_hash, j_id)
                        redis.call('DEL', job_meta_hash)
                        redis.call('HDEL', results_hash, j_id)
                        redis.call('DEL', errors_list_name)
                    end
                    redis.call('LTRIM', list_name, 0, max_len - 1)
                end
                return #job_ids_to_delete
            "#,
        );

        let trimmed_count: usize = trim_script
            .key(self.name())
            .key(self.success_list_name())
            .key(self.job_data_hash_name())
            .key(self.job_result_hash_name()) // results_hash
            .key(self.dedupe_set_name())
            .arg(self.options.max_success) // max_len (LTRIM is 0 to max_success-1)
            .invoke_async(&mut self.redis.clone())
            .await?;

        if trimmed_count > 0 {
            tracing::info!("Pruned {} successful jobs", trimmed_count);
        }

        Ok(())
    }

    fn add_nack_operations(
        &self,
        job: &BorrowedJob<H::JobData>,
        error: &H::ErrorData,
        delay: Option<Duration>,
        position: RequeuePosition,
        pipeline: &mut Pipeline,
    ) -> Result<(), TwmqError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let lease_key = self.lease_key_name(&job.job.id, &job.lease_token);

        // Delete the lease key to consume it
        pipeline.del(&lease_key);

        // Remove from active and clear lease token
        pipeline
            .hdel(self.active_hash_name(), &job.job.id)
            .hdel(self.job_meta_hash_name(&job.job.id), "lease_token");

        let error_record = JobErrorRecord {
            attempt: job.job.attempts,
            error,
            details: JobErrorType::nack(delay, position),
            created_at: now,
        };

        let error_json = serde_json::to_string(&error_record)?;
        pipeline.lpush(self.job_errors_list_name(&job.job.id), error_json);

        // Add to proper queue based on delay and position
        if let Some(delay_duration) = delay {
            let delay_until = now + delay_duration.as_secs();
            let pos_str = position.to_string();

            pipeline
                .hset(
                    self.job_meta_hash_name(&job.job.id),
                    "reentry_position",
                    pos_str,
                )
                .zadd(self.delayed_zset_name(), &job.job.id, delay_until);
        } else {
            match position {
                RequeuePosition::First => {
                    pipeline.lpush(self.pending_list_name(), &job.job.id);
                }
                RequeuePosition::Last => {
                    pipeline.rpush(self.pending_list_name(), &job.job.id);
                }
            }
        }

        Ok(())
    }

    async fn post_nack_completion(&self) -> Result<(), TwmqError> {
        // No pruning needed for nack
        Ok(())
    }

    fn add_fail_operations(
        &self,
        job: &BorrowedJob<H::JobData>,
        error: &H::ErrorData,
        pipeline: &mut Pipeline,
    ) -> Result<(), TwmqError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let lease_key = self.lease_key_name(&job.job.id, &job.lease_token);

        // Delete the lease key to consume it
        pipeline.del(&lease_key);

        // Remove from active, add to failed, clear lease token
        pipeline
            .hdel(self.active_hash_name(), &job.job.id)
            .lpush(self.failed_list_name(), &job.job.id)
            .hset(self.job_meta_hash_name(&job.job.id), "finished_at", now)
            .hdel(self.job_meta_hash_name(&job.job.id), "lease_token");

        // Store error
        let error_record = JobErrorRecord {
            attempt: job.job.attempts,
            error,
            details: JobErrorType::fail(),
            created_at: now,
        };
        let error_json = serde_json::to_string(&error_record)?;
        pipeline.lpush(self.job_errors_list_name(&job.job.id), error_json);

        // For "active" idempotency mode, remove from deduplication set immediately
        if self.options.idempotency_mode == queue::IdempotencyMode::Active {
            pipeline.srem(self.dedupe_set_name(), &job.job.id);
        }

        Ok(())
    }

    async fn post_fail_completion(&self) -> Result<(), TwmqError> {
        // Separate call for pruning with data deletion using Lua
        let trim_script = redis::Script::new(
            r#"
                local queue_id = KEYS[1]
                local list_name = KEYS[2]
                local job_data_hash = KEYS[3]
                local dedupe_set_name = KEYS[4]

                local max_len = tonumber(ARGV[1])

                local job_ids_to_delete = redis.call('LRANGE', list_name, max_len, -1)

                if #job_ids_to_delete > 0 then
                    for _, j_id in ipairs(job_ids_to_delete) do
                        local errors_list_name = 'twmq:' .. queue_id .. ':job:' .. j_id .. ':errors'
                        local job_meta_hash = 'twmq:' .. queue_id .. ':job:' .. j_id .. ':meta'

                        redis.call('SREM', dedupe_set_name, j_id)
                        redis.call('HDEL', job_data_hash, j_id)
                        redis.call('DEL', job_meta_hash)
                        redis.call('DEL', errors_list_name)
                    end
                    redis.call('LTRIM', list_name, 0, max_len - 1)
                end
                return #job_ids_to_delete
            "#,
        );

        let trimmed_count: usize = trim_script
            .key(self.name())
            .key(self.failed_list_name())
            .key(self.job_data_hash_name())
            .key(self.dedupe_set_name())
            .arg(self.options.max_failed)
            .invoke_async(&mut self.redis.clone())
            .await?;

        if trimmed_count > 0 {
            tracing::info!("Pruned {} failed jobs", trimmed_count);
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all, fields(job_id = job.id(), queue = self.name()))]
    async fn complete_job(
        &self,
        job: &BorrowedJob<H::JobData>,
        result: JobResult<H::Output, H::ErrorData>,
    ) -> Result<(), TwmqError> {
        // 1. Run hook once and build pipeline with all operations
        let mut hook_pipeline = redis::pipe();
        let mut tx_context = TransactionContext::new(&mut hook_pipeline, self.name().to_string());

        match &result {
            Ok(output) => {
                let success_hook_data = SuccessHookData { result: output };
                self.handler
                    .on_success(job, success_hook_data, &mut tx_context)
                    .await;
                self.add_success_operations(job, output, &mut hook_pipeline)?;
            }
            Err(JobError::Nack {
                error,
                delay,
                position,
            }) => {
                let nack_hook_data = NackHookData {
                    error,
                    delay: *delay,
                    position: *position,
                };
                self.handler
                    .on_nack(job, nack_hook_data, &mut tx_context)
                    .await;
                self.add_nack_operations(job, error, *delay, *position, &mut hook_pipeline)?;
            }
            Err(JobError::Fail(error)) => {
                let fail_hook_data = FailHookData { error };
                self.handler
                    .on_fail(job, fail_hook_data, &mut tx_context)
                    .await;
                self.add_fail_operations(job, error, &mut hook_pipeline)?;
            }
        }

        // 2. Now use this pipeline in unlimited retry loop with lease check
        let lease_key = self.lease_key_name(&job.job.id, &job.lease_token);

        loop {
            let mut conn = self.redis.clone();

            // WATCH the lease key
            redis::cmd("WATCH")
                .arg(&lease_key)
                .query_async::<()>(&mut conn)
                .await?;

            // Check if lease exists - if not, job was cancelled or timed out
            let lease_exists: bool = conn.exists(&lease_key).await?;
            if !lease_exists {
                redis::cmd("UNWATCH").query_async::<()>(&mut conn).await?;
                tracing::warn!(job_id = job.job.id, "Lease no longer exists, job was cancelled or timed out");
                return Ok(());
            }

            // Clone the pipeline and make it atomic for this attempt
            let mut atomic_pipeline = hook_pipeline.clone();
            atomic_pipeline.atomic();

            // Execute atomically with WATCH/MULTI/EXEC
            match atomic_pipeline
                .query_async::<Vec<redis::Value>>(&mut conn)
                .await
            {
                Ok(_) => {
                    // Success! Now run post-completion methods
                    match &result {
                        Ok(_) => self.post_success_completion().await?,
                        Err(JobError::Nack { .. }) => self.post_nack_completion().await?,
                        Err(JobError::Fail(_)) => self.post_fail_completion().await?,
                    }

                    tracing::debug!(job_id = job.job.id, "Job completion successful");
                    return Ok(());
                }
                Err(_) => {
                    // WATCH failed (lease key changed), retry
                    tracing::debug!(job_id = job.job.id, "WATCH failed during completion, retrying");
                    continue;
                }
            }
        }
    }

    // Special completion method for queue errors (deserialization failures) with lease token
    #[tracing::instrument(level = "debug", skip_all, fields(job_id = job.id, queue = self.name()))]
    async fn complete_job_queue_error(
        &self,
        job: &Job<Option<H::JobData>>,
        lease_token: &str,
        error: &H::ErrorData,
    ) -> Result<(), TwmqError> {
        // 1. Run queue error hook once and build pipeline
        let mut hook_pipeline = redis::pipe();
        let mut tx_context = TransactionContext::new(&mut hook_pipeline, self.name().to_string());

        let twmq_error = TwmqError::Runtime {
            message: "Job processing failed with user error".to_string(),
        };
        let queue_error_hook_data = QueueInternalErrorHookData { error: &twmq_error };
        self.handler
            .on_queue_error(job, queue_error_hook_data, &mut tx_context)
            .await;

        // Add fail operations to pipeline
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let lease_key = self.lease_key_name(&job.id, lease_token);

        // Delete the lease key to consume it
        hook_pipeline.del(&lease_key);

        // Remove from active, add to failed, clear lease token
        hook_pipeline
            .hdel(self.active_hash_name(), &job.id)
            .lpush(self.failed_list_name(), &job.id)
            .hset(self.job_meta_hash_name(&job.id), "finished_at", now)
            .hdel(self.job_meta_hash_name(&job.id), "lease_token");

        // Store error
        let error_record = JobErrorRecord {
            attempt: job.attempts,
            error,
            details: JobErrorType::fail(),
            created_at: now,
        };
        let error_json = serde_json::to_string(&error_record)?;
        hook_pipeline.lpush(self.job_errors_list_name(&job.id), error_json);

        // For "active" idempotency mode, remove from deduplication set immediately
        if self.options.idempotency_mode == queue::IdempotencyMode::Active {
            hook_pipeline.srem(self.dedupe_set_name(), &job.id);
        }

        // 2. Use pipeline in unlimited retry loop with lease check
        loop {
            let mut conn = self.redis.clone();

            // WATCH the lease key
            redis::cmd("WATCH")
                .arg(&lease_key)
                .query_async::<()>(&mut conn)
                .await?;

            // Check if lease exists - if not, job was cancelled or timed out
            let lease_exists: bool = conn.exists(&lease_key).await?;
            if !lease_exists {
                redis::cmd("UNWATCH").query_async::<()>(&mut conn).await?;
                tracing::warn!(job_id = job.id, "Lease no longer exists, job was cancelled or timed out");
                return Ok(());
            }

            // Clone the pipeline and make it atomic for this attempt
            let mut atomic_pipeline = hook_pipeline.clone();
            atomic_pipeline.atomic();

            // Execute atomically with WATCH/MULTI/EXEC
            match atomic_pipeline
                .query_async::<Vec<redis::Value>>(&mut conn)
                .await
            {
                Ok(_) => {
                    // Success! Run post-completion
                    self.post_fail_completion().await?;
                    tracing::debug!(job_id = job.id, "Queue error job completion successful");
                    return Ok(());
                }
                Err(_) => {
                    // WATCH failed (lease key changed), retry
                    tracing::debug!(job_id = job.id, "WATCH failed during queue error completion, retrying");
                    continue;
                }
            }
        }
    }

    pub async fn remove_from_dedupe_set(&self, job_id: &str) -> Result<(), TwmqError> {
        self.redis
            .clone()
            .srem::<&str, &str, ()>(&self.dedupe_set_name(), job_id)
            .await?;
        Ok(())
    }

    pub async fn empty_dedupe_set(&self) -> Result<(), TwmqError> {
        self.redis
            .clone()
            .del::<&str, ()>(&self.dedupe_set_name())
            .await?;
        Ok(())
    }
}
