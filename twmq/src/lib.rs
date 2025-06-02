pub mod error;
pub mod hooks;
pub mod job;
pub mod queue;
pub mod shutdown;

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use error::TwmqError;
use hooks::TransactionContext;
use job::{
    DelayOptions, Job, JobError, JobErrorRecord, JobErrorType, JobOptions, JobResult, JobStatus,
    PushableJob, RequeuePosition,
};
use queue::QueueOptions;
use redis::Pipeline;
use redis::{AsyncCommands, RedisResult, aio::ConnectionManager};
use serde::{Serialize, de::DeserializeOwned};
use shutdown::WorkerHandle;
use tokio::sync::Semaphore;
use tokio::time::sleep;

pub use redis;
use tracing::Instrument;

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
    type ErrorData: Serialize + DeserializeOwned + From<TwmqError> + Send + Sync;
    type JobData: Serialize + DeserializeOwned + Clone + Send + Sync + 'static;

    // Required method to process a job
    fn process(
        &self,
        job: &Job<Self::JobData>,
    ) -> impl Future<Output = JobResult<Self::Output, Self::ErrorData>> + Send;

    fn on_success(
        &self,
        _job: &Job<Self::JobData>,
        _d: SuccessHookData<Self::Output>,
        _tx: &mut TransactionContext<'_>,
    ) -> impl Future<Output = ()> + Send + Sync {
        std::future::ready(())
    }

    fn on_nack(
        &self,
        _job: &Job<Self::JobData>,
        _d: NackHookData<Self::ErrorData>,
        _tx: &mut TransactionContext<'_>,
    ) -> impl Future<Output = ()> + Send + Sync {
        std::future::ready(())
    }

    fn on_fail(
        &self,
        _job: &Job<Self::JobData>,
        _d: FailHookData<Self::ErrorData>,
        _tx: &mut TransactionContext<'_>,
    ) -> impl Future<Output = ()> + Send + Sync {
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
    ) -> impl Future<Output = ()> + Send + Sync {
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

    pub fn work(self: &Arc<Self>) -> WorkerHandle<H> {
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
                                    let job_id = job.id.clone();
                                    let handler_clone = handler_clone.clone();

                                    tokio::spawn(async move {
                                        // Process job - note we don't pass a context here
                                        let result = handler_clone.process(&job).await;

                                        // Mark job as complete (automatically happens in completion handlers)

                                        match result {
                                            Ok(output) => {
                                                // Create transaction pipeline for atomicity
                                                let mut pipeline = redis::pipe();
                                                pipeline.atomic(); // Use MULTI/EXEC

                                                // Create transaction context with mutable access to pipeline
                                                let mut tx_context = TransactionContext::new(
                                                    &mut pipeline,
                                                    queue_clone.name().to_string(),
                                                );

                                                let success_hook_data = SuccessHookData {
                                                    result: &output,
                                                };

                                                // Call success hook to populate transaction context
                                                handler_clone.on_success(&job, success_hook_data, &mut tx_context).await;

                                                // Complete job with success and execute transaction
                                                if let Err(e) = queue_clone
                                                    .complete_job_success(
                                                        &job,
                                                        &output,
                                                        tx_context.pipeline(),
                                                    )
                                                    .await
                                                {
                                                    tracing::error!(
                                                        "Failed to complete job {} complete handling successfully: {:?}",
                                                        job.id,
                                                        e
                                                    );
                                                }
                                            }
                                            Err(JobError::Nack {
                                                error,
                                                delay,
                                                position,
                                            }) => {
                                                // Create transaction pipeline for atomicity
                                                let mut pipeline = redis::pipe();
                                                pipeline.atomic(); // Use MULTI/EXEC

                                                // Create transaction context with mutable access to pipeline
                                                let mut tx_context = TransactionContext::new(
                                                    &mut pipeline,
                                                    queue_clone.name().to_string(),
                                                );

                                                let nack_hook_data = NackHookData {
                                                    error: &error,
                                                    delay,
                                                    position,
                                                };

                                                // Call nack hook to populate transaction context
                                                handler_clone
                                                    .on_nack(&job, nack_hook_data, &mut tx_context)
                                                    .await;

                                                // Complete job with nack and execute transaction
                                                if let Err(e) = queue_clone
                                                    .complete_job_nack(
                                                        &job,
                                                        &error,
                                                        delay,
                                                        position,
                                                        tx_context.pipeline(),
                                                    )
                                                    .await
                                                {
                                                    tracing::error!(
                                                        "Failed to complete job {} complete nack handling successfully: {:?}",
                                                        job.id,
                                                        e
                                                    );
                                                }
                                            }
                                            Err(JobError::Fail(error)) => {
                                                // Create transaction pipeline for atomicity
                                                let mut pipeline = redis::pipe();
                                                pipeline.atomic(); // Use MULTI/EXEC

                                                // Create transaction context with mutable access to pipeline
                                                let mut tx_context = TransactionContext::new(
                                                    &mut pipeline,
                                                    queue_clone.name.clone(),
                                                );

                                                let fail_hook_data = FailHookData {
                                                    error: &error
                                                };

                                                // Call fail hook to populate transaction context
                                                handler_clone.on_fail(&job, fail_hook_data, &mut tx_context).await;

                                                // Complete job with fail and execute transaction
                                                if let Err(e) = queue_clone
                                                    .complete_job_fail(&job.to_option_data(), &error, tx_context.pipeline())
                                                    .await
                                                {
                                                    tracing::error!(
                                                        "Failed to complete job {} complete fail handling successfully: {:?}",
                                                        job.id,
                                                        e
                                                    );
                                                }
                                            }
                                        }

                                        // Release permit when done
                                        drop(permit);
                                    }.instrument(tracing::info_span!("twmq_worker", job_id)));
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
                .map_err(|e| {
                    TwmqError::Runtime(format!("Failed to acquire permits during shutdown: {}", e))
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
    ) -> RedisResult<Vec<Job<H::JobData>>> {
        // Lua script that does:
        // 1. Process expired delayed jobs
        // 2. Check for timed out active jobs
        // 3. Pop up to batch_size jobs from pending
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


            local result_jobs = {}

            local timed_out_jobs = {}

            -- Step 1: Clean up all expired leases
            -- Get all active jobs
            local active_jobs = redis.call('HGETALL', active_hash_name)

            -- Process in pairs (job_id, lease_expiry)
            for i = 1, #active_jobs, 2 do
                local job_id = active_jobs[i]
                local lease_expiry = tonumber(active_jobs[i + 1])
                local job_meta_hash_name = 'twmq:' .. queue_id .. ':job:' .. job_id .. ':meta'

                -- Check if lease has expired
                if lease_expiry < now then
                    redis.call('HINCRBY', job_meta_hash_name, 'attempts', 1)

                    -- Move job back to pending
                    redis.call('HDEL', active_hash_name, job_id)
                    redis.call('LPUSH', pending_list_name, job_id)

                    -- Add to list of timed out jobs
                    table.insert(timed_out_jobs, job_id)
                end
            end

            -- Step 2: Move expired delayed jobs to pending
            local delayed_jobs = redis.call('ZRANGEBYSCORE', delayed_zset_name, 0, now)
            for i, job_id in ipairs(delayed_jobs) do
                -- Check position information

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

            -- Finally Step 3: Try to pop jobs from pending (up to batch_size)
            -- Try to pop jobs from pending (up to batch_size)
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

                    -- Set lease expiration
                    local lease_expiry = now + lease_seconds

                    -- Add to active set with lease expiry as score
                    redis.call('HSET', active_hash_name, job_id, lease_expiry)

                    -- Add to result with both id and data
                    table.insert(result_jobs, {job_id, job_data, tostring(attempts), tostring(created_at), tostring(now)})
                end
            end

            return result_jobs
            "#,
        );

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let results_from_lua: Vec<(String, String, String, String, String)> = script
            .key(self.name())
            .key(self.delayed_zset_name())
            .key(self.pending_list_name())
            .key(self.active_hash_name())
            .key(self.job_data_hash_name())
            .arg(now)
            .arg(batch_size)
            .arg(self.options.lease_duration.as_secs())
            .invoke_async(&mut self.redis.clone())
            .await?;

        let mut jobs = Vec::new();
        for (job_id_str, job_data_t_json, attempts_str, created_at_str, processed_at_str) in
            results_from_lua
        {
            match serde_json::from_str::<H::JobData>(&job_data_t_json) {
                Ok(data_t) => {
                    let attempts: u32 = attempts_str.parse().unwrap_or(1); // Default or handle error
                    let created_at: u64 = created_at_str.parse().unwrap_or(now); // Default or handle error
                    let processed_at: u64 = processed_at_str.parse().unwrap_or(now); // Default or handle error

                    jobs.push(Job {
                        id: job_id_str,
                        data: data_t,
                        attempts,
                        created_at,
                        processed_at: Some(processed_at),
                        finished_at: None, // Not finished yet
                    });
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

                        let mut tx_context =
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
                        let fail_hook_data = QueueInternalErrorHookData { error: &twmq_error };

                        // Call fail hook to populate transaction context
                        queue_clone
                            .handler
                            .on_queue_error(&job, fail_hook_data, &mut tx_context)
                            .await;

                        // Complete job with fail and execute transaction
                        if let Err(e) = queue_clone
                            .complete_job_fail(&job, &twmq_error.into(), tx_context.pipeline())
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

        Ok(jobs)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(job_id = job.id, queue = self.name()))]
    async fn complete_job_success(
        &self,
        job: &Job<H::JobData>,
        result: &H::Output,
        pipeline: &mut Pipeline,
    ) -> Result<(), TwmqError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Add basic job completion operations to pipeline
        pipeline
            .hdel(self.active_hash_name(), &job.id)
            .lpush(self.success_list_name(), &job.id)
            // Set finished_at in the job's metadata hash
            .hset(self.job_meta_hash_name(&job.id), "finished_at", now);

        let result_json = serde_json::to_string(result)?;
        pipeline.hset(self.job_result_hash_name(), &job.id, result_json);

        // Execute main pipeline first
        pipeline.query_async::<()>(&mut self.redis.clone()).await?;

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

    #[tracing::instrument(level = "debug", skip_all, fields(job_id = job.id, queue = self.name()))]
    async fn complete_job_nack(
        &self,
        job: &Job<H::JobData>,
        error: &H::ErrorData,
        delay: Option<Duration>,
        position: RequeuePosition,
        pipeline: &mut Pipeline,
    ) -> Result<(), TwmqError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Remove from active
        pipeline.hdel(self.active_hash_name(), &job.id);

        let error_record = JobErrorRecord {
            attempt: job.attempts,
            error,
            details: JobErrorType::nack(delay, position),
            created_at: now,
        };

        let error_json = serde_json::to_string(&error_record)?;

        pipeline.lpush(self.job_errors_list_name(&job.id), error_json);

        // Add to proper queue based on delay and position
        if let Some(delay_duration) = delay {
            let delay_until = now + delay_duration.as_secs();

            // Store position for when delay expires
            let pos_str = position.to_string();

            pipeline
                .hset(
                    self.job_meta_hash_name(&job.id),
                    "reentry_position",
                    pos_str,
                )
                .zadd(self.delayed_zset_name(), &job.id, delay_until);
        } else {
            match position {
                RequeuePosition::First => {
                    pipeline.lpush(self.pending_list_name(), &job.id);
                }
                RequeuePosition::Last => {
                    pipeline.rpush(self.pending_list_name(), &job.id);
                }
            }
        }

        // Execute pipeline
        pipeline.query_async::<()>(&mut self.redis.clone()).await?;

        tracing::debug!("Completed job nack handling");

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all, fields(job_id = job.id, queue = self.name()))]
    async fn complete_job_fail(
        &self,
        job: &Job<Option<H::JobData>>,
        error: &H::ErrorData,
        pipeline: &mut Pipeline,
    ) -> Result<(), TwmqError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Remove from active, add to failed
        pipeline
            .hdel(self.active_hash_name(), &job.id)
            .lpush(self.failed_list_name(), &job.id)
            // Set finished_at in the job's metadata hash
            .hset(self.job_meta_hash_name(&job.id), "finished_at", now);

        // Store error
        let error_record = JobErrorRecord {
            attempt: job.attempts,
            error,
            details: JobErrorType::fail(),
            created_at: now,
        };
        let error_json = serde_json::to_string(&error_record)?;

        pipeline.lpush(self.job_errors_list_name(&job.id), error_json);
        pipeline.query_async::<()>(&mut self.redis.clone()).await?;

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
            .arg(self.options.max_failed) // max_len (LTRIM is 0 to max_failed-1)
            .invoke_async(&mut self.redis.clone())
            .await?;

        tracing::debug!("completed job fail handling");
        if trimmed_count > 0 {
            tracing::info!("Pruned {} failed jobs", trimmed_count);
        }

        Ok(())
    }
}
