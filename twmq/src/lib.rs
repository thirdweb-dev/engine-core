pub mod error;
pub mod hooks;
pub mod job;
pub mod queue;

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use error::TwmqError;
use hooks::TransactionContext;
use job::{
    DelayOptions, Job, JobBuilder, JobErrorRecord, JobErrorType, JobOptions, JobResult, JobStatus,
    RequeuePosition,
};
use queue::QueueOptions;
use redis::Pipeline;
use redis::{AsyncCommands, RedisResult, aio::ConnectionManager};
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::Semaphore;
use tokio::time::sleep;

pub use redis;
use tracing::Instrument;

// Main DurableExecution trait
pub trait DurableExecution: Sized {
    type Output: Serialize + DeserializeOwned + Send + Sync;
    type ErrorData: Serialize + DeserializeOwned + Send + Sync;
    type ExecutionContext: Send + Sync + 'static;

    // Required method to process a job
    fn process(
        job: &Job<Self>,
    ) -> impl Future<Output = JobResult<Self::Output, Self::ErrorData>> + Send + Sync;

    fn on_success(
        &self,
        result: &Self::Output,
        tx: &mut TransactionContext<'_>,
        c: &Self::ExecutionContext,
    ) -> impl Future<Output = ()> + Send + Sync;

    fn on_nack(
        &self,
        error: &Self::ErrorData,
        delay: Option<Duration>,
        position: RequeuePosition,
        tx: &mut TransactionContext<'_>,
        c: &Self::ExecutionContext,
    ) -> impl Future<Output = ()> + Send + Sync;

    fn on_fail(
        &self,
        error: &Self::ErrorData,
        tx: &mut TransactionContext<'_>,
        c: &Self::ExecutionContext,
    ) -> impl Future<Output = ()> + Send + Sync;

    fn on_timeout(&self, tx: &mut TransactionContext<'_>)
    -> impl Future<Output = ()> + Send + Sync;
}

// Main Queue struct
pub struct Queue<T, R, E, C>
where
    T: Serialize
        + DeserializeOwned
        + Send
        + Sync
        + 'static
        + DurableExecution<ExecutionContext = C, Output = R, ErrorData = E>,
    R: Serialize + DeserializeOwned + Send + Sync + 'static,
    E: Serialize + DeserializeOwned + Send + Sync + 'static,
    C: Send + Sync + 'static,
{
    pub redis: ConnectionManager,
    options: QueueOptions,
    // concurrency: usize,
    name: String,
    execution_context: C,
    _phantom: PhantomData<(T, R, E)>,
}

impl<T, R, E, C> Queue<T, R, E, C>
where
    C: Send + Sync + Clone + 'static,
    T: Serialize
        + DeserializeOwned
        + Send
        + Sync
        + DurableExecution<Output = R, ErrorData = E, ExecutionContext = C>
        + 'static,
    R: Serialize + DeserializeOwned + Send + Sync + 'static,
    E: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub async fn new(
        redis_url: &str,
        name: &str,
        // concurrency: usize,
        options: Option<QueueOptions>,
        context: C,
    ) -> Result<Self, TwmqError> {
        let client = redis::Client::open(redis_url)?;
        let redis = client.get_connection_manager().await?;

        let queue = Self {
            redis,
            name: name.to_string(),
            // concurrency,
            options: options.unwrap_or_default(),
            execution_context: context,
            _phantom: PhantomData,
        };

        Ok(queue)
    }

    pub fn job(self: Arc<Self>, data: T) -> JobBuilder<T, R, E, C> {
        JobBuilder {
            options: JobOptions::new(data),
            queue: self,
        }
    }

    // Get queue name
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn pending_list_name(&self) -> String {
        format!("{}:pending", self.name())
    }

    pub fn active_hash_name(&self) -> String {
        format!("{}:active", self.name)
    }

    pub fn delayed_zset_name(&self) -> String {
        format!("{}:delayed", self.name)
    }

    pub fn success_list_name(&self) -> String {
        format!("{}:success", self.name)
    }

    pub fn failed_list_name(&self) -> String {
        format!("{}:failed", self.name)
    }

    pub fn job_data_hash_name(&self) -> String {
        format!("{}:jobs:data", self.name)
    }

    pub fn job_meta_hash_name(&self, job_id: &str) -> String {
        format!("{}:job:{}:meta", self.name, job_id)
    }

    pub fn job_errors_list_name(&self, job_id: &str) -> String {
        format!("{}:job:{}:errors", self.name, job_id)
    }

    pub fn job_result_hash_name(&self) -> String {
        format!("{}:jobs:result", self.name)
    }

    pub fn dedupe_set_name(&self) -> String {
        format!("{}:dedup", self.name)
    }

    pub async fn push(&self, job_options: JobOptions<T>) -> Result<Job<T>, TwmqError> {
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

    pub async fn get_job(&self, job_id: &str) -> Result<Option<Job<T>>, TwmqError> {
        let mut conn = self.redis.clone();
        let job_data_t_json: Option<String> = conn.hget(self.job_data_hash_name(), job_id).await?;

        if let Some(data_json) = job_data_t_json {
            let data_t: T = serde_json::from_str(&data_json)?;

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

    pub async fn work(self: Arc<Self>, local_concurrency: usize) -> Result<(), TwmqError> {
        // Local semaphore to limit concurrency per instance
        let semaphore = Arc::new(Semaphore::new(local_concurrency));

        // Start worker
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));

            loop {
                interval.tick().await;

                // Check available permits for batch size
                let available_permits = semaphore.available_permits();
                if available_permits == 0 && !self.options.always_poll {
                    tracing::debug!("No permits available, waiting...");
                    continue;
                }

                tracing::debug!("Available permits: {}", available_permits);
                // Try to get multiple jobs - as many as we have permits
                match self.pop_batch_jobs(available_permits).await {
                    Ok(jobs) => {
                        tracing::debug!("Got {} jobs", jobs.len());
                        for job in jobs {
                            let permit = semaphore.clone().acquire_owned().await.unwrap();
                            let queue_clone = self.clone();
                            let job_id = job.id.clone();
                            let execution_context = self.execution_context.clone();

                            tokio::spawn(async move {
                                // Process job - note we don't pass a context here
                                let result = DurableExecution::process(&job).await;

                                // Mark job as complete (automatically happens in completion handlers)

                                match result {
                                    JobResult::Success(output) => {
                                        // Create transaction pipeline for atomicity
                                        let mut pipeline = redis::pipe();
                                        pipeline.atomic(); // Use MULTI/EXEC

                                        // Create transaction context with mutable access to pipeline
                                        let mut tx_context = TransactionContext::new(
                                            &mut pipeline,
                                            queue_clone.name().to_string(),
                                        );

                                        // Call success hook to populate transaction context
                                        job.data.on_success(&output, &mut tx_context, &execution_context).await;

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
                                    JobResult::Nack {
                                        error,
                                        delay,
                                        position,
                                    } => {
                                        // Create transaction pipeline for atomicity
                                        let mut pipeline = redis::pipe();
                                        pipeline.atomic(); // Use MULTI/EXEC

                                        // Create transaction context with mutable access to pipeline
                                        let mut tx_context = TransactionContext::new(
                                            &mut pipeline,
                                            queue_clone.name().to_string(),
                                        );

                                        // Call nack hook to populate transaction context
                                        job.data
                                            .on_nack(&error, delay, position, &mut tx_context,&execution_context)
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
                                    JobResult::Fail(error) => {
                                        // Create transaction pipeline for atomicity
                                        let mut pipeline = redis::pipe();
                                        pipeline.atomic(); // Use MULTI/EXEC

                                        // Create transaction context with mutable access to pipeline
                                        let mut tx_context = TransactionContext::new(
                                            &mut pipeline,
                                            queue_clone.name.clone(),
                                        );

                                        // Call fail hook to populate transaction context
                                        job.data.on_fail(&error, &mut tx_context, &execution_context).await;

                                        // Complete job with fail and execute transaction
                                        if let Err(e) = queue_clone
                                            .complete_job_fail(&job, &error, tx_context.pipeline())
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
                        // No jobs found, wait a bit
                        tracing::error!("Failed to pop batch jobs: {:?}", e);
                        sleep(Duration::from_millis(1000)).await;
                    }
                };
            }
        });

        Ok(())
    }

    // Improved batch job popping - gets multiple jobs at once
    async fn pop_batch_jobs(&self, batch_size: usize) -> RedisResult<Vec<Job<T>>> {
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

            local job_data_hash_name = queue_id .. ':jobs:data'

            local result_jobs = {}

            local timed_out_jobs = {}

            -- Step 1: Clean up all expired leases
            -- Get all active jobs
            local active_jobs = redis.call('HGETALL', active_hash_name)

            -- Process in pairs (job_id, lease_expiry)
            for i = 1, #active_jobs, 2 do
                local job_id = active_jobs[i]
                local lease_expiry = tonumber(active_jobs[i + 1])
                local job_meta_hash_name = queue_id .. ':job:' .. job_id .. ':meta'

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

                local job_meta_hash_name = queue_id .. ':job:' .. job_id .. ':meta'
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
                    local job_meta_hash_name = queue_id .. ':job:' .. job_id .. ':meta'


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
            .arg(now)
            .arg(batch_size)
            .arg(self.options.lease_duration.as_secs())
            .invoke_async(&mut self.redis.clone())
            .await?;

        let mut jobs = Vec::new();
        for (job_id_str, job_data_t_json, attempts_str, created_at_str, processed_at_str) in
            results_from_lua
        {
            match serde_json::from_str::<T>(&job_data_t_json) {
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
                        "Failed to deserialize job data for job {}: {}",
                        job_id_str,
                        e
                    );
                    // TODO: Potentially move this job_id to a failed state or log for investigation
                }
            }
        }

        Ok(jobs)
    }

    async fn complete_job_success(
        &self,
        job: &Job<T>,
        result: &R,
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
                local max_len = tonumber(ARGV[1])

                local job_ids_to_delete = redis.call('LRANGE', list_name, max_len, -1)

                if #job_ids_to_delete > 0 then
                    for _, j_id in ipairs(job_ids_to_delete) do
                        local job_meta_hash = queue_id .. ':job:' .. j_id .. ':meta'
                        local errors_list_name = queue_id .. ':job:' .. j_id .. ':errors'

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

        let _trimmed_count: usize = trim_script
            .key(self.name())
            .key(self.success_list_name())
            .key(self.job_data_hash_name())
            .key(self.job_result_hash_name()) // results_hash
            .arg(self.options.max_success) // max_len (LTRIM is 0 to max_success-1)
            .invoke_async(&mut self.redis.clone())
            .await?;

        Ok(())
    }

    async fn complete_job_nack(
        &self,
        job: &Job<T>,
        error: &E,
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

        Ok(())
    }

    async fn complete_job_fail(
        &self,
        job: &Job<T>,
        error: &E,
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
                local max_len = tonumber(ARGV[1])


                local job_ids_to_delete = redis.call('LRANGE', list_name, max_len, -1)

                if #job_ids_to_delete > 0 then
                    for _, j_id in ipairs(job_ids_to_delete) do
                        local errors_list_name = queue_id .. ':job:' .. j_id .. ':errors'
                        local job_meta_hash = queue_id .. ':job:' .. j_id .. ':meta'

                        redis.call('HDEL', job_data_hash, j_id)
                        redis.call('DEL', job_meta_hash)
                        redis.call('DEL', errors_list_name)
                    end
                    redis.call('LTRIM', list_name, 0, max_len - 1)
                end
                return #job_ids_to_delete
            "#,
        );

        let _trimmed_count: usize = trim_script
            .key(self.name())
            .key(self.failed_list_name())
            .key(self.job_data_hash_name())
            .arg(self.options.max_failed) // max_len (LTRIM is 0 to max_failed-1)
            .invoke_async(&mut self.redis.clone())
            .await?;

        Ok(())
    }
}
