use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use redis::{AsyncCommands, Pipeline, RedisResult, aio::ConnectionManager};
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tracing::Instrument;

use crate::{
    CancelResult, DurableExecution, FailHookData, NackHookData, QueueInternalErrorHookData,
    SuccessHookData, UserCancellable,
    error::TwmqError,
    hooks::TransactionContext,
    job::{
        BorrowedJob, DelayOptions, Job, JobError, JobErrorRecord, JobErrorType, JobOptions,
        JobResult, JobStatus, RequeuePosition,
    },
    queue::QueueOptions,
    shutdown::WorkerHandle,
};

/// A multilane queue that provides fair load balancing across multiple lanes
/// while maintaining the same reliability guarantees as the single-lane queue.
pub struct MultilaneQueue<H>
where
    H: DurableExecution,
{
    pub redis: ConnectionManager,
    handler: Arc<H>,
    options: QueueOptions,
    /// Unique identifier for this multilane queue instance
    queue_id: String,
}

/// Represents a job that can be pushed to a specific lane
pub struct MultilanePushableJob<H>
where
    H: DurableExecution,
{
    options: JobOptions<H::JobData>,
    queue: Arc<MultilaneQueue<H>>,
    lane_id: String,
}

impl<H: DurableExecution> MultilaneQueue<H> {
    pub async fn new(
        redis_url: &str,
        queue_id: &str,
        options: Option<QueueOptions>,
        handler: H,
    ) -> Result<Self, TwmqError> {
        let client = redis::Client::open(redis_url)?;
        let redis = client.get_connection_manager().await?;

        let queue = Self {
            redis,
            queue_id: queue_id.to_string(),
            options: options.unwrap_or_default(),
            handler: Arc::new(handler),
        };

        Ok(queue)
    }

    pub fn arc(self) -> Arc<Self> {
        Arc::new(self)
    }

    /// Create a job for a specific lane
    pub fn job_for_lane(
        self: Arc<Self>,
        lane_id: &str,
        data: H::JobData,
    ) -> MultilanePushableJob<H> {
        MultilanePushableJob {
            options: JobOptions::new(data),
            queue: self,
            lane_id: lane_id.to_string(),
        }
    }

    pub fn queue_id(&self) -> &str {
        &self.queue_id
    }

    // Redis key naming methods with proper multilane namespacing
    pub fn lanes_zset_name(&self) -> String {
        format!("twmq_multilane:{}:lanes", self.queue_id)
    }

    pub fn lane_pending_list_name(&self, lane_id: &str) -> String {
        format!("twmq_multilane:{}:lane:{}:pending", self.queue_id, lane_id)
    }

    pub fn lane_delayed_zset_name(&self, lane_id: &str) -> String {
        format!("twmq_multilane:{}:lane:{}:delayed", self.queue_id, lane_id)
    }

    pub fn lane_active_hash_name(&self, lane_id: &str) -> String {
        format!("twmq_multilane:{}:lane:{}:active", self.queue_id, lane_id)
    }

    pub fn success_list_name(&self) -> String {
        format!("twmq_multilane:{}:success", self.queue_id)
    }

    pub fn failed_list_name(&self) -> String {
        format!("twmq_multilane:{}:failed", self.queue_id)
    }

    pub fn job_data_hash_name(&self) -> String {
        format!("twmq_multilane:{}:jobs:data", self.queue_id)
    }

    pub fn job_meta_hash_name(&self, job_id: &str) -> String {
        format!("twmq_multilane:{}:job:{}:meta", self.queue_id, job_id)
    }

    pub fn job_errors_list_name(&self, job_id: &str) -> String {
        format!("twmq_multilane:{}:job:{}:errors", self.queue_id, job_id)
    }

    pub fn job_result_hash_name(&self) -> String {
        format!("twmq_multilane:{}:jobs:result", self.queue_id)
    }

    pub fn dedupe_set_name(&self) -> String {
        format!("twmq_multilane:{}:dedup", self.queue_id)
    }

    pub fn pending_cancellation_set_name(&self) -> String {
        format!("twmq_multilane:{}:pending_cancellations", self.queue_id)
    }

    pub fn lease_key_name(&self, job_id: &str, lease_token: &str) -> String {
        format!(
            "twmq_multilane:{}:job:{}:lease:{}",
            self.queue_id, job_id, lease_token
        )
    }

    /// Push a job to a specific lane
    pub async fn push_to_lane(
        &self,
        lane_id: &str,
        job_options: JobOptions<H::JobData>,
    ) -> Result<Job<H::JobData>, TwmqError> {
        let script = redis::Script::new(
            r#"
            local queue_id = ARGV[1]
            local lane_id = ARGV[2]
            local job_id = ARGV[3]
            local job_data = ARGV[4]
            local now = ARGV[5]
            local delay = ARGV[6]
            local reentry_position = ARGV[7]  -- "first" or "last"

            local lanes_zset_name = KEYS[1]
            local lane_delayed_zset_name = KEYS[2]
            local lane_pending_list_name = KEYS[3]
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
            redis.call('HSET', job_meta_hash_name, 'lane_id', lane_id)

            -- Add to deduplication set
            redis.call('SADD', dedupe_set_name, job_id)

            -- Add lane to lanes zset if not exists (score 0 means never processed)
            redis.call('ZADD', lanes_zset_name, 'NX', 0, lane_id)

            -- Add to appropriate queue based on delay
            if tonumber(delay) > 0 then
                local process_at = now + tonumber(delay)
                -- Store position information for this delayed job
                redis.call('HSET', job_meta_hash_name, 'reentry_position', reentry_position)
                redis.call('ZADD', lane_delayed_zset_name, process_at, job_id)
            else
                -- Non-delayed job always goes to end of pending
                redis.call('RPUSH', lane_pending_list_name, job_id)
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
            .key(self.lanes_zset_name())
            .key(self.lane_delayed_zset_name(lane_id))
            .key(self.lane_pending_list_name(lane_id))
            .key(self.job_data_hash_name())
            .key(self.job_meta_hash_name(&job.id))
            .key(self.dedupe_set_name())
            .arg(&self.queue_id)
            .arg(lane_id)
            .arg(&job_options.id)
            .arg(job_data)
            .arg(now)
            .arg(delay_secs)
            .arg(position_string)
            .invoke_async(&mut self.redis.clone())
            .await?;

        Ok(job)
    }

    /// Get job by ID (works across all lanes)
    pub async fn get_job(&self, job_id: &str) -> Result<Option<Job<H::JobData>>, TwmqError> {
        let mut conn = self.redis.clone();
        let job_data_t_json: Option<String> = conn.hget(self.job_data_hash_name(), job_id).await?;

        if let Some(data_json) = job_data_t_json {
            let data_t: H::JobData = serde_json::from_str(&data_json)?;

            // Fetch metadata
            let meta_map: HashMap<String, String> =
                conn.hgetall(self.job_meta_hash_name(job_id)).await?;

            let attempts: u32 = meta_map
                .get("attempts")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let created_at: u64 = meta_map
                .get("created_at")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let processed_at: Option<u64> =
                meta_map.get("processed_at").and_then(|s| s.parse().ok());
            let finished_at: Option<u64> = meta_map.get("finished_at").and_then(|s| s.parse().ok());

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

    /// Count jobs by status across all lanes or for a specific lane
    pub async fn count(
        &self,
        status: JobStatus,
        lane_id: Option<&str>,
    ) -> Result<usize, TwmqError> {
        let mut conn = self.redis.clone();

        let count = match status {
            JobStatus::Pending => {
                if let Some(lane) = lane_id {
                    let count: usize = conn.llen(self.lane_pending_list_name(lane)).await?;
                    count
                } else {
                    // Sum across all active lanes
                    let lanes: Vec<String> = conn.zrange(self.lanes_zset_name(), 0, -1).await?;
                    let mut total = 0;
                    for lane in lanes {
                        let count: usize = conn.llen(self.lane_pending_list_name(&lane)).await?;
                        total += count;
                    }
                    total
                }
            }
            JobStatus::Active => {
                if let Some(lane) = lane_id {
                    let count: usize = conn.hlen(self.lane_active_hash_name(lane)).await?;
                    count
                } else {
                    // Sum across all active lanes
                    let lanes: Vec<String> = conn.zrange(self.lanes_zset_name(), 0, -1).await?;
                    let mut total = 0;
                    for lane in lanes {
                        let count: usize = conn.hlen(self.lane_active_hash_name(&lane)).await?;
                        total += count;
                    }
                    total
                }
            }
            JobStatus::Delayed => {
                if let Some(lane) = lane_id {
                    let count: usize = conn.zcard(self.lane_delayed_zset_name(lane)).await?;
                    count
                } else {
                    // Sum across all active lanes
                    let lanes: Vec<String> = conn.zrange(self.lanes_zset_name(), 0, -1).await?;
                    let mut total = 0;
                    for lane in lanes {
                        let count: usize = conn.zcard(self.lane_delayed_zset_name(&lane)).await?;
                        total += count;
                    }
                    total
                }
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

    pub async fn lanes_count(&self) -> Result<usize, TwmqError> {
        let mut conn = self.redis.clone();
        let count: usize = conn.zcard(self.lanes_zset_name()).await?;
        Ok(count)
    }

    /// Cancel a job by ID (works across all lanes)
    pub async fn cancel_job(&self, job_id: &str) -> Result<CancelResult, TwmqError> {
        let script = redis::Script::new(
            r#"
            local queue_id = ARGV[1]
            local job_id = ARGV[2]
            local now = ARGV[3]
            
            local lanes_zset = KEYS[1]
            local failed_list = KEYS[2]
            local pending_cancellation_set = KEYS[3]
            local job_meta_hash = KEYS[4]
            local job_data_hash = KEYS[5]
            
            -- Get the lane for this job
            local lane_id = redis.call('HGET', job_meta_hash, 'lane_id')
            if not lane_id then
                return "not_found"
            end
            
            local lane_pending_list = 'twmq_multilane:' .. queue_id .. ':lane:' .. lane_id .. ':pending'
            local lane_delayed_zset = 'twmq_multilane:' .. queue_id .. ':lane:' .. lane_id .. ':delayed'
            local lane_active_hash = 'twmq_multilane:' .. queue_id .. ':lane:' .. lane_id .. ':active'
            
            -- Try to remove from pending queue
            if redis.call('LREM', lane_pending_list, 0, job_id) > 0 then
                -- Move to failed state with cancellation
                redis.call('LPUSH', failed_list, job_id)
                redis.call('HSET', job_meta_hash, 'finished_at', now)
                return "cancelled_immediately"
            end
            
            -- Try to remove from delayed queue  
            if redis.call('ZREM', lane_delayed_zset, job_id) > 0 then
                -- Move to failed state with cancellation
                redis.call('LPUSH', failed_list, job_id)
                redis.call('HSET', job_meta_hash, 'finished_at', now)
                return "cancelled_immediately"
            end
            
            -- Check if job is active
            if redis.call('HEXISTS', lane_active_hash, job_id) == 1 then
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
            .key(self.lanes_zset_name())
            .key(self.failed_list_name())
            .key(self.pending_cancellation_set_name())
            .key(self.job_meta_hash_name(job_id))
            .key(self.job_data_hash_name())
            .arg(&self.queue_id)
            .arg(job_id)
            .arg(now)
            .invoke_async(&mut self.redis.clone())
            .await?;

        match result.as_str() {
            "cancelled_immediately" => {
                if let Err(e) = self.process_cancelled_job(job_id).await {
                    tracing::error!(
                        job_id = %job_id,
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

    /// Start the multilane worker
    pub fn work(self: &Arc<Self>) -> WorkerHandle<MultilaneQueue<H>> {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let semaphore = Arc::new(Semaphore::new(self.options.local_concurrency));
        let handler = self.handler.clone();
        let outer_queue_clone = self.clone();

        let join_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(outer_queue_clone.options.polling_interval);
            let handler_clone = handler.clone();
            let always_poll = outer_queue_clone.options.always_poll;

            tracing::info!(
                "Multilane worker started for queue: {}",
                outer_queue_clone.queue_id()
            );

            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => {
                        tracing::info!("Shutdown signal received for multilane queue: {}", outer_queue_clone.queue_id());
                        break;
                    }

                    _ = interval.tick() => {
                        let queue_clone = outer_queue_clone.clone();
                        let available_permits = semaphore.available_permits();

                        if available_permits == 0 && !always_poll {
                            tracing::trace!("No permits available, waiting...");
                            continue;
                        }

                        tracing::trace!("Available permits: {}", available_permits);

                        match queue_clone.pop_batch_jobs(available_permits).await {
                            Ok(jobs) => {
                                tracing::trace!("Got {} jobs across lanes", jobs.len());

                                for (lane_id, job) in jobs {
                                    let permit = semaphore.clone().acquire_owned().await.unwrap();
                                    let queue_clone = queue_clone.clone();
                                    let job_id = job.id().to_string();
                                    let handler_clone = handler_clone.clone();

                                    tokio::spawn(async move {
                                        let result = handler_clone.process(&job).await;

                                        if let Err(e) = queue_clone.complete_job(&job, result).await {
                                            tracing::error!(
                                                "Failed to complete job {} handling: {:?}",
                                                job.id(),
                                                e
                                            );
                                        }

                                        drop(permit);
                                    }.instrument(tracing::info_span!("twmq_multilane_worker", job_id, lane_id)));
                                }
                            }
                            Err(e) => {
                                tracing::error!("Failed to pop batch jobs: {:?}", e);
                                sleep(Duration::from_millis(1000)).await;
                            }
                        };
                    }
                }
            }

            // Graceful shutdown
            tracing::info!(
                "Waiting for {} active jobs to complete for multilane queue: {}",
                outer_queue_clone.options.local_concurrency - semaphore.available_permits(),
                outer_queue_clone.queue_id()
            );

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
                "All jobs completed, multilane worker shutdown complete for queue: {}",
                outer_queue_clone.queue_id()
            );
            Ok(())
        });

        WorkerHandle {
            join_handle,
            shutdown_tx,
            queue: self.clone(),
        }
    }

    /// Pop jobs from multiple lanes in a fair round-robin manner with full atomicity
    pub async fn pop_batch_jobs(
        self: &Arc<Self>,
        batch_size: usize,
    ) -> RedisResult<Vec<(String, BorrowedJob<H::JobData>)>> {
        let script = redis::Script::new(
            r#"
            local queue_id = ARGV[1]
            local now = tonumber(ARGV[2])
            local batch_size = tonumber(ARGV[3])
            local lease_seconds = tonumber(ARGV[4])

            local lanes_zset_name = KEYS[1]
            local job_data_hash_name = KEYS[2]
            local pending_cancellation_set = KEYS[3]
            local failed_list_name = KEYS[4]
            local success_list_name = KEYS[5]

            local result_jobs = {}
            local timed_out_jobs = {}
            local cancelled_jobs = {}

            -- Helper function to cleanup expired leases for a specific lane
            local function cleanup_lane_leases(lane_id)
                local lane_active_hash = 'twmq_multilane:' .. queue_id .. ':lane:' .. lane_id .. ':active'
                local lane_pending_list = 'twmq_multilane:' .. queue_id .. ':lane:' .. lane_id .. ':pending'
                
                local active_jobs = redis.call('HGETALL', lane_active_hash)
                
                for i = 1, #active_jobs, 2 do
                    local job_id = active_jobs[i]
                    local attempts = active_jobs[i + 1]
                    local job_meta_hash = 'twmq_multilane:' .. queue_id .. ':job:' .. job_id .. ':meta'
                    
                    local current_lease_token = redis.call('HGET', job_meta_hash, 'lease_token')
                    
                    if current_lease_token then
                        local lease_key = 'twmq_multilane:' .. queue_id .. ':job:' .. job_id .. ':lease:' .. current_lease_token
                        local lease_exists = redis.call('EXISTS', lease_key)
                        
                        if lease_exists == 0 then
                            redis.call('HINCRBY', job_meta_hash, 'attempts', 1)
                            redis.call('HDEL', job_meta_hash, 'lease_token')
                            redis.call('HDEL', lane_active_hash, job_id)
                            redis.call('LPUSH', lane_pending_list, job_id)
                            table.insert(timed_out_jobs, {lane_id, job_id})
                        end
                    else
                        redis.call('HINCRBY', job_meta_hash, 'attempts', 1)
                        redis.call('HDEL', lane_active_hash, job_id)
                        redis.call('LPUSH', lane_pending_list, job_id)
                        table.insert(timed_out_jobs, {lane_id, job_id})
                    end
                end
            end

            -- Helper function to move delayed jobs to pending for a specific lane
            local function process_delayed_jobs(lane_id)
                local lane_delayed_zset = 'twmq_multilane:' .. queue_id .. ':lane:' .. lane_id .. ':delayed'
                local lane_pending_list = 'twmq_multilane:' .. queue_id .. ':lane:' .. lane_id .. ':pending'
                
                local delayed_jobs = redis.call('ZRANGEBYSCORE', lane_delayed_zset, 0, now)
                for i, job_id in ipairs(delayed_jobs) do
                    local job_meta_hash = 'twmq_multilane:' .. queue_id .. ':job:' .. job_id .. ':meta'
                    local reentry_position = redis.call('HGET', job_meta_hash, 'reentry_position') or 'last'

                    redis.call('ZREM', lane_delayed_zset, job_id)
                    redis.call('HDEL', job_meta_hash, 'reentry_position')

                    if reentry_position == 'first' then
                        redis.call('LPUSH', lane_pending_list, job_id)
                    else
                        redis.call('RPUSH', lane_pending_list, job_id)
                    end
                end
            end

            -- Helper function to pop one job from a lane
            local function pop_job_from_lane(lane_id)
                local lane_pending_list = 'twmq_multilane:' .. queue_id .. ':lane:' .. lane_id .. ':pending'
                local lane_active_hash = 'twmq_multilane:' .. queue_id .. ':lane:' .. lane_id .. ':active'
                
                local job_id = redis.call('RPOP', lane_pending_list)
                if not job_id then
                    return nil
                end

                local job_data = redis.call('HGET', job_data_hash_name, job_id)
                if not job_data then
                    return nil
                end

                local job_meta_hash = 'twmq_multilane:' .. queue_id .. ':job:' .. job_id .. ':meta'
                redis.call('HSET', job_meta_hash, 'processed_at', now)
                local created_at = redis.call('HGET', job_meta_hash, 'created_at') or now
                local attempts = redis.call('HINCRBY', job_meta_hash, 'attempts', 1)

                local lease_token = now .. '_' .. job_id .. '_' .. attempts
                local lease_key = 'twmq_multilane:' .. queue_id .. ':job:' .. job_id .. ':lease:' .. lease_token
                
                redis.call('SET', lease_key, '1')
                redis.call('EXPIRE', lease_key, lease_seconds)
                redis.call('HSET', job_meta_hash, 'lease_token', lease_token)
                redis.call('HSET', lane_active_hash, job_id, attempts)

                return {job_id, job_data, tostring(attempts), tostring(created_at), tostring(now), lease_token}
            end

            -- Step 1: Process pending cancellations first
            local cancel_requests = redis.call('SMEMBERS', pending_cancellation_set)
            
            for i, job_id in ipairs(cancel_requests) do
                local job_meta_hash = 'twmq_multilane:' .. queue_id .. ':job:' .. job_id .. ':meta'
                local lane_id = redis.call('HGET', job_meta_hash, 'lane_id')
                
                if lane_id then
                    local lane_active_hash = 'twmq_multilane:' .. queue_id .. ':lane:' .. lane_id .. ':active'
                    
                    if redis.call('HEXISTS', lane_active_hash, job_id) == 1 then
                        -- Still processing, keep in cancellation set
                    else
                        -- Job finished processing, check outcome
                        local success_count = redis.call('LREM', success_list_name, 0, job_id)
                        if success_count > 0 then
                            -- Job succeeded, add it back to success list
                            redis.call('LPUSH', success_list_name, job_id)
                        else
                            redis.call('LPUSH', failed_list_name, job_id)
                            redis.call('HSET', job_meta_hash, 'finished_at', now)
                            table.insert(cancelled_jobs, {lane_id, job_id})
                        end
                        redis.call('SREM', pending_cancellation_set, job_id)
                    end
                end
            end

            -- Step 2: Efficient lane processing
            local jobs_popped = 0
            local lanes_with_scores = redis.call('ZRANGE', lanes_zset_name, 0, -1, 'WITHSCORES')
            local total_lanes = #lanes_with_scores / 2

            if total_lanes == 0 then
                return {result_jobs, cancelled_jobs, timed_out_jobs}
            end

            local lane_index = 1
            local empty_lanes_count = 0

            while jobs_popped < batch_size and empty_lanes_count < total_lanes do
                local lane_id = lanes_with_scores[lane_index * 2 - 1]
                
                -- Skip if we've already marked this lane as empty
                if lane_id == nil then
                    lane_index = lane_index + 1
                    if lane_index > total_lanes then
                        lane_index = 1
                    end
                else
                    local last_score = tonumber(lanes_with_scores[lane_index * 2])
                    
                    -- Only cleanup if not visited this batch (score != now)
                    if last_score ~= now then
                        cleanup_lane_leases(lane_id)
                        process_delayed_jobs(lane_id)
                        redis.call('ZADD', lanes_zset_name, now, lane_id)
                        lanes_with_scores[lane_index * 2] = tostring(now)
                    end
                    
                    -- Try to pop a job from this lane
                    local job_result = pop_job_from_lane(lane_id)
                    
                    if job_result then
                        table.insert(result_jobs, {lane_id, job_result[1], job_result[2], job_result[3], job_result[4], job_result[5], job_result[6]})
                        jobs_popped = jobs_popped + 1
                    else
                        -- Lane is empty, mark it and count it
                        lanes_with_scores[lane_index * 2 - 1] = nil
                        lanes_with_scores[lane_index * 2] = nil
                        empty_lanes_count = empty_lanes_count + 1
                        
                        -- Check if lane should be removed from Redis
                        local lane_pending_list = 'twmq_multilane:' .. queue_id .. ':lane:' .. lane_id .. ':pending'
                        local lane_delayed_zset = 'twmq_multilane:' .. queue_id .. ':lane:' .. lane_id .. ':delayed'
                        local lane_active_hash = 'twmq_multilane:' .. queue_id .. ':lane:' .. lane_id .. ':active'
                        
                        local pending_count = redis.call('LLEN', lane_pending_list)
                        local delayed_count = redis.call('ZCARD', lane_delayed_zset)
                        local active_count = redis.call('HLEN', lane_active_hash)
                        
                        if pending_count == 0 and delayed_count == 0 and active_count == 0 then
                            redis.call('ZREM', lanes_zset_name, lane_id)
                        end
                    end
                    
                    -- Move to next lane
                    lane_index = lane_index + 1
                    if lane_index > total_lanes then
                        lane_index = 1
                    end
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
            Vec<(String, String, String, String, String, String, String)>,
            Vec<(String, String)>,
            Vec<(String, String)>,
        ) = script
            .key(self.lanes_zset_name())
            .key(self.job_data_hash_name())
            .key(self.pending_cancellation_set_name())
            .key(self.failed_list_name())
            .key(self.success_list_name())
            .arg(&self.queue_id)
            .arg(now)
            .arg(batch_size)
            .arg(self.options.lease_duration.as_secs())
            .invoke_async(&mut self.redis.clone())
            .await?;

        let (job_results, cancelled_jobs, timed_out_jobs) = results_from_lua;

        // Log lease timeouts and cancellations with lane context
        for (lane_id, job_id) in &timed_out_jobs {
            tracing::warn!(job_id = %job_id, lane_id = %lane_id, "Job lease expired, moved back to pending");
        }
        for (lane_id, job_id) in &cancelled_jobs {
            tracing::info!(job_id = %job_id, lane_id = %lane_id, "Job cancelled by user request");
        }

        let mut jobs = Vec::new();
        for (
            lane_id,
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
                    let attempts: u32 = attempts_str.parse().unwrap_or(1);
                    let created_at: u64 = created_at_str.parse().unwrap_or(now);
                    let processed_at: u64 = processed_at_str.parse().unwrap_or(now);

                    let job = Job {
                        id: job_id_str,
                        data: data_t,
                        attempts,
                        created_at,
                        processed_at: Some(processed_at),
                        finished_at: None,
                    };

                    jobs.push((lane_id, BorrowedJob::new(job, lease_token)));
                }
                Err(e) => {
                    tracing::error!(
                        job_id = job_id_str,
                        lane_id = lane_id,
                        error = ?e,
                        "Failed to deserialize job data. Spawning task to move job to failed state.",
                    );

                    let queue_clone = self.clone();
                    tokio::spawn(async move {
                        let mut pipeline = redis::pipe();
                        pipeline.atomic();

                        let mut _tx_context = TransactionContext::new(
                            &mut pipeline,
                            queue_clone.queue_id().to_string(),
                        );

                        let job: Job<Option<H::JobData>> = Job {
                            id: job_id_str.to_string(),
                            data: None,
                            attempts: attempts_str.parse().unwrap_or(1),
                            created_at: created_at_str.parse().unwrap_or(now),
                            processed_at: processed_at_str.parse().ok(),
                            finished_at: Some(now),
                        };

                        let twmq_error: TwmqError = e.into();

                        if let Err(e) = queue_clone
                            .complete_job_queue_error(&job, &lease_token, &twmq_error.into())
                            .await
                        {
                            tracing::error!(
                                job_id = job.id,
                                lane_id = lane_id,
                                error = ?e,
                                "Failed to complete job fail handling successfully",
                            );
                        }
                    });
                }
            }
        }

        // Process cancelled jobs through hook system
        for (lane_id, job_id) in cancelled_jobs {
            let queue_clone = self.clone();
            tokio::spawn(async move {
                if let Err(e) = queue_clone.process_cancelled_job(&job_id).await {
                    tracing::error!(
                        job_id = %job_id,
                        lane_id = %lane_id,
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
        match self.get_job(job_id).await? {
            Some(job) => {
                let cancellation_error = H::ErrorData::user_cancelled();

                let mut pipeline = redis::pipe();
                pipeline.atomic();

                let mut tx_context =
                    TransactionContext::new(&mut pipeline, self.queue_id().to_string());

                let fail_hook_data = FailHookData {
                    error: &cancellation_error,
                };

                let borrowed_job = BorrowedJob::new(job, "cancelled".to_string());
                self.handler
                    .on_fail(&borrowed_job, fail_hook_data, &mut tx_context)
                    .await;

                pipeline.query_async::<()>(&mut self.redis.clone()).await?;

                tracing::info!(
                    job_id = %job_id,
                    "Successfully processed job cancellation hooks"
                );

                Ok(())
            }
            None => {
                tracing::warn!(
                    job_id = %job_id,
                    "Cancelled job not found when trying to process hooks"
                );
                Ok(())
            }
        }
    }

    // Job completion methods (same as single-lane queue but with multilane naming)
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

        pipeline.del(&lease_key);

        // Get lane_id from job metadata to remove from correct lane active hash
        let job_meta_hash = self.job_meta_hash_name(&job.job.id);

        // We need to get lane_id first, then remove from that lane's active hash
        // This requires a separate Redis call before the pipeline, but ensures atomicity within the pipeline
        pipeline
            .lpush(self.success_list_name(), &job.job.id)
            .hset(&job_meta_hash, "finished_at", now)
            .hdel(&job_meta_hash, "lease_token");

        let result_json = serde_json::to_string(result)?;
        pipeline.hset(self.job_result_hash_name(), &job.job.id, result_json);

        // For "active" idempotency mode, remove from deduplication set immediately
        if self.options.idempotency_mode == crate::queue::IdempotencyMode::Active {
            pipeline.srem(self.dedupe_set_name(), &job.job.id);
        }

        Ok(())
    }

    async fn post_success_completion(&self) -> Result<(), TwmqError> {
        let trim_script = redis::Script::new(
            r#"
                local queue_id = KEYS[1]
                local list_name = KEYS[2]
                local job_data_hash = KEYS[3]
                local results_hash = KEYS[4]
                local dedupe_set_name = KEYS[5]

                local max_len = tonumber(ARGV[1])

                local job_ids_to_delete = redis.call('LRANGE', list_name, max_len, -1)

                if #job_ids_to_delete > 0 then
                    for _, j_id in ipairs(job_ids_to_delete) do
                        local job_meta_hash = 'twmq_multilane:' .. queue_id .. ':job:' .. j_id .. ':meta'
                        local errors_list_name = 'twmq_multilane:' .. queue_id .. ':job:' .. j_id .. ':errors'

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
            .key(self.queue_id())
            .key(self.success_list_name())
            .key(self.job_data_hash_name())
            .key(self.job_result_hash_name())
            .key(self.dedupe_set_name())
            .arg(self.options.max_success)
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
        let job_meta_hash = self.job_meta_hash_name(&job.job.id);

        pipeline.del(&lease_key);
        pipeline.hdel(&job_meta_hash, "lease_token");

        let error_record = JobErrorRecord {
            attempt: job.job.attempts,
            error,
            details: JobErrorType::nack(delay, position),
            created_at: now,
        };

        let error_json = serde_json::to_string(&error_record)?;
        pipeline.lpush(self.job_errors_list_name(&job.job.id), error_json);

        // Note: The actual requeuing logic needs to be handled by a separate operation
        // since we need the lane_id from metadata. This will be done in the complete_job method.

        Ok(())
    }

    async fn post_nack_completion(&self) -> Result<(), TwmqError> {
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
        let job_meta_hash = self.job_meta_hash_name(&job.job.id);

        pipeline.del(&lease_key);
        pipeline
            .lpush(self.failed_list_name(), &job.job.id)
            .hset(&job_meta_hash, "finished_at", now)
            .hdel(&job_meta_hash, "lease_token");

        let error_record = JobErrorRecord {
            attempt: job.job.attempts,
            error,
            details: JobErrorType::fail(),
            created_at: now,
        };
        let error_json = serde_json::to_string(&error_record)?;
        pipeline.lpush(self.job_errors_list_name(&job.job.id), error_json);

        // For "active" idempotency mode, remove from deduplication set immediately
        if self.options.idempotency_mode == crate::queue::IdempotencyMode::Active {
            pipeline.srem(self.dedupe_set_name(), &job.job.id);
        }

        Ok(())
    }

    async fn post_fail_completion(&self) -> Result<(), TwmqError> {
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
                        local errors_list_name = 'twmq_multilane:' .. queue_id .. ':job:' .. j_id .. ':errors'
                        local job_meta_hash = 'twmq_multilane:' .. queue_id .. ':job:' .. j_id .. ':meta'

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
            .key(self.queue_id())
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

    #[tracing::instrument(level = "debug", skip_all, fields(job_id = job.id(), queue = self.queue_id()))]
    async fn complete_job(
        &self,
        job: &BorrowedJob<H::JobData>,
        result: JobResult<H::Output, H::ErrorData>,
    ) -> Result<(), TwmqError> {
        // First, we need to get the lane_id and remove from appropriate lane's active hash
        let mut conn = self.redis.clone();
        let lane_id: Option<String> = conn
            .hget(self.job_meta_hash_name(&job.job.id), "lane_id")
            .await?;

        let lane_id = lane_id.ok_or_else(|| TwmqError::Runtime {
            message: format!("Job {} missing lane_id in metadata", job.job.id),
        })?;

        // Build pipeline with hooks and operations
        let mut hook_pipeline = redis::pipe();
        let mut tx_context =
            TransactionContext::new(&mut hook_pipeline, self.queue_id().to_string());

        match &result {
            Ok(output) => {
                let success_hook_data = SuccessHookData { result: output };
                self.handler
                    .on_success(job, success_hook_data, &mut tx_context)
                    .await;
                self.add_success_operations(job, output, &mut hook_pipeline)?;
                // Remove from lane's active hash
                hook_pipeline.hdel(self.lane_active_hash_name(&lane_id), &job.job.id);
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

                // Remove from lane's active hash and requeue to appropriate lane queue
                hook_pipeline.hdel(self.lane_active_hash_name(&lane_id), &job.job.id);

                if let Some(delay_duration) = delay {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    let delay_until = now + delay_duration.as_secs();
                    let pos_str = position.to_string();

                    hook_pipeline
                        .hset(
                            self.job_meta_hash_name(&job.job.id),
                            "reentry_position",
                            pos_str,
                        )
                        .zadd(
                            self.lane_delayed_zset_name(&lane_id),
                            &job.job.id,
                            delay_until,
                        );
                } else {
                    match position {
                        RequeuePosition::First => {
                            hook_pipeline.lpush(self.lane_pending_list_name(&lane_id), &job.job.id);
                        }
                        RequeuePosition::Last => {
                            hook_pipeline.rpush(self.lane_pending_list_name(&lane_id), &job.job.id);
                        }
                    }
                }
            }
            Err(JobError::Fail(error)) => {
                let fail_hook_data = FailHookData { error };
                self.handler
                    .on_fail(job, fail_hook_data, &mut tx_context)
                    .await;
                self.add_fail_operations(job, error, &mut hook_pipeline)?;
                // Remove from lane's active hash
                hook_pipeline.hdel(self.lane_active_hash_name(&lane_id), &job.job.id);
            }
        }

        // Execute with lease protection (same pattern as single-lane queue)
        let lease_key = self.lease_key_name(&job.job.id, &job.lease_token);

        loop {
            let mut conn = self.redis.clone();

            redis::cmd("WATCH")
                .arg(&lease_key)
                .query_async::<()>(&mut conn)
                .await?;

            let lease_exists: bool = conn.exists(&lease_key).await?;
            if !lease_exists {
                redis::cmd("UNWATCH").query_async::<()>(&mut conn).await?;
                tracing::warn!(job_id = %job.job.id, "Lease no longer exists, job was cancelled or timed out");
                return Ok(());
            }

            let mut atomic_pipeline = hook_pipeline.clone();
            atomic_pipeline.atomic();

            match atomic_pipeline
                .query_async::<Vec<redis::Value>>(&mut conn)
                .await
            {
                Ok(_) => {
                    match &result {
                        Ok(_) => self.post_success_completion().await?,
                        Err(JobError::Nack { .. }) => self.post_nack_completion().await?,
                        Err(JobError::Fail(_)) => self.post_fail_completion().await?,
                    }

                    tracing::debug!(job_id = %job.job.id, lane_id = %lane_id, "Job completion successful");
                    return Ok(());
                }
                Err(_) => {
                    tracing::debug!(job_id = %job.job.id, "WATCH failed during completion, retrying");
                    continue;
                }
            }
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(job_id = job.id, queue = self.queue_id()))]
    async fn complete_job_queue_error(
        &self,
        job: &Job<Option<H::JobData>>,
        lease_token: &str,
        error: &H::ErrorData,
    ) -> Result<(), TwmqError> {
        // Get lane_id for proper cleanup
        let mut conn = self.redis.clone();
        let lane_id: Option<String> = conn
            .hget(self.job_meta_hash_name(&job.id), "lane_id")
            .await?;

        let lane_id = lane_id.unwrap_or_else(|| "unknown".to_string());

        let mut hook_pipeline = redis::pipe();
        let mut tx_context =
            TransactionContext::new(&mut hook_pipeline, self.queue_id().to_string());

        let twmq_error = TwmqError::Runtime {
            message: "Job processing failed with user error".to_string(),
        };
        let queue_error_hook_data = QueueInternalErrorHookData { error: &twmq_error };
        self.handler
            .on_queue_error(job, queue_error_hook_data, &mut tx_context)
            .await;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let lease_key = self.lease_key_name(&job.id, lease_token);
        let job_meta_hash = self.job_meta_hash_name(&job.id);

        hook_pipeline.del(&lease_key);
        hook_pipeline
            .hdel(self.lane_active_hash_name(&lane_id), &job.id)
            .lpush(self.failed_list_name(), &job.id)
            .hset(&job_meta_hash, "finished_at", now)
            .hdel(&job_meta_hash, "lease_token");

        let error_record = JobErrorRecord {
            attempt: job.attempts,
            error,
            details: JobErrorType::fail(),
            created_at: now,
        };
        let error_json = serde_json::to_string(&error_record)?;
        hook_pipeline.lpush(self.job_errors_list_name(&job.id), error_json);

        // For "active" idempotency mode, remove from deduplication set immediately
        if self.options.idempotency_mode == crate::queue::IdempotencyMode::Active {
            hook_pipeline.srem(self.dedupe_set_name(), &job.id);
        }

        // Execute with lease protection
        loop {
            let mut conn = self.redis.clone();

            redis::cmd("WATCH")
                .arg(&lease_key)
                .query_async::<()>(&mut conn)
                .await?;

            let lease_exists: bool = conn.exists(&lease_key).await?;
            if !lease_exists {
                redis::cmd("UNWATCH").query_async::<()>(&mut conn).await?;
                tracing::warn!(job_id = %job.id, "Lease no longer exists, job was cancelled or timed out");
                return Ok(());
            }

            let mut atomic_pipeline = hook_pipeline.clone();
            atomic_pipeline.atomic();

            match atomic_pipeline
                .query_async::<Vec<redis::Value>>(&mut conn)
                .await
            {
                Ok(_) => {
                    self.post_fail_completion().await?;
                    tracing::debug!(job_id = %job.id, lane_id = %lane_id, "Queue error job completion successful");
                    return Ok(());
                }
                Err(_) => {
                    tracing::debug!(job_id = %job.id, "WATCH failed during queue error completion, retrying");
                    continue;
                }
            }
        }
    }
}

impl<H: DurableExecution> MultilanePushableJob<H> {
    pub fn delay(mut self, delay: Duration) -> Self {
        self.options.delay = Some(DelayOptions {
            delay,
            position: RequeuePosition::Last,
        });
        self
    }

    pub fn delay_with_position(mut self, delay: Duration, position: RequeuePosition) -> Self {
        self.options.delay = Some(DelayOptions { delay, position });
        self
    }

    pub fn id(mut self, id: String) -> Self {
        self.options.id = id;
        self
    }

    pub async fn push(self) -> Result<Job<H::JobData>, TwmqError> {
        self.queue.push_to_lane(&self.lane_id, self.options).await
    }
}
