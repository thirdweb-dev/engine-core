use std::time::{SystemTime, UNIX_EPOCH};

use crate::{DurableExecution, error::TwmqError, job::PushableJob};

// A minimal transaction context that hooks can use
pub struct TransactionContext<'a> {
    // Redis pipeline for arbitrary commands
    pipeline: &'a mut redis::Pipeline,
    // Queue name for context
    queue_name: String,
}

impl<'a> TransactionContext<'a> {
    pub fn new(pipeline: &'a mut redis::Pipeline, queue_name: String) -> Self {
        Self {
            pipeline,
            queue_name,
        }
    }

    // Return pipeline for arbitrary commands
    pub fn pipeline(&mut self) -> &mut redis::Pipeline {
        self.pipeline
    }

    // Queue name accessor
    pub fn queue_name(&self) -> &str {
        &self.queue_name
    }

    // Helper for job scheduling (maintains type safety)
    pub fn queue_job<H: DurableExecution>(
        &mut self,
        job: PushableJob<H>,
    ) -> Result<&mut Self, TwmqError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let job_data = serde_json::to_string(&job.options.data)?;

        // Add job to the target queue (could be different from current queue!)
        self.pipeline
            .hset(job.queue.job_data_hash_name(), &job.options.id, job_data)
            .hset(
                job.queue.job_meta_hash_name(&job.options.id),
                "created_at",
                now,
            )
            .hset(job.queue.job_meta_hash_name(&job.options.id), "attempts", 0)
            .sadd(job.queue.dedupe_set_name(), &job.options.id);

        if let Some(delay_options) = job.options.delay {
            let process_at = now + delay_options.delay.as_secs();
            self.pipeline
                .hset(
                    job.queue.job_meta_hash_name(&job.options.id),
                    "reentry_position",
                    delay_options.position.to_string(),
                )
                .zadd(job.queue.delayed_zset_name(), &job.options.id, process_at);
        } else {
            self.pipeline
                .rpush(job.queue.pending_list_name(), &job.options.id);
        }

        Ok(self)
    }
}
