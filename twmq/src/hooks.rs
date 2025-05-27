use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Serialize, de::DeserializeOwned};

use crate::{DurableExecution, error::TwmqError, job::JobBuilder};

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
    pub fn queue_job<T, R, E, C>(
        &mut self,
        job_builder: JobBuilder<T, R, E, C>,
    ) -> Result<&mut Self, TwmqError>
    where
        T: Serialize
            + DeserializeOwned
            + Send
            + Sync
            + DurableExecution<Output = R, ErrorData = E, ExecutionContext = C>
            + 'static,
        R: Serialize + DeserializeOwned + Send + Sync + 'static,
        E: Serialize + DeserializeOwned + Send + Sync + 'static,
        C: Send + Clone + Sync + 'static,
    {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let job_data = serde_json::to_string(&job_builder.options.data)?;

        // Add job to the target queue (could be different from current queue!)
        self.pipeline
            .hset(
                job_builder.queue.job_data_hash_name(),
                &job_builder.options.id,
                job_data,
            )
            .hset(
                job_builder
                    .queue
                    .job_meta_hash_name(&job_builder.options.id),
                "created_at",
                now,
            )
            .hset(
                job_builder
                    .queue
                    .job_meta_hash_name(&job_builder.options.id),
                "attempts",
                0,
            )
            .sadd(job_builder.queue.dedupe_set_name(), &job_builder.options.id);

        if let Some(delay_options) = job_builder.options.delay {
            let process_at = now + delay_options.delay.as_secs();
            self.pipeline
                .hset(
                    job_builder
                        .queue
                        .job_meta_hash_name(&job_builder.options.id),
                    "reentry_position",
                    delay_options.position.to_string(),
                )
                .zadd(
                    job_builder.queue.delayed_zset_name(),
                    &job_builder.options.id,
                    process_at,
                );
        } else {
            self.pipeline.rpush(
                job_builder.queue.pending_list_name(),
                &job_builder.options.id,
            );
        }

        Ok(self)
    }
}
