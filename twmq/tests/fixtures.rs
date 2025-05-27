// In your test file or test module

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};

// Assuming your library's root is `twmq` or you use `crate::` if in lib.rs
use twmq::DurableExecution;
use twmq::hooks::TransactionContext;
use twmq::job::{Job, JobResult, RequeuePosition};

// --- Test Job Definition ---

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TestJobPayload {
    pub message: String,
    pub id_to_check: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct TestJobOutput {
    pub reply: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TestJobErrorData {
    pub reason: String,
}

// Use a static AtomicBool to signal from the job process to the test
// In a real scenario, you'd check queue state or results in Redis.
pub static TEST_JOB_PROCESSED_SUCCESSFULLY: AtomicBool = AtomicBool::new(false);

// If DurableExecution is in the root of your crate (lib.rs)
// use crate::DurableExecution;

// If DurableExecution is in job.rs and job.rs is a module
// use crate::job::DurableExecution;

// If using async_trait:
// #[async_trait]
impl DurableExecution for TestJobPayload {
    type Output = TestJobOutput;
    type ErrorData = TestJobErrorData;
    type ExecutionContext = ();

    // If not using async_trait, the signature is:
    // fn process(&self) -> impl std::future::Future<Output = JobResult<Self::Output, Self::ErrorData>> + Send + Sync {
    async fn process(job: &Job<Self>) -> JobResult<Self::Output, Self::ErrorData> {
        println!(
            "TEST_JOB: Processing job with id_to_check: {}",
            job.data.id_to_check
        );
        // Simulate some work
        tokio::time::sleep(Duration::from_millis(50)).await;
        TEST_JOB_PROCESSED_SUCCESSFULLY.store(true, Ordering::SeqCst);
        JobResult::Success(TestJobOutput {
            reply: format!("Successfully processed '{}'", job.data.message),
        })
    }

    async fn on_success(&self, _result: &Self::Output, _tx: &mut TransactionContext<'_>, _: &()) {
        println!(
            "TEST_JOB: on_success hook for id_to_check: {}",
            self.id_to_check
        );
    }

    async fn on_nack(
        &self,
        _error: &Self::ErrorData,
        _delay: Option<Duration>,
        _position: RequeuePosition,
        _tx: &mut TransactionContext<'_>,
        _: &(),
    ) {
        // Not expected for this test
        println!(
            "TEST_JOB: on_nack hook for id_to_check: {}",
            self.id_to_check
        );
    }

    async fn on_fail(&self, _error: &Self::ErrorData, _tx: &mut TransactionContext<'_>, _: &()) {
        // Not expected for this test
        println!(
            "TEST_JOB: on_fail hook for id_to_check: {}",
            self.id_to_check
        );
    }

    async fn on_timeout(&self, _tx: &mut TransactionContext<'_>) {
        // Not expected for this test
        println!(
            "TEST_JOB: on_timeout hook for id_to_check: {}",
            self.id_to_check
        );
    }
}
