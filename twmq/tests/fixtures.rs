// In your test file or test module

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use twmq::error::TwmqError;
use twmq::hooks::TransactionContext;
use twmq::job::{BorrowedJob, JobResult};
use twmq::{DurableExecution, SuccessHookData, UserCancellable};

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

impl From<TwmqError> for TestJobErrorData {
    fn from(error: TwmqError) -> Self {
        TestJobErrorData {
            reason: error.to_string(),
        }
    }
}

impl UserCancellable for TestJobErrorData {
    fn user_cancelled() -> Self {
        TestJobErrorData {
            reason: "Transaction cancelled by user".to_string(),
        }
    }
}

// Use a static AtomicBool to signal from the job process to the test
// In a real scenario, you'd check queue state or results in Redis.
pub static TEST_JOB_PROCESSED_SUCCESSFULLY: AtomicBool = AtomicBool::new(false);

pub struct TestJobHandler;

impl DurableExecution for TestJobHandler {
    type Output = TestJobOutput;
    type ErrorData = TestJobErrorData;
    type JobData = TestJobPayload;

    // If not using async_trait, the signature is:
    // fn process(&self) -> impl std::future::Future<Output = JobResult<Self::Output, Self::ErrorData>> + Send + Sync {
    async fn process(
        &self,
        job: &BorrowedJob<Self::JobData>,
    ) -> JobResult<Self::Output, Self::ErrorData> {
        println!(
            "TEST_JOB: Processing job with id_to_check: {}",
            job.job.data.id_to_check
        );
        // Simulate some work
        tokio::time::sleep(Duration::from_millis(50)).await;
        TEST_JOB_PROCESSED_SUCCESSFULLY.store(true, Ordering::SeqCst);
        Ok(TestJobOutput {
            reply: format!("Successfully processed '{}'", job.job.data.message),
        })
    }

    async fn on_success(
        &self,
        job: &BorrowedJob<Self::JobData>,
        _d: SuccessHookData<'_, Self::Output>,
        _tx: &mut TransactionContext<'_>,
    ) {
        tracing::info!(
            "TEST_JOB: on_success hook for id_to_check: {}",
            job.job.data.id_to_check
        );
    }
}
