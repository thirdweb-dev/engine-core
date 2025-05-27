use crate::{DurableExecution, Queue, error::TwmqError};
use nanoid::nanoid;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{fmt::Display, sync::Arc, time::Duration};

// Position for nack operations
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum RequeuePosition {
    #[serde(rename = "first")]
    First,
    #[serde(rename = "last")]
    Last,
}

impl Display for RequeuePosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RequeuePosition::First => write!(f, "first"),
            RequeuePosition::Last => write!(f, "last"),
        }
    }
}

impl TryFrom<&str> for RequeuePosition {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "first" => Ok(RequeuePosition::First),
            "last" => Ok(RequeuePosition::Last),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct DelayOptions {
    pub delay: Duration,
    pub position: RequeuePosition,
}

// Job result type
pub enum JobResult<T, E> {
    Success(T),
    Nack {
        error: E,
        delay: Option<Duration>,
        position: RequeuePosition,
    },
    Fail(E),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequeueOptions {
    pub delay: Option<Duration>,
    pub position: RequeuePosition,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobErrorType {
    #[serde(rename = "nack")]
    Nack(RequeueOptions),
    #[serde(rename = "fail")]
    Fail,
}

impl JobErrorType {
    pub fn nack(delay: Option<Duration>, position: RequeuePosition) -> Self {
        Self::Nack(RequeueOptions { delay, position })
    }

    pub fn fail() -> Self {
        Self::Fail
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobErrorRecord<E> {
    pub error: E,
    pub attempt: u32,
    pub details: JobErrorType,
    pub created_at: u64,
}

// Job structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job<T> {
    pub id: String,
    pub data: T,
    pub attempts: u32,
    pub created_at: u64,
    pub processed_at: Option<u64>,
    pub finished_at: Option<u64>,
}

// Job status enum
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Pending,
    Active,
    Delayed,
    Success,
    Failed,
}

pub struct JobOptions<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub data: T,
    pub id: String,
    pub delay: Option<DelayOptions>,
}

impl<T> JobOptions<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn new(data: T) -> Self {
        Self {
            data,
            id: nanoid!(),
            delay: None,
        }
    }

    // Set custom ID (for deduplication)
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }

    // Set delay
    pub fn with_delay(mut self, delay: DelayOptions) -> Self {
        self.delay = Some(delay);
        self
    }
}

pub struct JobBuilder<T, R, E, C>
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
    pub options: JobOptions<T>,
    pub queue: Arc<Queue<T, R, E, C>>,
}

impl<T, R, E, C> JobBuilder<T, R, E, C>
where
    T: Serialize
        + DeserializeOwned
        + DurableExecution<Output = R, ErrorData = E, ExecutionContext = C>
        + Send
        + Sync
        + 'static,
    R: Serialize + DeserializeOwned + Send + Sync + 'static,
    E: Serialize + DeserializeOwned + Send + Sync + 'static,
    C: Send + Sync + Clone + 'static,
{
    pub async fn push(self) -> Result<Job<T>, TwmqError> {
        self.queue.push(self.options).await
    }

    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.options.id = id.into();
        self
    }

    pub fn with_delay(mut self, delay: DelayOptions) -> Self {
        self.options.delay = Some(delay);
        self
    }
}
