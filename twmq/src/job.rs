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
pub type JobResult<T, E> = Result<T, JobError<E>>;

pub enum JobError<E> {
    Nack {
        error: E,
        delay: Option<Duration>,
        position: RequeuePosition,
    },
    Fail(E),
}

pub trait ToJobResult<T, E> {
    fn map_err_nack(self, delay: Option<Duration>, position: RequeuePosition) -> JobResult<T, E>;
    fn map_err_fail(self) -> JobResult<T, E>;
}

impl<T, E> ToJobResult<T, E> for Result<T, E> {
    fn map_err_nack(self, delay: Option<Duration>, position: RequeuePosition) -> JobResult<T, E> {
        self.map_err(|e| JobError::Nack {
            error: e,
            delay,
            position,
        })
    }

    fn map_err_fail(self) -> JobResult<T, E> {
        self.map_err(|e| JobError::Fail(e))
    }
}

pub trait ToJobError<E> {
    fn nack(self, delay: Option<Duration>, position: RequeuePosition) -> JobError<E>;
    fn fail(self) -> JobError<E>;
}

impl<E> ToJobError<E> for E {
    fn nack(self, delay: Option<Duration>, position: RequeuePosition) -> JobError<E> {
        JobError::Nack {
            error: self,
            delay,
            position,
        }
    }

    fn fail(self) -> JobError<E> {
        JobError::Fail(self)
    }
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
pub struct Job<T: Clone> {
    pub id: String,
    pub data: T,
    pub attempts: u32,
    pub created_at: u64,
    pub processed_at: Option<u64>,
    pub finished_at: Option<u64>,
}

impl<T: Clone> Job<T> {
    pub fn to_option_data(&self) -> Job<Option<T>> {
        Job {
            id: self.id.clone(),
            data: Some(self.data.clone()),
            attempts: self.attempts,
            created_at: self.created_at,
            processed_at: self.processed_at,
            finished_at: self.finished_at,
        }
    }
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

pub struct PushableJob<H: DurableExecution> {
    pub options: JobOptions<H::JobData>,
    pub queue: Arc<Queue<H>>,
}

impl<H: DurableExecution> PushableJob<H> {
    pub async fn push(self) -> Result<Job<H::JobData>, TwmqError> {
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
