#[derive(thiserror::Error, Debug)]
pub enum TwmqError {
    #[error("Redis error: {0}")]
    RedisError(#[from] redis::RedisError),

    #[error("JSON Serialization error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Runtime error: {0}")]
    Runtime(String),

    #[error("Worker panic: {0}")]
    WorkerPanic(String),
}
