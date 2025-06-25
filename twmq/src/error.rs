use serde::{Deserialize, Serialize};

#[derive(thiserror::Error, Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TwmqError {
    #[error("Redis error: {message}")]
    RedisError { message: String },

    #[error("JSON Serialization error: {message}")]
    JsonError { message: String },

    #[error("Runtime error: {message}")]
    Runtime { message: String },

    #[error("Worker panic: {message}")]
    WorkerPanic { message: String },
}

impl From<redis::RedisError> for TwmqError {
    fn from(error: redis::RedisError) -> Self {
        TwmqError::RedisError {
            message: error.to_string(),
        }
    }
}

impl From<serde_json::Error> for TwmqError {
    fn from(error: serde_json::Error) -> Self {
        TwmqError::JsonError {
            message: error.to_string(),
        }
    }
}
