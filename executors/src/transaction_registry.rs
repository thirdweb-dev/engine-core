use twmq::redis::{AsyncCommands, aio::ConnectionManager, Pipeline};
use engine_core::error::EngineError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TransactionRegistryError {
    #[error("Redis error: {0}")]
    RedisError(#[from] twmq::redis::RedisError),
    
    #[error("Transaction not found: {transaction_id}")]
    TransactionNotFound { transaction_id: String },
}

impl From<TransactionRegistryError> for EngineError {
    fn from(err: TransactionRegistryError) -> Self {
        EngineError::InternalError { message: err.to_string() }
    }
}

pub struct TransactionRegistry {
    redis: ConnectionManager,
    namespace: Option<String>,
}

impl TransactionRegistry {
    pub fn new(redis: ConnectionManager, namespace: Option<String>) -> Self {
        Self { redis, namespace }
    }

    fn registry_key(&self) -> String {
        match &self.namespace {
            Some(ns) => format!("{}:tx_registry", ns),
            None => "tx_registry".to_string(),
        }
    }

    pub async fn get_transaction_queue(&self, transaction_id: &str) -> Result<Option<String>, TransactionRegistryError> {
        let mut conn = self.redis.clone();
        let queue_name: Option<String> = conn.hget(self.registry_key(), transaction_id).await?;
        Ok(queue_name)
    }

    pub async fn set_transaction_queue(
        &self,
        transaction_id: &str,
        queue_name: &str,
    ) -> Result<(), TransactionRegistryError> {
        let mut conn = self.redis.clone();
        let _: () = conn.hset(self.registry_key(), transaction_id, queue_name).await?;
        Ok(())
    }

    pub async fn remove_transaction(&self, transaction_id: &str) -> Result<(), TransactionRegistryError> {
        let mut conn = self.redis.clone();
        let _: u32 = conn.hdel(self.registry_key(), transaction_id).await?;
        Ok(())
    }

    /// Add registry update commands to a Redis pipeline for atomic execution
    pub fn add_set_command(&self, pipeline: &mut Pipeline, transaction_id: &str, queue_name: &str) {
        pipeline.hset(self.registry_key(), transaction_id, queue_name);
    }

    /// Add registry removal commands to a Redis pipeline for atomic execution
    pub fn add_remove_command(&self, pipeline: &mut Pipeline, transaction_id: &str) {
        pipeline.hdel(self.registry_key(), transaction_id);
    }
}