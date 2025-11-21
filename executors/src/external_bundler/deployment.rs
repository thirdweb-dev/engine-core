use alloy::primitives::Address;
use engine_aa_core::userop::deployment::{
    AcquireLockResult, DeploymentCache, DeploymentLock, LockId,
};
use engine_core::error::EngineError;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use twmq::{
    error::TwmqError,
    redis::{AsyncCommands, Pipeline, aio::ConnectionManager},
};
use uuid::Uuid;

const CACHE_PREFIX: &str = "deployment_cache";
const LOCK_PREFIX: &str = "deployment_lock";

#[derive(Clone)]
pub struct RedisDeploymentCache {
    connection_manager: twmq::redis::aio::ConnectionManager,
}

#[derive(Clone)]
pub struct RedisDeploymentLock {
    connection_manager: twmq::redis::aio::ConnectionManager,
}

#[derive(Serialize, Deserialize)]
struct LockData {
    lock_id: String,
    acquired_at: u64,
}

impl RedisDeploymentCache {
    pub async fn new(client: twmq::redis::Client) -> Result<Self, TwmqError> {
        Ok(Self {
            connection_manager: ConnectionManager::new(client).await?,
        })
    }

    pub fn conn(&self) -> &ConnectionManager {
        &self.connection_manager
    }

    fn cache_key(&self, chain_id: u64, account_address: &Address) -> String {
        format!("{CACHE_PREFIX}:{chain_id}:{account_address}")
    }
}

impl DeploymentCache for RedisDeploymentCache {
    async fn is_deployed(&self, chain_id: u64, account_address: &Address) -> Option<bool> {
        let mut conn = self.conn().clone();
        let key = self.cache_key(chain_id, account_address);

        match conn.get::<_, Option<String>>(&key).await {
            Ok(Some(value)) if value == "deployed" => Some(true),
            Ok(Some(value)) if value == "not_deployed" => Some(false),
            _ => None,
        }
    }
}

impl RedisDeploymentLock {
    pub async fn new(client: twmq::redis::Client) -> Result<Self, TwmqError> {
        Ok(Self {
            connection_manager: ConnectionManager::new(client).await?,
        })
    }

    pub fn conn(&self) -> &ConnectionManager {
        &self.connection_manager
    }

    fn lock_key(&self, chain_id: u64, account_address: &Address) -> String {
        format!("{LOCK_PREFIX}:{chain_id}:{account_address}")
    }

    fn cache_key(&self, chain_id: u64, account_address: &Address) -> String {
        format!("{CACHE_PREFIX}:{chain_id}:{account_address}")
    }

    /// Release a deployment lock using the provided pipeline
    pub fn release_lock_with_pipeline(
        &self,
        pipeline: &mut Pipeline,
        chain_id: u64,
        account_address: &Address,
    ) {
        let key = self.lock_key(chain_id, account_address);
        pipeline.del(&key);
    }

    /// Update deployment cache using the provided pipeline
    pub fn update_cache_with_pipeline(
        &self,
        pipeline: &mut Pipeline,
        chain_id: u64,
        account_address: &Address,
        is_deployed: bool,
    ) {
        let cache_key = self.cache_key(chain_id, account_address);
        let value = if is_deployed {
            "deployed"
        } else {
            "not_deployed"
        };
        pipeline.set_ex(&cache_key, value, 3600); // Cache for 1 hour
    }

    /// Atomically release lock and update cache using the provided pipeline
    pub fn release_lock_and_update_cache_with_pipeline(
        &self,
        pipeline: &mut Pipeline,
        chain_id: u64,
        account_address: &Address,
        is_deployed: bool,
    ) {
        self.release_lock_with_pipeline(pipeline, chain_id, account_address);
        self.update_cache_with_pipeline(pipeline, chain_id, account_address, is_deployed);
    }
}

impl DeploymentLock for RedisDeploymentLock {
    async fn check_lock(
        &self,
        chain_id: u64,
        account_address: &Address,
    ) -> Option<(LockId, Duration)> {
        let mut conn = self.conn().clone();
        let key = self.lock_key(chain_id, account_address);

        let lock_data_str: Option<String> = conn.get(key).await.ok()?;
        let lock_data_str = lock_data_str?;

        let lock_data: LockData = serde_json::from_str(&lock_data_str).ok()?;

        let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
        let duration = Duration::from_secs(now.saturating_sub(lock_data.acquired_at));

        Some((lock_data.lock_id, duration))
    }

    async fn acquire_lock(
        &self,
        chain_id: u64,
        account_address: &Address,
    ) -> Result<AcquireLockResult, EngineError> {
        let mut conn = self.conn().clone();

        let key = self.lock_key(chain_id, account_address);
        let lock_id = Uuid::new_v4().to_string();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| EngineError::InternalError {
                message: format!("System time error: {e}"),
            })?
            .as_secs();

        let lock_data = LockData {
            lock_id: lock_id.clone(),
            acquired_at: now,
        };

        let lock_data_str =
            serde_json::to_string(&lock_data).map_err(|e| EngineError::InternalError {
                message: format!("Serialization failed: {e}"),
            })?;

        // Use SET NX EX for atomic acquire
        let result: Option<String> =
            conn.set_nx(&key, &lock_data_str)
                .await
                .map_err(|e| EngineError::InternalError {
                    message: format!("Lock acquire failed: {e}"),
                })?;

        match result {
            Some(_) => Ok(AcquireLockResult::Acquired),
            None => {
                // Lock already exists, get the lock_id
                let existing_data: Option<String> =
                    conn.get(&key)
                        .await
                        .map_err(|e| EngineError::InternalError {
                            message: format!("Failed to read existing lock: {e}"),
                        })?;

                let existing_lock_id = existing_data
                    .and_then(|data| serde_json::from_str::<LockData>(&data).ok())
                    .map(|data| data.lock_id)
                    .unwrap_or_else(|| "unknown".to_string());

                Ok(AcquireLockResult::AlreadyLocked(existing_lock_id))
            }
        }
    }

    async fn release_lock(
        &self,
        chain_id: u64,
        account_address: &Address,
    ) -> Result<bool, EngineError> {
        let mut conn = self.conn().clone();

        let key = self.lock_key(chain_id, account_address);

        let deleted =
            conn.del::<&str, usize>(&key)
                .await
                .map_err(|e| EngineError::InternalError {
                    message: format!("Failed to delete lock for account {account_address}: {e}"),
                })?;

        Ok(deleted > 0)
    }
}
