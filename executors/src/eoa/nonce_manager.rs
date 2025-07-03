use alloy::primitives::{Address, B256, U256};
use engine_core::error::EngineError;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use twmq::redis::{AsyncCommands, Pipeline, aio::ConnectionManager};

#[derive(Debug, Error)]
pub enum NonceManagerError {
    #[error("Redis error: {0}")]
    RedisError(#[from] twmq::redis::RedisError),

    #[error("Max in-flight transactions reached for EOA {eoa}: {current}/{max}")]
    MaxInFlightReached {
        eoa: Address,
        current: u32,
        max: u32,
    },

    #[error("EOA {eoa} needs sync - no optimistic nonce found")]
    NeedsSync { eoa: Address },

    #[error("Nonce assignment failed: {reason}")]
    NonceAssignmentFailed { reason: String },
}

impl From<NonceManagerError> for EngineError {
    fn from(err: NonceManagerError) -> Self {
        EngineError::InternalError {
            message: err.to_string(),
        }
    }
}

/// Tracks nonce assignment for a specific transaction
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NonceAssignment {
    pub transaction_id: String,
    pub transaction_hash: B256,
    pub assigned_at: u64,
}

/// Health tracking for an EOA on a specific chain
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EoaHealth {
    pub in_flight_count: u32,
    pub consecutive_errors: u32,
    pub last_error_time: u64,
    pub last_success_time: u64,
    pub recycled_nonce_count: u32,
    pub last_sync_time: u64,
    pub is_synced: bool,
}

impl Default for EoaHealth {
    fn default() -> Self {
        Self {
            in_flight_count: 0,
            consecutive_errors: 0,
            last_error_time: 0,
            last_success_time: 0,
            recycled_nonce_count: 0,
            last_sync_time: 0,
            is_synced: false,
        }
    }
}

/// Manages nonce assignment and recycling for EOA transactions
pub struct NonceManager {
    redis: ConnectionManager,
    namespace: Option<String>,
    max_in_flight: u32,
    max_recycled: u32,
}

impl NonceManager {
    pub fn new(
        redis: ConnectionManager,
        namespace: Option<String>,
        max_in_flight: u32,
        max_recycled: u32,
    ) -> Self {
        Self {
            redis,
            namespace,
            max_in_flight,
            max_recycled,
        }
    }

    // Redis key naming methods with proper EOA namespacing
    fn eoa_key(&self, eoa: Address, chain_id: u64) -> String {
        match &self.namespace {
            Some(ns) => format!("{}:eoa:{}:{}", ns, chain_id, eoa),
            None => format!("eoa:{}:{}", chain_id, eoa),
        }
    }

    fn optimistic_nonce_key(&self, chain_id: u64) -> String {
        match &self.namespace {
            Some(ns) => format!("{}:eoa_nonce:optimistic:{}", ns, chain_id),
            None => format!("eoa_nonce:optimistic:{}", chain_id),
        }
    }

    fn recycled_nonces_key(&self, eoa: Address, chain_id: u64) -> String {
        match &self.namespace {
            Some(ns) => format!("{}:eoa_nonce:recycled:{}:{}", ns, chain_id, eoa),
            None => format!("eoa_nonce:recycled:{}:{}", chain_id, eoa),
        }
    }

    fn nonce_assignments_key(&self, eoa: Address, chain_id: u64) -> String {
        match &self.namespace {
            Some(ns) => format!("{}:eoa_nonce:assigned:{}:{}", ns, chain_id, eoa),
            None => format!("eoa_nonce:assigned:{}:{}", chain_id, eoa),
        }
    }

    fn onchain_nonce_cache_key(&self, chain_id: u64) -> String {
        match &self.namespace {
            Some(ns) => format!("{}:eoa_nonce:onchain:{}", ns, chain_id),
            None => format!("eoa_nonce:onchain:{}", chain_id),
        }
    }

    fn health_status_key(&self, chain_id: u64) -> String {
        match &self.namespace {
            Some(ns) => format!("{}:eoa_health:{}", ns, chain_id),
            None => format!("eoa_health:{}", chain_id),
        }
    }

    fn epoch_key(&self, eoa: Address, chain_id: u64) -> String {
        match &self.namespace {
            Some(ns) => format!("{}:eoa_epoch:{}:{}", ns, chain_id, eoa),
            None => format!("eoa_epoch:{}:{}", chain_id, eoa),
        }
    }

    fn sync_lock_key(&self, eoa: Address, chain_id: u64) -> String {
        match &self.namespace {
            Some(ns) => format!("{}:eoa_sync_lock:{}:{}", ns, chain_id, eoa),
            None => format!("eoa_sync_lock:{}:{}", chain_id, eoa),
        }
    }

    /// Get current health status for an EOA
    pub async fn get_eoa_health(
        &self,
        eoa: Address,
        chain_id: u64,
    ) -> Result<EoaHealth, NonceManagerError> {
        let mut conn = self.redis.clone();
        let health_key = self.health_status_key(chain_id);
        let eoa_field = eoa.to_string();

        let health_json: Option<String> = conn.hget(&health_key, &eoa_field).await?;

        match health_json {
            Some(json) => Ok(serde_json::from_str(&json).unwrap_or_default()),
            None => Ok(EoaHealth::default()),
        }
    }

    /// Update health status for an EOA
    pub async fn update_eoa_health(
        &self,
        eoa: Address,
        chain_id: u64,
        health: &EoaHealth,
    ) -> Result<(), NonceManagerError> {
        let mut conn = self.redis.clone();
        let health_key = self.health_status_key(chain_id);
        let eoa_field = eoa.to_string();
        let health_json = serde_json::to_string(health).unwrap();

        let _: () = conn.hset(&health_key, &eoa_field, health_json).await?;
        Ok(())
    }

    /// Atomic nonce assignment using cached onchain nonce with epoch-based recycling protection
    pub async fn assign_nonce(
        &self,
        eoa: Address,
        chain_id: u64,
    ) -> Result<(u64, u64), NonceManagerError> {
        let script = twmq::redis::Script::new(
            r#"
            local eoa = ARGV[1]
            local max_recycled = tonumber(ARGV[2])
            local max_in_flight = tonumber(ARGV[3])
            local now = tonumber(ARGV[4])

            local optimistic_nonce_key = KEYS[1]
            local recycled_nonces_key = KEYS[2]
            local health_key = KEYS[3]
            local epoch_key = KEYS[4]
            local onchain_cache_key = KEYS[5]

            -- Get current epoch (or initialize)
            local current_epoch = redis.call('GET', epoch_key)
            if not current_epoch then
                current_epoch = tostring(now)
                redis.call('SET', epoch_key, current_epoch)
            end

            -- Derive recycled count
            local recycled_count = redis.call('ZCARD', recycled_nonces_key)
            
            -- Get optimistic nonce
            local optimistic_nonce = redis.call('HGET', optimistic_nonce_key, eoa)
            if not optimistic_nonce then
                -- Not initialized, need sync
                return {-3, "needs_sync", "0", current_epoch}
            end
            optimistic_nonce = tonumber(optimistic_nonce)

            -- Get cached onchain nonce
            local onchain_nonce = redis.call('HGET', onchain_cache_key, eoa)
            if not onchain_nonce then
                -- No cached onchain nonce, need sync
                return {-3, "needs_sync", "0", current_epoch}
            end
            onchain_nonce = tonumber(onchain_nonce)

            -- Derive in-flight count
            local in_flight_count = math.max(0, optimistic_nonce - onchain_nonce)

            -- Check if recycled count exceeds threshold
            if recycled_count > max_recycled then
                -- Force reset: increment epoch, clear recycled nonces, trigger resync
                local reset_epoch = tostring(now)
                
                -- Update epoch (this invalidates any stale recycling attempts)
                redis.call('SET', epoch_key, reset_epoch)
                
                -- Clear all recycled nonces
                redis.call('DEL', recycled_nonces_key)
                
                -- Clear optimistic nonce to force resync
                redis.call('HDEL', optimistic_nonce_key, eoa)
                
                -- Clear cached onchain nonce to force fresh fetch
                redis.call('HDEL', onchain_cache_key, eoa)
                
                -- Update health to indicate reset occurred
                local health_json = redis.call('HGET', health_key, eoa)
                local health = {}
                if health_json then
                    health = cjson.decode(health_json)
                end
                health.last_sync_time = now
                health.is_synced = false
                redis.call('HSET', health_key, eoa, cjson.encode(health))
                
                return {-1, "too_many_recycled_reset", "0", reset_epoch}
            end

            -- Check in-flight threshold
            if in_flight_count >= max_in_flight then
                return {-2, "max_in_flight", tostring(in_flight_count), current_epoch}
            end

            -- Try to pop the lowest recycled nonce first
            if recycled_count > 0 then
                local recycled_nonce = redis.call('ZPOPMIN', recycled_nonces_key)
                if #recycled_nonce > 0 then
                    -- Update health with successful assignment
                    local health_json = redis.call('HGET', health_key, eoa)
                    local health = {}
                    if health_json then
                        health = cjson.decode(health_json)
                    end
                    health.last_success_time = now
                    redis.call('HSET', health_key, eoa, cjson.encode(health))
                    
                    return {0, recycled_nonce[1], current_epoch}
                end
            end

            -- No recycled nonce, increment optimistic nonce
            local nonce = optimistic_nonce
            redis.call('HSET', optimistic_nonce_key, eoa, nonce + 1)
            
            -- Update health with successful assignment
            local health_json = redis.call('HGET', health_key, eoa)
            local health = {}
            if health_json then
                health = cjson.decode(health_json)
            end
            health.last_success_time = now
            redis.call('HSET', health_key, eoa, cjson.encode(health))

            return {1, tostring(nonce), current_epoch}
            "#,
        );

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let result: (i32, String, String, String) = script
            .key(self.optimistic_nonce_key(chain_id))
            .key(self.recycled_nonces_key(eoa, chain_id))
            .key(self.health_status_key(chain_id))
            .key(self.epoch_key(eoa, chain_id))
            .key(self.onchain_nonce_cache_key(chain_id))
            .arg(eoa.to_string())
            .arg(self.max_recycled)
            .arg(self.max_in_flight)
            .arg(now)
            .invoke_async(&mut self.redis.clone())
            .await?;

        match result.0 {
            -1 => {
                // Reset occurred due to too many recycled nonces - force resync needed
                Err(NonceManagerError::NeedsSync { eoa })
            }
            -2 => Err(NonceManagerError::MaxInFlightReached {
                eoa,
                current: result.2.parse().unwrap_or(0),
                max: self.max_in_flight,
            }),
            -3 => Err(NonceManagerError::NeedsSync { eoa }),
            0 | 1 => {
                let nonce: u64 =
                    result
                        .1
                        .parse()
                        .map_err(|e| NonceManagerError::NonceAssignmentFailed {
                            reason: format!("Failed to parse nonce: {}", e),
                        })?;
                let epoch: u64 =
                    result
                        .3
                        .parse()
                        .map_err(|e| NonceManagerError::NonceAssignmentFailed {
                            reason: format!("Failed to parse epoch: {}", e),
                        })?;
                Ok((nonce, epoch))
            }
            _ => Err(NonceManagerError::NonceAssignmentFailed {
                reason: "Unexpected result from nonce assignment".to_string(),
            }),
        }
    }

    /// Record a nonce assignment for tracking
    pub fn add_nonce_assignment_command(
        &self,
        pipeline: &mut Pipeline,
        eoa: Address,
        chain_id: u64,
        nonce: U256,
        transaction_id: &str,
        transaction_hash: B256,
    ) {
        let assignment = NonceAssignment {
            transaction_id: transaction_id.to_string(),
            transaction_hash,
            assigned_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        let assignment_json = serde_json::to_string(&assignment).unwrap();
        let assignments_key = self.nonce_assignments_key(eoa, chain_id);

        pipeline.hset(&assignments_key, nonce.to_string(), assignment_json);
    }

    /// Recycle a nonce back to the recycled set
    pub fn add_recycle_nonce_command(
        &self,
        pipeline: &mut Pipeline,
        eoa: Address,
        chain_id: u64,
        nonce: U256,
    ) {
        let recycled_key = self.recycled_nonces_key(eoa, chain_id);
        let health_key = self.health_status_key(chain_id);

        // Add to recycled sorted set (score = nonce value for ordering)
        pipeline.zadd(&recycled_key, nonce.to_string(), nonce.to::<u64>());

        // Update health to increment recycled count
        pipeline.hincr(&health_key, format!("{}:recycled_nonce_count", eoa), 1);
    }

    /// Remove a nonce assignment after successful confirmation
    pub fn add_remove_assignment_command(
        &self,
        pipeline: &mut Pipeline,
        eoa: Address,
        chain_id: u64,
        nonce: U256,
    ) {
        let assignments_key = self.nonce_assignments_key(eoa, chain_id);
        let health_key = self.health_status_key(chain_id);

        pipeline.hdel(&assignments_key, nonce.to_string());

        // Decrement in-flight count
        pipeline.hincr(&health_key, format!("{}:in_flight_count", eoa), -1);
    }

    /// Get all nonce assignments for an EOA
    pub async fn get_nonce_assignments(
        &self,
        eoa: Address,
        chain_id: u64,
        nonce: U256,
    ) -> Result<Vec<NonceAssignment>, NonceManagerError> {
        let mut conn = self.redis.clone();
        let assignments_key = self.nonce_assignments_key(eoa, chain_id);

        let assignment_json: Option<String> =
            conn.hget(&assignments_key, nonce.to_string()).await?;

        match assignment_json {
            Some(json) => {
                let assignment: NonceAssignment = serde_json::from_str(&json).map_err(|e| {
                    NonceManagerError::NonceAssignmentFailed {
                        reason: format!("Failed to deserialize assignment: {}", e),
                    }
                })?;
                Ok(vec![assignment])
            }
            None => Ok(vec![]),
        }
    }

    /// Attempt to acquire sync lock and sync nonce for an EOA
    pub async fn try_sync_nonce(
        &self,
        eoa: Address,
        chain_id: u64,
        onchain_nonce: u64,
    ) -> Result<bool, NonceManagerError> {
        let script = twmq::redis::Script::new(
            r#"
            local eoa = ARGV[1]
            local onchain_nonce = tonumber(ARGV[2])
            local now = tonumber(ARGV[3])

            local optimistic_nonce_key = KEYS[1]
            local recycled_nonces_key = KEYS[2]
            local health_key = KEYS[3]
            local onchain_cache_key = KEYS[4]
            local sync_lock_key = KEYS[5]
            local epoch_key = KEYS[6]

            -- Try to acquire sync lock (60 second expiry)
            local lock_acquired = redis.call('SET', sync_lock_key, now, 'NX', 'EX', '60')
            if not lock_acquired then
                -- Another process is syncing
                return {0, "sync_in_progress"}
            end

            -- Successfully acquired lock, perform sync
            -- Clear recycled nonces and reset optimistic nonce
            redis.call('DEL', recycled_nonces_key)
            redis.call('HSET', optimistic_nonce_key, eoa, onchain_nonce)
            redis.call('HSET', onchain_cache_key, eoa, onchain_nonce)

            -- Update epoch to invalidate any stale recycling attempts
            local new_epoch = tostring(now)
            redis.call('SET', epoch_key, new_epoch)

            -- Update health status
            local health_json = redis.call('HGET', health_key, eoa)
            local health = {}
            if health_json then
                health = cjson.decode(health_json)
            end
            health.is_synced = true
            health.last_sync_time = now
            health.consecutive_errors = 0
            redis.call('HSET', health_key, eoa, cjson.encode(health))

            return {1, "synced", new_epoch}
            "#,
        );

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let result: (i32, String, Option<String>) = script
            .key(self.optimistic_nonce_key(chain_id))
            .key(self.recycled_nonces_key(eoa, chain_id))
            .key(self.health_status_key(chain_id))
            .key(self.onchain_nonce_cache_key(chain_id))
            .key(self.sync_lock_key(eoa, chain_id))
            .key(self.epoch_key(eoa, chain_id))
            .arg(eoa.to_string())
            .arg(onchain_nonce)
            .arg(now)
            .invoke_async(&mut self.redis.clone())
            .await?;

        Ok(result.0 == 1)
    }

    /// Get cached onchain nonce
    pub async fn get_cached_onchain_nonce(
        &self,
        eoa: Address,
        chain_id: u64,
    ) -> Result<Option<u64>, NonceManagerError> {
        let mut conn = self.redis.clone();
        let cache_key = self.onchain_nonce_cache_key(chain_id);

        let nonce_str: Option<String> = conn.hget(&cache_key, eoa.to_string()).await?;

        match nonce_str {
            Some(s) => {
                let nonce =
                    s.parse::<u64>()
                        .map_err(|e| NonceManagerError::NonceAssignmentFailed {
                            reason: format!("Failed to parse cached nonce: {}", e),
                        })?;
                Ok(Some(nonce))
            }
            None => Ok(None),
        }
    }

    /// Update cached onchain nonce
    pub async fn update_cached_onchain_nonce(
        &self,
        eoa: Address,
        chain_id: u64,
        nonce: u64,
    ) -> Result<(), NonceManagerError> {
        let mut conn = self.redis.clone();
        let cache_key = self.onchain_nonce_cache_key(chain_id);

        let _: () = conn
            .hset(&cache_key, eoa.to_string(), nonce.to_string())
            .await?;
        Ok(())
    }
}
