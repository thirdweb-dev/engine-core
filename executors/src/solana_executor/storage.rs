use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use solana_sdk::{hash::Hash, signature::Signature};
use twmq::{
    redis,
    redis::{AsyncCommands, aio::ConnectionManager},
};

/// Represents a single attempt to send a Solana transaction
/// This is stored in Redis BEFORE sending to prevent duplicate transactions
#[serde_as]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SolanaTransactionAttempt {
    /// The transaction signature
    #[serde_as(as = "DisplayFromStr")]
    pub signature: Signature,
    /// The blockhash used for this attempt
    #[serde_as(as = "DisplayFromStr")]
    pub blockhash: Hash,
    /// The block height at which this blockhash expires
    pub blockhash_last_valid_height: u64,
    /// Timestamp when the transaction was sent (milliseconds)
    pub sent_at: u64,
    /// Resubmission attempt number (1 = first submission, 2 = first resubmission, etc.)
    pub submission_attempt_number: u32,
}

impl SolanaTransactionAttempt {
    pub fn new(
        signature: Signature,
        blockhash: Hash,
        blockhash_last_valid_height: u64,
        submission_attempt_number: u32,
    ) -> Self {
        Self {
            signature,
            blockhash,
            blockhash_last_valid_height,
            sent_at: crate::metrics::current_timestamp_ms(),
            submission_attempt_number,
        }
    }
}

/// Represents a lock held on a transaction
/// When dropped, the lock is automatically released
pub struct TransactionLock {
    redis: ConnectionManager,
    lock_key: String,
    lock_value: String,
}

impl TransactionLock {
    /// Check if we still hold the lock
    pub async fn still_held(&self) -> Result<bool, redis::RedisError> {
        let current_value: Option<String> = self.redis.clone().get(&self.lock_key).await?;
        Ok(current_value.as_deref() == Some(&self.lock_value))
    }

    /// Explicitly release the lock
    pub async fn release(self) -> Result<(), redis::RedisError> {
        // Use Lua script to atomically check and delete only if we own the lock
        let script = r#"
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("del", KEYS[1])
            else
                return 0
            end
        "#;

        let _: i32 = redis::Script::new(script)
            .key(&self.lock_key)
            .arg(&self.lock_value)
            .invoke_async(&mut self.redis.clone())
            .await?;

        Ok(())
    }
}

impl Drop for TransactionLock {
    fn drop(&mut self) {
        // Best effort release on drop (fire and forget)
        let redis = self.redis.clone();
        let lock_key = self.lock_key.clone();
        let lock_value = self.lock_value.clone();

        tokio::spawn(async move {
            let script = r#"
                if redis.call("get", KEYS[1]) == ARGV[1] then
                    return redis.call("del", KEYS[1])
                else
                    return 0
                end
            "#;

            let _: Result<i32, _> = redis::Script::new(script)
                .key(&lock_key)
                .arg(&lock_value)
                .invoke_async(&mut redis.clone())
                .await;
        });
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum LockError {
    #[error("Failed to acquire lock: another worker is processing this transaction")]
    AlreadyLocked,

    #[error("Redis error: {0}")]
    RedisError(String),
}

impl From<redis::RedisError> for LockError {
    fn from(e: redis::RedisError) -> Self {
        LockError::RedisError(e.to_string())
    }
}

/// Storage for Solana transaction attempts
/// Provides atomic operations to prevent duplicate transactions
pub struct SolanaTransactionStorage {
    redis: ConnectionManager,
    namespace: Option<String>,
}

impl SolanaTransactionStorage {
    pub fn new(redis: ConnectionManager, namespace: Option<String>) -> Self {
        Self { redis, namespace }
    }

    /// Get the Redis key for a transaction's attempt
    fn attempt_key(&self, transaction_id: &str) -> String {
        match &self.namespace {
            Some(ns) => format!("{ns}:solana_tx_attempt:{transaction_id}"),
            None => format!("solana_tx_attempt:{transaction_id}"),
        }
    }

    /// Get the Redis key for a transaction's lock
    fn lock_key(&self, transaction_id: &str) -> String {
        match &self.namespace {
            Some(ns) => format!("{ns}:solana_tx_lock:{transaction_id}"),
            None => format!("solana_tx_lock:{transaction_id}"),
        }
    }

    /// Try to acquire a distributed lock on a transaction
    ///
    /// Returns Ok(Some(lock)) if lock acquired
    /// Returns Ok(None) if lock is held by another worker
    /// Returns Err on Redis errors
    pub async fn try_acquire_lock(
        &self,
        transaction_id: &str,
    ) -> Result<TransactionLock, LockError> {
        let lock_key = self.lock_key(transaction_id);
        // Use a unique value (random UUID) so we can safely release only our lock
        let lock_value = uuid::Uuid::new_v4().to_string();

        // Try to acquire lock with SET NX EX (set if not exists with expiry)
        // Lock expires after 120 seconds to prevent deadlocks from crashes
        let result: Option<String> = redis::cmd("SET")
            .arg(&lock_key)
            .arg(&lock_value)
            .arg("NX") // Only set if key doesn't exist
            .arg("EX")
            .arg(120) // Expire after 120 seconds
            .arg("GET") // Return old value if any
            .query_async(&mut self.redis.clone())
            .await?;

        if result.is_some() {
            // Another worker holds the lock
            Err(LockError::AlreadyLocked)
        } else {
            // We acquired the lock!
            Ok(TransactionLock {
                redis: self.redis.clone(),
                lock_key,
                lock_value,
            })
        }
    }

    /// Store an attempt ATOMICALLY before sending to RPC (must hold lock)
    /// This is critical to prevent duplicate transactions from crashes
    /// Uses Lua script to atomically check lock and store attempt
    ///
    /// Returns Ok(true) if stored successfully (first time)
    /// Returns Ok(false) if attempt already exists (duplicate prevention)
    /// Returns Err if lock was lost or other error occurred
    pub async fn store_attempt_if_not_exists(
        &self,
        transaction_id: &str,
        attempt: &SolanaTransactionAttempt,
        lock: &TransactionLock,
    ) -> Result<bool, redis::RedisError> {
        let attempt_key = self.attempt_key(transaction_id);
        let json = serde_json::to_string(attempt).map_err(|e| {
            redis::RedisError::from((redis::ErrorKind::TypeError, "serialization", e.to_string()))
        })?;

        // Lua script to atomically verify lock and store attempt
        let script = redis::Script::new(
            r#"
            local lock_key = KEYS[1]
            local lock_value = ARGV[1]
            local attempt_key = KEYS[2]
            local attempt_json = ARGV[2]
            local expiry = tonumber(ARGV[3])
            
            -- Check if we still hold the lock
            local current_lock = redis.call('GET', lock_key)
            if current_lock ~= lock_value then
                return redis.error_reply('lock lost')
            end
            
            -- Try to store attempt if it doesn't exist
            local result = redis.call('SET', attempt_key, attempt_json, 'NX', 'EX', expiry)
            if result then
                return 1
            else
                return 0
            end
            "#,
        );

        let result: Result<i32, redis::RedisError> = script
            .key(&lock.lock_key)
            .key(&attempt_key)
            .arg(&lock.lock_value)
            .arg(&json)
            .arg(600) // 10 minutes expiry
            .invoke_async(&mut self.redis.clone())
            .await;

        match result {
            Ok(1) => Ok(true),
            Ok(0) => Ok(false),
            Ok(_) => Err(redis::RedisError::from((
                redis::ErrorKind::IoError,
                "unexpected result",
                "Unexpected result from Lua script".to_string(),
            ))),
            Err(e) => {
                if e.to_string().contains("lock lost") {
                    Err(redis::RedisError::from((
                        redis::ErrorKind::IoError,
                        "lock lost",
                        "Lost lock while storing attempt".to_string(),
                    )))
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Get the current attempt for a transaction (must hold lock)
    /// Uses Lua script to atomically verify lock and read attempt
    pub async fn get_attempt(
        &self,
        transaction_id: &str,
        lock: &TransactionLock,
    ) -> Result<Option<SolanaTransactionAttempt>, redis::RedisError> {
        let attempt_key = self.attempt_key(transaction_id);

        // Lua script to atomically verify lock and get attempt
        let script = redis::Script::new(
            r#"
            local lock_key = KEYS[1]
            local lock_value = ARGV[1]
            local attempt_key = KEYS[2]
            
            -- Check if we still hold the lock
            local current_lock = redis.call('GET', lock_key)
            if current_lock ~= lock_value then
                return redis.error_reply('lock lost')
            end
            
            -- Get the attempt
            return redis.call('GET', attempt_key)
            "#,
        );

        let result: Result<Option<String>, redis::RedisError> = script
            .key(&lock.lock_key)
            .key(&attempt_key)
            .arg(&lock.lock_value)
            .invoke_async(&mut self.redis.clone())
            .await;

        match result {
            Ok(Some(json)) => {
                let attempt = serde_json::from_str(&json).map_err(|e| {
                    redis::RedisError::from((
                        redis::ErrorKind::TypeError,
                        "deserialization",
                        e.to_string(),
                    ))
                })?;
                Ok(Some(attempt))
            }
            Ok(None) => Ok(None),
            Err(e) => {
                if e.to_string().contains("lock lost") {
                    Err(redis::RedisError::from((
                        redis::ErrorKind::IoError,
                        "lock lost",
                        "Lost lock while reading attempt".to_string(),
                    )))
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Update an existing attempt (for retry scenarios, must hold lock)
    /// This will overwrite the existing attempt
    /// Uses Lua script to atomically verify lock and update attempt
    pub async fn update_attempt(
        &self,
        transaction_id: &str,
        attempt: &SolanaTransactionAttempt,
        lock: &TransactionLock,
    ) -> Result<(), redis::RedisError> {
        let attempt_key = self.attempt_key(transaction_id);
        let json = serde_json::to_string(attempt).map_err(|e| {
            redis::RedisError::from((redis::ErrorKind::TypeError, "serialization", e.to_string()))
        })?;

        // Lua script to atomically verify lock and update attempt
        let script = redis::Script::new(
            r#"
            local lock_key = KEYS[1]
            local lock_value = ARGV[1]
            local attempt_key = KEYS[2]
            local attempt_json = ARGV[2]
            local expiry = tonumber(ARGV[3])
            
            -- Check if we still hold the lock
            local current_lock = redis.call('GET', lock_key)
            if current_lock ~= lock_value then
                return redis.error_reply('lock lost')
            end
            
            -- Update the attempt
            redis.call('SETEX', attempt_key, expiry, attempt_json)
            return 1
            "#,
        );

        let result: Result<i32, redis::RedisError> = script
            .key(&lock.lock_key)
            .key(&attempt_key)
            .arg(&lock.lock_value)
            .arg(&json)
            .arg(600) // 10 minutes expiry
            .invoke_async(&mut self.redis.clone())
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                if e.to_string().contains("lock lost") {
                    Err(redis::RedisError::from((
                        redis::ErrorKind::IoError,
                        "lock lost",
                        "Lost lock while updating attempt".to_string(),
                    )))
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Delete an attempt after successful confirmation or permanent failure
    pub async fn delete_attempt(&self, transaction_id: &str) -> Result<(), redis::RedisError> {
        let key = self.attempt_key(transaction_id);
        self.redis.clone().del::<_, ()>(&key).await?;
        Ok(())
    }

    /// Check if an attempt exists without retrieving it
    pub async fn has_attempt(&self, transaction_id: &str) -> Result<bool, redis::RedisError> {
        let key = self.attempt_key(transaction_id);
        let exists: bool = self.redis.clone().exists(&key).await?;
        Ok(exists)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_attempt() -> SolanaTransactionAttempt {
        use std::str::FromStr;
        SolanaTransactionAttempt {
            signature: solana_sdk::signature::Signature::from_str(
                "5j7s6NiJS3JAkvgkoc18WVAsiSaci2pxB2A6ueCJP4tprA2TFg9wSyTLeYouxPBJEMzJinENTkpA52YStRW5Dia7"
            ).unwrap(),
            blockhash: "EkSnNWid2cvwEVnVx9aBqawnmiCNiDgp3gUdkDPTKN1N".parse().unwrap(),
            blockhash_last_valid_height: 123456789,
            sent_at: 1234567890000,
            submission_attempt_number: 1,
        }
    }

    #[test]
    fn test_attempt_serialization() {
        let attempt = mock_attempt();
        let json = serde_json::to_string(&attempt).unwrap();
        let deserialized: SolanaTransactionAttempt = serde_json::from_str(&json).unwrap();

        assert_eq!(attempt.signature, deserialized.signature);
        assert_eq!(attempt.blockhash, deserialized.blockhash);
        assert_eq!(
            attempt.submission_attempt_number,
            deserialized.submission_attempt_number
        );
    }

    #[test]
    fn test_attempt_parse_signature() {
        let attempt = mock_attempt();
        assert_eq!(
            attempt.signature.to_string(),
            "5j7s6NiJS3JAkvgkoc18WVAsiSaci2pxB2A6ueCJP4tprA2TFg9wSyTLeYouxPBJEMzJinENTkpA52YStRW5Dia7"
        );
    }
}
