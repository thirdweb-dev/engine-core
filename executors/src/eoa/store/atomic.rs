use alloy::{
    consensus::{Signed, TypedTransaction},
    primitives::Address,
};
use twmq::redis::{AsyncCommands, Pipeline, aio::ConnectionManager};

use crate::eoa::{
    EoaExecutorStore,
    store::{
        BorrowedTransactionData, ConfirmedTransaction, EoaHealth, TransactionAttempt,
        TransactionStoreError,
        submitted::{CleanSubmittedTransactions, CleanupReport, SubmittedTransaction},
    },
};

const MAX_RETRIES: u32 = 10;
const RETRY_BASE_DELAY_MS: u64 = 10;

pub trait SafeRedisTransaction: Send + Sync {
    type ValidationData;
    type OperationResult;

    fn name(&self) -> &str;
    fn operation(
        &self,
        pipeline: &mut Pipeline,
        validation_data: Self::ValidationData,
    ) -> Self::OperationResult;
    fn validation(
        &self,
        conn: &mut ConnectionManager,
    ) -> impl Future<Output = Result<Self::ValidationData, TransactionStoreError>> + Send;
    fn watch_keys(&self) -> Vec<String>;
}

struct MovePendingToBorrowedWithRecycledNonce {
    recycled_key: String,
    pending_key: String,
    transaction_id: String,
    borrowed_key: String,
    nonce: u64,
    prepared_tx_json: String,
}

impl SafeRedisTransaction for MovePendingToBorrowedWithRecycledNonce {
    type ValidationData = ();
    type OperationResult = ();

    fn name(&self) -> &str {
        "pending->borrowed with recycled nonce"
    }

    fn operation(&self, pipeline: &mut Pipeline, _: Self::ValidationData) {
        // Remove nonce from recycled set (we know it exists)
        pipeline.zrem(&self.recycled_key, self.nonce);
        // Remove transaction from pending (we know it exists)
        pipeline.lrem(&self.pending_key, 0, &self.transaction_id);
        // Store borrowed transaction
        pipeline.hset(
            &self.borrowed_key,
            self.nonce.to_string(),
            &self.prepared_tx_json,
        );
    }

    fn watch_keys(&self) -> Vec<String> {
        vec![self.recycled_key.clone(), self.pending_key.clone()]
    }

    async fn validation(&self, conn: &mut ConnectionManager) -> Result<(), TransactionStoreError> {
        // Check if nonce exists in recycled set
        let nonce_score: Option<f64> = conn.zscore(&self.recycled_key, self.nonce).await?;
        if nonce_score.is_none() {
            return Err(TransactionStoreError::NonceNotInRecycledSet { nonce: self.nonce });
        }

        // Check if transaction exists in pending
        let pending_transactions: Vec<String> = conn.lrange(&self.pending_key, 0, -1).await?;
        if !pending_transactions.contains(&self.transaction_id) {
            return Err(TransactionStoreError::TransactionNotInPendingQueue {
                transaction_id: self.transaction_id.clone(),
            });
        }

        Ok(())
    }
}

struct MovePendingToBorrowedWithNewNonce {
    optimistic_key: String,
    pending_key: String,
    nonce: u64,
    prepared_tx_json: String,
    transaction_id: String,
    borrowed_key: String,
    eoa: Address,
    chain_id: u64,
}

impl SafeRedisTransaction for MovePendingToBorrowedWithNewNonce {
    type ValidationData = ();
    type OperationResult = ();

    fn name(&self) -> &str {
        "pending->borrowed with new nonce"
    }

    fn operation(&self, pipeline: &mut Pipeline, _: Self::ValidationData) {
        // Increment optimistic nonce
        pipeline.incr(&self.optimistic_key, 1);
        // Remove transaction from pending
        pipeline.lrem(&self.pending_key, 0, &self.transaction_id);
        // Store borrowed transaction
        pipeline.hset(
            &self.borrowed_key,
            self.nonce.to_string(),
            &self.prepared_tx_json,
        );
    }

    fn watch_keys(&self) -> Vec<String> {
        vec![self.optimistic_key.clone(), self.pending_key.clone()]
    }

    async fn validation(&self, conn: &mut ConnectionManager) -> Result<(), TransactionStoreError> {
        // Check current optimistic nonce
        let current_optimistic: Option<u64> = conn.get(&self.optimistic_key).await?;
        let current_nonce = match current_optimistic {
            Some(nonce) => nonce,
            None => {
                return Err(TransactionStoreError::NonceSyncRequired {
                    eoa: self.eoa,
                    chain_id: self.chain_id,
                });
            }
        };

        if current_nonce != self.nonce {
            return Err(TransactionStoreError::OptimisticNonceChanged {
                expected: self.nonce,
                actual: current_nonce,
            });
        }

        // Check if transaction exists in pending
        let pending_transactions: Vec<String> = conn.lrange(&self.pending_key, 0, -1).await?;
        if !pending_transactions.contains(&self.transaction_id) {
            return Err(TransactionStoreError::TransactionNotInPendingQueue {
                transaction_id: self.transaction_id.clone(),
            });
        }

        Ok(())
    }
}

struct MoveBorrowedToSubmitted {
    nonce: u64,
    hash: String,
    transaction_id: String,
    borrowed_key: String,
    submitted_key: String,
    hash_to_id_key: String,
}

impl SafeRedisTransaction for MoveBorrowedToSubmitted {
    type ValidationData = ();
    type OperationResult = ();

    fn name(&self) -> &str {
        "borrowed->submitted"
    }

    fn operation(&self, pipeline: &mut Pipeline, _: Self::ValidationData) {
        // Remove from borrowed (we know it exists)
        pipeline.hdel(&self.borrowed_key, self.nonce.to_string());

        // Add to submitted with hash:id format
        let hash_id_value = format!("{}:{}", self.hash, self.transaction_id);
        pipeline.zadd(&self.submitted_key, &hash_id_value, self.nonce);

        // Still maintain hash-to-ID mapping for backward compatibility and external lookups
        pipeline.set(&self.hash_to_id_key, &self.transaction_id);
    }

    fn watch_keys(&self) -> Vec<String> {
        vec![self.borrowed_key.clone()]
    }

    async fn validation(&self, conn: &mut ConnectionManager) -> Result<(), TransactionStoreError> {
        // Validate that borrowed transaction actually exists
        let borrowed_tx: Option<String> = conn
            .hget(&self.borrowed_key, self.nonce.to_string())
            .await?;
        if borrowed_tx.is_none() {
            return Err(TransactionStoreError::TransactionNotInBorrowedState {
                transaction_id: self.transaction_id.clone(),
                nonce: self.nonce,
            });
        }
        Ok(())
    }
}

struct MoveBorrowedToRecycled {
    nonce: u64,
    transaction_id: String,
    borrowed_key: String,
    recycled_key: String,
    pending_key: String,
}

impl SafeRedisTransaction for MoveBorrowedToRecycled {
    type ValidationData = ();
    type OperationResult = ();

    fn name(&self) -> &str {
        "borrowed->recycled"
    }

    fn operation(&self, pipeline: &mut Pipeline, _: Self::ValidationData) {
        // Remove from borrowed (we know it exists)
        pipeline.hdel(&self.borrowed_key, self.nonce.to_string());

        // Add nonce to recycled set (with timestamp as score)
        pipeline.zadd(&self.recycled_key, self.nonce, self.nonce);

        // Add transaction back to pending
        pipeline.lpush(&self.pending_key, &self.transaction_id);
    }

    fn watch_keys(&self) -> Vec<String> {
        vec![self.borrowed_key.clone()]
    }

    async fn validation(&self, conn: &mut ConnectionManager) -> Result<(), TransactionStoreError> {
        // Validate that borrowed transaction actually exists
        let borrowed_tx: Option<String> = conn
            .hget(&self.borrowed_key, self.nonce.to_string())
            .await?;
        if borrowed_tx.is_none() {
            return Err(TransactionStoreError::TransactionNotInBorrowedState {
                transaction_id: self.transaction_id.clone(),
                nonce: self.nonce,
            });
        }
        Ok(())
    }
}

/// Atomic transaction store that owns the base store and provides atomic operations
///
/// This store is created by calling `acquire_lock()` on the base store and provides
/// access to both atomic (lock-protected) and non-atomic operations.
///
/// ## Usage:
/// ```rust
/// let base_store = EoaExecutorStore::new(redis, namespace, );
/// let atomic_store = base_store.acquire_lock(worker_id).await?;
///
/// // Atomic operations:
/// atomic_store.move_borrowed_to_submitted(nonce, hash, tx_id).await?;
///
/// // Non-atomic operations via deref:
/// atomic_store.peek_pending_transactions(limit).await?;
/// ```
pub struct AtomicEoaExecutorStore {
    pub store: EoaExecutorStore,
    pub worker_id: String,
}

impl std::ops::Deref for AtomicEoaExecutorStore {
    type Target = EoaExecutorStore;

    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

impl AtomicEoaExecutorStore {
    /// Get the EOA address this store is scoped to
    pub fn eoa(&self) -> Address {
        self.store.eoa
    }

    /// Get the chain ID this store is scoped to
    pub fn chain_id(&self) -> u64 {
        self.store.chain_id
    }

    /// Get the worker ID this store is scoped to
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Release EOA lock following the spec's finally pattern
    pub async fn release_eoa_lock(self) -> Result<EoaExecutorStore, TransactionStoreError> {
        // Use existing utility method that handles all the atomic lock checking
        match self
            .with_lock_check(|pipeline| {
                let lock_key = self.eoa_lock_key_name();
                pipeline.del(&lock_key);
            })
            .await
        {
            Ok(()) => {
                tracing::debug!(
                    eoa = %self.eoa(),
                    chain_id = %self.chain_id(),
                    worker_id = %self.worker_id(),
                    "Successfully released EOA lock"
                );
                Ok(self.store)
            }
            Err(TransactionStoreError::LockLost { .. }) => {
                // Lock was already taken over, which is fine for release
                tracing::debug!(
                    eoa = %self.eoa(),
                    chain_id = %self.chain_id(),
                    worker_id = %self.worker_id(),
                    "Lock already released or taken over by another worker"
                );
                Ok(self.store)
            }
            Err(e) => {
                // Other errors shouldn't fail the worker, just log
                tracing::warn!(
                    eoa = %self.eoa(),
                    chain_id = %self.chain_id(),
                    worker_id = %self.worker_id(),
                    error = %e,
                    "Failed to release EOA lock"
                );
                Ok(self.store)
            }
        }
    }

    /// Example of how to refactor a complex method using the helper to reduce boilerplate
    /// This shows the pattern for atomic_move_pending_to_borrowed_with_recycled_nonce
    pub async fn atomic_move_pending_to_borrowed_with_recycled_nonce(
        &self,
        transaction_id: &str,
        nonce: u64,
        prepared_tx: &BorrowedTransactionData,
    ) -> Result<(), TransactionStoreError> {
        let safe_tx = MovePendingToBorrowedWithRecycledNonce {
            recycled_key: self.recycled_nonces_set_name(),
            pending_key: self.pending_transactions_zset_name(),
            transaction_id: transaction_id.to_string(),
            borrowed_key: self.borrowed_transactions_hashmap_name(),
            nonce,
            prepared_tx_json: serde_json::to_string(prepared_tx)?,
        };

        self.execute_with_watch_and_retry(&safe_tx).await?;

        Ok(())
    }

    /// Atomically move specific transaction from pending to borrowed with new nonce allocation
    pub async fn atomic_move_pending_to_borrowed_with_new_nonce(
        &self,
        transaction_id: &str,
        expected_nonce: u64,
        prepared_tx: &BorrowedTransactionData,
    ) -> Result<(), TransactionStoreError> {
        let optimistic_key = self.optimistic_transaction_count_key_name();
        let borrowed_key = self.borrowed_transactions_hashmap_name();
        let pending_key = self.pending_transactions_zset_name();
        let prepared_tx_json = serde_json::to_string(prepared_tx)?;
        let transaction_id = transaction_id.to_string();

        self.execute_with_watch_and_retry(&MovePendingToBorrowedWithNewNonce {
            nonce: expected_nonce,
            prepared_tx_json,
            transaction_id,
            borrowed_key,
            optimistic_key,
            pending_key,
            eoa: self.eoa,
            chain_id: self.chain_id,
        })
        .await
    }

    /// Wrapper that executes operations with lock validation using WATCH/MULTI/EXEC
    pub async fn with_lock_check<F, T, R>(&self, operation: F) -> Result<T, TransactionStoreError>
    where
        F: Fn(&mut Pipeline) -> R,
        T: From<R>,
    {
        let lock_key = self.eoa_lock_key_name();
        let mut conn = self.redis.clone();
        let mut retry_count = 0;

        loop {
            if retry_count >= MAX_RETRIES {
                return Err(TransactionStoreError::InternalError {
                    message: format!(
                        "Exceeded max retries ({}) for lock check on {}:{}",
                        MAX_RETRIES,
                        self.eoa(),
                        self.chain_id()
                    ),
                });
            }

            // Exponential backoff after first retry
            if retry_count > 0 {
                let delay_ms = RETRY_BASE_DELAY_MS * (1 << (retry_count - 1).min(6));
                tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                tracing::debug!(
                    retry_count = retry_count,
                    delay_ms = delay_ms,
                    eoa = %self.eoa(),
                    chain_id = self.chain_id(),
                    "Retrying lock check operation"
                );
            }

            // WATCH the EOA lock
            let _: () = twmq::redis::cmd("WATCH")
                .arg(&lock_key)
                .query_async(&mut conn)
                .await?;

            // Check if we still own the lock
            let current_owner: Option<String> = conn.get(&lock_key).await?;
            match current_owner {
                Some(owner) if owner == self.worker_id() => {
                    // We still own it, proceed
                }
                _ => {
                    // Lost ownership - immediately fail
                    let _: () = twmq::redis::cmd("UNWATCH").query_async(&mut conn).await?;
                    return Err(self.eoa_lock_lost_error());
                }
            }

            // Build pipeline with operation
            let mut pipeline = twmq::redis::pipe();
            pipeline.atomic();
            let result = operation(&mut pipeline);

            // Execute with WATCH protection
            match pipeline
                .query_async::<Vec<twmq::redis::Value>>(&mut conn)
                .await
            {
                Ok(_) => return Ok(T::from(result)),
                Err(_) => {
                    // WATCH failed, check if it was our lock or someone else's
                    let still_own_lock: Option<String> = conn.get(&lock_key).await?;
                    if still_own_lock.as_deref() != Some(self.worker_id()) {
                        return Err(self.eoa_lock_lost_error());
                    }
                    // Our lock is fine, someone else's WATCH failed - retry
                    retry_count += 1;
                    continue;
                }
            }
        }
    }

    /// Helper to execute atomic operations with proper retry logic and watch handling
    ///
    /// This helper centralizes all the boilerplate for WATCH/MULTI/EXEC operations:
    /// - Retry logic with exponential backoff
    /// - Lock ownership validation
    /// - WATCH key management
    /// - Error handling and UNWATCH cleanup
    ///
    /// ## Usage:
    /// Implement the `SafeRedisTransaction` trait for your operation, then call this method.
    /// The trait separates validation (async) from pipeline operations (sync) for clean patterns.
    ///
    /// ## Example:
    /// ```rust
    /// let safe_tx = MovePendingToBorrowedWithNewNonce {
    ///     nonce: expected_nonce,
    ///     prepared_tx_json,
    ///     transaction_id,
    ///     borrowed_key,
    ///     optimistic_key,
    ///     pending_key,
    ///     eoa,
    ///     chain_id,
    /// };
    ///
    /// self.execute_with_watch_and_retry(, worker_id, &safe_tx).await?;
    /// ```
    ///
    /// ## When to use this helper:
    /// - Operations that implement `SafeRedisTransaction` trait
    /// - Need atomic WATCH/MULTI/EXEC with retry logic
    /// - Want centralized lock checking and error handling
    ///
    /// ## When NOT to use this helper:
    /// - Simple operations that can use `with_lock_check` instead
    /// - Operations that don't need WATCH on multiple keys
    /// - Read-only operations that don't modify state
    async fn execute_with_watch_and_retry<T: SafeRedisTransaction>(
        &self,
        safe_tx: &T,
    ) -> Result<T::OperationResult, TransactionStoreError> {
        let lock_key = self.eoa_lock_key_name();
        let mut conn = self.redis.clone();
        let mut retry_count = 0;

        loop {
            if retry_count >= MAX_RETRIES {
                return Err(TransactionStoreError::InternalError {
                    message: format!(
                        "Exceeded max retries ({}) for {} on {}:{}",
                        MAX_RETRIES,
                        safe_tx.name(),
                        self.eoa,
                        self.chain_id
                    ),
                });
            }

            // Exponential backoff after first retry
            if retry_count > 0 {
                let delay_ms = RETRY_BASE_DELAY_MS * (1 << (retry_count - 1).min(6));
                tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                tracing::debug!(
                    retry_count = retry_count,
                    delay_ms = delay_ms,
                    eoa = %self.eoa,
                    chain_id = self.chain_id,
                    operation = safe_tx.name(),
                    "Retrying atomic operation"
                );
            }

            // WATCH all specified keys including lock
            let mut watch_cmd = twmq::redis::cmd("WATCH");
            watch_cmd.arg(&lock_key);
            for key in safe_tx.watch_keys() {
                watch_cmd.arg(key);
            }
            let _: () = watch_cmd.query_async(&mut conn).await?;

            // Check lock ownership
            let current_owner: Option<String> = conn.get(&lock_key).await?;
            if current_owner.as_deref() != Some(self.worker_id()) {
                let _: () = twmq::redis::cmd("UNWATCH").query_async(&mut conn).await?;
                return Err(TransactionStoreError::LockLost {
                    eoa: self.eoa,
                    chain_id: self.chain_id,
                    worker_id: self.worker_id().to_string(),
                });
            }

            // Execute validation
            match safe_tx.validation(&mut conn).await {
                Ok(validation_data) => {
                    // Build and execute pipeline
                    let mut pipeline = twmq::redis::pipe();
                    pipeline.atomic();
                    let result = safe_tx.operation(&mut pipeline, validation_data);

                    match pipeline
                        .query_async::<Vec<twmq::redis::Value>>(&mut conn)
                        .await
                    {
                        Ok(_) => return Ok(result), // Success
                        Err(_) => {
                            // WATCH failed, check if it was our lock
                            let still_own_lock: Option<String> = conn.get(&lock_key).await?;
                            if still_own_lock.as_deref() != Some(self.worker_id()) {
                                return Err(TransactionStoreError::LockLost {
                                    eoa: self.eoa,
                                    chain_id: self.chain_id,
                                    worker_id: self.worker_id().to_string(),
                                });
                            }
                            // State changed, retry
                            retry_count += 1;
                            continue;
                        }
                    }
                }
                Err(e) => {
                    // Validation failed, unwatch and return error
                    let _: () = twmq::redis::cmd("UNWATCH").query_async(&mut conn).await?;
                    return Err(e);
                }
            }
        }
    }

    /// Atomically move borrowed transaction to submitted state
    /// Returns error if transaction not found in borrowed state
    pub async fn atomic_move_borrowed_to_submitted(
        &self,
        nonce: u64,
        hash: &str,
        transaction_id: &str,
    ) -> Result<(), TransactionStoreError> {
        let borrowed_key = self.borrowed_transactions_hashmap_name();
        let submitted_key = self.submitted_transactions_zset_name();
        let hash_to_id_key = self.transaction_hash_to_id_key_name(hash);
        let hash = hash.to_string();
        let transaction_id = transaction_id.to_string();

        self.execute_with_watch_and_retry(&MoveBorrowedToSubmitted {
            nonce,
            hash: hash.to_string(),
            transaction_id,
            borrowed_key,
            submitted_key,
            hash_to_id_key,
        })
        .await
    }

    /// Atomically move borrowed transaction back to recycled nonces and pending queue
    /// Returns error if transaction not found in borrowed state
    pub async fn atomic_move_borrowed_to_recycled(
        &self,
        nonce: u64,
        transaction_id: &str,
    ) -> Result<(), TransactionStoreError> {
        let borrowed_key = self.borrowed_transactions_hashmap_name();
        let recycled_key = self.recycled_nonces_set_name();
        let pending_key = self.pending_transactions_zset_name();
        let transaction_id = transaction_id.to_string();

        self.execute_with_watch_and_retry(&MoveBorrowedToRecycled {
            nonce,
            transaction_id,
            borrowed_key,
            recycled_key,
            pending_key,
        })
        .await
    }

    /// Update EOA health data
    pub async fn update_health_data(
        &self,
        health: &EoaHealth,
    ) -> Result<(), TransactionStoreError> {
        let health_json = serde_json::to_string(health)?;
        self.with_lock_check(|pipeline| {
            let health_key = self.eoa_health_key_name();
            pipeline.set(&health_key, &health_json);
        })
        .await
    }

    /// Synchronize nonces with the chain
    ///
    /// Part of standard nonce management flow, called in the confirm stage when chain nonce advances, and we need to update our cached nonce
    pub async fn update_cached_transaction_count(
        &self,
        current_chain_tx_count: u64,
    ) -> Result<(), TransactionStoreError> {
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;

        // First, read current health data
        let current_health = self.check_eoa_health().await?;

        // Prepare health update if health data exists
        let health_update = if let Some(mut health) = current_health {
            health.last_nonce_movement_at = now;
            health.last_confirmation_at = now;
            Some(serde_json::to_string(&health)?)
        } else {
            None
        };

        self.with_lock_check(|pipeline| {
            let tx_count_key = self.last_transaction_count_key_name();

            // Update cached transaction count
            pipeline.set(&tx_count_key, current_chain_tx_count);

            // Update health data only if it exists
            if let Some(ref health_json) = health_update {
                let health_key = self.eoa_health_key_name();
                pipeline.set(&health_key, health_json);
            }
        })
        .await
    }

    /// Add a gas bump attempt (new hash) to submitted transactions
    pub async fn add_gas_bump_attempt(
        &self,
        submitted_transaction: &SubmittedTransaction,
        signed_transaction: Signed<TypedTransaction>,
    ) -> Result<(), TransactionStoreError> {
        let new_hash = signed_transaction.hash().to_string();

        // Create new attempt
        let new_attempt = TransactionAttempt {
            transaction_id: submitted_transaction.transaction_id.clone(),
            details: signed_transaction,
            sent_at: chrono::Utc::now().timestamp_millis().max(0) as u64,
            attempt_number: 0, // Will be set correctly when reading all attempts
        };

        // Serialize the new attempt
        let attempt_json = serde_json::to_string(&new_attempt)?;

        // Get key names
        let attempts_list_key =
            self.transaction_attempts_list_name(&submitted_transaction.transaction_id);
        let submitted_key = self.submitted_transactions_zset_name();

        let hash_to_id_key = self.transaction_hash_to_id_key_name(&new_hash);

        let (submitted_transaction_string, nonce) =
            submitted_transaction.to_redis_string_with_nonce();

        // Now perform the atomic update
        self.with_lock_check(|pipeline| {
            // Add new hash:id to submitted (keeping old ones)
            pipeline.zadd(&submitted_key, &submitted_transaction_string, nonce);

            // Still maintain separate hash-to-ID mapping for backward compatibility
            pipeline.set(&hash_to_id_key, &submitted_transaction.transaction_id);

            // Simply push the new attempt to the attempts list
            pipeline.lpush(&attempts_list_key, &attempt_json);
        })
        .await
    }

    /// Reset nonces to specified value
    ///
    /// This is called when we have too many recycled nonces and detect something wrong
    /// We want to start fresh, with the chain nonce as the new optimistic nonce
    pub async fn reset_nonces(
        &self,
        current_chain_tx_count: u64,
    ) -> Result<(), TransactionStoreError> {
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;

        let current_health = self.check_eoa_health().await?;

        // Prepare health update if health data exists
        let health_update = if let Some(mut health) = current_health {
            health.nonce_resets.push(now);
            Some(serde_json::to_string(&health)?)
        } else {
            None
        };

        self.with_lock_check(|pipeline| {
            let optimistic_key = self.optimistic_transaction_count_key_name();
            let cached_nonce_key = self.last_transaction_count_key_name();
            let recycled_key = self.recycled_nonces_set_name();

            // Update health data only if it exists
            if let Some(ref health_json) = health_update {
                let health_key = self.eoa_health_key_name();
                pipeline.set(&health_key, health_json);
            }

            // Reset the optimistic nonce
            pipeline.set(&optimistic_key, current_chain_tx_count);

            // Reset the cached nonce
            pipeline.set(&cached_nonce_key, current_chain_tx_count);

            // Reset the recycled nonces
            pipeline.del(recycled_key);
        })
        .await
    }

    /// Fail a transaction that's in the borrowed state (we know the nonce)
    pub async fn fail_borrowed_transaction(
        &self,
        transaction_id: &str,
        nonce: u64,
        failure_reason: &str,
    ) -> Result<(), TransactionStoreError> {
        self.with_lock_check(|pipeline| {
            let borrowed_key = self.borrowed_transactions_hashmap_name();
            let tx_data_key = self.transaction_data_key_name(transaction_id);
            let now = chrono::Utc::now().timestamp_millis().max(0) as u64;

            // Remove from borrowed state using the known nonce
            pipeline.hdel(&borrowed_key, nonce.to_string());

            // Update transaction data with failure
            pipeline.hset(&tx_data_key, "completed_at", now);
            pipeline.hset(&tx_data_key, "failure_reason", failure_reason);
            pipeline.hset(&tx_data_key, "status", "failed");
        })
        .await
    }

    pub async fn clean_submitted_transactions(
        &self,
        confirmed_transactions: &[ConfirmedTransaction],
        last_confirmed_nonce: u64,
    ) -> Result<CleanupReport, TransactionStoreError> {
        self.execute_with_watch_and_retry(&CleanSubmittedTransactions {
            confirmed_transactions,
            last_confirmed_nonce,
            keys: &self.keys,
        })
        .await
    }
}
