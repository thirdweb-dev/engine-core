use std::sync::Arc;

use alloy::{
    consensus::{Signed, TypedTransaction},
    primitives::Address,
};
use twmq::redis::{AsyncCommands, Pipeline, aio::ConnectionManager};

use crate::{
    eoa::{
        EoaExecutorStore,
        events::EoaExecutorEvent,
        store::{
            BorrowedTransactionData, ConfirmedTransaction, EoaHealth, PendingTransaction,
            SubmittedTransactionDehydrated, TransactionAttempt, TransactionStoreError,
            borrowed::{BorrowedProcessingReport, ProcessBorrowedTransactions, SubmissionResult},
            pending::{
                MovePendingToBorrowedWithIncrementedNonces, MovePendingToBorrowedWithRecycledNonces,
            },
            submitted::{
                CleanAndGetRecycledNonces, CleanSubmittedTransactions, CleanupReport,
                SubmittedNoopTransaction,
            },
        },
        worker::error::EoaExecutorWorkerError,
    },
    webhook::{WebhookJobHandler, queue_webhook_envelopes},
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
        store: &EoaExecutorStore,
    ) -> impl Future<Output = Result<Self::ValidationData, TransactionStoreError>> + Send;
    fn watch_keys(&self) -> Vec<String>;
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

    /// Atomically move multiple pending transactions to borrowed state using incremented nonces
    ///
    /// The transactions must have sequential nonces starting from the current optimistic count.
    /// This operation validates nonce ordering and atomically moves all transactions.
    pub async fn atomic_move_pending_to_borrowed_with_incremented_nonces(
        &self,
        transactions: &[BorrowedTransactionData],
    ) -> Result<usize, TransactionStoreError> {
        self.execute_with_watch_and_retry(&MovePendingToBorrowedWithIncrementedNonces {
            transactions,
            keys: &self.keys,
            eoa: self.eoa,
            chain_id: self.chain_id,
        })
        .await
    }

    /// Atomically move multiple pending transactions to borrowed state using recycled nonces
    ///
    /// All nonces must exist in the recycled nonces set. This operation validates nonce
    /// availability and atomically moves all transactions.
    pub async fn atomic_move_pending_to_borrowed_with_recycled_nonces(
        &self,
        transactions: &[BorrowedTransactionData],
    ) -> Result<usize, TransactionStoreError> {
        self.execute_with_watch_and_retry(&MovePendingToBorrowedWithRecycledNonces {
            transactions,
            keys: &self.keys,
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
            match safe_tx.validation(&mut conn, &self.store).await {
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
                        Err(e) => {
                            tracing::error!("WATCH failed: {}", e);
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
        let current_health = self.get_eoa_health().await?;

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
        submitted_transaction: &SubmittedTransactionDehydrated,
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

        let current_health = self.get_eoa_health().await?;

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
            let recycled_key = self.recycled_nonces_zset_name();

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

    /// Fail a transaction that's in the pending state (remove from pending and fail)
    /// This is used for deterministic failures during preparation that should not retry
    pub async fn fail_pending_transaction(
        &self,
        pending_transaction: &PendingTransaction,
        error: EoaExecutorWorkerError,
        webhook_queue: Arc<twmq::Queue<WebhookJobHandler>>,
    ) -> Result<(), TransactionStoreError> {
        self.with_lock_check(|pipeline| {
            let pending_key = self.pending_transactions_zset_name();
            let tx_data_key = self.transaction_data_key_name(&pending_transaction.transaction_id);
            let now = chrono::Utc::now().timestamp_millis().max(0) as u64;

            // Remove from pending state
            pipeline.zrem(&pending_key, &pending_transaction.transaction_id);

            // Update transaction data with failure
            pipeline.hset(&tx_data_key, "completed_at", now);
            pipeline.hset(&tx_data_key, "failure_reason", error.to_string());
            pipeline.hset(&tx_data_key, "status", "failed");

            let event = EoaExecutorEvent {
                transaction_id: pending_transaction.transaction_id.clone(),
                address: pending_transaction.user_request.from,
            };

            let fail_envelope = event.transaction_failed_envelope(error.clone(), 1);

            if !pending_transaction.user_request.webhook_options.is_empty() {
                let mut tx_context = webhook_queue.transaction_context_from_pipeline(pipeline);
                if let Err(e) = queue_webhook_envelopes(
                    fail_envelope,
                    pending_transaction.user_request.webhook_options.clone(),
                    &mut tx_context,
                    webhook_queue.clone(),
                ) {
                    tracing::error!("Failed to queue webhook for fail: {}", e);
                }
            }
        })
        .await
    }

    pub async fn clean_submitted_transactions(
        &self,
        confirmed_transactions: &[ConfirmedTransaction],
        last_confirmed_nonce: u64,
        webhook_queue: Arc<twmq::Queue<WebhookJobHandler>>,
    ) -> Result<CleanupReport, TransactionStoreError> {
        self.execute_with_watch_and_retry(&CleanSubmittedTransactions {
            confirmed_transactions,
            last_confirmed_nonce,
            keys: &self.keys,
            webhook_queue,
        })
        .await
    }

    /// Process borrowed transactions with given submission results
    /// This method moves transactions from borrowed state to submitted/pending/failed states
    /// based on the submission results, and queues appropriate webhook events
    pub async fn process_borrowed_transactions(
        &self,
        results: Vec<SubmissionResult>,
        webhook_queue: Arc<twmq::Queue<WebhookJobHandler>>,
    ) -> Result<BorrowedProcessingReport, TransactionStoreError> {
        self.execute_with_watch_and_retry(&ProcessBorrowedTransactions {
            results,
            keys: &self.keys,
            webhook_queue,
        })
        .await
    }

    pub async fn clean_and_get_recycled_nonces(&self) -> Result<Vec<u64>, TransactionStoreError> {
        self.execute_with_watch_and_retry(&CleanAndGetRecycledNonces { keys: &self.keys })
            .await
    }

    pub async fn process_noop_transactions(
        &self,
        noop_transactions: &[SubmittedNoopTransaction],
    ) -> Result<(), TransactionStoreError> {
        self.with_lock_check(|pipeline| {
            let recycled_key = self.recycled_nonces_zset_name();
            let submitted_key = self.submitted_transactions_zset_name();

            pipeline.zrem(
                &recycled_key,
                noop_transactions
                    .iter()
                    .map(|tx| tx.nonce)
                    .collect::<Vec<_>>(),
            );

            pipeline.zadd_multiple(
                submitted_key,
                &noop_transactions
                    .iter()
                    .map(|tx| {
                        let (tx_string, nonce) = tx.to_redis_string_with_nonce();
                        (nonce, tx_string)
                    })
                    .collect::<Vec<_>>(),
            );
        })
        .await
    }
}
