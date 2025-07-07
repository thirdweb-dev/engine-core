use alloy::consensus::{Signed, Transaction, TypedTransaction};
use alloy::network::AnyTransactionReceipt;
use alloy::primitives::{Address, B256, Bytes, U256};
use chrono;
use engine_core::chain::RpcCredentials;
use engine_core::credentials::SigningCredential;
use engine_core::execution_options::WebhookOptions;
use engine_core::execution_options::eoa::EoaTransactionTypeData;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use twmq::redis::{AsyncCommands, Pipeline, aio::ConnectionManager};

pub trait SafeRedisTransaction: Send + Sync {
    fn name(&self) -> &str;
    fn operation(&self, pipeline: &mut Pipeline);
    fn validation(
        &self,
        conn: &mut ConnectionManager,
    ) -> impl Future<Output = Result<(), TransactionStoreError>> + Send;
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
    fn name(&self) -> &str {
        "pending->borrowed with recycled nonce"
    }

    fn operation(&self, pipeline: &mut Pipeline) {
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
    fn name(&self) -> &str {
        "pending->borrowed with new nonce"
    }

    fn operation(&self, pipeline: &mut Pipeline) {
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
    fn name(&self) -> &str {
        "borrowed->submitted"
    }

    fn operation(&self, pipeline: &mut Pipeline) {
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
    fn name(&self) -> &str {
        "borrowed->recycled"
    }

    fn operation(&self, pipeline: &mut Pipeline) {
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

/// The actual user request data
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EoaTransactionRequest {
    pub transaction_id: String,
    pub chain_id: u64,

    pub from: Address,
    pub to: Option<Address>,
    pub value: U256,
    pub data: Bytes,

    #[serde(alias = "gas")]
    pub gas_limit: Option<u64>,

    pub webhook_options: Option<Vec<WebhookOptions>>,

    pub signing_credential: SigningCredential,
    pub rpc_credentials: RpcCredentials,

    #[serde(flatten)]
    pub transaction_type_data: Option<EoaTransactionTypeData>,
}

/// Active attempt for a transaction (full alloy transaction + metadata)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionAttempt {
    pub transaction_id: String,
    pub details: Signed<TypedTransaction>,
    pub sent_at: u64, // Unix timestamp in milliseconds
    pub attempt_number: u32,
}

/// Transaction data for a transaction_id
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionData {
    pub transaction_id: String,
    pub user_request: EoaTransactionRequest,
    pub receipt: Option<AnyTransactionReceipt>,
    pub attempts: Vec<TransactionAttempt>,
}

pub struct BorrowedTransaction {
    pub transaction_id: String,
    pub data: Signed<TypedTransaction>,
    pub borrowed_at: chrono::DateTime<chrono::Utc>,
}

/// Transaction store focused on transaction_id operations and nonce indexing
pub struct EoaExecutorStore {
    pub redis: ConnectionManager,
    pub namespace: Option<String>,
}

impl EoaExecutorStore {
    pub fn new(redis: ConnectionManager, namespace: Option<String>) -> Self {
        Self { redis, namespace }
    }

    /// Name of the key for the transaction data
    ///
    /// Transaction data is stored as a Redis HSET with the following fields:
    /// - "user_request": JSON string containing EoaTransactionRequest
    /// - "receipt": JSON string containing AnyTransactionReceipt (optional)
    /// - "status": String status ("confirmed", "failed", etc.)
    /// - "completed_at": String Unix timestamp (optional)
    fn transaction_data_key_name(&self, transaction_id: &str) -> String {
        match &self.namespace {
            Some(ns) => format!("{ns}:eoa_executor:tx_data:{transaction_id}"),
            None => format!("eoa_tx_data:{transaction_id}"),
        }
    }

    /// Name of the list for transaction attempts
    ///
    /// Attempts are stored as a separate Redis LIST where each element is a JSON blob
    /// of a TransactionAttempt. This allows efficient append operations.
    fn transaction_attempts_list_name(&self, transaction_id: &str) -> String {
        match &self.namespace {
            Some(ns) => format!("{ns}:eoa_executor:tx_attempts:{transaction_id}"),
            None => format!("eoa_executor:tx_attempts:{transaction_id}"),
        }
    }

    /// Name of the list for pending transactions
    fn pending_transactions_list_name(&self, eoa: Address, chain_id: u64) -> String {
        match &self.namespace {
            Some(ns) => format!("{ns}:eoa_executor:pending_txs:{chain_id}:{eoa}"),
            None => format!("eoa_executor:pending_txs:{chain_id}:{eoa}"),
        }
    }

    /// Name of the zset for submitted transactions. nonce -> hash:id
    /// Same transaction might appear multiple times in the zset with different nonces/gas prices (and thus different hashes)
    fn submitted_transactions_zset_name(&self, eoa: Address, chain_id: u64) -> String {
        match &self.namespace {
            Some(ns) => format!("{ns}:eoa_executor:submitted_txs:{chain_id}:{eoa}"),
            None => format!("eoa_executor:submitted_txs:{chain_id}:{eoa}"),
        }
    }

    /// Name of the key that maps transaction hash to transaction id
    fn transaction_hash_to_id_key_name(&self, hash: &str) -> String {
        match &self.namespace {
            Some(ns) => format!("{ns}:eoa_executor:tx_hash_to_id:{hash}"),
            None => format!("eoa_executor:tx_hash_to_id:{hash}"),
        }
    }

    /// Name of the hashmap that maps `transaction_id` -> `BorrowedTransactionData`
    ///
    /// This is used for crash recovery. Before submitting a transaction, we atomically move from pending to this borrowed hashmap.
    ///
    /// On worker recovery, if any borrowed transactions are found, we rebroadcast them and move back to pending or submitted
    ///
    /// If there's no crash, happy path moves borrowed transactions back to pending or submitted
    fn borrowed_transactions_hashmap_name(&self, eoa: Address, chain_id: u64) -> String {
        match &self.namespace {
            Some(ns) => format!("{ns}:eoa_executor:borrowed_txs:{chain_id}:{eoa}"),
            None => format!("eoa_executor:borrowed_txs:{chain_id}:{eoa}"),
        }
    }

    /// Name of the set that contains recycled nonces.
    ///
    /// If a transaction was submitted but failed (ie, we know with certainty it didn't enter the mempool),
    ///
    /// we add the nonce to this set.
    ///
    /// These nonces are used with priority, before any other nonces.
    fn recycled_nonces_set_name(&self, eoa: Address, chain_id: u64) -> String {
        match &self.namespace {
            Some(ns) => format!("{ns}:eoa_executor:recycled_nonces:{chain_id}:{eoa}"),
            None => format!("eoa_executor:recycled_nonces:{chain_id}:{eoa}"),
        }
    }

    /// Optimistic nonce key name.
    ///
    /// This is used for optimistic nonce tracking.
    ///
    /// We store the nonce of the last successfuly sent transaction for each EOA.
    ///
    /// We increment this nonce for each new transaction.
    ///
    /// !IMPORTANT! When sending a transaction, we use this nonce as the assigned nonce, NOT the incremented nonce.
    fn optimistic_transaction_count_key_name(&self, eoa: Address, chain_id: u64) -> String {
        match &self.namespace {
            Some(ns) => format!("{ns}:eoa_executor:optimistic_nonce:{chain_id}:{eoa}"),
            None => format!("eoa_executor:optimistic_nonce:{chain_id}:{eoa}"),
        }
    }

    /// Name of the key that contains the nonce of the last fetched ONCHAIN transaction count for each EOA.
    ///
    /// This is a cache for the actual transaction count, which is fetched from the RPC.
    ///
    /// The nonce for the NEXT transaction is the ONCHAIN transaction count (NOT + 1)
    ///
    /// Eg: transaction count is 0, so we use nonce 0 for sending the next transaction. Once successful, transaction count will be 1.
    fn last_transaction_count_key_name(&self, eoa: Address, chain_id: u64) -> String {
        match &self.namespace {
            Some(ns) => format!("{ns}:eoa_executor:last_tx_nonce:{chain_id}:{eoa}"),
            None => format!("eoa_executor:last_tx_nonce:{chain_id}:{eoa}"),
        }
    }

    /// EOA health key name.
    ///
    /// EOA health stores:
    /// - cached balance, the timestamp of the last balance fetch
    /// - timestamp of the last successful transaction confirmation
    /// - timestamp of the last 5 nonce resets
    fn eoa_health_key_name(&self, eoa: Address, chain_id: u64) -> String {
        match &self.namespace {
            Some(ns) => format!("{ns}:eoa_executor:health:{chain_id}:{eoa}"),
            None => format!("eoa_executor:health:{chain_id}:{eoa}"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EoaHealth {
    pub balance: U256,
    /// Update the balance threshold when we see out of funds errors
    pub balance_threshold: U256,
    pub balance_fetched_at: u64,
    pub last_confirmation_at: u64,
    pub last_nonce_movement_at: u64, // Track when nonce last moved for gas bump detection
    pub nonce_resets: Vec<u64>,      // Last 5 reset timestamps
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BorrowedTransactionData {
    pub transaction_id: String,
    pub signed_transaction: Signed<TypedTransaction>,
    pub hash: String,
    pub borrowed_at: u64,
}

/// Type of nonce allocation for transaction processing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NonceType {
    /// Nonce was recycled from a previously failed transaction
    Recycled(u64),
    /// Nonce was incremented from the current optimistic counter
    Incremented(u64),
}

impl NonceType {
    /// Get the nonce value regardless of type
    pub fn nonce(&self) -> u64 {
        match self {
            NonceType::Recycled(nonce) => *nonce,
            NonceType::Incremented(nonce) => *nonce,
        }
    }

    /// Check if this is a recycled nonce
    pub fn is_recycled(&self) -> bool {
        matches!(self, NonceType::Recycled(_))
    }

    /// Check if this is an incremented nonce
    pub fn is_incremented(&self) -> bool {
        matches!(self, NonceType::Incremented(_))
    }
}

impl EoaExecutorStore {
    // ========== BOILERPLATE REDUCTION PATTERN ==========
    //
    // This implementation uses a helper method `execute_with_watch_and_retry` to reduce
    // boilerplate in atomic Redis operations. The pattern separates:
    // 1. Validation phase: async closure that checks preconditions
    // 2. Pipeline phase: sync closure that builds Redis commands
    //
    // Benefits:
    // - Eliminates ~80 lines of boilerplate per method
    // - Centralizes retry logic, lock checking, and error handling
    // - Makes individual methods focus on business logic
    // - Reduces chance of bugs in WATCH/MULTI/EXEC handling
    //
    // See examples in:
    // - atomic_move_pending_to_borrowed_with_recycled_nonce_v2()
    // - atomic_move_pending_to_borrowed_with_new_nonce()
    // - move_borrowed_to_submitted()
    // - move_borrowed_to_recycled()

    /// Aggressively acquire EOA lock, forcefully taking over from stalled workers
    pub async fn acquire_eoa_lock_aggressively(
        &self,
        eoa: Address,
        chain_id: u64,
        worker_id: &str,
    ) -> Result<(), TransactionStoreError> {
        let lock_key = self.eoa_lock_key_name(eoa, chain_id);
        let mut conn = self.redis.clone();

        // First try normal acquisition
        let acquired: bool = conn.set_nx(&lock_key, worker_id).await?;
        if acquired {
            return Ok(());
        }
        // Lock exists, forcefully take it over
        tracing::warn!(
            eoa = %eoa,
            chain_id = %chain_id,
            worker_id = %worker_id,
            "Forcefully taking over EOA lock from stalled worker"
        );
        // Force set - no expiry, only released by explicit takeover
        let _: () = conn.set(&lock_key, worker_id).await?;
        Ok(())
    }

    /// Release EOA lock following the spec's finally pattern
    pub async fn release_eoa_lock(
        &self,
        eoa: Address,
        chain_id: u64,
        worker_id: &str,
    ) -> Result<(), TransactionStoreError> {
        // Use existing utility method that handles all the atomic lock checking
        match self
            .with_lock_check(eoa, chain_id, worker_id, |pipeline| {
                let lock_key = self.eoa_lock_key_name(eoa, chain_id);
                pipeline.del(&lock_key);
            })
            .await
        {
            Ok(()) => {
                tracing::debug!(
                    eoa = %eoa,
                    chain_id = %chain_id,
                    worker_id = %worker_id,
                    "Successfully released EOA lock"
                );
                Ok(())
            }
            Err(TransactionStoreError::LockLost { .. }) => {
                // Lock was already taken over, which is fine for release
                tracing::debug!(
                    eoa = %eoa,
                    chain_id = %chain_id,
                    worker_id = %worker_id,
                    "Lock already released or taken over by another worker"
                );
                Ok(())
            }
            Err(e) => {
                // Other errors shouldn't fail the worker, just log
                tracing::warn!(
                    eoa = %eoa,
                    chain_id = %chain_id,
                    worker_id = %worker_id,
                    error = %e,
                    "Failed to release EOA lock"
                );
                Ok(())
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
    /// self.execute_with_watch_and_retry(eoa, chain_id, worker_id, &safe_tx).await?;
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
    async fn execute_with_watch_and_retry(
        &self,
        eoa: Address,
        chain_id: u64,
        worker_id: &str,
        safe_tx: &impl SafeRedisTransaction,
    ) -> Result<(), TransactionStoreError> {
        let lock_key = self.eoa_lock_key_name(eoa, chain_id);
        let mut conn = self.redis.clone();
        let mut retry_count = 0;

        loop {
            if retry_count >= MAX_RETRIES {
                return Err(TransactionStoreError::InternalError {
                    message: format!(
                        "Exceeded max retries ({}) for {} on {}:{}",
                        MAX_RETRIES,
                        safe_tx.name(),
                        eoa,
                        chain_id
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
                    eoa = %eoa,
                    chain_id = chain_id,
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
            if current_owner.as_deref() != Some(worker_id) {
                let _: () = twmq::redis::cmd("UNWATCH").query_async(&mut conn).await?;
                return Err(TransactionStoreError::LockLost {
                    eoa,
                    chain_id,
                    worker_id: worker_id.to_string(),
                });
            }

            // Execute validation
            match safe_tx.validation(&mut conn).await {
                Ok(()) => {
                    // Build and execute pipeline
                    let mut pipeline = twmq::redis::pipe();
                    pipeline.atomic();
                    safe_tx.operation(&mut pipeline);

                    match pipeline
                        .query_async::<Vec<twmq::redis::Value>>(&mut conn)
                        .await
                    {
                        Ok(_) => return Ok(()), // Success
                        Err(_) => {
                            // WATCH failed, check if it was our lock
                            let still_own_lock: Option<String> = conn.get(&lock_key).await?;
                            if still_own_lock.as_deref() != Some(worker_id) {
                                return Err(TransactionStoreError::LockLost {
                                    eoa,
                                    chain_id,
                                    worker_id: worker_id.to_string(),
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

    /// Example of how to refactor a complex method using the helper to reduce boilerplate
    /// This shows the pattern for atomic_move_pending_to_borrowed_with_recycled_nonce
    pub async fn atomic_move_pending_to_borrowed_with_recycled_nonce(
        &self,
        eoa: Address,
        chain_id: u64,
        worker_id: &str,
        transaction_id: &str,
        nonce: u64,
        prepared_tx: &BorrowedTransactionData,
    ) -> Result<(), TransactionStoreError> {
        let safe_tx = MovePendingToBorrowedWithRecycledNonce {
            recycled_key: self.recycled_nonces_set_name(eoa, chain_id),
            pending_key: self.pending_transactions_list_name(eoa, chain_id),
            transaction_id: transaction_id.to_string(),
            borrowed_key: self.borrowed_transactions_hashmap_name(eoa, chain_id),
            nonce,
            prepared_tx_json: serde_json::to_string(prepared_tx)?,
        };

        self.execute_with_watch_and_retry(eoa, chain_id, worker_id, &safe_tx)
            .await?;

        Ok(())
    }

    /// Atomically move specific transaction from pending to borrowed with new nonce allocation
    pub async fn atomic_move_pending_to_borrowed_with_new_nonce(
        &self,
        eoa: Address,
        chain_id: u64,
        worker_id: &str,
        transaction_id: &str,
        expected_nonce: u64,
        prepared_tx: &BorrowedTransactionData,
    ) -> Result<(), TransactionStoreError> {
        let optimistic_key = self.optimistic_transaction_count_key_name(eoa, chain_id);
        let borrowed_key = self.borrowed_transactions_hashmap_name(eoa, chain_id);
        let pending_key = self.pending_transactions_list_name(eoa, chain_id);
        let prepared_tx_json = serde_json::to_string(prepared_tx)?;
        let transaction_id = transaction_id.to_string();

        self.execute_with_watch_and_retry(
            eoa,
            chain_id,
            worker_id,
            &MovePendingToBorrowedWithNewNonce {
                nonce: expected_nonce,
                prepared_tx_json,
                transaction_id,
                borrowed_key,
                optimistic_key,
                pending_key,
                eoa,
                chain_id,
            },
        )
        .await
    }

    /// Generic helper that handles WATCH + retry logic for atomic operations
    /// The operation closure receives a mutable connection and should:
    /// 1. Perform any validation (return early errors if needed)
    /// 2. Build and execute the pipeline
    /// 3. Return the result
    pub async fn with_atomic_operation<F, Fut, T>(
        &self,
        eoa: Address,
        chain_id: u64,
        worker_id: &str,
        watch_keys: Vec<String>,
        operation_name: &str,
        operation: F,
    ) -> Result<T, TransactionStoreError>
    where
        F: Fn(&mut ConnectionManager) -> Fut,
        Fut: std::future::Future<Output = Result<T, TransactionStoreError>>,
    {
        let lock_key = self.eoa_lock_key_name(eoa, chain_id);
        let mut conn = self.redis.clone();
        let mut retry_count = 0;

        loop {
            if retry_count >= MAX_RETRIES {
                return Err(TransactionStoreError::InternalError {
                    message: format!(
                        "Exceeded max retries ({}) for {} on {}:{}",
                        MAX_RETRIES, operation_name, eoa, chain_id
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
                    eoa = %eoa,
                    chain_id = chain_id,
                    operation = operation_name,
                    "Retrying atomic operation"
                );
            }

            // WATCH all specified keys (lock is always included)
            let mut watch_cmd = twmq::redis::cmd("WATCH");
            watch_cmd.arg(&lock_key);
            for key in &watch_keys {
                watch_cmd.arg(key);
            }
            let _: () = watch_cmd.query_async(&mut conn).await?;

            // Check if we still own the lock
            let current_owner: Option<String> = conn.get(&lock_key).await?;
            match current_owner {
                Some(owner) if owner == worker_id => {
                    // We still own it, proceed
                }
                _ => {
                    // Lost ownership - immediately fail
                    let _: () = twmq::redis::cmd("UNWATCH").query_async(&mut conn).await?;
                    return Err(TransactionStoreError::LockLost {
                        eoa,
                        chain_id,
                        worker_id: worker_id.to_string(),
                    });
                }
            }

            // Execute operation (includes validation and pipeline execution)
            match operation(&mut conn).await {
                Ok(result) => return Ok(result),
                Err(TransactionStoreError::LockLost { .. }) => {
                    // Lock was lost during operation, propagate immediately
                    return Err(TransactionStoreError::LockLost {
                        eoa,
                        chain_id,
                        worker_id: worker_id.to_string(),
                    });
                }
                Err(TransactionStoreError::WatchFailed) => {
                    // WATCH failed, check if it was our lock
                    let still_own_lock: Option<String> = conn.get(&lock_key).await?;
                    if still_own_lock.as_deref() != Some(worker_id) {
                        return Err(TransactionStoreError::LockLost {
                            eoa,
                            chain_id,
                            worker_id: worker_id.to_string(),
                        });
                    }
                    // Our lock is fine, retry
                    retry_count += 1;
                    continue;
                }
                Err(other_error) => {
                    // Other errors propagate immediately (validation failures, etc.)
                    let _: () = twmq::redis::cmd("UNWATCH").query_async(&mut conn).await?;
                    return Err(other_error);
                }
            }
        }
    }

    /// Wrapper that executes operations with lock validation using WATCH/MULTI/EXEC
    pub async fn with_lock_check<F, T, R>(
        &self,
        eoa: Address,
        chain_id: u64,
        worker_id: &str,
        operation: F,
    ) -> Result<T, TransactionStoreError>
    where
        F: Fn(&mut Pipeline) -> R,
        T: From<R>,
    {
        let lock_key = self.eoa_lock_key_name(eoa, chain_id);
        let mut conn = self.redis.clone();
        let mut retry_count = 0;

        loop {
            if retry_count >= MAX_RETRIES {
                return Err(TransactionStoreError::InternalError {
                    message: format!(
                        "Exceeded max retries ({}) for lock check on {}:{}",
                        MAX_RETRIES, eoa, chain_id
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
                    eoa = %eoa,
                    chain_id = chain_id,
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
                Some(owner) if owner == worker_id => {
                    // We still own it, proceed
                }
                _ => {
                    // Lost ownership - immediately fail
                    let _: () = twmq::redis::cmd("UNWATCH").query_async(&mut conn).await?;
                    return Err(TransactionStoreError::LockLost {
                        eoa,
                        chain_id,
                        worker_id: worker_id.to_string(),
                    });
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
                    if still_own_lock.as_deref() != Some(worker_id) {
                        return Err(TransactionStoreError::LockLost {
                            eoa,
                            chain_id,
                            worker_id: worker_id.to_string(),
                        });
                    }
                    // Our lock is fine, someone else's WATCH failed - retry
                    retry_count += 1;
                    continue;
                }
            }
        }
    }

    // ========== ATOMIC OPERATIONS ==========

    /// Peek all borrowed transactions without removing them
    pub async fn peek_borrowed_transactions(
        &self,
        eoa: Address,
        chain_id: u64,
    ) -> Result<Vec<BorrowedTransactionData>, TransactionStoreError> {
        let borrowed_key = self.borrowed_transactions_hashmap_name(eoa, chain_id);
        let mut conn = self.redis.clone();

        let borrowed_map: HashMap<String, String> = conn.hgetall(&borrowed_key).await?;
        let mut result = Vec::new();

        for (_nonce_str, transaction_json) in borrowed_map {
            let borrowed_data: BorrowedTransactionData = serde_json::from_str(&transaction_json)?;
            result.push(borrowed_data);
        }

        Ok(result)
    }

    /// Atomically move borrowed transaction to submitted state
    /// Returns error if transaction not found in borrowed state
    pub async fn move_borrowed_to_submitted(
        &self,
        eoa: Address,
        chain_id: u64,
        worker_id: &str,
        nonce: u64,
        hash: &str,
        transaction_id: &str,
    ) -> Result<(), TransactionStoreError> {
        let borrowed_key = self.borrowed_transactions_hashmap_name(eoa, chain_id);
        let submitted_key = self.submitted_transactions_zset_name(eoa, chain_id);
        let hash_to_id_key = self.transaction_hash_to_id_key_name(hash);
        let hash = hash.to_string();
        let transaction_id = transaction_id.to_string();

        self.execute_with_watch_and_retry(
            eoa,
            chain_id,
            worker_id,
            &MoveBorrowedToSubmitted {
                nonce,
                hash: hash.to_string(),
                transaction_id,
                borrowed_key,
                submitted_key,
                hash_to_id_key,
            },
        )
        .await
    }

    /// Atomically move borrowed transaction back to recycled nonces and pending queue
    /// Returns error if transaction not found in borrowed state
    pub async fn move_borrowed_to_recycled(
        &self,
        eoa: Address,
        chain_id: u64,
        worker_id: &str,
        nonce: u64,
        transaction_id: &str,
    ) -> Result<(), TransactionStoreError> {
        let borrowed_key = self.borrowed_transactions_hashmap_name(eoa, chain_id);
        let recycled_key = self.recycled_nonces_set_name(eoa, chain_id);
        let pending_key = self.pending_transactions_list_name(eoa, chain_id);
        let transaction_id = transaction_id.to_string();

        self.execute_with_watch_and_retry(
            eoa,
            chain_id,
            worker_id,
            &MoveBorrowedToRecycled {
                nonce,
                transaction_id,
                borrowed_key,
                recycled_key,
                pending_key,
            },
        )
        .await
    }

    /// Get all hashes below a certain nonce from submitted transactions
    /// Returns (nonce, hash, transaction_id) tuples
    pub async fn get_hashes_below_nonce(
        &self,
        eoa: Address,
        chain_id: u64,
        below_nonce: u64,
    ) -> Result<Vec<(u64, String, String)>, TransactionStoreError> {
        let submitted_key = self.submitted_transactions_zset_name(eoa, chain_id);
        let mut conn = self.redis.clone();

        // Get all entries with nonce < below_nonce
        let results: Vec<(String, u64)> = conn
            .zrangebyscore_withscores(&submitted_key, 0, below_nonce - 1)
            .await?;

        let mut parsed_results = Vec::new();
        for (hash_id_value, nonce) in results {
            // Parse hash:id format
            if let Some((hash, transaction_id)) = hash_id_value.split_once(':') {
                parsed_results.push((nonce, hash.to_string(), transaction_id.to_string()));
            } else {
                // Fallback for old format (just hash) - look up transaction ID
                if let Some(transaction_id) =
                    self.get_transaction_id_for_hash(&hash_id_value).await?
                {
                    parsed_results.push((nonce, hash_id_value, transaction_id));
                }
            }
        }

        Ok(parsed_results)
    }

    /// Get all transaction IDs for a specific nonce
    pub async fn get_transaction_ids_for_nonce(
        &self,
        eoa: Address,
        chain_id: u64,
        nonce: u64,
    ) -> Result<Vec<String>, TransactionStoreError> {
        let submitted_key = self.submitted_transactions_zset_name(eoa, chain_id);
        let mut conn = self.redis.clone();

        // Get all members with the exact nonce
        let members: Vec<String> = conn
            .zrangebyscore(&submitted_key, nonce, nonce)
            .await
            .map_err(|e| TransactionStoreError::RedisError {
                message: format!("Failed to get transaction IDs for nonce {}: {}", nonce, e),
            })?;

        let mut transaction_ids = Vec::new();
        for value in members {
            // Parse the value as hash:id format, with fallback to old format
            if let Some((_, transaction_id)) = value.split_once(':') {
                // New format: hash:id
                transaction_ids.push(transaction_id.to_string());
            } else {
                // Old format: just hash - look up transaction ID
                if let Some(transaction_id) = self.get_transaction_id_for_hash(&value).await? {
                    transaction_ids.push(transaction_id);
                }
            }
        }

        Ok(transaction_ids)
    }

    /// Remove all hashes for a transaction and requeue it
    /// Returns error if no hashes found for this transaction in submitted state
    /// NOTE: This method keeps the original boilerplate pattern because it needs to pass
    /// complex data (transaction_hashes) from validation to pipeline phase.
    /// The helper pattern works best for simple validation that doesn't need to pass data.
    pub async fn fail_and_requeue_transaction(
        &self,
        eoa: Address,
        chain_id: u64,
        worker_id: &str,
        transaction_id: &str,
    ) -> Result<(), TransactionStoreError> {
        let submitted_key = self.submitted_transactions_zset_name(eoa, chain_id);
        let pending_key = self.pending_transactions_list_name(eoa, chain_id);
        let lock_key = self.eoa_lock_key_name(eoa, chain_id);
        let mut conn = self.redis.clone();
        let mut retry_count = 0;

        loop {
            if retry_count >= MAX_RETRIES {
                return Err(TransactionStoreError::InternalError {
                    message: format!(
                        "Exceeded max retries ({}) for fail and requeue transaction {}:{} tx:{}",
                        MAX_RETRIES, eoa, chain_id, transaction_id
                    ),
                });
            }

            if retry_count > 0 {
                let delay_ms = RETRY_BASE_DELAY_MS * (1 << (retry_count - 1).min(6));
                tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
            }

            // WATCH lock and submitted state
            let _: () = twmq::redis::cmd("WATCH")
                .arg(&lock_key)
                .arg(&submitted_key)
                .query_async(&mut conn)
                .await?;

            // Check lock ownership
            let current_owner: Option<String> = conn.get(&lock_key).await?;
            if current_owner.as_deref() != Some(worker_id) {
                let _: () = twmq::redis::cmd("UNWATCH").query_async(&mut conn).await?;
                return Err(TransactionStoreError::LockLost {
                    eoa,
                    chain_id,
                    worker_id: worker_id.to_string(),
                });
            }

            // Find all hashes for this transaction that actually exist in submitted
            let all_hash_id_values: Vec<String> = conn.zrange(&submitted_key, 0, -1).await?;
            let mut transaction_hashes = Vec::new();

            for hash_id_value in all_hash_id_values {
                // Parse hash:id format
                if let Some((hash, tx_id)) = hash_id_value.split_once(':') {
                    if tx_id == transaction_id {
                        transaction_hashes.push(hash.to_string());
                    }
                } else {
                    // Fallback for old format (just hash) - look up transaction ID
                    if let Some(tx_id) = self.get_transaction_id_for_hash(&hash_id_value).await? {
                        if tx_id == transaction_id {
                            transaction_hashes.push(hash_id_value);
                        }
                    }
                }
            }

            if transaction_hashes.is_empty() {
                let _: () = twmq::redis::cmd("UNWATCH").query_async(&mut conn).await?;
                return Err(TransactionStoreError::TransactionNotInSubmittedState {
                    transaction_id: transaction_id.to_string(),
                });
            }

            // Transaction has hashes in submitted, proceed with atomic removal and requeue
            let mut pipeline = twmq::redis::pipe();
            pipeline.atomic();

            // Remove all hash:id values for this transaction (we know they exist)
            for hash in &transaction_hashes {
                // Remove the hash:id value from the zset
                let hash_id_value = format!("{}:{}", hash, transaction_id);
                pipeline.zrem(&submitted_key, &hash_id_value);

                // Also remove the separate hash-to-ID mapping for backward compatibility
                let hash_to_id_key = self.transaction_hash_to_id_key_name(hash);
                pipeline.del(&hash_to_id_key);
            }

            // Add back to pending
            pipeline.lpush(&pending_key, transaction_id);

            match pipeline
                .query_async::<Vec<twmq::redis::Value>>(&mut conn)
                .await
            {
                Ok(_) => return Ok(()), // Success
                Err(_) => {
                    // WATCH failed, check if it was our lock
                    let still_own_lock: Option<String> = conn.get(&lock_key).await?;
                    if still_own_lock.as_deref() != Some(worker_id) {
                        return Err(TransactionStoreError::LockLost {
                            eoa,
                            chain_id,
                            worker_id: worker_id.to_string(),
                        });
                    }
                    // Submitted state changed, retry
                    retry_count += 1;
                    continue;
                }
            }
        }
    }

    /// Check EOA health (balance, etc.)
    pub async fn check_eoa_health(
        &self,
        eoa: Address,
        chain_id: u64,
    ) -> Result<Option<EoaHealth>, TransactionStoreError> {
        let health_key = self.eoa_health_key_name(eoa, chain_id);
        let mut conn = self.redis.clone();

        let health_json: Option<String> = conn.get(&health_key).await?;
        if let Some(json) = health_json {
            let health: EoaHealth = serde_json::from_str(&json)?;
            Ok(Some(health))
        } else {
            Ok(None)
        }
    }

    /// Update EOA health data
    pub async fn update_health_data(
        &self,
        eoa: Address,
        chain_id: u64,
        worker_id: &str,
        health: &EoaHealth,
    ) -> Result<(), TransactionStoreError> {
        let health_json = serde_json::to_string(health)?;
        self.with_lock_check(eoa, chain_id, worker_id, |pipeline| {
            let health_key = self.eoa_health_key_name(eoa, chain_id);
            pipeline.set(&health_key, &health_json);
        })
        .await
    }

    /// Update cached transaction count
    pub async fn update_cached_transaction_count(
        &self,
        eoa: Address,
        chain_id: u64,
        worker_id: &str,
        transaction_count: u64,
    ) -> Result<(), TransactionStoreError> {
        self.with_lock_check(eoa, chain_id, worker_id, |pipeline| {
            let tx_count_key = self.last_transaction_count_key_name(eoa, chain_id);
            pipeline.set(&tx_count_key, transaction_count);
        })
        .await
    }

    /// Peek recycled nonces without removing them
    pub async fn peek_recycled_nonces(
        &self,
        eoa: Address,
        chain_id: u64,
    ) -> Result<Vec<u64>, TransactionStoreError> {
        let recycled_key = self.recycled_nonces_set_name(eoa, chain_id);
        let mut conn = self.redis.clone();

        let nonces: Vec<u64> = conn.zrange(&recycled_key, 0, -1).await?;
        Ok(nonces)
    }

    /// Peek at pending transactions without removing them (safe for planning)
    pub async fn peek_pending_transactions(
        &self,
        eoa: Address,
        chain_id: u64,
        limit: u64,
    ) -> Result<Vec<String>, TransactionStoreError> {
        let pending_key = self.pending_transactions_list_name(eoa, chain_id);
        let mut conn = self.redis.clone();

        // Use LRANGE to peek without removing
        let transaction_ids: Vec<String> =
            conn.lrange(&pending_key, 0, (limit as isize) - 1).await?;
        Ok(transaction_ids)
    }

    /// Get inflight budget (how many new transactions can be sent)
    pub async fn get_inflight_budget(
        &self,
        eoa: Address,
        chain_id: u64,
        max_inflight: u64,
    ) -> Result<u64, TransactionStoreError> {
        let optimistic_key = self.optimistic_transaction_count_key_name(eoa, chain_id);
        let last_tx_count_key = self.last_transaction_count_key_name(eoa, chain_id);
        let mut conn = self.redis.clone();

        // Read both values atomically to avoid race conditions
        let (optimistic_nonce, last_tx_count): (Option<u64>, Option<u64>) = twmq::redis::pipe()
            .get(&optimistic_key)
            .get(&last_tx_count_key)
            .query_async(&mut conn)
            .await?;

        let optimistic = match optimistic_nonce {
            Some(nonce) => nonce,
            None => return Err(TransactionStoreError::NonceSyncRequired { eoa, chain_id }),
        };
        let last_count = match last_tx_count {
            Some(count) => count,
            None => return Err(TransactionStoreError::NonceSyncRequired { eoa, chain_id }),
        };

        let current_inflight = optimistic.saturating_sub(last_count);
        let available_budget = max_inflight.saturating_sub(current_inflight);

        Ok(available_budget)
    }

    /// Get current optimistic nonce (without incrementing)
    pub async fn get_optimistic_nonce(
        &self,
        eoa: Address,
        chain_id: u64,
    ) -> Result<u64, TransactionStoreError> {
        let optimistic_key = self.optimistic_transaction_count_key_name(eoa, chain_id);
        let mut conn = self.redis.clone();

        let current: Option<u64> = conn.get(&optimistic_key).await?;
        match current {
            Some(nonce) => Ok(nonce),
            None => Err(TransactionStoreError::NonceSyncRequired { eoa, chain_id }),
        }
    }

    /// Lock key name for EOA processing
    fn eoa_lock_key_name(&self, eoa: Address, chain_id: u64) -> String {
        match &self.namespace {
            Some(ns) => format!("{ns}:eoa_executor:lock:{chain_id}:{eoa}"),
            None => format!("eoa_executor:lock:{chain_id}:{eoa}"),
        }
    }

    /// Get transaction ID for a given hash
    pub async fn get_transaction_id_for_hash(
        &self,
        hash: &str,
    ) -> Result<Option<String>, TransactionStoreError> {
        let hash_to_id_key = self.transaction_hash_to_id_key_name(hash);
        let mut conn = self.redis.clone();

        let transaction_id: Option<String> = conn.get(&hash_to_id_key).await?;
        Ok(transaction_id)
    }

    /// Get transaction data by transaction ID
    pub async fn get_transaction_data(
        &self,
        transaction_id: &str,
    ) -> Result<Option<TransactionData>, TransactionStoreError> {
        let tx_data_key = self.transaction_data_key_name(transaction_id);
        let mut conn = self.redis.clone();

        // Get the hash data (the transaction data is stored as a hash)
        let hash_data: HashMap<String, String> = conn.hgetall(&tx_data_key).await?;

        if hash_data.is_empty() {
            return Ok(None);
        }

        // Extract user_request from the hash data
        let user_request_json = hash_data.get("user_request").ok_or_else(|| {
            TransactionStoreError::TransactionNotFound {
                transaction_id: transaction_id.to_string(),
            }
        })?;

        let user_request: EoaTransactionRequest = serde_json::from_str(user_request_json)?;

        // Extract receipt if present
        let receipt = hash_data
            .get("receipt")
            .and_then(|receipt_str| serde_json::from_str(receipt_str).ok());

        // Extract attempts from separate list
        let attempts_key = self.transaction_attempts_list_name(transaction_id);
        let attempts_json_list: Vec<String> = conn.lrange(&attempts_key, 0, -1).await?;
        let mut attempts = Vec::new();
        for attempt_json in attempts_json_list {
            if let Ok(attempt) = serde_json::from_str::<TransactionAttempt>(&attempt_json) {
                attempts.push(attempt);
            }
        }

        Ok(Some(TransactionData {
            transaction_id: transaction_id.to_string(),
            user_request,
            receipt,
            attempts,
        }))
    }

    /// Mark transaction as successful and remove from submitted
    pub async fn succeed_transaction(
        &self,
        eoa: Address,
        chain_id: u64,
        worker_id: &str,
        transaction_id: &str,
        hash: &str,
        receipt: &str,
    ) -> Result<(), TransactionStoreError> {
        self.with_lock_check(eoa, chain_id, worker_id, |pipeline| {
            let submitted_key = self.submitted_transactions_zset_name(eoa, chain_id);
            let hash_to_id_key = self.transaction_hash_to_id_key_name(hash);
            let tx_data_key = self.transaction_data_key_name(transaction_id);
            let now = chrono::Utc::now().timestamp_millis().max(0) as u64;

            // Remove this hash:id from submitted
            let hash_id_value = format!("{}:{}", hash, transaction_id);
            pipeline.zrem(&submitted_key, &hash_id_value);

            // Remove hash mapping
            pipeline.del(&hash_to_id_key);

            // Update transaction data with success
            pipeline.hset(&tx_data_key, "completed_at", now);
            pipeline.hset(&tx_data_key, "receipt", receipt);
            pipeline.hset(&tx_data_key, "status", "confirmed");
        })
        .await
    }

    /// Add a gas bump attempt (new hash) to submitted transactions
    pub async fn add_gas_bump_attempt(
        &self,
        eoa: Address,
        chain_id: u64,
        worker_id: &str,
        transaction_id: &str,
        signed_transaction: Signed<TypedTransaction>,
    ) -> Result<(), TransactionStoreError> {
        let new_hash = signed_transaction.hash().to_string();
        let nonce = signed_transaction.nonce();

        // Create new attempt
        let new_attempt = TransactionAttempt {
            transaction_id: transaction_id.to_string(),
            details: signed_transaction,
            sent_at: chrono::Utc::now().timestamp_millis().max(0) as u64,
            attempt_number: 0, // Will be set correctly when reading all attempts
        };

        // Serialize the new attempt
        let attempt_json = serde_json::to_string(&new_attempt)?;

        // Get key names
        let attempts_list_key = self.transaction_attempts_list_name(transaction_id);
        let submitted_key = self.submitted_transactions_zset_name(eoa, chain_id);
        let hash_to_id_key = self.transaction_hash_to_id_key_name(&new_hash);
        let hash_id_value = format!("{}:{}", new_hash, transaction_id);

        // Now perform the atomic update
        self.with_lock_check(eoa, chain_id, worker_id, |pipeline| {
            // Add new hash:id to submitted (keeping old ones)
            pipeline.zadd(&submitted_key, &hash_id_value, nonce);

            // Still maintain separate hash-to-ID mapping for backward compatibility
            pipeline.set(&hash_to_id_key, transaction_id);

            // Simply push the new attempt to the attempts list
            pipeline.lpush(&attempts_list_key, &attempt_json);
        })
        .await
    }

    /// Efficiently batch fail and requeue multiple transactions
    /// This avoids hash-to-ID lookups since we already have both pieces of information
    pub async fn batch_fail_and_requeue_transactions(
        &self,
        eoa: Address,
        chain_id: u64,
        worker_id: &str,
        failures: Vec<crate::eoa::worker::TransactionFailure>,
    ) -> Result<(), TransactionStoreError> {
        if failures.is_empty() {
            return Ok(());
        }

        self.with_lock_check(eoa, chain_id, worker_id, |pipeline| {
            let submitted_key = self.submitted_transactions_zset_name(eoa, chain_id);
            let pending_key = self.pending_transactions_list_name(eoa, chain_id);

            // Remove all hash:id values from submitted
            for failure in &failures {
                let hash_id_value = format!("{}:{}", failure.hash, failure.transaction_id);
                pipeline.zrem(&submitted_key, &hash_id_value);

                // Remove separate hash-to-ID mapping
                let hash_to_id_key = self.transaction_hash_to_id_key_name(&failure.hash);
                pipeline.del(&hash_to_id_key);
            }

            // Add unique transaction IDs back to pending (avoid duplicates)
            let mut unique_tx_ids = std::collections::HashSet::new();
            for failure in &failures {
                unique_tx_ids.insert(&failure.transaction_id);
            }

            for transaction_id in unique_tx_ids {
                pipeline.lpush(&pending_key, transaction_id);
            }
        })
        .await
    }

    /// Efficiently batch succeed multiple transactions
    /// This avoids hash-to-ID lookups since we already have both pieces of information
    pub async fn batch_succeed_transactions(
        &self,
        eoa: Address,
        chain_id: u64,
        worker_id: &str,
        successes: Vec<crate::eoa::worker::TransactionSuccess>,
    ) -> Result<(), TransactionStoreError> {
        if successes.is_empty() {
            return Ok(());
        }

        self.with_lock_check(eoa, chain_id, worker_id, |pipeline| {
            let submitted_key = self.submitted_transactions_zset_name(eoa, chain_id);
            let now = chrono::Utc::now().timestamp_millis().max(0) as u64;

            for success in &successes {
                // Remove hash:id from submitted
                let hash_id_value = format!("{}:{}", success.hash, success.transaction_id);
                pipeline.zrem(&submitted_key, &hash_id_value);

                // Remove separate hash-to-ID mapping
                let hash_to_id_key = self.transaction_hash_to_id_key_name(&success.hash);
                pipeline.del(&hash_to_id_key);

                // Update transaction data with success (following existing Redis hash pattern)
                let tx_data_key = self.transaction_data_key_name(&success.transaction_id);
                pipeline.hset(&tx_data_key, "completed_at", now);
                pipeline.hset(&tx_data_key, "receipt", &success.receipt_data);
                pipeline.hset(&tx_data_key, "status", "confirmed");
            }
        })
        .await
    }

    // ========== SEND FLOW ==========

    /// Get cached transaction count
    pub async fn get_cached_transaction_count(
        &self,
        eoa: Address,
        chain_id: u64,
    ) -> Result<u64, TransactionStoreError> {
        let tx_count_key = self.last_transaction_count_key_name(eoa, chain_id);
        let mut conn = self.redis.clone();

        let count: Option<u64> = conn.get(&tx_count_key).await?;
        match count {
            Some(count) => Ok(count),
            None => Err(TransactionStoreError::NonceSyncRequired { eoa, chain_id }),
        }
    }

    /// Peek next available nonce (recycled or new)
    pub async fn peek_next_available_nonce(
        &self,
        eoa: Address,
        chain_id: u64,
    ) -> Result<NonceType, TransactionStoreError> {
        // Check recycled nonces first
        let recycled = self.peek_recycled_nonces(eoa, chain_id).await?;
        if !recycled.is_empty() {
            return Ok(NonceType::Recycled(recycled[0]));
        }

        // Get next optimistic nonce
        let optimistic_key = self.optimistic_transaction_count_key_name(eoa, chain_id);
        let mut conn = self.redis.clone();
        let current_optimistic: Option<u64> = conn.get(&optimistic_key).await?;

        match current_optimistic {
            Some(nonce) => Ok(NonceType::Incremented(nonce)),
            None => Err(TransactionStoreError::NonceSyncRequired { eoa, chain_id }),
        }
    }

    /// Synchronize nonces with the chain
    ///
    /// Part of standard nonce management flow, called in the confirm stage when chain nonce advances, and we need to update our cached nonce
    pub async fn synchronize_nonces_with_chain(
        &self,
        eoa: Address,
        chain_id: u64,
        worker_id: &str,
        current_chain_tx_count: u64,
    ) -> Result<(), TransactionStoreError> {
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;

        // First, read current health data
        let current_health = self.check_eoa_health(eoa, chain_id).await?;

        // Prepare health update if health data exists
        let health_update = if let Some(mut health) = current_health {
            health.last_nonce_movement_at = now;
            health.last_confirmation_at = now;
            Some(serde_json::to_string(&health)?)
        } else {
            None
        };

        self.with_lock_check(eoa, chain_id, worker_id, |pipeline| {
            let tx_count_key = self.last_transaction_count_key_name(eoa, chain_id);

            // Update cached transaction count
            pipeline.set(&tx_count_key, current_chain_tx_count);

            // Update health data only if it exists
            if let Some(ref health_json) = health_update {
                let health_key = self.eoa_health_key_name(eoa, chain_id);
                pipeline.set(&health_key, health_json);
            }
        })
        .await
    }

    /// Reset nonces to specified value
    ///
    /// This is called when we have too many recycled nonces and detect something wrong
    /// We want to start fresh, with the chain nonce as the new optimistic nonce
    pub async fn reset_nonces(
        &self,
        eoa: Address,
        chain_id: u64,
        worker_id: &str,
        current_chain_tx_count: u64,
    ) -> Result<(), TransactionStoreError> {
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;

        let current_health = self.check_eoa_health(eoa, chain_id).await?;

        // Prepare health update if health data exists
        let health_update = if let Some(mut health) = current_health {
            health.nonce_resets.push(now);
            Some(serde_json::to_string(&health)?)
        } else {
            None
        };

        self.with_lock_check(eoa, chain_id, worker_id, |pipeline| {
            let optimistic_key = self.optimistic_transaction_count_key_name(eoa, chain_id);
            let cached_nonce_key = self.last_transaction_count_key_name(eoa, chain_id);
            let recycled_key = self.recycled_nonces_set_name(eoa, chain_id);

            // Update health data only if it exists
            if let Some(ref health_json) = health_update {
                let health_key = self.eoa_health_key_name(eoa, chain_id);
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

    /// Add a transaction to the pending queue and store its data
    /// This is called when a new transaction request comes in for an EOA
    pub async fn add_transaction(
        &self,
        transaction_request: EoaTransactionRequest,
    ) -> Result<(), TransactionStoreError> {
        let transaction_id = &transaction_request.transaction_id;
        let eoa = transaction_request.from;
        let chain_id = transaction_request.chain_id;

        let tx_data_key = self.transaction_data_key_name(transaction_id);
        let pending_key = self.pending_transactions_list_name(eoa, chain_id);

        // Store transaction data as JSON in the user_request field of the hash
        let user_request_json = serde_json::to_string(&transaction_request)?;
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;

        let mut conn = self.redis.clone();

        // Use a pipeline to atomically store data and add to pending queue
        let mut pipeline = twmq::redis::pipe();

        // Store transaction data
        pipeline.hset(&tx_data_key, "user_request", &user_request_json);
        pipeline.hset(&tx_data_key, "status", "pending");
        pipeline.hset(&tx_data_key, "created_at", now);

        // Add to pending queue
        pipeline.lpush(&pending_key, transaction_id);

        pipeline.query_async::<()>(&mut conn).await?;

        Ok(())
    }
}

// Additional error types
#[derive(Debug, thiserror::Error, Serialize, Deserialize, Clone)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "errorCode")]
pub enum TransactionStoreError {
    #[error("Redis error: {message}")]
    RedisError { message: String },

    #[error("Serialization error: {message}")]
    DeserError { message: String, text: String },

    #[error("Transaction not found: {transaction_id}")]
    TransactionNotFound { transaction_id: String },

    #[error("Lost EOA lock: {eoa}:{chain_id} worker: {worker_id}")]
    LockLost {
        eoa: Address,
        chain_id: u64,
        worker_id: String,
    },

    #[error("Internal error - worker should quit: {message}")]
    InternalError { message: String },

    #[error("Transaction {transaction_id} not in borrowed state for nonce {nonce}")]
    TransactionNotInBorrowedState { transaction_id: String, nonce: u64 },

    #[error("Hash {hash} not found in submitted transactions")]
    HashNotInSubmittedState { hash: String },

    #[error("Transaction {transaction_id} has no hashes in submitted state")]
    TransactionNotInSubmittedState { transaction_id: String },

    #[error("Nonce {nonce} not available in recycled set")]
    NonceNotInRecycledSet { nonce: u64 },

    #[error("Transaction {transaction_id} not found in pending queue")]
    TransactionNotInPendingQueue { transaction_id: String },

    #[error("Optimistic nonce changed: expected {expected}, found {actual}")]
    OptimisticNonceChanged { expected: u64, actual: u64 },

    #[error("WATCH failed - state changed during operation")]
    WatchFailed,

    #[error(
        "Nonce synchronization required for {eoa}:{chain_id} - no cached transaction count available"
    )]
    NonceSyncRequired { eoa: Address, chain_id: u64 },
}

impl From<twmq::redis::RedisError> for TransactionStoreError {
    fn from(error: twmq::redis::RedisError) -> Self {
        TransactionStoreError::RedisError {
            message: error.to_string(),
        }
    }
}

impl From<serde_json::Error> for TransactionStoreError {
    fn from(error: serde_json::Error) -> Self {
        TransactionStoreError::DeserError {
            message: error.to_string(),
            text: error.to_string(),
        }
    }
}

const MAX_RETRIES: u32 = 10;
const RETRY_BASE_DELAY_MS: u64 = 10;

/// Scoped transaction store for a specific EOA, chain, and worker
///
/// This wrapper eliminates the need to repeatedly pass EOA, chain_id, and worker_id
/// to every method call. It provides the same interface as TransactionStore but with
/// these parameters already bound.
///
/// ## Usage:
/// ```rust
/// let scoped = ScopedTransactionStore::build(store, eoa, chain_id, worker_id).await?;
///
/// // Much cleaner method calls:
/// scoped.peek_pending_transactions(limit).await?;
/// scoped.move_borrowed_to_submitted(nonce, hash, tx_id, attempt).await?;
/// ```
pub struct ScopedEoaExecutorStore<'a> {
    store: &'a EoaExecutorStore,
    eoa: Address,
    chain_id: u64,
    worker_id: String,
}

impl<'a> ScopedEoaExecutorStore<'a> {
    /// Build a scoped transaction store for a specific EOA, chain, and worker
    ///
    /// This acquires the lock for the given EOA/chain.
    /// If the lock is not acquired, returns a LockLost error.
    #[tracing::instrument(skip_all, fields(eoa = %eoa, chain_id = chain_id, worker_id = %worker_id))]
    pub async fn build(
        store: &'a EoaExecutorStore,
        eoa: Address,
        chain_id: u64,
        worker_id: String,
    ) -> Result<Self, TransactionStoreError> {
        // 1. ACQUIRE LOCK AGGRESSIVELY
        tracing::info!("Acquiring EOA lock aggressively");
        store
            .acquire_eoa_lock_aggressively(eoa, chain_id, &worker_id)
            .await
            .map_err(|e| {
                tracing::error!("Failed to acquire EOA lock: {}", e);
                TransactionStoreError::LockLost {
                    eoa,
                    chain_id,
                    worker_id: worker_id.clone(),
                }
            })?;

        Ok(Self {
            store,
            eoa,
            chain_id,
            worker_id,
        })
    }

    /// Create a scoped store without lock validation (for read-only operations)
    pub fn new_unchecked(
        store: &'a EoaExecutorStore,
        eoa: Address,
        chain_id: u64,
        worker_id: String,
    ) -> Self {
        Self {
            store,
            eoa,
            chain_id,
            worker_id,
        }
    }

    // ========== ATOMIC OPERATIONS ==========

    /// Atomically move specific transaction from pending to borrowed with recycled nonce allocation
    pub async fn atomic_move_pending_to_borrowed_with_recycled_nonce(
        &self,
        transaction_id: &str,
        nonce: u64,
        prepared_tx: &BorrowedTransactionData,
    ) -> Result<(), TransactionStoreError> {
        self.store
            .atomic_move_pending_to_borrowed_with_recycled_nonce(
                self.eoa,
                self.chain_id,
                &self.worker_id,
                transaction_id,
                nonce,
                prepared_tx,
            )
            .await
    }

    /// Atomically move specific transaction from pending to borrowed with new nonce allocation
    pub async fn atomic_move_pending_to_borrowed_with_new_nonce(
        &self,
        transaction_id: &str,
        expected_nonce: u64,
        prepared_tx: &BorrowedTransactionData,
    ) -> Result<(), TransactionStoreError> {
        self.store
            .atomic_move_pending_to_borrowed_with_new_nonce(
                self.eoa,
                self.chain_id,
                &self.worker_id,
                transaction_id,
                expected_nonce,
                prepared_tx,
            )
            .await
    }

    /// Peek all borrowed transactions without removing them
    pub async fn peek_borrowed_transactions(
        &self,
    ) -> Result<Vec<BorrowedTransactionData>, TransactionStoreError> {
        self.store
            .peek_borrowed_transactions(self.eoa, self.chain_id)
            .await
    }

    /// Atomically move borrowed transaction to submitted state
    pub async fn move_borrowed_to_submitted(
        &self,
        nonce: u64,
        hash: &str,
        transaction_id: &str,
    ) -> Result<(), TransactionStoreError> {
        self.store
            .move_borrowed_to_submitted(
                self.eoa,
                self.chain_id,
                &self.worker_id,
                nonce,
                hash,
                transaction_id,
            )
            .await
    }

    /// Atomically move borrowed transaction back to recycled nonces and pending queue
    pub async fn move_borrowed_to_recycled(
        &self,
        nonce: u64,
        transaction_id: &str,
    ) -> Result<(), TransactionStoreError> {
        self.store
            .move_borrowed_to_recycled(
                self.eoa,
                self.chain_id,
                &self.worker_id,
                nonce,
                transaction_id,
            )
            .await
    }

    /// Get all hashes below a certain nonce from submitted transactions
    /// Returns (nonce, hash, transaction_id) tuples
    pub async fn get_hashes_below_nonce(
        &self,
        below_nonce: u64,
    ) -> Result<Vec<(u64, String, String)>, TransactionStoreError> {
        self.store
            .get_hashes_below_nonce(self.eoa, self.chain_id, below_nonce)
            .await
    }

    /// Get all transaction IDs for a specific nonce
    pub async fn get_transaction_ids_for_nonce(
        &self,
        nonce: u64,
    ) -> Result<Vec<String>, TransactionStoreError> {
        self.store
            .get_transaction_ids_for_nonce(self.eoa, self.chain_id, nonce)
            .await
    }

    /// Remove all hashes for a transaction and requeue it
    pub async fn fail_and_requeue_transaction(
        &self,
        transaction_id: &str,
    ) -> Result<(), TransactionStoreError> {
        self.store
            .fail_and_requeue_transaction(self.eoa, self.chain_id, &self.worker_id, transaction_id)
            .await
    }

    /// Efficiently batch fail and requeue multiple transactions
    pub async fn batch_fail_and_requeue_transactions(
        &self,
        failures: Vec<crate::eoa::worker::TransactionFailure>,
    ) -> Result<(), TransactionStoreError> {
        self.store
            .batch_fail_and_requeue_transactions(self.eoa, self.chain_id, &self.worker_id, failures)
            .await
    }

    /// Efficiently batch succeed multiple transactions
    pub async fn batch_succeed_transactions(
        &self,
        successes: Vec<crate::eoa::worker::TransactionSuccess>,
    ) -> Result<(), TransactionStoreError> {
        self.store
            .batch_succeed_transactions(self.eoa, self.chain_id, &self.worker_id, successes)
            .await
    }

    // ========== EOA HEALTH & NONCE MANAGEMENT ==========

    /// Check EOA health (balance, etc.)
    pub async fn check_eoa_health(&self) -> Result<Option<EoaHealth>, TransactionStoreError> {
        self.store.check_eoa_health(self.eoa, self.chain_id).await
    }

    /// Update EOA health data
    pub async fn update_health_data(
        &self,
        health: &EoaHealth,
    ) -> Result<(), TransactionStoreError> {
        self.store
            .update_health_data(self.eoa, self.chain_id, &self.worker_id, health)
            .await
    }

    /// Update cached transaction count
    pub async fn update_cached_transaction_count(
        &self,
        transaction_count: u64,
    ) -> Result<(), TransactionStoreError> {
        self.store
            .update_cached_transaction_count(
                self.eoa,
                self.chain_id,
                &self.worker_id,
                transaction_count,
            )
            .await
    }

    /// Peek recycled nonces without removing them
    pub async fn peek_recycled_nonces(&self) -> Result<Vec<u64>, TransactionStoreError> {
        self.store
            .peek_recycled_nonces(self.eoa, self.chain_id)
            .await
    }

    /// Peek at pending transactions without removing them
    pub async fn peek_pending_transactions(
        &self,
        limit: u64,
    ) -> Result<Vec<String>, TransactionStoreError> {
        self.store
            .peek_pending_transactions(self.eoa, self.chain_id, limit)
            .await
    }

    /// Get inflight budget (how many new transactions can be sent)
    pub async fn get_inflight_budget(
        &self,
        max_inflight: u64,
    ) -> Result<u64, TransactionStoreError> {
        self.store
            .get_inflight_budget(self.eoa, self.chain_id, max_inflight)
            .await
    }

    /// Get current optimistic nonce (without incrementing)
    pub async fn get_optimistic_nonce(&self) -> Result<u64, TransactionStoreError> {
        self.store
            .get_optimistic_nonce(self.eoa, self.chain_id)
            .await
    }

    /// Mark transaction as successful and remove from submitted
    pub async fn succeed_transaction(
        &self,
        transaction_id: &str,
        hash: &str,
        receipt: &str,
    ) -> Result<(), TransactionStoreError> {
        self.store
            .succeed_transaction(
                self.eoa,
                self.chain_id,
                &self.worker_id,
                transaction_id,
                hash,
                receipt,
            )
            .await
    }

    /// Add a gas bump attempt (new hash) to submitted transactions
    pub async fn add_gas_bump_attempt(
        &self,
        transaction_id: &str,
        signed_transaction: Signed<TypedTransaction>,
    ) -> Result<(), TransactionStoreError> {
        self.store
            .add_gas_bump_attempt(
                self.eoa,
                self.chain_id,
                &self.worker_id,
                transaction_id,
                signed_transaction,
            )
            .await
    }

    pub async fn synchronize_nonces_with_chain(
        &self,
        nonce: u64,
    ) -> Result<(), TransactionStoreError> {
        self.store
            .synchronize_nonces_with_chain(self.eoa, self.chain_id, &self.worker_id, nonce)
            .await
    }

    pub async fn reset_nonces(&self, nonce: u64) -> Result<(), TransactionStoreError> {
        self.store
            .reset_nonces(self.eoa, self.chain_id, &self.worker_id, nonce)
            .await
    }

    // ========== READ-ONLY OPERATIONS ==========

    /// Get cached transaction count
    pub async fn get_cached_transaction_count(&self) -> Result<u64, TransactionStoreError> {
        self.store
            .get_cached_transaction_count(self.eoa, self.chain_id)
            .await
    }

    /// Peek next available nonce (recycled or new)
    pub async fn peek_next_available_nonce(&self) -> Result<NonceType, TransactionStoreError> {
        self.store
            .peek_next_available_nonce(self.eoa, self.chain_id)
            .await
    }

    // ========== ACCESSORS ==========

    /// Get the EOA address this store is scoped to
    pub fn eoa(&self) -> Address {
        self.eoa
    }

    /// Get the chain ID this store is scoped to
    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    /// Get the worker ID this store is scoped to
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Get a reference to the underlying transaction store
    pub fn inner(&self) -> &EoaExecutorStore {
        self.store
    }

    /// Get transaction data by transaction ID
    pub async fn get_transaction_data(
        &self,
        transaction_id: &str,
    ) -> Result<Option<TransactionData>, TransactionStoreError> {
        self.store.get_transaction_data(transaction_id).await
    }
}
