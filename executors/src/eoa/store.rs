use alloy::consensus::{Signed, TypedTransaction};
use alloy::eips::eip7702::SignedAuthorization;
use alloy::network::AnyTransactionReceipt;
use alloy::primitives::{Address, B256, Bytes, U256};
use chrono;
use engine_core::chain::RpcCredentials;
use engine_core::credentials::SigningCredential;
use engine_core::execution_options::WebhookOptions;
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
        pipeline.hset(&self.borrowed_key, self.nonce.to_string(), &self.prepared_tx_json);
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
        pipeline.hset(&self.borrowed_key, self.nonce.to_string(), &self.prepared_tx_json);
    }

    fn watch_keys(&self) -> Vec<String> {
        vec![self.optimistic_key.clone(), self.pending_key.clone()]
    }

    async fn validation(&self, conn: &mut ConnectionManager) -> Result<(), TransactionStoreError> {
        // Check current optimistic nonce
        let current_optimistic: Option<u64> = conn.get(&self.optimistic_key).await?;
        let current_nonce = match current_optimistic {
            Some(nonce) => nonce,
            None => return Err(TransactionStoreError::NonceSyncRequired { 
                eoa: self.eoa, 
                chain_id: self.chain_id 
            }),
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
        
        // Add to submitted  
        pipeline.zadd(&self.submitted_key, self.nonce, &self.hash);
        
        // Map hash to transaction ID
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
        let now = chrono::Utc::now().timestamp_millis();
        
        // Remove from borrowed (we know it exists)
        pipeline.hdel(&self.borrowed_key, self.nonce.to_string());
        
        // Add nonce to recycled set (with timestamp as score)
        pipeline.zadd(&self.recycled_key, now, self.nonce);
        
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

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum EoaTransactionTypeData {
    Eip7702(EoaSend7702JobData),
    Eip1559(EoaSend1559JobData),
    Legacy(EoaSendLegacyJobData),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EoaSend7702JobData {
    pub authorization_list: Option<Vec<SignedAuthorization>>,
    pub max_fee_per_gas: Option<u128>,
    pub max_priority_fee_per_gas: Option<u128>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EoaSend1559JobData {
    pub max_fee_per_gas: Option<u128>,
    pub max_priority_fee_per_gas: Option<u128>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EoaSendLegacyJobData {
    pub gas_price: Option<u128>,
}
/// Active attempt for a transaction (full alloy transaction + metadata)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionAttempt {
    pub transaction_id: String,
    pub details: Signed<TypedTransaction>,
    pub sent_at: chrono::DateTime<chrono::Utc>,
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
    fn transaction_data_key_name(&self, transaction_id: &str) -> String {
        match &self.namespace {
            Some(ns) => format!("{ns}:eoa_executor:tx_data:{transaction_id}"),
            None => format!("eoa_tx_data:{transaction_id}"),
        }
    }

    /// Name of the list for pending transactions
    fn pending_transactions_list_name(&self, eoa: Address, chain_id: u64) -> String {
        match &self.namespace {
            Some(ns) => format!("{ns}:eoa_executor:pending_txs:{chain_id}:{eoa}"),
            None => format!("eoa_executor:pending_txs:{chain_id}:{eoa}"),
        }
    }

    /// Name of the zset for submitted transactions. nonce -> hash
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

    /// Name of the hashmap that maps transaction id to borrowed transactions
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
    pub balance_fetched_at: u64,
    pub last_confirmation_at: Option<u64>,
    pub nonce_resets: Vec<u64>, // Last 5 reset timestamps
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BorrowedTransactionData {
    pub transaction_id: String,
    pub signed_transaction: Signed<TypedTransaction>,
    pub hash: B256,
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

    /// Helper to execute atomic operations with proper retry logic and watch handling
    ///
    /// This helper centralizes all the boilerplate for WATCH/MULTI/EXEC operations:
    /// - Retry logic with exponential backoff
    /// - Lock ownership validation
    /// - WATCH key management
    /// - Error handling and UNWATCH cleanup
    ///
    /// ## When to use this helper:
    /// - Simple validation that doesn't need to pass data to pipeline phase
    /// - Operations that can cleanly separate validation from pipeline commands
    /// - Cases where reducing boilerplate is more important than complex data flow
    ///
    /// ## When NOT to use this helper:
    /// - Complex validation that needs to pass computed data to pipeline
    /// - Operations requiring custom retry logic
    /// - Cases where validation and pipeline phases are tightly coupled
    ///
    /// ## Example usage:
    /// ```
    /// self.execute_with_watch_and_retry(
    ///     eoa, chain_id, worker_id,
    ///     &[key1, key2], // Keys to WATCH
    ///     "operation name",
    ///     async |conn| { // Validation phase
    ///         let data = conn.get("key").await?;
    ///         if !is_valid(data) {
    ///             return Err(SomeError);
    ///         }
    ///         Ok(())
    ///     },
    ///     |pipeline| { // Pipeline phase  
    ///         pipeline.set("key", "value");
    ///         pipeline.incr("counter", 1);
    ///     }
    /// ).await
    /// ```
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
        worker_id: &str,
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
    pub async fn get_hashes_below_nonce(
        &self,
        eoa: Address,
        chain_id: u64,
        below_nonce: u64,
    ) -> Result<Vec<(u64, String)>, TransactionStoreError> {
        let submitted_key = self.submitted_transactions_zset_name(eoa, chain_id);
        let mut conn = self.redis.clone();

        // Get all entries with nonce < below_nonce
        let results: Vec<(String, u64)> = conn
            .zrangebyscore_withscores(&submitted_key, 0, below_nonce - 1)
            .await?;

        Ok(results
            .into_iter()
            .map(|(hash, nonce)| (nonce, hash))
            .collect())
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
            let all_hashes: Vec<String> = conn.zrange(&submitted_key, 0, -1).await?;
            let mut transaction_hashes = Vec::new();

            for hash in all_hashes {
                if let Some(tx_id) = self.get_transaction_id_for_hash(&hash).await? {
                    if tx_id == transaction_id {
                        transaction_hashes.push(hash);
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

            // Remove all hashes for this transaction (we know they exist)
            for hash in &transaction_hashes {
                pipeline.zrem(&submitted_key, hash);
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
        self.with_lock_check(eoa, chain_id, worker_id, |pipeline| {
            let health_key = self.eoa_health_key_name(eoa, chain_id);
            let health_json = serde_json::to_string(health).unwrap();
            pipeline.set(&health_key, health_json);
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

        // Get all nonces ordered by score (timestamp)
        let nonces: Vec<u64> = conn.zrange(&recycled_key, 0, -1).await?;
        Ok(nonces)
    }

    /// Nuke all recycled nonces
    pub async fn nuke_recycled_nonces(
        &self,
        eoa: Address,
        chain_id: u64,
        worker_id: &str,
    ) -> Result<(), TransactionStoreError> {
        self.with_lock_check(eoa, chain_id, worker_id, |pipeline| {
            let recycled_key = self.recycled_nonces_set_name(eoa, chain_id);
            pipeline.del(&recycled_key);
        })
        .await
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
        let (optimistic_nonce, last_tx_count): (Option<u64>, Option<u64>) = 
            twmq::redis::pipe()
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

    /// Complete safe transaction processing flow combining all atomic operations
    /// Returns (success, used_recycled_nonce, actual_nonce)
    ///
    /// On specific failures (nonce not available, transaction not in pending),
    /// returns success=false. On other errors, propagates the error.
    pub async fn process_transaction_atomically(
        &self,
        eoa: Address,
        chain_id: u64,
        worker_id: &str,
        transaction_id: &str,
        signed_tx: &Signed<TypedTransaction>,
    ) -> Result<(bool, bool, Option<u64>), TransactionStoreError> {
        // Prepare borrowed transaction data
        let borrowed_data = BorrowedTransactionData {
            transaction_id: transaction_id.to_string(),
            signed_transaction: signed_tx.clone(),
            hash: *signed_tx.hash(),
            borrowed_at: chrono::Utc::now().timestamp_millis() as u64,
        };

        // Try recycled nonces first
        let recycled_nonces = self.peek_recycled_nonces(eoa, chain_id).await?;
        if let Some(&nonce) = recycled_nonces.first() {
            match self
                .atomic_move_pending_to_borrowed_with_recycled_nonce(
                    eoa,
                    chain_id,
                    worker_id,
                    transaction_id,
                    nonce,
                    &borrowed_data,
                )
                .await
            {
                Ok(()) => return Ok((true, true, Some(nonce))), // Success with recycled nonce
                Err(TransactionStoreError::NonceNotInRecycledSet { .. }) => {
                    // Nonce was consumed by another worker, try new nonce
                }
                Err(TransactionStoreError::TransactionNotInPendingQueue { .. }) => {
                    // Transaction was processed by another worker
                    return Ok((false, false, None));
                }
                Err(e) => return Err(e), // Other errors propagate
            }
        }

        // Try new nonce
        let expected_nonce = self.get_optimistic_nonce(eoa, chain_id).await?;
        match self
            .atomic_move_pending_to_borrowed_with_new_nonce(
                eoa,
                chain_id,
                worker_id,
                transaction_id,
                expected_nonce,
                &borrowed_data,
            )
            .await
        {
            Ok(()) => Ok((true, false, Some(expected_nonce))), // Success with new nonce
            Err(TransactionStoreError::OptimisticNonceChanged { .. }) => {
                // Nonce changed while we were processing, try again
                Ok((false, false, None))
            }
            Err(TransactionStoreError::TransactionNotInPendingQueue { .. }) => {
                // Transaction was processed by another worker
                Ok((false, false, None))
            }
            Err(e) => Err(e), // Other errors propagate
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

        // Extract attempts if present (could be multiple attempt_N fields)
        let mut attempts = Vec::new();
        for (key, value) in &hash_data {
            if key.starts_with("attempt_") {
                if let Ok(attempt) = serde_json::from_str::<TransactionAttempt>(value) {
                    attempts.push(attempt);
                }
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
            let now = chrono::Utc::now();

            // Remove this hash from submitted
            pipeline.zrem(&submitted_key, hash);

            // Remove hash mapping
            pipeline.del(&hash_to_id_key);

            // Update transaction data with success
            pipeline.hset(&tx_data_key, "completed_at", now.timestamp());
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
        nonce: u64,
        new_hash: &str,
        transaction_id: &str,
        attempt_number: u32,
    ) -> Result<(), TransactionStoreError> {
        self.with_lock_check(eoa, chain_id, worker_id, |pipeline| {
            let submitted_key = self.submitted_transactions_zset_name(eoa, chain_id);
            let hash_to_id_key = self.transaction_hash_to_id_key_name(new_hash);
            let tx_data_key = self.transaction_data_key_name(transaction_id);

            // Add new hash to submitted (keeping old ones)
            pipeline.zadd(&submitted_key, nonce, new_hash);

            // Map new hash to transaction ID
            pipeline.set(&hash_to_id_key, transaction_id);

            // Record gas bump attempt
            let now = chrono::Utc::now();
            let attempt_json = serde_json::json!({
                "attempt_number": attempt_number,
                "hash": new_hash,
                "gas_bumped_at": now.timestamp(),
                "nonce": nonce,
                "type": "gas_bump"
            });
            pipeline.hset(
                &tx_data_key,
                format!("attempt_{}", attempt_number),
                attempt_json.to_string(),
            );
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
}

// Additional error types
#[derive(Debug, thiserror::Error)]
pub enum TransactionStoreError {
    #[error("Redis error: {0}")]
    RedisError(#[from] twmq::redis::RedisError),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

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
    /// This validates that the worker currently owns the lock for the given EOA/chain.
    /// If the lock is not owned, returns a LockLost error.
    pub async fn build(
        store: &'a EoaExecutorStore,
        eoa: Address,
        chain_id: u64,
        worker_id: String,
    ) -> Result<Self, TransactionStoreError> {
        let lock_key = store.eoa_lock_key_name(eoa, chain_id);
        let mut conn = store.redis.clone();

        // Verify the worker owns the lock
        let current_owner: Option<String> = conn.get(&lock_key).await?;
        if current_owner.as_deref() != Some(&worker_id) {
            return Err(TransactionStoreError::LockLost {
                eoa,
                chain_id,
                worker_id,
            });
        }

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
            .peek_borrowed_transactions(self.eoa, self.chain_id, &self.worker_id)
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
    pub async fn get_hashes_below_nonce(
        &self,
        below_nonce: u64,
    ) -> Result<Vec<(u64, String)>, TransactionStoreError> {
        self.store
            .get_hashes_below_nonce(self.eoa, self.chain_id, below_nonce)
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

    /// Nuke all recycled nonces
    pub async fn nuke_recycled_nonces(&self) -> Result<(), TransactionStoreError> {
        self.store
            .nuke_recycled_nonces(self.eoa, self.chain_id, &self.worker_id)
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

    /// Complete safe transaction processing flow combining all atomic operations
    pub async fn process_transaction_atomically(
        &self,
        transaction_id: &str,
        signed_tx: &Signed<TypedTransaction>,
    ) -> Result<(bool, bool, Option<u64>), TransactionStoreError> {
        self.store
            .process_transaction_atomically(
                self.eoa,
                self.chain_id,
                &self.worker_id,
                transaction_id,
                signed_tx,
            )
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
        nonce: u64,
        new_hash: &str,
        transaction_id: &str,
        attempt_number: u32,
    ) -> Result<(), TransactionStoreError> {
        self.store
            .add_gas_bump_attempt(
                self.eoa,
                self.chain_id,
                &self.worker_id,
                nonce,
                new_hash,
                transaction_id,
                attempt_number,
            )
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
