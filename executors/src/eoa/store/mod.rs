use alloy::consensus::{Signed, TypedTransaction};
use alloy::network::AnyTransactionReceipt;
use alloy::primitives::{Address, Bytes, U256};
use chrono;
use engine_core::chain::RpcCredentials;
use engine_core::credentials::SigningCredential;
use engine_core::execution_options::WebhookOptions;
use engine_core::transaction::TransactionTypeData;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use twmq::redis::{AsyncCommands, aio::ConnectionManager};

mod atomic;
mod submitted;

pub mod error;
pub use atomic::AtomicEoaExecutorStore;
pub use submitted::{CleanupReport, SubmittedTransaction};

use crate::eoa::store::submitted::SubmittedTransactionStringWithNonce;

pub const NO_OP_TRANSACTION_ID: &str = "noop";

#[derive(Debug, Clone)]
pub struct ReplacedTransaction {
    pub hash: String,
    pub transaction_id: String,
}

#[derive(Debug, Clone)]
pub struct ConfirmedTransaction {
    pub hash: String,
    pub transaction_id: String,
    pub receipt_data: String,
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
    pub transaction_type_data: Option<TransactionTypeData>,
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
    pub created_at: u64, // Unix timestamp in milliseconds
}

pub struct BorrowedTransaction {
    pub transaction_id: String,
    pub data: Signed<TypedTransaction>,
    pub borrowed_at: chrono::DateTime<chrono::Utc>,
}

/// Transaction store focused on transaction_id operations and nonce indexing
pub struct EoaExecutorStore {
    pub redis: ConnectionManager,
    pub keys: EoaExecutorStoreKeys,
}

pub struct EoaExecutorStoreKeys {
    pub eoa: Address,
    pub chain_id: u64,
    pub namespace: Option<String>,
}

impl EoaExecutorStoreKeys {
    pub fn new(eoa: Address, chain_id: u64, namespace: Option<String>) -> Self {
        Self {
            eoa,
            chain_id,
            namespace,
        }
    }

    /// Lock key name for EOA processing
    pub fn eoa_lock_key_name(&self) -> String {
        match &self.namespace {
            Some(ns) => format!("{ns}:eoa_executor:lock:{}:{}", self.chain_id, self.eoa),
            None => format!("eoa_executor:lock:{}:{}", self.chain_id, self.eoa),
        }
    }

    /// Name of the key for the transaction data
    ///
    /// Transaction data is stored as a Redis HSET with the following fields:
    /// - "user_request": JSON string containing EoaTransactionRequest
    /// - "receipt": JSON string containing AnyTransactionReceipt (optional)
    /// - "status": String status ("confirmed", "failed", etc.)
    /// - "completed_at": String Unix timestamp (optional)
    /// - "created_at": String Unix timestamp (optional)
    pub fn transaction_data_key_name(&self, transaction_id: &str) -> String {
        match &self.namespace {
            Some(ns) => format!("{ns}:eoa_executor:tx_data:{transaction_id}"),
            None => format!("eoa_executor:_tx_data:{transaction_id}"),
        }
    }

    /// Name of the list for transaction attempts
    ///
    /// Attempts are stored as a separate Redis LIST where each element is a JSON blob
    /// of a TransactionAttempt. This allows efficient append operations.
    pub fn transaction_attempts_list_name(&self, transaction_id: &str) -> String {
        match &self.namespace {
            Some(ns) => format!("{ns}:eoa_executor:tx_attempts:{transaction_id}"),
            None => format!("eoa_executor:tx_attempts:{transaction_id}"),
        }
    }

    /// Name of the list for pending transactions
    pub fn pending_transactions_zset_name(&self) -> String {
        match &self.namespace {
            Some(ns) => format!(
                "{ns}:eoa_executor:pending_txs:{}:{}",
                self.chain_id, self.eoa
            ),
            None => format!("eoa_executor:pending_txs:{}:{}", self.chain_id, self.eoa),
        }
    }

    /// Name of the zset for submitted transactions. nonce -> hash:id
    ///
    /// Same transaction might appear multiple times in the zset with different nonces/gas prices (and thus different hashes)
    pub fn submitted_transactions_zset_name(&self) -> String {
        match &self.namespace {
            Some(ns) => format!(
                "{ns}:eoa_executor:submitted_txs:{}:{}",
                self.chain_id, self.eoa
            ),
            None => format!("eoa_executor:submitted_txs:{}:{}", self.chain_id, self.eoa),
        }
    }

    /// Name of the key that maps transaction hash to transaction id
    pub fn transaction_hash_to_id_key_name(&self, hash: &str) -> String {
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
    pub fn borrowed_transactions_hashmap_name(&self) -> String {
        match &self.namespace {
            Some(ns) => format!(
                "{ns}:eoa_executor:borrowed_txs:{}:{}",
                self.chain_id, self.eoa
            ),
            None => format!("eoa_executor:borrowed_txs:{}:{}", self.chain_id, self.eoa),
        }
    }

    /// Name of the set that contains recycled nonces.
    ///
    /// If a transaction was submitted but failed (ie, we know with certainty it didn't enter the mempool),
    ///
    /// we add the nonce to this set.
    ///
    /// These nonces are used with priority, before any other nonces.
    pub fn recycled_nonces_set_name(&self) -> String {
        match &self.namespace {
            Some(ns) => format!(
                "{ns}:eoa_executor:recycled_nonces:{}:{}",
                self.chain_id, self.eoa
            ),
            None => format!(
                "eoa_executor:recycled_nonces:{}:{}",
                self.chain_id, self.eoa
            ),
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
    pub fn optimistic_transaction_count_key_name(&self) -> String {
        match &self.namespace {
            Some(ns) => format!(
                "{ns}:eoa_executor:optimistic_nonce:{}:{}",
                self.chain_id, self.eoa
            ),
            None => format!(
                "eoa_executor:optimistic_nonce:{}:{}",
                self.chain_id, self.eoa
            ),
        }
    }

    /// Name of the key that contains the nonce of the last fetched ONCHAIN transaction count for each EOA.
    ///
    /// This is a cache for the actual transaction count, which is fetched from the RPC.
    ///
    /// The nonce for the NEXT transaction is the ONCHAIN transaction count (NOT + 1)
    ///
    /// Eg: transaction count is 0, so we use nonce 0 for sending the next transaction. Once successful, transaction count will be 1.
    pub fn last_transaction_count_key_name(&self) -> String {
        match &self.namespace {
            Some(ns) => format!(
                "{ns}:eoa_executor:last_tx_nonce:{}:{}",
                self.chain_id, self.eoa
            ),
            None => format!("eoa_executor:last_tx_nonce:{}:{}", self.chain_id, self.eoa),
        }
    }

    /// EOA health key name.
    ///
    /// EOA health stores:
    /// - cached balance, the timestamp of the last balance fetch
    /// - timestamp of the last successful transaction confirmation
    /// - timestamp of the last 5 nonce resets
    pub fn eoa_health_key_name(&self) -> String {
        match &self.namespace {
            Some(ns) => format!("{ns}:eoa_executor:health:{}:{}", self.chain_id, self.eoa),
            None => format!("eoa_executor:health:{}:{}", self.chain_id, self.eoa),
        }
    }
}

impl EoaExecutorStore {
    pub fn new(
        redis: ConnectionManager,
        namespace: Option<String>,
        eoa: Address,
        chain_id: u64,
    ) -> Self {
        Self {
            redis,
            keys: EoaExecutorStoreKeys {
                eoa,
                chain_id,
                namespace,
            },
        }
    }
}

impl std::ops::Deref for EoaExecutorStore {
    type Target = EoaExecutorStoreKeys;
    fn deref(&self) -> &Self::Target {
        &self.keys
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
    /// Aggressively acquire EOA lock, forcefully taking over from stalled workers
    ///
    /// Creates an AtomicEoaExecutorStore that owns the lock.
    pub async fn acquire_eoa_lock_aggressively(
        self,
        worker_id: &str,
    ) -> Result<AtomicEoaExecutorStore, TransactionStoreError> {
        let lock_key = self.eoa_lock_key_name();
        let mut conn = self.redis.clone();

        // First try normal acquisition
        let acquired: bool = conn.set_nx(&lock_key, worker_id).await?;
        if acquired {
            return Ok(AtomicEoaExecutorStore {
                store: self,
                worker_id: worker_id.to_string(),
            });
        }
        // Lock exists, forcefully take it over
        tracing::warn!(
            eoa = %self.eoa,
            chain_id = %self.chain_id,
            worker_id = %worker_id,
            "Forcefully taking over EOA lock from stalled worker"
        );
        // Force set - no expiry, only released by explicit takeover
        let _: () = conn.set(&lock_key, worker_id).await?;
        Ok(AtomicEoaExecutorStore {
            store: self,
            worker_id: worker_id.to_string(),
        })
    }

    /// Peek all borrowed transactions without removing them
    pub async fn peek_borrowed_transactions(
        &self,
    ) -> Result<Vec<BorrowedTransactionData>, TransactionStoreError> {
        let borrowed_key = self.borrowed_transactions_hashmap_name();
        let mut conn = self.redis.clone();

        let borrowed_map: HashMap<String, String> = conn.hgetall(&borrowed_key).await?;
        let mut result = Vec::new();

        for (_nonce_str, transaction_json) in borrowed_map {
            let borrowed_data: BorrowedTransactionData = serde_json::from_str(&transaction_json)?;
            result.push(borrowed_data);
        }

        Ok(result)
    }

    /// Get all hashes below a certain nonce from submitted transactions
    /// Returns (nonce, hash, transaction_id) tuples
    pub async fn get_submitted_transactions_below_nonce(
        &self,
        below_nonce: u64,
    ) -> Result<Vec<SubmittedTransaction>, TransactionStoreError> {
        let submitted_key = self.submitted_transactions_zset_name();
        let mut conn = self.redis.clone();

        // Get all entries with nonce < below_nonce
        let results: Vec<SubmittedTransactionStringWithNonce> = conn
            .zrangebyscore_withscores(&submitted_key, 0, below_nonce - 1)
            .await?;

        let submitted_txs: Vec<SubmittedTransaction> =
            SubmittedTransaction::from_redis_strings(&results);

        Ok(submitted_txs)
    }

    /// Get all transaction IDs for a specific nonce
    pub async fn get_submitted_transactions_for_nonce(
        &self,
        nonce: u64,
    ) -> Result<Vec<SubmittedTransaction>, TransactionStoreError> {
        let submitted_key = self.submitted_transactions_zset_name();
        let mut conn = self.redis.clone();

        let results: Vec<SubmittedTransactionStringWithNonce> = conn
            .zrangebyscore_withscores(&submitted_key, nonce, nonce)
            .await?;

        let submitted_txs: Vec<SubmittedTransaction> =
            SubmittedTransaction::from_redis_strings(&results);

        Ok(submitted_txs)
    }

    /// Check EOA health (balance, etc.)
    pub async fn check_eoa_health(&self) -> Result<Option<EoaHealth>, TransactionStoreError> {
        let mut conn = self.redis.clone();

        let health_json: Option<String> = conn.get(self.eoa_health_key_name()).await?;
        if let Some(json) = health_json {
            let health: EoaHealth = serde_json::from_str(&json)?;
            Ok(Some(health))
        } else {
            Ok(None)
        }
    }

    /// Peek recycled nonces without removing them
    pub async fn peek_recycled_nonces(&self) -> Result<Vec<u64>, TransactionStoreError> {
        let recycled_key = self.recycled_nonces_set_name();
        let mut conn = self.redis.clone();

        let nonces: Vec<u64> = conn.zrange(&recycled_key, 0, -1).await?;
        Ok(nonces)
    }

    /// Peek at pending transactions without removing them (safe for planning)
    pub async fn peek_pending_transactions(
        &self,
        limit: u64,
    ) -> Result<Vec<String>, TransactionStoreError> {
        let pending_key = self.pending_transactions_zset_name();
        let mut conn = self.redis.clone();

        // Use LRANGE to peek without removing
        let transaction_ids: Vec<String> =
            conn.lrange(&pending_key, 0, (limit as isize) - 1).await?;
        Ok(transaction_ids)
    }

    /// Get inflight budget (how many new transactions can be sent)
    pub async fn get_inflight_budget(
        &self,
        max_inflight: u64,
    ) -> Result<u64, TransactionStoreError> {
        let optimistic_key = self.optimistic_transaction_count_key_name();
        let last_tx_count_key = self.last_transaction_count_key_name();
        let mut conn = self.redis.clone();

        // Read both values atomically to avoid race conditions
        let (optimistic_nonce, last_tx_count): (Option<u64>, Option<u64>) = twmq::redis::pipe()
            .get(&optimistic_key)
            .get(&last_tx_count_key)
            .query_async(&mut conn)
            .await?;

        let optimistic = match optimistic_nonce {
            Some(nonce) => nonce,
            None => return Err(self.nonce_sync_required_error()),
        };
        let last_count = match last_tx_count {
            Some(count) => count,
            None => return Err(self.nonce_sync_required_error()),
        };

        let current_inflight = optimistic.saturating_sub(last_count);
        let available_budget = max_inflight.saturating_sub(current_inflight);

        Ok(available_budget)
    }

    /// Get current optimistic nonce (without incrementing)
    pub async fn get_optimistic_nonce(&self) -> Result<u64, TransactionStoreError> {
        let optimistic_key = self.optimistic_transaction_count_key_name();
        let mut conn = self.redis.clone();

        let current: Option<u64> = conn.get(&optimistic_key).await?;
        match current {
            Some(nonce) => Ok(nonce),
            None => Err(self.nonce_sync_required_error()),
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

        let created_at = hash_data.get("created_at").ok_or_else(|| {
            TransactionStoreError::TransactionNotFound {
                transaction_id: transaction_id.to_string(),
            }
        })?;

        // todo: in case of non-existent created_at, we should return a default value
        let created_at =
            created_at
                .parse::<u64>()
                .map_err(|_| TransactionStoreError::TransactionNotFound {
                    transaction_id: transaction_id.to_string(),
                })?;

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
            created_at,
            user_request,
            receipt,
            attempts,
        }))
    }

    /// Get cached transaction count
    pub async fn get_cached_transaction_count(&self) -> Result<u64, TransactionStoreError> {
        let tx_count_key = self.last_transaction_count_key_name();
        let mut conn = self.redis.clone();

        let count: Option<u64> = conn.get(&tx_count_key).await?;
        match count {
            Some(count) => Ok(count),
            None => Err(self.nonce_sync_required_error()),
        }
    }

    /// Add a transaction to the pending queue and store its data
    /// This is called when a new transaction request comes in for an EOA
    pub async fn add_transaction(
        &self,
        transaction_request: EoaTransactionRequest,
    ) -> Result<(), TransactionStoreError> {
        let transaction_id = &transaction_request.transaction_id;

        let tx_data_key = self.transaction_data_key_name(transaction_id);
        let pending_key = self.pending_transactions_zset_name();

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
        pipeline.zadd(&pending_key, transaction_id, now);

        pipeline.query_async::<()>(&mut conn).await?;

        Ok(())
    }

    /// Get count of submitted transactions awaiting confirmation
    pub async fn get_submitted_transactions_count(&self) -> Result<u64, TransactionStoreError> {
        let submitted_key = self.submitted_transactions_zset_name();
        let mut conn = self.redis.clone();

        let count: u64 = conn.zcard(&submitted_key).await?;
        Ok(count)
    }

    /// Get the submitted transactions for the highest nonce value
    ///
    /// Internally submissions are stored in a zset by nonce -> hash:id
    ///
    /// This will return all hash:id pairs for the highest nonce
    #[tracing::instrument(skip_all)]
    pub async fn get_highest_submitted_nonce_tranasactions(
        &self,
    ) -> Result<Vec<SubmittedTransaction>, TransactionStoreError> {
        let submitted_key = self.submitted_transactions_zset_name();
        let mut conn = self.redis.clone();

        let highest_nonce_txs: Vec<SubmittedTransactionStringWithNonce> =
            conn.zrange_withscores(&submitted_key, -1, -1).await?;

        let submitted_txs: Vec<SubmittedTransaction> =
            SubmittedTransaction::from_redis_strings(&highest_nonce_txs);

        Ok(submitted_txs)
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
