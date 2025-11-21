use alloy::consensus::{Signed, Transaction, TypedTransaction};
use alloy::network::AnyTransactionReceipt;
use alloy::primitives::{Address, Bytes, U256};
use chrono;
use engine_core::chain::RpcCredentials;
use engine_core::credentials::SigningCredential;
use engine_core::execution_options::WebhookOptions;
use engine_core::transaction::TransactionTypeData;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::Deref;
use twmq::redis::{AsyncCommands, aio::ConnectionManager};

mod atomic;
mod borrowed;
mod pending;
mod submitted;

pub mod hydrate;

pub mod error;
pub use atomic::AtomicEoaExecutorStore;
pub use borrowed::{BorrowedProcessingReport, SubmissionResult, SubmissionResultType};
pub use submitted::{
    CleanupReport, SubmittedNoopTransaction, SubmittedTransaction, SubmittedTransactionDehydrated,
    SubmittedTransactionHydrated, SubmittedTransactionStringWithNonce,
};

pub const NO_OP_TRANSACTION_ID: &str = "noop";

#[derive(Debug, Clone)]
pub struct ReplacedTransaction {
    pub transaction_hash: String,
    pub transaction_id: String,
}

#[derive(Debug, Clone)]
pub struct ConfirmedTransaction {
    pub transaction_hash: String,
    pub transaction_id: String,
    pub receipt: alloy::rpc::types::TransactionReceipt,
    pub receipt_serialized: String,
}

/// (transaction_id, queued_at)
type PendingTransactionStringWithQueuedAt = (String, u64);

#[derive(Debug, Clone)]
pub struct PendingTransaction {
    pub transaction_id: String,
    pub queued_at: u64,
    pub user_request: EoaTransactionRequest,
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

    #[serde(default)]
    pub webhook_options: Vec<WebhookOptions>,

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
    /// - "failure_reason": String failure reason (optional)
    pub fn transaction_data_key_name(&self, transaction_id: &str) -> String {
        match &self.namespace {
            Some(ns) => format!("{ns}:eoa_executor:tx_data:{transaction_id}"),
            None => format!("eoa_executor:tx_data:{transaction_id}"),
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

    /// Name of the zset for pending transactions
    ///
    /// zset contains the `transaction_id` scored by the queued_at timestamp (unix timestamp in milliseconds)
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
    pub fn recycled_nonces_zset_name(&self) -> String {
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

    /// Manual reset key name.
    ///
    /// This holds a timestamp if a manual reset is scheduled.
    pub fn manual_reset_key_name(&self) -> String {
        match &self.namespace {
            Some(ns) => format!(
                "{ns}:eoa_executor:pending_manual_reset:{}:{}",
                self.chain_id, self.eoa
            ),
            None => format!(
                "eoa_executor:pending_manual_reset:{}:{}",
                self.chain_id, self.eoa
            ),
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
    pub queued_at: u64,
    pub hash: String,
    pub borrowed_at: u64,
}

#[derive(Debug, Clone)]
pub struct BorrowedTransaction {
    pub data: BorrowedTransactionData,
    pub user_request: EoaTransactionRequest,
}

impl Deref for BorrowedTransaction {
    type Target = BorrowedTransactionData;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl From<BorrowedTransactionData> for SubmittedTransactionDehydrated {
    fn from(data: BorrowedTransactionData) -> Self {
        SubmittedTransactionDehydrated {
            nonce: data.signed_transaction.nonce(),
            transaction_hash: data.signed_transaction.hash().to_string(),
            transaction_id: data.transaction_id.clone(),
            queued_at: data.queued_at,
            submitted_at: EoaExecutorStore::now(),
        }
    }
}

impl From<BorrowedTransaction> for SubmittedTransaction {
    fn from(data: BorrowedTransaction) -> Self {
        SubmittedTransaction {
            data: data.data.into(),
            user_request: data.user_request.clone(),
        }
    }
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

pub struct EoaExecutorCounts {
    pub pending_transactions: u64,
    pub submitted_transactions: u64,
    pub borrowed_transactions: u64,
    pub recycled_nonces: u64,
}

impl EoaExecutorStore {
    /// Aggressively acquire EOA lock, forcefully taking over from stalled workers
    ///
    /// Creates an AtomicEoaExecutorStore that owns the lock.
    pub async fn acquire_eoa_lock_aggressively(
        self,
        worker_id: &str,
        eoa_metrics: crate::metrics::EoaMetrics,
    ) -> Result<AtomicEoaExecutorStore, TransactionStoreError> {
        let lock_key = self.eoa_lock_key_name();
        let mut conn = self.redis.clone();

        // First try normal acquisition
        let acquired: bool = conn.set_nx(&lock_key, worker_id).await?;
        if acquired {
            return Ok(AtomicEoaExecutorStore {
                store: self,
                worker_id: worker_id.to_string(),
                eoa_metrics,
            });
        }
        let conflict_worker_id = conn.get::<_, Option<String>>(&lock_key).await?;

        // Lock exists, forcefully take it over
        tracing::warn!(
            eoa = ?self.eoa,
            chain_id = self.chain_id,
            worker_id = worker_id,
            conflict_worker_id = ?conflict_worker_id,
            "Forcefully taking over EOA lock from stalled worker."
        );
        // Force set - no expiry, only released by explicit takeover
        let _: () = conn.set(&lock_key, worker_id).await?;
        Ok(AtomicEoaExecutorStore {
            store: self,
            worker_id: worker_id.to_string(),
            eoa_metrics,
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

        for (_transaction_id, transaction_json) in borrowed_map {
            let borrowed_data: BorrowedTransactionData = serde_json::from_str(&transaction_json)?;
            result.push(borrowed_data);
        }

        Ok(result)
    }

    /// Get all hashes below a certain nonce from submitted transactions
    /// Returns (nonce, hash, transaction_id) tuples
    pub async fn get_submitted_transactions_below_chain_transaction_count(
        &self,
        count: u64,
    ) -> Result<Vec<SubmittedTransactionDehydrated>, TransactionStoreError> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let submitted_key = self.submitted_transactions_zset_name();
        let mut conn = self.redis.clone();

        // Get all entries with nonce < transaction count
        let results: Vec<SubmittedTransactionStringWithNonce> = conn
            .zrangebyscore_withscores(&submitted_key, 0, count - 1)
            .await?;

        let submitted_txs: Vec<SubmittedTransactionDehydrated> =
            SubmittedTransactionDehydrated::from_redis_strings(&results);

        Ok(submitted_txs)
    }

    /// Get all transaction IDs for a specific nonce
    pub async fn get_submitted_transactions_for_nonce(
        &self,
        nonce: u64,
    ) -> Result<Vec<SubmittedTransactionDehydrated>, TransactionStoreError> {
        let submitted_key = self.submitted_transactions_zset_name();
        let mut conn = self.redis.clone();

        let results: Vec<SubmittedTransactionStringWithNonce> = conn
            .zrangebyscore_withscores(&submitted_key, nonce, nonce)
            .await?;

        let submitted_txs: Vec<SubmittedTransactionDehydrated> =
            SubmittedTransactionDehydrated::from_redis_strings(&results);

        Ok(submitted_txs)
    }

    /// Check EOA health (balance, etc.)
    pub async fn get_eoa_health(&self) -> Result<Option<EoaHealth>, TransactionStoreError> {
        let mut conn = self.redis.clone();

        let health_json: Option<String> = conn.get(self.eoa_health_key_name()).await?;
        if let Some(json) = health_json {
            let health: EoaHealth = serde_json::from_str(&json)?;
            Ok(Some(health))
        } else {
            Ok(None)
        }
    }

    pub async fn get_all_counts(&self) -> Result<EoaExecutorCounts, TransactionStoreError> {
        let mut conn = self.redis.clone();
        let mut pipeline = twmq::redis::pipe();

        pipeline.zcard(self.pending_transactions_zset_name());
        pipeline.zcard(self.submitted_transactions_zset_name());
        pipeline.hlen(self.borrowed_transactions_hashmap_name());
        pipeline.zcard(self.recycled_nonces_zset_name());

        let counts: (u64, u64, u64, u64) = pipeline.query_async(&mut conn).await?;
        Ok(EoaExecutorCounts {
            pending_transactions: counts.0,
            submitted_transactions: counts.1,
            borrowed_transactions: counts.2,
            recycled_nonces: counts.3,
        })
    }

    /// Peek at pending transactions without removing them (safe for planning)
    pub async fn peek_pending_transactions(
        &self,
        limit: u64,
    ) -> Result<Vec<PendingTransaction>, TransactionStoreError> {
        self.peek_pending_transactions_paginated(0, limit).await
    }

    /// Peek at pending transactions with pagination support
    pub async fn peek_pending_transactions_paginated(
        &self,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<PendingTransaction>, TransactionStoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let pending_key = self.pending_transactions_zset_name();
        let mut conn = self.redis.clone();

        // Use ZRANGE to peek without removing, with offset support
        let start = offset as isize;
        let stop = (offset + limit - 1) as isize;

        let transaction_ids: Vec<PendingTransactionStringWithQueuedAt> =
            conn.zrange_withscores(&pending_key, start, stop).await?;

        if transaction_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut pipe = twmq::redis::pipe();

        for (transaction_id, _) in &transaction_ids {
            let tx_data_key = self.transaction_data_key_name(transaction_id);
            pipe.hget(&tx_data_key, "user_request");
        }

        let user_requests: Vec<Option<String>> = pipe.query_async(&mut conn).await?;

        let mut pending_transactions: Vec<PendingTransaction> = Vec::new();
        let mut deletion_pipe = twmq::redis::pipe();

        for ((transaction_id, queued_at), user_request) in
            transaction_ids.into_iter().zip(user_requests)
        {
            match user_request {
                Some(user_request) => {
                    let user_request_parsed = serde_json::from_str(&user_request)?;
                    pending_transactions.push(PendingTransaction {
                        transaction_id,
                        queued_at,
                        user_request: user_request_parsed,
                    });
                }
                None => {
                    tracing::warn!(
                        "Transaction {} data was missing, deleting transaction from redis",
                        transaction_id
                    );
                    deletion_pipe.zrem(self.keys.pending_transactions_zset_name(), transaction_id);
                }
            }
        }

        if !deletion_pipe.is_empty() {
            deletion_pipe.query_async::<()>(&mut conn).await?;
        }

        // let user_requests: Vec<EoaTransactionRequest> = user_requests
        //     .into_iter()
        //     .map(|user_request_json| serde_json::from_str(&user_request_json))
        //     .collect::<Result<Vec<EoaTransactionRequest>, serde_json::Error>>()?;

        // let pending_transactions: Vec<PendingTransaction> = transaction_ids
        //     .into_iter()
        //     .zip(user_requests)
        //     .map(
        //         |((transaction_id, queued_at), user_request)| PendingTransaction {
        //             transaction_id,
        //             queued_at,
        //             user_request,
        //         },
        //     )
        //     .collect();

        Ok(pending_transactions)
    }

    /// Peek at pending transactions and get optimistic nonce in a single operation
    /// This is optimized for the send flow to reduce Redis round-trips
    pub async fn peek_pending_transactions_with_optimistic_nonce(
        &self,
        limit: u64,
    ) -> Result<(Vec<PendingTransaction>, u64), TransactionStoreError> {
        if limit == 0 {
            let optimistic = self.get_optimistic_transaction_count().await?;
            return Ok((Vec::new(), optimistic));
        }

        let pending_key = self.pending_transactions_zset_name();
        let optimistic_key = self.optimistic_transaction_count_key_name();
        let mut conn = self.redis.clone();

        // First pipeline: Get transaction IDs and optimistic nonce together
        let start = 0isize;
        let stop = (limit - 1) as isize;

        let (transaction_ids, optimistic_nonce): (
            Vec<PendingTransactionStringWithQueuedAt>,
            Option<u64>,
        ) = twmq::redis::pipe()
            .zrange_withscores(&pending_key, start, stop)
            .get(&optimistic_key)
            .query_async(&mut conn)
            .await?;

        let optimistic = optimistic_nonce.ok_or_else(|| self.nonce_sync_required_error())?;

        if transaction_ids.is_empty() {
            return Ok((Vec::new(), optimistic));
        }

        // Second pipeline: Get transaction data
        let mut pipe = twmq::redis::pipe();
        for (transaction_id, _) in &transaction_ids {
            let tx_data_key = self.transaction_data_key_name(transaction_id);
            pipe.hget(&tx_data_key, "user_request");
        }

        let user_requests: Vec<Option<String>> = pipe.query_async(&mut conn).await?;

        let mut pending_transactions: Vec<PendingTransaction> = Vec::new();
        let mut deletion_pipe = twmq::redis::pipe();

        for ((transaction_id, queued_at), user_request) in
            transaction_ids.into_iter().zip(user_requests)
        {
            match user_request {
                Some(user_request) => {
                    let user_request_parsed = serde_json::from_str(&user_request)?;
                    pending_transactions.push(PendingTransaction {
                        transaction_id,
                        queued_at,
                        user_request: user_request_parsed,
                    });
                }
                None => {
                    tracing::warn!(
                        "Transaction {} data was missing, deleting transaction from redis",
                        transaction_id
                    );
                    deletion_pipe.zrem(self.keys.pending_transactions_zset_name(), transaction_id);
                }
            }
        }

        if !deletion_pipe.is_empty() {
            deletion_pipe.query_async::<()>(&mut conn).await?;
        }

        Ok((pending_transactions, optimistic))
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
    pub async fn get_optimistic_transaction_count(&self) -> Result<u64, TransactionStoreError> {
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

    /// Schedule a manual reset for the EOA
    pub async fn schedule_manual_reset(&self) -> Result<(), TransactionStoreError> {
        let manual_reset_key = self.manual_reset_key_name();
        let mut conn = self.redis.clone();
        conn.set::<_, _, ()>(&manual_reset_key, EoaExecutorStore::now())
            .await?;
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
    ) -> Result<Vec<SubmittedTransactionDehydrated>, TransactionStoreError> {
        let submitted_key = self.submitted_transactions_zset_name();
        let mut conn = self.redis.clone();

        let highest_nonce_txs: Vec<SubmittedTransactionStringWithNonce> =
            conn.zrange_withscores(&submitted_key, -1, -1).await?;

        let submitted_txs: Vec<SubmittedTransactionDehydrated> =
            SubmittedTransactionDehydrated::from_redis_strings(&highest_nonce_txs);

        Ok(submitted_txs)
    }

    /// Get the current time in milliseconds
    ///
    /// Used as the canonical time representation for this store
    pub fn now() -> u64 {
        chrono::Utc::now().timestamp_millis().max(0) as u64
    }

    /// Get count of pending transactions
    pub async fn get_pending_transactions_count(&self) -> Result<u64, TransactionStoreError> {
        let pending_key = self.pending_transactions_zset_name();
        let mut conn = self.redis.clone();

        let count: u64 = conn.zcard(&pending_key).await?;
        Ok(count)
    }

    /// Get count of borrowed transactions  
    pub async fn get_borrowed_transactions_count(&self) -> Result<u64, TransactionStoreError> {
        let borrowed_key = self.borrowed_transactions_hashmap_name();
        let mut conn = self.redis.clone();

        let count: u64 = conn.hlen(&borrowed_key).await?;
        Ok(count)
    }

    /// Get all recycled nonces
    pub async fn get_recycled_nonces(&self) -> Result<Vec<u64>, TransactionStoreError> {
        let recycled_key = self.recycled_nonces_zset_name();
        let mut conn = self.redis.clone();

        let nonces: Vec<u64> = conn.zrange(&recycled_key, 0, -1).await?;
        Ok(nonces)
    }

    /// Get count of recycled nonces
    pub async fn get_recycled_nonces_count(&self) -> Result<u64, TransactionStoreError> {
        let recycled_key = self.recycled_nonces_zset_name();
        let mut conn = self.redis.clone();

        let count: u64 = conn.zcard(&recycled_key).await?;
        Ok(count)
    }

    /// Get all submitted transactions (raw data)
    pub async fn get_all_submitted_transactions(
        &self,
    ) -> Result<Vec<SubmittedTransactionDehydrated>, TransactionStoreError> {
        let submitted_key = self.submitted_transactions_zset_name();
        let mut conn = self.redis.clone();

        let submitted_data: Vec<SubmittedTransactionStringWithNonce> =
            conn.zrange_withscores(&submitted_key, 0, -1).await?;

        let submitted_txs: Vec<SubmittedTransactionDehydrated> =
            SubmittedTransactionDehydrated::from_redis_strings(&submitted_data);

        Ok(submitted_txs)
    }

    /// Get attempts count for a specific transaction
    pub async fn get_transaction_attempts_count(
        &self,
        transaction_id: &str,
    ) -> Result<u64, TransactionStoreError> {
        let attempts_key = self.transaction_attempts_list_name(transaction_id);
        let mut conn = self.redis.clone();

        let count: u64 = conn.llen(&attempts_key).await?;
        Ok(count)
    }

    /// Get all transaction attempts for a specific transaction
    pub async fn get_transaction_attempts(
        &self,
        transaction_id: &str,
    ) -> Result<Vec<TransactionAttempt>, TransactionStoreError> {
        let attempts_key = self.transaction_attempts_list_name(transaction_id);
        let mut conn = self.redis.clone();

        let attempts_data: Vec<String> = conn.lrange(&attempts_key, 0, -1).await?;

        let mut attempts = Vec::new();
        for attempt_json in attempts_data {
            let attempt: TransactionAttempt = serde_json::from_str(&attempt_json)?;
            attempts.push(attempt);
        }

        Ok(attempts)
    }

    pub async fn is_manual_reset_scheduled(&self) -> Result<bool, TransactionStoreError> {
        let manual_reset_key = self.manual_reset_key_name();
        let mut conn = self.redis.clone();

        let manual_reset: Option<u64> = conn.get(&manual_reset_key).await?;
        Ok(manual_reset.is_some())
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
