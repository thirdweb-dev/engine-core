use alloy::consensus::Transaction;
use alloy::network::AnyTransactionReceipt;
use alloy::primitives::{Address, B256, U256};
use alloy::rpc::types::{TransactionReceipt, TransactionRequest};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use twmq::redis::{AsyncCommands, Pipeline, aio::ConnectionManager};

#[derive(Debug, Error)]
pub enum TransactionStoreError {
    #[error("Redis error: {0}")]
    RedisError(#[from] twmq::redis::RedisError),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Transaction not found: {transaction_id}")]
    TransactionNotFound { transaction_id: String },
}

/// Initial transaction data from user request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionData {
    pub transaction_id: String,
    pub eoa: Address,
    pub chain_id: u64,
    pub to: Option<Address>,
    pub value: U256,
    pub data: Vec<u8>,
    pub gas_limit: Option<u64>,
    pub created_at: u64,
}

/// Active attempt for a transaction (full alloy transaction + metadata)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveAttempt {
    pub transaction_id: String,
    pub nonce: U256,
    pub transaction_hash: B256,
    pub alloy_transaction: TransactionRequest, // Full serializable alloy transaction
    pub sent_at: u64,
    pub attempt_number: u32,
}

impl ActiveAttempt {
    /// Get the queue job ID for this attempt (includes attempt number)
    pub fn queue_job_id(&self) -> String {
        format!("{}_{}", self.transaction_id, self.attempt_number)
    }
}

/// Confirmation data for a successful transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfirmationData {
    pub transaction_hash: B256,
    pub confirmed_at: u64,
    pub receipt: AnyTransactionReceipt,
}

/// Transaction store focused on transaction_id operations and nonce indexing
pub struct TransactionStore {
    pub redis: ConnectionManager,
    pub namespace: Option<String>,
}

impl TransactionStore {
    pub fn new(redis: ConnectionManager, namespace: Option<String>) -> Self {
        Self { redis, namespace }
    }

    // Redis key methods
    fn transaction_data_key(&self, transaction_id: &str) -> String {
        match &self.namespace {
            Some(ns) => format!("{}:eoa_tx_data:{}", ns, transaction_id),
            None => format!("eoa_tx_data:{}", transaction_id),
        }
    }

    fn active_attempt_key(&self, transaction_id: &str) -> String {
        match &self.namespace {
            Some(ns) => format!("{}:eoa_active_attempt:{}", ns, transaction_id),
            None => format!("eoa_active_attempt:{}", transaction_id),
        }
    }

    fn nonce_to_transactions_key(&self, eoa: Address, chain_id: u64, nonce: U256) -> String {
        match &self.namespace {
            Some(ns) => format!("{}:eoa_nonce_to_txs:{}:{}:{}", ns, chain_id, eoa, nonce),
            None => format!("eoa_nonce_to_txs:{}:{}:{}", chain_id, eoa, nonce),
        }
    }

    fn eoa_active_transactions_key(&self, eoa: Address, chain_id: u64) -> String {
        match &self.namespace {
            Some(ns) => format!("{}:eoa_active_txs:{}:{}", ns, chain_id, eoa),
            None => format!("eoa_active_txs:{}:{}", chain_id, eoa),
        }
    }

    fn confirmation_key(&self, transaction_id: &str) -> String {
        match &self.namespace {
            Some(ns) => format!("{}:eoa_confirmation:{}", ns, transaction_id),
            None => format!("eoa_confirmation:{}", transaction_id),
        }
    }

    fn attempt_counter_key(&self, transaction_id: &str) -> String {
        match &self.namespace {
            Some(ns) => format!("{}:eoa_attempt_counter:{}", ns, transaction_id),
            None => format!("eoa_attempt_counter:{}", transaction_id),
        }
    }

    /// Store initial transaction data
    pub async fn store_transaction_data(
        &self,
        transaction_data: &TransactionData,
    ) -> Result<(), TransactionStoreError> {
        let mut conn = self.redis.clone();
        let data_key = self.transaction_data_key(&transaction_data.transaction_id);
        let active_key =
            self.eoa_active_transactions_key(transaction_data.eoa, transaction_data.chain_id);

        let data_json = serde_json::to_string(transaction_data)?;

        // Store transaction data
        let _: () = conn.set(&data_key, data_json).await?;

        // Add to active transactions set
        let _: () = conn
            .sadd(&active_key, &transaction_data.transaction_id)
            .await?;

        Ok(())
    }

    /// Get initial transaction data by transaction ID
    pub async fn get_transaction_data(
        &self,
        transaction_id: &str,
    ) -> Result<Option<TransactionData>, TransactionStoreError> {
        let mut conn = self.redis.clone();
        let data_key = self.transaction_data_key(transaction_id);

        let data_json: Option<String> = conn.get(&data_key).await?;

        match data_json {
            Some(json) => {
                let data: TransactionData = serde_json::from_str(&json)?;
                Ok(Some(data))
            }
            None => Ok(None),
        }
    }

    /// Add/update active attempt for a transaction
    pub async fn add_active_attempt(
        &self,
        attempt: &ActiveAttempt,
    ) -> Result<(), TransactionStoreError> {
        let mut conn = self.redis.clone();
        let attempt_key = self.active_attempt_key(&attempt.transaction_id);

        // Get transaction data to determine EOA and chain_id for indexing
        let tx_data = self
            .get_transaction_data(&attempt.transaction_id)
            .await?
            .ok_or_else(|| TransactionStoreError::TransactionNotFound {
                transaction_id: attempt.transaction_id.clone(),
            })?;

        let nonce_key =
            self.nonce_to_transactions_key(tx_data.eoa, tx_data.chain_id, attempt.nonce);
        let counter_key = self.attempt_counter_key(&attempt.transaction_id);

        let attempt_json = serde_json::to_string(attempt)?;

        // Store active attempt
        let _: () = conn.set(&attempt_key, attempt_json).await?;

        // Index by nonce (multiple transactions can compete for same nonce)
        let _: () = conn.sadd(&nonce_key, &attempt.transaction_id).await?;

        // Increment attempt counter for observability
        let _: () = conn.incr(&counter_key, 1).await?;

        Ok(())
    }

    /// Get active attempt for a transaction
    pub async fn get_active_attempt(
        &self,
        transaction_id: &str,
    ) -> Result<Option<ActiveAttempt>, TransactionStoreError> {
        let mut conn = self.redis.clone();
        let attempt_key = self.active_attempt_key(transaction_id);

        let attempt_json: Option<String> = conn.get(&attempt_key).await?;

        match attempt_json {
            Some(json) => {
                let attempt: ActiveAttempt = serde_json::from_str(&json)?;
                Ok(Some(attempt))
            }
            None => Ok(None),
        }
    }

    /// Get all transaction IDs competing for a specific nonce
    pub async fn get_transactions_by_nonce(
        &self,
        eoa: Address,
        chain_id: u64,
        nonce: U256,
    ) -> Result<Vec<String>, TransactionStoreError> {
        let mut conn = self.redis.clone();
        let nonce_key = self.nonce_to_transactions_key(eoa, chain_id, nonce);

        let transaction_ids: Vec<String> = conn.smembers(&nonce_key).await?;
        Ok(transaction_ids)
    }

    /// Get all active transaction IDs for an EOA
    pub async fn get_active_transactions(
        &self,
        eoa: Address,
        chain_id: u64,
    ) -> Result<Vec<String>, TransactionStoreError> {
        let mut conn = self.redis.clone();
        let active_key = self.eoa_active_transactions_key(eoa, chain_id);

        let transaction_ids: Vec<String> = conn.smembers(&active_key).await?;
        Ok(transaction_ids)
    }

    /// Get all sent transactions (have active attempts) for an EOA
    pub async fn get_sent_transactions(
        &self,
        eoa: Address,
        chain_id: u64,
    ) -> Result<Vec<(String, ActiveAttempt)>, TransactionStoreError> {
        let transaction_ids = self.get_active_transactions(eoa, chain_id).await?;

        let mut sent_transactions = Vec::new();
        for transaction_id in transaction_ids {
            if let Some(attempt) = self.get_active_attempt(&transaction_id).await? {
                sent_transactions.push((transaction_id, attempt));
            }
        }

        Ok(sent_transactions)
    }

    /// Mark transaction as confirmed and clean up
    pub async fn mark_transaction_confirmed(
        &self,
        transaction_id: &str,
        confirmation_data: &ConfirmationData,
    ) -> Result<(), TransactionStoreError> {
        let mut conn = self.redis.clone();
        let confirmation_key = self.confirmation_key(transaction_id);

        // Get transaction data to determine EOA and chain_id
        let tx_data = self
            .get_transaction_data(transaction_id)
            .await?
            .ok_or_else(|| TransactionStoreError::TransactionNotFound {
                transaction_id: transaction_id.to_string(),
            })?;

        let active_key = self.eoa_active_transactions_key(tx_data.eoa, tx_data.chain_id);
        let attempt_key = self.active_attempt_key(transaction_id);

        // Get current attempt to clean up nonce index
        if let Some(attempt) = self.get_active_attempt(transaction_id).await? {
            let nonce_key =
                self.nonce_to_transactions_key(tx_data.eoa, tx_data.chain_id, attempt.nonce);
            let _: () = conn.srem(&nonce_key, transaction_id).await?;
        }

        // Store confirmation data
        let confirmation_json = serde_json::to_string(confirmation_data)?;
        let _: () = conn.set(&confirmation_key, confirmation_json).await?;

        // Remove from active set
        let _: () = conn.srem(&active_key, transaction_id).await?;

        // Remove active attempt
        let _: () = conn.del(&attempt_key).await?;

        Ok(())
    }

    /// Mark transaction as failed and clean up
    pub async fn mark_transaction_failed(
        &self,
        transaction_id: &str,
        error_message: &str,
    ) -> Result<(), TransactionStoreError> {
        let mut conn = self.redis.clone();

        // Get transaction data to determine EOA and chain_id
        let tx_data = self
            .get_transaction_data(transaction_id)
            .await?
            .ok_or_else(|| TransactionStoreError::TransactionNotFound {
                transaction_id: transaction_id.to_string(),
            })?;

        let active_key = self.eoa_active_transactions_key(tx_data.eoa, tx_data.chain_id);
        let attempt_key = self.active_attempt_key(transaction_id);

        // Get current attempt to clean up nonce index
        if let Some(attempt) = self.get_active_attempt(transaction_id).await? {
            let nonce_key =
                self.nonce_to_transactions_key(tx_data.eoa, tx_data.chain_id, attempt.nonce);
            let _: () = conn.srem(&nonce_key, transaction_id).await?;
        }

        // Remove from active set
        let _: () = conn.srem(&active_key, transaction_id).await?;

        // Remove active attempt
        let _: () = conn.del(&attempt_key).await?;

        Ok(())
    }

    /// Remove active attempt (for requeuing after race loss)
    pub async fn remove_active_attempt(
        &self,
        transaction_id: &str,
    ) -> Result<(), TransactionStoreError> {
        let mut conn = self.redis.clone();
        let attempt_key = self.active_attempt_key(transaction_id);

        // Get current attempt to clean up nonce index
        if let Some(attempt) = self.get_active_attempt(transaction_id).await? {
            let tx_data = self
                .get_transaction_data(transaction_id)
                .await?
                .ok_or_else(|| TransactionStoreError::TransactionNotFound {
                    transaction_id: transaction_id.to_string(),
                })?;

            let nonce_key =
                self.nonce_to_transactions_key(tx_data.eoa, tx_data.chain_id, attempt.nonce);
            let _: () = conn.srem(&nonce_key, transaction_id).await?;
        }

        // Remove active attempt (transaction stays in active set for requeuing)
        let _: () = conn.del(&attempt_key).await?;

        Ok(())
    }

    /// Pipeline commands for atomic operations in hooks
    pub fn add_store_transaction_command(
        &self,
        pipeline: &mut Pipeline,
        transaction_data: &TransactionData,
    ) {
        let data_key = self.transaction_data_key(&transaction_data.transaction_id);
        let active_key =
            self.eoa_active_transactions_key(transaction_data.eoa, transaction_data.chain_id);

        let data_json = serde_json::to_string(transaction_data).unwrap();

        pipeline.set(&data_key, data_json);
        pipeline.sadd(&active_key, &transaction_data.transaction_id);
    }

    pub fn add_active_attempt_command(
        &self,
        pipeline: &mut Pipeline,
        attempt: &ActiveAttempt,
        eoa: Address,
        chain_id: u64,
    ) {
        let attempt_key = self.active_attempt_key(&attempt.transaction_id);
        let nonce_key = self.nonce_to_transactions_key(eoa, chain_id, attempt.nonce);
        let counter_key = self.attempt_counter_key(&attempt.transaction_id);

        let attempt_json = serde_json::to_string(attempt).unwrap();

        pipeline.set(&attempt_key, attempt_json);
        pipeline.sadd(&nonce_key, &attempt.transaction_id);
        pipeline.incr(&counter_key, 1);
    }

    pub fn add_remove_active_attempt_command(
        &self,
        pipeline: &mut Pipeline,
        transaction_id: &str,
        eoa: Address,
        chain_id: u64,
        nonce: U256,
    ) {
        let attempt_key = self.active_attempt_key(transaction_id);
        let nonce_key = self.nonce_to_transactions_key(eoa, chain_id, nonce);

        pipeline.del(&attempt_key);
        pipeline.srem(&nonce_key, transaction_id);
    }

    pub fn add_mark_confirmed_command(
        &self,
        pipeline: &mut Pipeline,
        transaction_id: &str,
        confirmation_data: &ConfirmationData,
        eoa: Address,
        chain_id: u64,
        nonce: U256,
    ) {
        let confirmation_key = self.confirmation_key(transaction_id);
        let active_key = self.eoa_active_transactions_key(eoa, chain_id);
        let attempt_key = self.active_attempt_key(transaction_id);
        let nonce_key = self.nonce_to_transactions_key(eoa, chain_id, nonce);

        let confirmation_json = serde_json::to_string(confirmation_data).unwrap();

        pipeline.set(&confirmation_key, confirmation_json);
        pipeline.srem(&active_key, transaction_id);
        pipeline.del(&attempt_key);
        pipeline.srem(&nonce_key, transaction_id);
    }

    pub fn add_mark_failed_command(
        &self,
        pipeline: &mut Pipeline,
        transaction_id: &str,
        eoa: Address,
        chain_id: u64,
        nonce: U256,
    ) {
        let active_key = self.eoa_active_transactions_key(eoa, chain_id);
        let attempt_key = self.active_attempt_key(transaction_id);
        let nonce_key = self.nonce_to_transactions_key(eoa, chain_id, nonce);

        pipeline.srem(&active_key, transaction_id);
        pipeline.del(&attempt_key);
        pipeline.srem(&nonce_key, transaction_id);
    }
}
