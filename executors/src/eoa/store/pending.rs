use std::collections::HashSet;

use alloy::{consensus::Transaction, primitives::Address};
use twmq::redis::{AsyncCommands, Pipeline, aio::ConnectionManager};

use crate::eoa::{
    EoaExecutorStore,
    store::{
        BorrowedTransactionData, EoaExecutorStoreKeys, TransactionStoreError,
        atomic::SafeRedisTransaction,
    },
};

/// Atomic operation to move pending transactions to borrowed state using incremented nonces
///
/// This operation validates that:
/// 1. The nonces in the vector are sequential with no gaps
/// 2. The lowest nonce matches the current optimistic transaction count
/// 3. All transactions exist in the pending queue
///
/// Then atomically:
/// 1. Removes transactions from pending queue
/// 2. Adds transactions to borrowed state
/// 3. Updates optimistic transaction count to highest nonce + 1
pub struct MovePendingToBorrowedWithIncrementedNonces<'a> {
    pub transactions: &'a [BorrowedTransactionData],
    pub keys: &'a EoaExecutorStoreKeys,
    pub eoa: Address,
    pub chain_id: u64,
}

impl SafeRedisTransaction for MovePendingToBorrowedWithIncrementedNonces<'_> {
    type ValidationData = Vec<String>; // serialized borrowed transactions
    type OperationResult = usize; // number of transactions processed

    fn name(&self) -> &str {
        "pending->borrowed with incremented nonces"
    }

    fn watch_keys(&self) -> Vec<String> {
        vec![
            self.keys.optimistic_transaction_count_key_name(),
            self.keys.borrowed_transactions_hashmap_name(),
        ]
    }

    async fn validation(
        &self,
        conn: &mut ConnectionManager,
        _store: &EoaExecutorStore,
    ) -> Result<Self::ValidationData, TransactionStoreError> {
        if self.transactions.is_empty() {
            return Err(TransactionStoreError::InternalError {
                message: "Cannot process empty transaction list".to_string(),
            });
        }

        // Get current optimistic nonce
        let current_optimistic: Option<u64> = conn
            .get(self.keys.optimistic_transaction_count_key_name())
            .await?;
        let current_nonce = current_optimistic.ok_or(TransactionStoreError::NonceSyncRequired {
            eoa: self.eoa,
            chain_id: self.chain_id,
        })?;

        // Extract and validate nonces
        let mut nonces: Vec<u64> = self
            .transactions
            .iter()
            .map(|tx| tx.signed_transaction.nonce())
            .collect();
        nonces.sort();

        // Check that nonces are sequential with no gaps
        for (i, &nonce) in nonces.iter().enumerate() {
            let expected_nonce = current_nonce + i as u64;
            if nonce != expected_nonce {
                return Err(TransactionStoreError::InternalError {
                    message: format!(
                        "Non-sequential nonces detected: expected {expected_nonce}, found {nonce} at position {i}"
                    ),
                });
            }
        }

        // Verify all transactions exist in pending queue using batched ZSCORE calls
        if !self.transactions.is_empty() {
            let mut pipe = twmq::redis::pipe();
            for tx in self.transactions {
                pipe.zscore(
                    self.keys.pending_transactions_zset_name(),
                    &tx.transaction_id,
                );
            }
            let scores: Vec<Option<u64>> = pipe.query_async(conn).await?;

            for (tx, score) in self.transactions.iter().zip(scores.iter()) {
                if score.is_none() {
                    return Err(TransactionStoreError::TransactionNotInPendingQueue {
                        transaction_id: tx.transaction_id.clone(),
                    });
                }
            }
        }

        // Pre-serialize all borrowed transaction data
        let mut serialized_transactions = Vec::with_capacity(self.transactions.len());
        for tx in self.transactions {
            let borrowed_json =
                serde_json::to_string(tx).map_err(|e| TransactionStoreError::InternalError {
                    message: format!("Failed to serialize borrowed transaction: {e}"),
                })?;
            serialized_transactions.push(borrowed_json);
        }

        Ok(serialized_transactions)
    }

    fn operation(
        &self,
        pipeline: &mut Pipeline,
        serialized_transactions: Self::ValidationData,
    ) -> Self::OperationResult {
        let borrowed_key = self.keys.borrowed_transactions_hashmap_name();
        let pending_key = self.keys.pending_transactions_zset_name();
        let optimistic_key = self.keys.optimistic_transaction_count_key_name();

        for (tx, borrowed_json) in self.transactions.iter().zip(serialized_transactions.iter()) {
            // Remove from pending queue
            pipeline.zrem(&pending_key, &tx.transaction_id);

            // Add to borrowed state
            pipeline.hset(&borrowed_key, &tx.transaction_id, borrowed_json);
        }

        // Update optimistic tx count to highest nonce + 1
        if let Some(last_tx) = self.transactions.last() {
            let new_optimistic_tx_count = last_tx.signed_transaction.nonce() + 1;
            pipeline.set(&optimistic_key, new_optimistic_tx_count);
        }

        self.transactions.len()
    }
}

/// Atomic operation to move pending transactions to borrowed state using recycled nonces
///
/// This operation validates that:
/// 1. All nonces exist in the recycled nonces set
/// 2. All transactions exist in the pending queue
///
/// Then atomically:
/// 1. Removes nonces from recycled set
/// 2. Removes transactions from pending queue
/// 3. Adds transactions to borrowed state
pub struct MovePendingToBorrowedWithRecycledNonces<'a> {
    pub transactions: &'a [BorrowedTransactionData],
    pub keys: &'a EoaExecutorStoreKeys,
}

impl SafeRedisTransaction for MovePendingToBorrowedWithRecycledNonces<'_> {
    type ValidationData = Vec<String>; // serialized borrowed transactions
    type OperationResult = usize; // number of transactions processed

    fn name(&self) -> &str {
        "pending->borrowed with recycled nonces"
    }

    fn watch_keys(&self) -> Vec<String> {
        vec![
            self.keys.recycled_nonces_zset_name(),
            self.keys.borrowed_transactions_hashmap_name(),
        ]
    }

    async fn validation(
        &self,
        conn: &mut ConnectionManager,
        _store: &EoaExecutorStore,
    ) -> Result<Self::ValidationData, TransactionStoreError> {
        if self.transactions.is_empty() {
            return Err(TransactionStoreError::InternalError {
                message: "Cannot process empty transaction list".to_string(),
            });
        }

        // Get all recycled nonces
        let recycled_nonces: HashSet<u64> = conn
            .zrange(self.keys.recycled_nonces_zset_name(), 0, -1)
            .await?;

        // Verify all nonces are in recycled set
        for tx in self.transactions {
            let nonce = tx.signed_transaction.nonce();
            if !recycled_nonces.contains(&nonce) {
                return Err(TransactionStoreError::NonceNotInRecycledSet { nonce });
            }
        }

        // Verify all transactions exist in pending queue using batched ZSCORE calls
        if !self.transactions.is_empty() {
            let mut pipe = twmq::redis::pipe();
            for tx in self.transactions {
                pipe.zscore(
                    self.keys.pending_transactions_zset_name(),
                    &tx.transaction_id,
                );
            }
            let scores: Vec<Option<u64>> = pipe.query_async(conn).await?;

            for (tx, score) in self.transactions.iter().zip(scores.iter()) {
                if score.is_none() {
                    return Err(TransactionStoreError::TransactionNotInPendingQueue {
                        transaction_id: tx.transaction_id.clone(),
                    });
                }
            }
        }

        // Pre-serialize all borrowed transaction data
        let mut serialized_transactions = Vec::with_capacity(self.transactions.len());
        for tx in self.transactions {
            let borrowed_json =
                serde_json::to_string(tx).map_err(|e| TransactionStoreError::InternalError {
                    message: format!("Failed to serialize borrowed transaction: {e}"),
                })?;
            serialized_transactions.push(borrowed_json);
        }

        Ok(serialized_transactions)
    }

    fn operation(
        &self,
        pipeline: &mut Pipeline,
        serialized_transactions: Self::ValidationData,
    ) -> Self::OperationResult {
        let recycled_key = self.keys.recycled_nonces_zset_name();
        let pending_key = self.keys.pending_transactions_zset_name();
        let borrowed_key = self.keys.borrowed_transactions_hashmap_name();

        for (tx, borrowed_json) in self.transactions.iter().zip(serialized_transactions.iter()) {
            let nonce = tx.signed_transaction.nonce();

            // Remove nonce from recycled set
            pipeline.zrem(&recycled_key, nonce);

            // Remove from pending queue
            pipeline.zrem(&pending_key, &tx.transaction_id);

            // Add to borrowed state
            pipeline.hset(&borrowed_key, &tx.transaction_id, borrowed_json);
        }

        self.transactions.len()
    }
}
