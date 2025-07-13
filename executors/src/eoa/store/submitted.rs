use std::collections::{BTreeMap, HashMap, HashSet};

use serde::{Deserialize, Serialize};
use twmq::redis::{AsyncCommands, Pipeline, aio::ConnectionManager};

use crate::eoa::store::{
    ConfirmedTransaction, EoaExecutorStoreKeys, TransactionStoreError, atomic::SafeRedisTransaction,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubmittedTransaction {
    pub nonce: u64,
    pub hash: String,
    pub transaction_id: String,
    pub queued_at: u64,
}

pub type SubmittedTransactionStringWithNonce = (String, u64);

impl SubmittedTransaction {
    pub fn from_redis_strings(redis_strings: &[SubmittedTransactionStringWithNonce]) -> Vec<Self> {
        redis_strings
            .iter()
            .filter_map(|tx| {
                let parts: Vec<&str> = tx.0.split(':').collect();
                if parts.len() == 3 {
                    if let Ok(queued_at) = parts[2].parse::<u64>() {
                        Some(SubmittedTransaction {
                            hash: parts[0].to_string(),
                            transaction_id: parts[1].to_string(),
                            nonce: tx.1,
                            queued_at,
                        })
                    } else {
                        tracing::error!("Invalid queued_at timestamp: {}", tx.0);
                        None
                    }
                } else {
                    tracing::error!(
                        "Invalid transaction format, expected 3 parts separated by ':': {}",
                        tx.0
                    );
                    None
                }
            })
            .collect()
    }

    /// Returns the string representation of the submitted transaction with the nonce
    ///
    /// This is used to add the transaction to the submitted state in Redis
    ///
    /// The format is:
    ///
    /// ```text
    /// hash:transaction_id:queued_at
    /// ```
    ///
    /// The nonce is the value of the transaction in the submitted state, and is used as the score of the submitted zset
    pub fn to_redis_string_with_nonce(&self) -> SubmittedTransactionStringWithNonce {
        (
            format!("{}:{}:{}", self.hash, self.transaction_id, self.queued_at),
            self.nonce,
        )
    }
}

pub struct CleanSubmittedTransactions<'a> {
    pub last_confirmed_nonce: u64,
    pub confirmed_transactions: &'a [ConfirmedTransaction],
    pub keys: &'a EoaExecutorStoreKeys,
}

#[derive(Debug, Default)]
pub struct CleanupReport {
    pub total_hashes_processed: usize,
    pub unique_transaction_ids: usize,
    pub noop_count: usize,
    pub moved_to_success: usize,
    pub moved_to_pending: usize,

    /// Any transaction ID values that have multiple nonces in the submitted state
    pub cross_nonce_violations: Vec<(String, Vec<u64>)>, // (transaction_id, nonces)

    /// Any nonces that have multiple confirmations (very rare, indicates re-org)
    pub per_nonce_violations: Vec<(u64, Vec<String>)>, // (nonce, confirmed_hashes)

    /// Any nonces that have no confirmations (transactions we sent got replaced by a different one uknown to us)
    pub nonces_without_receipts: Vec<(u64, Vec<String>)>, // (nonce, hashes)
}

/// This operation takes a list of confirmed transactions and the last confirmed nonce
///
/// It will fetch all submitted transactions with a nonce less than or equal to the last confirmed nonce.
/// For each nonce:
/// - it will go through all the hashes for that nonce
/// - if the hash is in the confirmed transactions, it will be removed from submitted to success
/// - if the hash is not in the confirmed transactions, it will be removed from submitted to pending
///
/// It will also deduplicate transactions by ID, so if any of the hashes for that ID are in the confirmed transactions,
/// this hash will not be moved back to pending.
///
/// ***IMPORTANT***: This should not happen with different nonces. A transaction ID should only appear once in the submitted state.
/// Multiple submissions for the same transaction ID with different nonces can cause duplicate transactions
/// Multiple submissions for the same transaction ID with the same nonce is fine, because this indicated gas bumps.
impl SafeRedisTransaction for CleanSubmittedTransactions<'_> {
    type ValidationData = Vec<SubmittedTransaction>;
    type OperationResult = CleanupReport;

    fn name(&self) -> &str {
        "clean submitted transactions"
    }

    fn watch_keys(&self) -> Vec<String> {
        vec![self.keys.submitted_transactions_zset_name()]
    }

    async fn validation(
        &self,
        conn: &mut ConnectionManager,
    ) -> Result<Self::ValidationData, TransactionStoreError> {
        let submitted_txs: Vec<SubmittedTransactionStringWithNonce> = conn
            .zrangebyscore_withscores(
                self.keys.submitted_transactions_zset_name(),
                0,
                self.last_confirmed_nonce as isize,
            )
            .await?;

        let submitted_txs = SubmittedTransaction::from_redis_strings(&submitted_txs);
        Ok(submitted_txs)
    }

    fn operation(
        &self,
        pipeline: &mut Pipeline,
        submitted_txs: Self::ValidationData,
    ) -> Self::OperationResult {
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;

        // Build confirmed lookups
        let confirmed_hashes: HashSet<&str> = self
            .confirmed_transactions
            .iter()
            .map(|tx| tx.hash.as_str())
            .collect();

        let confirmed_ids: BTreeMap<&str, &ConfirmedTransaction> = self
            .confirmed_transactions
            .iter()
            .map(|tx| (tx.transaction_id.as_str(), tx))
            .collect();

        // Detect violations and get grouped data
        let (_, _, mut report) = detect_violations(&submitted_txs, &confirmed_hashes);

        // Process every hash and track unique IDs
        let mut processed_ids = HashSet::new();

        let mut replaced_transactions = Vec::with_capacity(submitted_txs.len());

        for tx in &submitted_txs {
            // Clean up this hash from Redis (happens for ALL hashes)
            let (submitted_tx_redis_string, _nonce) = tx.clone().to_redis_string_with_nonce();

            pipeline.zrem(
                self.keys.submitted_transactions_zset_name(),
                &submitted_tx_redis_string,
            );
            pipeline.del(self.keys.transaction_hash_to_id_key_name(&tx.hash));

            // Process each unique transaction_id once
            if processed_ids.insert(&tx.transaction_id) {
                match (
                    tx.transaction_id.as_str(),
                    confirmed_ids.get(tx.transaction_id.as_str()),
                ) {
                    // if the transaction id is noop, we don't do anything
                    ("noop", _) => report.noop_count += 1,

                    // in case of a valid ID, we check if it's in the confirmed transactions
                    // if it is confirmed, we succeed it and queue success jobs
                    (id, Some(confirmed_tx)) => {
                        let data_key_name = self.keys.transaction_data_key_name(id);
                        pipeline.hset(&data_key_name, "status", "confirmed");
                        pipeline.hset(&data_key_name, "completed_at", now);
                        pipeline.hset(&data_key_name, "receipt", confirmed_tx.receipt_data.clone());

                        // TODO:
                        // queue success jobs here

                        report.moved_to_success += 1;
                    }

                    // if the ID is not in the confirmed transactions, we queue it for pending
                    _ => {
                        replaced_transactions.push((&tx.transaction_id, tx.queued_at));
                        report.moved_to_pending += 1;
                    }
                }
            }
        }

        pipeline.zadd_multiple(
            self.keys.pending_transactions_zset_name(),
            &replaced_transactions,
        );

        // Finalize report stats
        report.total_hashes_processed = submitted_txs.len();
        report.unique_transaction_ids = processed_ids.len();

        report
    }
}

fn detect_violations<'a>(
    submitted_txs: &'a [SubmittedTransaction],
    confirmed_hashes: &'a HashSet<&str>,
) -> (
    HashMap<&'a str, Vec<u64>>,
    BTreeMap<u64, Vec<&'a SubmittedTransaction>>,
    CleanupReport,
) {
    let mut report = CleanupReport::default();
    let mut txs_by_nonce: BTreeMap<u64, Vec<&SubmittedTransaction>> = BTreeMap::new();
    let mut transaction_id_to_nonces: HashMap<&str, Vec<u64>> = HashMap::new();

    // Group data
    for tx in submitted_txs {
        txs_by_nonce.entry(tx.nonce).or_default().push(tx);
        transaction_id_to_nonces
            .entry(&tx.transaction_id)
            .or_default()
            .push(tx.nonce);
    }

    // Check cross-nonce violations
    for (transaction_id, nonces) in &transaction_id_to_nonces {
        let mut unique_nonces = nonces.clone();
        unique_nonces.sort();
        unique_nonces.dedup();
        if unique_nonces.len() > 1 {
            report
                .cross_nonce_violations
                .push((transaction_id.to_string(), unique_nonces));
        }
    }

    // Check per-nonce violations
    for (nonce, txs) in &txs_by_nonce {
        let confirmed_hashes_for_nonce: Vec<String> = txs
            .iter()
            .filter(|tx| confirmed_hashes.contains(tx.hash.as_str()))
            .map(|tx| tx.hash.clone())
            .collect();

        if confirmed_hashes_for_nonce.len() > 1 {
            report
                .per_nonce_violations
                .push((*nonce, confirmed_hashes_for_nonce));
        }
    }

    // Check nonces without receipts
    for (nonce, txs) in &txs_by_nonce {
        let has_confirmed = txs
            .iter()
            .any(|tx| confirmed_hashes.contains(tx.hash.as_str()));
        if !has_confirmed {
            let hashes: Vec<String> = txs.iter().map(|tx| tx.hash.clone()).collect();
            report.nonces_without_receipts.push((*nonce, hashes));
        }
    }

    (transaction_id_to_nonces, txs_by_nonce, report)
}
