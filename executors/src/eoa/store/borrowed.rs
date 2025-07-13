use std::collections::HashMap;
use std::sync::Arc;

use alloy::consensus::Transaction;
use alloy::primitives::Address;
use serde::{Deserialize, Serialize};
use twmq::redis::{AsyncCommands, Pipeline, aio::ConnectionManager};
use twmq::{Queue, hooks::TransactionContext};

use crate::eoa::{
    events::EoaExecutorEvent,
    store::{
        BorrowedTransactionData, EoaExecutorStoreKeys, TransactionData, TransactionStoreError,
        atomic::SafeRedisTransaction, submitted::SubmittedTransaction,
    },
    worker::EoaExecutorWorkerError,
};
use crate::webhook::{WebhookJobHandler, queue_webhook_envelopes};

/// Error information for NACK operations (retryable errors)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmissionErrorNack {
    pub transaction_id: String,
    pub error: EoaExecutorWorkerError,
    pub user_data: Option<TransactionData>,
}

/// Error information for FAIL operations (permanent failures)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmissionErrorFail {
    pub transaction_id: String,
    pub error: EoaExecutorWorkerError,
    pub user_data: Option<TransactionData>,
}

/// Result of a submission attempt
#[derive(Debug, Clone)]
pub enum SubmissionResult {
    Success(SubmittedTransaction),
    Nack(SubmissionErrorNack),
    Fail(SubmissionErrorFail),
}

/// Internal representation where all user data is guaranteed to be present
#[derive(Debug, Clone)]
pub enum SubmissionResultWithUserData {
    Success(SubmittedTransaction, TransactionData),
    Nack(SubmissionErrorNack, TransactionData),
    Fail(SubmissionErrorFail, TransactionData),
}

impl SubmissionResultWithUserData {
    fn transaction_id(&self) -> &str {
        match self {
            SubmissionResultWithUserData::Success(tx, _) => &tx.transaction_id,
            SubmissionResultWithUserData::Nack(err, _) => &err.transaction_id,
            SubmissionResultWithUserData::Fail(err, _) => &err.transaction_id,
        }
    }

    fn user_data(&self) -> &TransactionData {
        match self {
            SubmissionResultWithUserData::Success(_, data) => data,
            SubmissionResultWithUserData::Nack(_, data) => data,
            SubmissionResultWithUserData::Fail(_, data) => data,
        }
    }
}

/// Batch operation to process borrowed transactions
pub struct ProcessBorrowedTransactions<'a> {
    pub results: Vec<SubmissionResult>,
    pub keys: &'a EoaExecutorStoreKeys,
    pub webhook_queue: Arc<Queue<WebhookJobHandler>>,
}

#[derive(Debug, Default)]
pub struct BorrowedProcessingReport {
    pub total_processed: usize,
    pub moved_to_submitted: usize,
    pub moved_to_pending: usize,
    pub failed_transactions: usize,
    pub webhook_events_queued: usize,
}

impl SafeRedisTransaction for ProcessBorrowedTransactions<'_> {
    type ValidationData = (
        Vec<SubmissionResultWithUserData>,
        Vec<BorrowedTransactionData>,
    );
    type OperationResult = BorrowedProcessingReport;

    fn name(&self) -> &str {
        "process borrowed transactions"
    }

    fn watch_keys(&self) -> Vec<String> {
        vec![self.keys.borrowed_transactions_hashmap_name()]
    }

    async fn validation(
        &self,
        conn: &mut ConnectionManager,
    ) -> Result<Self::ValidationData, TransactionStoreError> {
        // Collect all transaction IDs that need user data
        let mut transactions_needing_data = Vec::new();
        let mut results_with_partial_data = Vec::new();

        for result in &self.results {
            match result {
                SubmissionResult::Success(tx) => {
                    transactions_needing_data.push(tx.transaction_id.clone());
                    results_with_partial_data.push(result.clone());
                }
                SubmissionResult::Nack(err) => {
                    if err.user_data.is_none() {
                        transactions_needing_data.push(err.transaction_id.clone());
                    }
                    results_with_partial_data.push(result.clone());
                }
                SubmissionResult::Fail(err) => {
                    if err.user_data.is_none() {
                        transactions_needing_data.push(err.transaction_id.clone());
                    }
                    results_with_partial_data.push(result.clone());
                }
            }
        }

        // Batch fetch missing user data
        let mut user_data_map = HashMap::new();
        for transaction_id in transactions_needing_data {
            let data_key = self.keys.transaction_data_key_name(&transaction_id);
            if let Some(data_json) = conn.get::<&str, Option<String>>(&data_key).await? {
                let transaction_data: TransactionData = serde_json::from_str(&data_json)?;
                user_data_map.insert(transaction_id, transaction_data);
            }
        }

        // Get all borrowed transactions to validate they exist
        let borrowed_transactions_map: HashMap<String, String> = conn
            .hgetall(self.keys.borrowed_transactions_hashmap_name())
            .await?;

        let borrowed_transactions: Vec<BorrowedTransactionData> = borrowed_transactions_map
            .into_iter()
            .filter_map(|(nonce_str, data_json)| {
                let borrowed_data: BorrowedTransactionData =
                    serde_json::from_str(&data_json).ok()?;
                Some(borrowed_data)
            })
            .collect();

        // Convert to results with guaranteed user data
        let mut results_with_user_data = Vec::new();
        for result in results_with_partial_data {
            match result {
                SubmissionResult::Success(tx) => {
                    if let Some(user_data) = user_data_map.get(&tx.transaction_id) {
                        results_with_user_data
                            .push(SubmissionResultWithUserData::Success(tx, user_data.clone()));
                    } else {
                        return Err(TransactionStoreError::TransactionNotFound {
                            transaction_id: tx.transaction_id.clone(),
                        });
                    }
                }
                SubmissionResult::Nack(mut err) => {
                    let user_data = if let Some(data) = err.user_data.take() {
                        data
                    } else if let Some(data) = user_data_map.get(&err.transaction_id) {
                        data.clone()
                    } else {
                        return Err(TransactionStoreError::TransactionNotFound {
                            transaction_id: err.transaction_id.clone(),
                        });
                    };
                    results_with_user_data.push(SubmissionResultWithUserData::Nack(err, user_data));
                }
                SubmissionResult::Fail(mut err) => {
                    let user_data = if let Some(data) = err.user_data.take() {
                        data
                    } else if let Some(data) = user_data_map.get(&err.transaction_id) {
                        data.clone()
                    } else {
                        return Err(TransactionStoreError::TransactionNotFound {
                            transaction_id: err.transaction_id.clone(),
                        });
                    };
                    results_with_user_data.push(SubmissionResultWithUserData::Fail(err, user_data));
                }
            }
        }

        Ok((results_with_user_data, borrowed_transactions))
    }

    fn operation(
        &self,
        pipeline: &mut Pipeline,
        validation_data: Self::ValidationData,
    ) -> Self::OperationResult {
        let (results_with_user_data, borrowed_transactions) = validation_data;
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;

        // Create borrowed transactions lookup by transaction_id
        let borrowed_by_id: HashMap<String, &BorrowedTransactionData> = borrowed_transactions
            .iter()
            .map(|tx| (tx.transaction_id.clone(), tx))
            .collect();

        let mut report = BorrowedProcessingReport::default();

        for result in &results_with_user_data {
            let transaction_id = result.transaction_id();
            let user_data = result.user_data();

            // Find the corresponding borrowed transaction to get the nonce
            let borrowed_tx = match borrowed_by_id.get(transaction_id) {
                Some(tx) => tx,
                None => {
                    // Transaction not in borrowed state, skip
                    continue;
                }
            };

            let nonce = borrowed_tx.signed_transaction.nonce();

            // We'll set attempt_number to 1 for simplicity in the operation phase
            // The actual attempt tracking is handled by the attempts list
            let attempt_number = 1;

            // Define attempts_key for all match arms
            let attempts_key = self.keys.transaction_attempts_list_name(transaction_id);

            match result {
                SubmissionResultWithUserData::Success(tx, user_data) => {
                    // Remove from borrowed
                    pipeline.hdel(
                        self.keys.borrowed_transactions_hashmap_name(),
                        nonce.to_string(),
                    );

                    // Add to submitted
                    let (submitted_tx_redis_string, nonce) = tx.to_redis_string_with_nonce();
                    pipeline.zadd(
                        self.keys.submitted_transactions_zset_name(),
                        &submitted_tx_redis_string,
                        nonce,
                    );

                    // Update hash-to-ID mapping
                    let hash_to_id_key = self.keys.transaction_hash_to_id_key_name(&tx.hash);
                    pipeline.set(&hash_to_id_key, &tx.transaction_id);

                    // Update transaction data status
                    let tx_data_key = self.keys.transaction_data_key_name(&tx.transaction_id);
                    pipeline.hset(&tx_data_key, "status", "submitted");

                    // Add attempt to attempts list
                    let attempt_json =
                        serde_json::to_string(&borrowed_tx.signed_transaction).unwrap();
                    pipeline.lpush(&attempts_key, &attempt_json);

                    // Queue webhook event
                    let event = EoaExecutorEvent {
                        transaction_data: user_data.clone(),
                    };
                    let envelope = event.send_attempt_success_envelope(tx.clone());
                    if let Some(webhook_options) = &user_data.user_request.webhook_options {
                        let mut tx_context = self
                            .webhook_queue
                            .transaction_context_from_pipeline(pipeline);
                        if let Err(e) = queue_webhook_envelopes(
                            envelope,
                            webhook_options.clone(),
                            &mut tx_context,
                            self.webhook_queue.clone(),
                        ) {
                            tracing::error!("Failed to queue webhook for success: {}", e);
                        } else {
                            report.webhook_events_queued += 1;
                        }
                    }

                    report.moved_to_submitted += 1;
                }
                SubmissionResultWithUserData::Nack(err, user_data) => {
                    // Remove from borrowed
                    pipeline.hdel(
                        self.keys.borrowed_transactions_hashmap_name(),
                        nonce.to_string(),
                    );

                    // Add back to pending
                    pipeline.zadd(
                        self.keys.pending_transactions_zset_name(),
                        &err.transaction_id,
                        now,
                    );

                    // Update transaction data status
                    let tx_data_key = self.keys.transaction_data_key_name(&err.transaction_id);
                    pipeline.hset(&tx_data_key, "status", "pending");

                    // Add attempt to attempts list
                    let attempt_json =
                        serde_json::to_string(&borrowed_tx.signed_transaction).unwrap();
                    pipeline.lpush(&attempts_key, &attempt_json);

                    // Queue webhook event
                    let event = EoaExecutorEvent {
                        transaction_data: user_data.clone(),
                    };
                    let envelope =
                        event.send_attempt_nack_envelope(nonce, err.error.clone(), attempt_number);
                    if let Some(webhook_options) = &user_data.user_request.webhook_options {
                        let mut tx_context = self
                            .webhook_queue
                            .transaction_context_from_pipeline(pipeline);
                        if let Err(e) = queue_webhook_envelopes(
                            envelope,
                            webhook_options.clone(),
                            &mut tx_context,
                            self.webhook_queue.clone(),
                        ) {
                            tracing::error!("Failed to queue webhook for nack: {}", e);
                        } else {
                            report.webhook_events_queued += 1;
                        }
                    }

                    report.moved_to_pending += 1;
                }
                SubmissionResultWithUserData::Fail(err, user_data) => {
                    // Remove from borrowed
                    pipeline.hdel(
                        self.keys.borrowed_transactions_hashmap_name(),
                        nonce.to_string(),
                    );

                    // Update transaction data with failure
                    let tx_data_key = self.keys.transaction_data_key_name(&err.transaction_id);
                    pipeline.hset(&tx_data_key, "status", "failed");
                    pipeline.hset(&tx_data_key, "completed_at", now);
                    pipeline.hset(&tx_data_key, "failure_reason", err.error.to_string());

                    // Add attempt to attempts list
                    let attempt_json =
                        serde_json::to_string(&borrowed_tx.signed_transaction).unwrap();
                    pipeline.lpush(&attempts_key, &attempt_json);

                    // Queue webhook event
                    let event = EoaExecutorEvent {
                        transaction_data: user_data.clone(),
                    };
                    let envelope =
                        event.transaction_failed_envelope(err.error.clone(), attempt_number);
                    if let Some(webhook_options) = &user_data.user_request.webhook_options {
                        let mut tx_context = self
                            .webhook_queue
                            .transaction_context_from_pipeline(pipeline);
                        if let Err(e) = queue_webhook_envelopes(
                            envelope,
                            webhook_options.clone(),
                            &mut tx_context,
                            self.webhook_queue.clone(),
                        ) {
                            tracing::error!("Failed to queue webhook for fail: {}", e);
                        } else {
                            report.webhook_events_queued += 1;
                        }
                    }

                    report.failed_transactions += 1;
                }
            }
        }

        report.total_processed = results_with_user_data.len();
        report
    }
}
