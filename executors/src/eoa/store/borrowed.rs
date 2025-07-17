use std::sync::Arc;

use twmq::Queue;
use twmq::redis::{AsyncCommands, Pipeline, aio::ConnectionManager};

use crate::eoa::EoaExecutorStore;
use crate::eoa::{
    events::EoaExecutorEvent,
    store::{
        EoaExecutorStoreKeys, TransactionStoreError, atomic::SafeRedisTransaction,
        submitted::SubmittedTransaction,
    },
    worker::error::EoaExecutorWorkerError,
};
use crate::webhook::{WebhookJobHandler, queue_webhook_envelopes};

#[derive(Debug, Clone)]
pub enum SubmissionResultType {
    Success,
    Nack(EoaExecutorWorkerError),
    Fail(EoaExecutorWorkerError),
}

/// Result of a submission attempt
#[derive(Debug, Clone)]
pub struct SubmissionResult {
    pub transaction: SubmittedTransaction,
    pub result: SubmissionResultType,
}

impl SubmissionResult {
    pub fn transaction_id(&self) -> &str {
        &self.transaction.transaction_id
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
    pub ignored_not_in_borrowed: usize,
}

impl SafeRedisTransaction for ProcessBorrowedTransactions<'_> {
    type ValidationData = Vec<SubmissionResult>;
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
        _store: &EoaExecutorStore,
    ) -> Result<Self::ValidationData, TransactionStoreError> {
        // Get all borrowed transaction IDs
        let borrowed_transaction_ids: Vec<String> = conn
            .hkeys(self.keys.borrowed_transactions_hashmap_name())
            .await?;

        // Filter submission results to only include those that exist in borrowed state
        let mut valid_results = Vec::new();
        for result in &self.results {
            let transaction_id = result.transaction_id();
            if borrowed_transaction_ids.contains(&transaction_id.to_string()) {
                valid_results.push(result.clone());
            } else {
                tracing::warn!(
                    transaction_id = %transaction_id,
                    nonce = %result.transaction.nonce,
                    "Submission result not found in borrowed state, ignoring"
                );
            }
        }

        Ok(valid_results)
    }

    fn operation(
        &self,
        pipeline: &mut Pipeline,
        validation_data: Self::ValidationData,
    ) -> Self::OperationResult {
        let valid_results = validation_data;
        let mut report = BorrowedProcessingReport::default();
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;

        for result in &valid_results {
            let transaction_id = result.transaction_id();
            let nonce = result.transaction.nonce;

            // Remove from borrowed hashmap
            pipeline.hdel(
                self.keys.borrowed_transactions_hashmap_name(),
                transaction_id,
            );

            // Add attempt to attempts list (using transaction hash as attempt data)
            // let attempt_json = serde_json::to_string(&result.transaction.hash).unwrap();
            // todo figure out what to do about attempts
            // pipeline.lpush(&attempts_key, &attempt_json);

            match &result.result {
                SubmissionResultType::Success => {
                    // Add to submitted zset
                    let (submitted_tx_redis_string, nonce) =
                        result.transaction.to_redis_string_with_nonce();
                    pipeline.zadd(
                        self.keys.submitted_transactions_zset_name(),
                        &submitted_tx_redis_string,
                        nonce,
                    );

                    // Update hash-to-ID mapping
                    let hash_to_id_key = self
                        .keys
                        .transaction_hash_to_id_key_name(&result.transaction.transaction_hash);

                    pipeline.set(&hash_to_id_key, transaction_id);

                    // Update transaction data status
                    let tx_data_key = self.keys.transaction_data_key_name(transaction_id);
                    pipeline.hset(&tx_data_key, "status", "submitted");

                    // Queue webhook event using user_request from SubmissionResult
                    let event = EoaExecutorEvent {
                        transaction_id: transaction_id.to_string(),
                    };

                    let envelope =
                        event.send_attempt_success_envelope(result.transaction.data.clone());
                    if !result.transaction.user_request.webhook_options.is_empty() {
                        let mut tx_context = self
                            .webhook_queue
                            .transaction_context_from_pipeline(pipeline);
                        if let Err(e) = queue_webhook_envelopes(
                            envelope,
                            result.transaction.user_request.webhook_options.clone(),
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
                SubmissionResultType::Nack(err) => {
                    // Add back to pending
                    pipeline.zadd(
                        self.keys.pending_transactions_zset_name(),
                        transaction_id,
                        now,
                    );

                    // Update transaction data status
                    let tx_data_key = self.keys.transaction_data_key_name(transaction_id);
                    pipeline.hset(&tx_data_key, "status", "pending");

                    // Queue webhook event using user_request from SubmissionResult
                    let event = EoaExecutorEvent {
                        transaction_id: transaction_id.to_string(),
                    };
                    let envelope = event.send_attempt_nack_envelope(nonce, err.clone(), 1);

                    if !result.transaction.user_request.webhook_options.is_empty() {
                        let mut tx_context = self
                            .webhook_queue
                            .transaction_context_from_pipeline(pipeline);
                        if let Err(e) = queue_webhook_envelopes(
                            envelope,
                            result.transaction.user_request.webhook_options.clone(),
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
                SubmissionResultType::Fail(err) => {
                    // Mark as failed
                    let tx_data_key = self.keys.transaction_data_key_name(transaction_id);
                    pipeline.hset(&tx_data_key, "status", "failed");
                    pipeline.hset(&tx_data_key, "completed_at", now);
                    pipeline.hset(&tx_data_key, "failure_reason", err.to_string());

                    // Queue webhook event using user_request from SubmissionResult
                    let event = EoaExecutorEvent {
                        transaction_id: transaction_id.to_string(),
                    };
                    let envelope = event.transaction_failed_envelope(err.clone(), 1);
                    if !result.transaction.user_request.webhook_options.is_empty() {
                        let mut tx_context = self
                            .webhook_queue
                            .transaction_context_from_pipeline(pipeline);
                        if let Err(e) = queue_webhook_envelopes(
                            envelope,
                            result.transaction.user_request.webhook_options.clone(),
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

        report.total_processed = valid_results.len();
        report.ignored_not_in_borrowed = self.results.len() - valid_results.len();
        report
    }
}
