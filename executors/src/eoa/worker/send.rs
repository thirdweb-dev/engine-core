use alloy::{consensus::Transaction, providers::Provider};
use engine_core::{chain::Chain, error::AlloyRpcErrorToEngineError};

use crate::{
    eoa::{
        EoaExecutorStore,
        store::{BorrowedTransaction, PendingTransaction, SubmissionResult, SubmissionResultType},
        worker::{
            EoaExecutorWorker,
            error::{
                EoaExecutorWorkerError, SendContext, is_retryable_preparation_error,
                should_update_balance_threshold,
            },
        },
    },
    metrics::{calculate_duration_seconds, current_timestamp_ms},
};

const HEALTH_CHECK_INTERVAL_MS: u64 = 60 * 5 * 1000; // 5 minutes in milliseconds

impl<C: Chain> EoaExecutorWorker<C> {
    // ========== SEND FLOW ==========
    #[tracing::instrument(skip_all, fields(worker_id = self.store.worker_id))]
    pub async fn send_flow(&self) -> Result<u32, EoaExecutorWorkerError> {
        let start_time = current_timestamp_ms();

        // 1. Get EOA health (initializes if needed) and check if we should update balance
        let mut health = self.get_eoa_health().await?;
        let now = EoaExecutorStore::now();

        tracing::info!(
            duration_seconds = calculate_duration_seconds(start_time, current_timestamp_ms()),
            eoa = ?self.eoa,
            chain_id = self.chain_id,
            worker_id = %self.store.worker_id,
            "JOB_LIFECYCLE - send_flow: Got EOA health"
        );

        // Update balance if it's stale
        // TODO: refactor this, very ugly
        if health.balance <= health.balance_threshold {
            if now - health.balance_fetched_at > HEALTH_CHECK_INTERVAL_MS {
                let balance = self
                    .chain
                    .provider()
                    .get_balance(self.eoa)
                    .await
                    .map_err(|e| {
                        let engine_error = e.to_engine_error(&self.chain);
                        EoaExecutorWorkerError::RpcError {
                            message: format!("Failed to get balance: {engine_error}"),
                            inner_error: engine_error,
                        }
                    })?;

                health.balance = balance;
                health.balance_fetched_at = now;
                self.store.update_health_data(&health).await?;
            }

            if health.balance <= health.balance_threshold {
                tracing::warn!(
                    "EOA has insufficient balance (<= {} wei), skipping send flow",
                    health.balance_threshold
                );
                return Err(EoaExecutorWorkerError::EoaOutOfFunds {
                    balance: health.balance,
                    balance_threshold: health.balance_threshold,
                });
            }
        }

        tracing::info!(
            duration_seconds = calculate_duration_seconds(start_time, current_timestamp_ms()),
            eoa = ?self.eoa,
            chain_id = self.chain_id,
            worker_id = %self.store.worker_id,
            "JOB_LIFECYCLE - send_flow: Completed balance check"
        );

        let mut total_sent = 0;

        // 2. Process recycled nonces first
        let recycled_start = current_timestamp_ms();
        total_sent += self.process_recycled_nonces().await?;

        tracing::info!(
            duration_seconds = calculate_duration_seconds(recycled_start, current_timestamp_ms()),
            total_duration_seconds = calculate_duration_seconds(start_time, current_timestamp_ms()),
            transactions_sent = total_sent,
            eoa = ?self.eoa,
            chain_id = self.chain_id,
            worker_id = %self.store.worker_id,
            "JOB_LIFECYCLE - send_flow: Completed processing recycled nonces"
        );

        // 3. Only proceed to new nonces if we successfully used all recycled nonces
        let clean_start = current_timestamp_ms();
        let remaining_recycled = self.store.clean_and_get_recycled_nonces().await?.len();

        tracing::info!(
            duration_seconds = calculate_duration_seconds(clean_start, current_timestamp_ms()),
            remaining_recycled = remaining_recycled,
            eoa = ?self.eoa,
            chain_id = self.chain_id,
            worker_id = %self.store.worker_id,
            "JOB_LIFECYCLE - send_flow: Cleaned and got recycled nonces"
        );

        if remaining_recycled == 0 {
            let budget_start = current_timestamp_ms();
            let inflight_budget = self.store.get_inflight_budget(self.max_inflight).await?;

            tracing::info!(
                duration_seconds = calculate_duration_seconds(budget_start, current_timestamp_ms()),
                total_duration_seconds = calculate_duration_seconds(start_time, current_timestamp_ms()),
                inflight_budget = inflight_budget,
                eoa = ?self.eoa,
                chain_id = self.chain_id,
                worker_id = %self.store.worker_id,
                "JOB_LIFECYCLE - send_flow: Got inflight budget"
            );

            if inflight_budget > 0 {
                let new_tx_start = current_timestamp_ms();
                total_sent += self.process_new_transactions(inflight_budget).await?;

                tracing::info!(
                    duration_seconds = calculate_duration_seconds(new_tx_start, current_timestamp_ms()),
                    total_duration_seconds = calculate_duration_seconds(start_time, current_timestamp_ms()),
                    transactions_sent = total_sent,
                    eoa = ?self.eoa,
                    chain_id = self.chain_id,
                    worker_id = %self.store.worker_id,
                    "JOB_LIFECYCLE - send_flow: Completed processing new transactions"
                );
            } else {
                tracing::warn!("No inflight budget, not sending new transactions");
            }
        } else {
            tracing::warn!(
                "Still have {} recycled nonces, not sending new transactions",
                remaining_recycled
            );
        }

        tracing::info!(
            total_sent = total_sent,
            total_duration_seconds = calculate_duration_seconds(start_time, current_timestamp_ms()),
            eoa = ?self.eoa,
            chain_id = self.chain_id,
            worker_id = %self.store.worker_id,
            "JOB_LIFECYCLE - send_flow: Completed send flow"
        );

        Ok(total_sent)
    }

    async fn process_recycled_nonces(&self) -> Result<u32, EoaExecutorWorkerError> {
        let mut total_sent: usize = 0;
        let mut is_pending_empty = false;

        // Loop to handle preparation failures and refill with new transactions
        for _ in 0..10 {
            let recycled_nonces = self.store.clean_and_get_recycled_nonces().await?;

            if recycled_nonces.is_empty() {
                return Ok(total_sent as u32);
            }

            // Get pending transactions to match with recycled nonces
            let pending_txs = self
                .store
                .peek_pending_transactions(recycled_nonces.len() as u64)
                .await?;

            // Pair recycled nonces with pending transactions
            let mut build_tasks = Vec::new();

            for (i, nonce) in recycled_nonces.iter().enumerate() {
                if let Some(p_tx) = pending_txs.get(i) {
                    build_tasks
                        .push(self.build_and_sign_single_transaction_with_retries(p_tx, *nonce));
                } else {
                    // No more pending transactions for this recycled nonce
                    is_pending_empty = true;
                    break;
                }
            }

            if build_tasks.is_empty() {
                break;
            }

            // Build and sign all transactions in parallel
            let prepared_results = futures::future::join_all(build_tasks).await;
            let prepared_results_with_pending = pending_txs
                .iter()
                .take(prepared_results.len())
                .zip(prepared_results.into_iter())
                .collect::<Vec<_>>();

            let cleaned_results = self
                .clean_prepration_results(prepared_results_with_pending, false)
                .await?;

            if cleaned_results.is_empty() {
                // No successful preparations, try again with more pending transactions
                continue;
            }

            // Move prepared transactions to borrowed state with recycled nonces
            let moved_count = self
                .store
                .atomic_move_pending_to_borrowed_with_recycled_nonces(
                    &cleaned_results
                        .iter()
                        .map(|borrowed_tx| borrowed_tx.data.clone())
                        .collect::<Vec<_>>(),
                )
                .await?;

            tracing::debug!(
                moved_count = moved_count,
                total_prepared = cleaned_results.len(),
                "Moved transactions to borrowed state using recycled nonces"
            );

            // Actually send the transactions to the blockchain
            let send_tasks: Vec<_> = cleaned_results
                .iter()
                .map(|borrowed_tx| {
                    let signed_tx = borrowed_tx.signed_transaction.clone();
                    async move {
                        self.chain
                            .provider()
                            .send_tx_envelope(signed_tx.into())
                            .await
                    }
                })
                .collect();

            let send_results = futures::future::join_all(send_tasks).await;

            // Process send results and update states
            let submission_results = send_results
                .into_iter()
                .zip(cleaned_results.into_iter())
                .map(|(send_result, borrowed_tx)| {
                    SubmissionResult::from_send_result(
                        &borrowed_tx,
                        send_result,
                        SendContext::InitialBroadcast,
                        &self.chain,
                    )
                })
                .collect();

            // Use batch processing to handle all submission results
            let processing_report = self
                .store
                .process_borrowed_transactions(submission_results, self.webhook_queue.clone())
                .await?;

            tracing::debug!(
                "Processed {} borrowed transactions: {} moved to submitted, {} moved to pending, {} failed",
                processing_report.total_processed,
                processing_report.moved_to_submitted,
                processing_report.moved_to_pending,
                processing_report.failed_transactions
            );

            total_sent += processing_report.moved_to_submitted;
        }

        if is_pending_empty {
            let recycled_nonces = self.store.clean_and_get_recycled_nonces().await?;
            let mut build_tasks = Vec::new();

            for nonce in recycled_nonces {
                build_tasks.push(self.send_noop_transaction(nonce));
            }

            let send_results = futures::future::join_all(build_tasks).await;

            let successful_sends = send_results
                .into_iter()
                .filter_map(|result| result.ok())
                .collect::<Vec<_>>();

            self.store
                .process_noop_transactions(&successful_sends)
                .await?;
        }

        Ok(total_sent as u32)
    }

    /// Clean preparation results to only contain successful transactions.
    ///
    /// If `should_break_on_failure` is true, the function will break on the first failure.
    ///
    /// Otherwise, it will continue to process all transactions.
    ///
    /// `should_break_on_failure` is used to handle incremented nonce processing
    /// where we want to break on the first failure to maintain nonce continuity.
    ///
    /// Regardless of break condition, all errors are still processed for non-retryable errors, and cleaned up
    async fn clean_prepration_results(
        &self,
        results: Vec<(
            &PendingTransaction,
            Result<BorrowedTransaction, EoaExecutorWorkerError>,
        )>,
        should_break_on_failure: bool,
    ) -> Result<Vec<BorrowedTransaction>, EoaExecutorWorkerError> {
        let mut cleaned_results = Vec::new();
        let mut balance_threshold_update_needed = false;
        let mut failure_occurred = false;
        let mut non_retryable_failures = Vec::new();

        for (pending, result) in results.into_iter() {
            match (failure_occurred, result) {
                (false, Ok(borrowed_data)) => {
                    cleaned_results.push(borrowed_data);
                }
                (_, Err(e)) => {
                    // Track balance threshold issues

                    if should_break_on_failure {
                        failure_occurred = true;
                    }

                    if let EoaExecutorWorkerError::TransactionSimulationFailed {
                        inner_error, ..
                    } = &e
                    {
                        if should_update_balance_threshold(inner_error) {
                            balance_threshold_update_needed = true;
                        }
                    } else if let EoaExecutorWorkerError::RpcError { inner_error, .. } = &e
                        && should_update_balance_threshold(inner_error)
                    {
                        balance_threshold_update_needed = true;
                    }

                    // For deterministic build failures, collect for batch processing
                    if !is_retryable_preparation_error(&e) {
                        tracing::error!(
                            error = ?e,
                            transaction_id = pending.transaction_id,
                            "Transaction permanently failed due to non-retryable preparation error",
                        );
                        non_retryable_failures.push((pending, e.clone()));
                    }
                }
                (true, Ok(_)) => continue,
            }
        }

        // Batch fail all non-retryable failures in a single Redis pipeline
        if !non_retryable_failures.is_empty() {
            if let Err(e) = self
                .store
                .fail_pending_transactions_batch(non_retryable_failures, self.webhook_queue.clone())
                .await
            {
                tracing::error!(
                    error = ?e,
                    "Failed to batch mark transactions as failed - some transactions may be stuck in pending state"
                );
                // Don't propagate the error, continue processing
            }
        }

        if balance_threshold_update_needed && let Err(e) = self.update_balance_threshold().await {
            tracing::error!(error = ?e, "Failed to update balance threshold");
        }

        Ok(cleaned_results)
    }

    /// Process new transactions with fixed iterations and simple sequential nonces
    async fn process_new_transactions(&self, budget: u64) -> Result<u32, EoaExecutorWorkerError> {
        if budget == 0 {
            tracing::warn!("No budget to process new transactions");
            return Ok(0);
        }

        let function_start = current_timestamp_ms();
        let mut total_sent: usize = 0;
        let mut remaining_budget = budget;

        // Fixed number of iterations to avoid infinite loops
        for iteration in 0..10 {
            if remaining_budget == 0 {
                break;
            }

            let iteration_start = current_timestamp_ms();

            // Get pending transactions
            let pending_start = current_timestamp_ms();
            let pending_txs = self
                .store
                .peek_pending_transactions(remaining_budget)
                .await?;

            tracing::info!(
                duration_seconds = calculate_duration_seconds(pending_start, current_timestamp_ms()),
                iteration = iteration,
                pending_count = pending_txs.len(),
                eoa = ?self.eoa,
                chain_id = self.chain_id,
                worker_id = %self.store.worker_id,
                "JOB_LIFECYCLE - process_new_transactions: Got pending transactions"
            );

            if pending_txs.is_empty() {
                break;
            }

            let nonce_start = current_timestamp_ms();
            let optimistic_nonce = self.store.get_optimistic_transaction_count().await?;
            let batch_size = pending_txs.len().min(remaining_budget as usize);

            tracing::info!(
                duration_seconds = calculate_duration_seconds(nonce_start, current_timestamp_ms()),
                iteration = iteration,
                optimistic_nonce = optimistic_nonce,
                batch_size = batch_size,
                eoa = ?self.eoa,
                chain_id = self.chain_id,
                worker_id = %self.store.worker_id,
                "JOB_LIFECYCLE - process_new_transactions: Got optimistic nonce"
            );

            tracing::debug!(
                iteration = iteration,
                batch_size = batch_size,
                starting_nonce = optimistic_nonce,
                remaining_budget = remaining_budget,
                "Processing new transaction batch"
            );

            // Build and sign all transactions in parallel with sequential nonces
            let build_start = current_timestamp_ms();
            let build_tasks: Vec<_> = pending_txs
                .iter()
                .take(batch_size)
                .enumerate()
                .map(|(i, tx)| {
                    let expected_nonce = optimistic_nonce + i as u64;
                    self.build_and_sign_single_transaction_with_retries(tx, expected_nonce)
                })
                .collect();

            let prepared_results = futures::future::join_all(build_tasks).await;

            tracing::info!(
                duration_seconds = calculate_duration_seconds(build_start, current_timestamp_ms()),
                iteration = iteration,
                batch_size = batch_size,
                eoa = ?self.eoa,
                chain_id = self.chain_id,
                worker_id = %self.store.worker_id,
                "JOB_LIFECYCLE - process_new_transactions: Built and signed transactions"
            );

            let prepared_results_with_pending = pending_txs
                .iter()
                .take(batch_size)
                .zip(prepared_results.into_iter())
                .collect::<Vec<_>>();

            // Clean preparation results (handles failures and removes bad transactions)
            let clean_start = current_timestamp_ms();
            let cleaned_results = self
                .clean_prepration_results(prepared_results_with_pending, true)
                .await?;

            tracing::info!(
                duration_seconds = calculate_duration_seconds(clean_start, current_timestamp_ms()),
                iteration = iteration,
                cleaned_count = cleaned_results.len(),
                eoa = ?self.eoa,
                chain_id = self.chain_id,
                worker_id = %self.store.worker_id,
                "JOB_LIFECYCLE - process_new_transactions: Cleaned preparation results"
            );

            if cleaned_results.is_empty() {
                // No successful preparations, reduce budget and continue
                continue;
            }

            // Move prepared transactions to borrowed state with incremented nonces
            let move_start = current_timestamp_ms();
            let moved_count = self
                .store
                .atomic_move_pending_to_borrowed_with_incremented_nonces(
                    &cleaned_results
                        .iter()
                        .map(|borrowed_tx| borrowed_tx.data.clone())
                        .collect::<Vec<_>>(),
                )
                .await?;

            tracing::info!(
                duration_seconds = calculate_duration_seconds(move_start, current_timestamp_ms()),
                iteration = iteration,
                moved_count = moved_count,
                total_prepared = cleaned_results.len(),
                eoa = ?self.eoa,
                chain_id = self.chain_id,
                worker_id = %self.store.worker_id,
                "JOB_LIFECYCLE - process_new_transactions: Moved transactions to borrowed state"
            );

            // Send the transactions to the blockchain
            let send_start = current_timestamp_ms();
            let send_tasks: Vec<_> = cleaned_results
                .iter()
                .map(|borrowed_tx| {
                    let signed_tx = borrowed_tx.signed_transaction.clone();
                    async move {
                        self.chain
                            .provider()
                            .send_tx_envelope(signed_tx.into())
                            .await
                    }
                })
                .collect();

            let send_results = futures::future::join_all(send_tasks).await;

            tracing::info!(
                duration_seconds = calculate_duration_seconds(send_start, current_timestamp_ms()),
                iteration = iteration,
                transactions_sent = cleaned_results.len(),
                eoa = ?self.eoa,
                chain_id = self.chain_id,
                worker_id = %self.store.worker_id,
                "JOB_LIFECYCLE - process_new_transactions: Sent transactions to blockchain"
            );

            // Process send results and update states
            let submission_results = send_results
                .into_iter()
                .zip(cleaned_results.into_iter())
                .map(|(send_result, borrowed_tx)| {
                    let result = SubmissionResult::from_send_result(
                        &borrowed_tx,
                        send_result,
                        SendContext::InitialBroadcast,
                        &self.chain,
                    );

                    match &result.result {
                        SubmissionResultType::Success => result,
                        SubmissionResultType::Nack(e) => {
                            tracing::error!(error = ?e, transaction_id = borrowed_tx.transaction_id, nonce = borrowed_tx.data.signed_transaction.nonce(), "Transaction nack error during send");
                            result
                        }
                        SubmissionResultType::Fail(e) => {
                            tracing::error!(error = ?e, transaction_id = borrowed_tx.transaction_id, nonce = borrowed_tx.data.signed_transaction.nonce(), "Transaction failed during send");
                            result
                        }
                    }
                })
                .collect();

            // Use batch processing to handle all submission results
            let process_start = current_timestamp_ms();
            let processing_report = self
                .store
                .process_borrowed_transactions(submission_results, self.webhook_queue.clone())
                .await?;

            tracing::info!(
                duration_seconds = calculate_duration_seconds(process_start, current_timestamp_ms()),
                iteration = iteration,
                total_processed = processing_report.total_processed,
                moved_to_submitted = processing_report.moved_to_submitted,
                moved_to_pending = processing_report.moved_to_pending,
                failed_transactions = processing_report.failed_transactions,
                eoa = ?self.eoa,
                chain_id = self.chain_id,
                worker_id = %self.store.worker_id,
                "JOB_LIFECYCLE - process_new_transactions: Processed submission results"
            );

            total_sent += processing_report.moved_to_submitted;

            // Update remaining budget by actual nonce consumption
            remaining_budget =
                remaining_budget.saturating_sub(processing_report.moved_to_submitted as u64);

            tracing::info!(
                iteration_duration_seconds = calculate_duration_seconds(iteration_start, current_timestamp_ms()),
                total_duration_seconds = calculate_duration_seconds(function_start, current_timestamp_ms()),
                iteration = iteration,
                eoa = ?self.eoa,
                chain_id = self.chain_id,
                worker_id = %self.store.worker_id,
                "JOB_LIFECYCLE - process_new_transactions: Completed iteration"
            );
        }

        tracing::info!(
            total_sent = total_sent,
            initial_budget = budget,
            remaining_budget = remaining_budget,
            total_duration_seconds = calculate_duration_seconds(function_start, current_timestamp_ms()),
            eoa = ?self.eoa,
            chain_id = self.chain_id,
            worker_id = %self.store.worker_id,
            "JOB_LIFECYCLE - process_new_transactions: Completed processing new transactions"
        );

        Ok(total_sent as u32)
    }
}
