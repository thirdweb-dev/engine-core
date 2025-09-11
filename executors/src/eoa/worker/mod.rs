use alloy::consensus::Transaction;
use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use engine_core::{
    chain::{Chain, ChainService},
    credentials::SigningCredential,
    error::AlloyRpcErrorToEngineError,
    signer::EoaSigner,
};
use engine_eip7702_core::delegated_account::DelegatedAccount;
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use twmq::Queue;
use twmq::redis::AsyncCommands;
use twmq::redis::aio::ConnectionManager;
use twmq::{
    DurableExecution, FailHookData, NackHookData, SuccessHookData,
    hooks::TransactionContext,
    job::{BorrowedJob, JobResult, RequeuePosition, ToJobResult},
};

use crate::eoa::authorization_cache::EoaAuthorizationCache;
use crate::eoa::store::{
    AtomicEoaExecutorStore, EoaExecutorStore, EoaExecutorStoreKeys, EoaHealth, SubmissionResult,
    TransactionStoreError,
};
use crate::metrics::{
    EoaMetrics, calculate_duration_seconds, current_timestamp_ms, record_eoa_job_processing_time,
};
use crate::webhook::WebhookJobHandler;

pub mod confirm;
pub mod error;
mod send;
mod transaction;

use error::{EoaExecutorWorkerError, SendContext};

// ========== SPEC-COMPLIANT CONSTANTS ==========
const MAX_INFLIGHT_PER_EOA: u64 = 100; // Default from spec
const MAX_RECYCLED_THRESHOLD: u64 = 50; // Circuit breaker from spec
const TARGET_TRANSACTIONS_PER_EOA: u64 = 10; // Fleet management from spec
const MIN_TRANSACTIONS_PER_EOA: u64 = 1; // Fleet management from spec

// ========== JOB DATA ==========
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EoaExecutorWorkerJobData {
    pub eoa_address: Address,
    pub chain_id: u64,
    pub noop_signing_credential: SigningCredential,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EoaExecutorWorkerResult {
    // what we did
    /// Number of transactions we recovered from borrowed state
    pub recovered_transactions: u32,

    /// Number of transactions we confirmed
    pub confirmed_transactions: u32,

    /// Number of transactions we failed due to deterministic errors
    pub failed_transactions: u32,

    /// Number of transactions we sent
    pub sent_transactions: u32,

    /// Number of transactions that got replaced in the mempool and are now pending
    pub replaced_transactions: u32,

    // what we have left
    /// Number of transactions currently in the submitted state
    pub submitted_transactions: u32,

    /// Number of transactions currently in the pending state
    pub pending_transactions: u32,

    /// Number of transactions currently in the borrowed state
    pub borrowed_transactions: u32,

    /// Number of recycled nonces
    pub recycled_nonces: u32,
}

impl EoaExecutorWorkerResult {
    pub fn is_work_remaining(&self) -> bool {
        self.pending_transactions > 0
            || self.borrowed_transactions > 0
            || self.recycled_nonces > 0
            || self.submitted_transactions > 0
    }
}

// ========== MAIN WORKER ==========
/// EOA Executor Worker
///
/// ## Core Workflow:
/// 1. **Acquire Lock Aggressively** - Takes over stalled workers using force acquisition. This is a lock over EOA:CHAIN
/// 2. **Crash Recovery** - Rebroadcasts borrowed transactions, handles deterministic failures
/// 3. **Confirmation Flow** - Fetches receipts, confirms transactions, handles nonce sync, requeues replaced transactions
/// 4. **Send Flow** - Processes recycled nonces first, then new transactions with in-flight budget control
/// 5. **Lock Release** - Explicit release in finally pattern as per spec
///
/// ## Key Features:
/// - **Atomic Operations**: All state transitions use Redis WATCH/MULTI/EXEC for durability
/// - **Borrowed State**: Mid-send crash recovery with atomic pending->borrowed->submitted transitions
/// - **Nonce Management**: Optimistic nonce tracking with recycled nonce priority
/// - **Error Classification**: Spec-compliant deterministic vs. possibly-sent error handling
/// - **Circuit Breakers**: Automatic recycled nonce nuking when threshold exceeded
/// - **Health Monitoring**: Balance checking with configurable thresholds
pub struct EoaExecutorJobHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    pub chain_service: Arc<CS>,
    pub webhook_queue: Arc<Queue<WebhookJobHandler>>,
    pub authorization_cache: EoaAuthorizationCache,

    pub redis: ConnectionManager,
    pub namespace: Option<String>,

    pub eoa_signer: Arc<EoaSigner>,
    pub max_inflight: u64, // Note: Spec uses MAX_INFLIGHT_PER_EOA constant
    pub max_recycled_nonces: u64, // Note: Spec uses MAX_RECYCLED_THRESHOLD constant

    // EOA metrics abstraction with encapsulated configuration
    pub eoa_metrics: EoaMetrics,
}

impl<CS> DurableExecution for EoaExecutorJobHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    type Output = EoaExecutorWorkerResult;
    type ErrorData = EoaExecutorWorkerError;
    type JobData = EoaExecutorWorkerJobData;

    #[tracing::instrument(name = "eoa_executor_worker", skip_all, fields(eoa = ?job.job.data.eoa_address, chain_id = job.job.data.chain_id))]
    async fn process(
        &self,
        job: &BorrowedJob<Self::JobData>,
    ) -> JobResult<Self::Output, Self::ErrorData> {
        let data = &job.job.data;

        // 1. GET CHAIN
        let chain = self
            .chain_service
            .get_chain(data.chain_id)
            .map_err(|e| EoaExecutorWorkerError::ChainServiceError {
                chain_id: data.chain_id,
                message: format!("Failed to get chain: {e}"),
            })
            .map_err_nack(Some(Duration::from_secs(10)), RequeuePosition::Last)?;

        let worker_id = format!("{}:{}", uuid::Uuid::new_v4(), job.lease_token);

        // 2. CREATE SCOPED STORE (acquires lock)
        let scoped = EoaExecutorStore::new(
            self.redis.clone(),
            self.namespace.clone(),
            data.eoa_address,
            data.chain_id,
        )
        .acquire_eoa_lock_aggressively(&worker_id, self.eoa_metrics.clone())
        .await
        .map_err(|e| Into::<EoaExecutorWorkerError>::into(e).handle())?;

        let delegated_account = DelegatedAccount::new(data.eoa_address, chain.clone());

        // if there's an error checking 7702 delegation here, we'll just assume it's not a minimal account for the purposes of max in flight
        let is_minimal_account = self
            .authorization_cache
            .is_minimal_account(&delegated_account)
            .await
            .inspect_err(|e| {
                tracing::error!(error = ?e, "Error checking 7702 delegation");
            })
            .ok()
            .unwrap_or(false);

        let worker = EoaExecutorWorker {
            store: scoped,
            chain,
            eoa: data.eoa_address,
            chain_id: data.chain_id,
            noop_signing_credential: data.noop_signing_credential.clone(),

            max_inflight: if is_minimal_account {
                1
            } else {
                self.max_inflight
            },
            max_recycled_nonces: self.max_recycled_nonces,
            webhook_queue: self.webhook_queue.clone(),
            signer: self.eoa_signer.clone(),
        };

        let job_start_time = current_timestamp_ms();
        let result = worker.execute_main_workflow().await?;
        if let Err(e) = worker.release_eoa_lock().await {
            tracing::error!(error = ?e, worker_id = worker_id, "Error releasing EOA lock");
        }

        // Record EOA job processing metrics
        let job_end_time = current_timestamp_ms();
        let job_duration = calculate_duration_seconds(job_start_time, job_end_time);
        record_eoa_job_processing_time(data.chain_id, job_duration);

        if result.is_work_remaining() {
            Err(EoaExecutorWorkerError::WorkRemaining { result })
                .map_err_nack(Some(Duration::from_millis(200)), RequeuePosition::Last)
        } else {
            Ok(result)
        }

        // // initiate health data if doesn't exist
        // self.get_eoa_health(&scoped, &chain)
        //     .await
        //     .map_err_nack(Some(Duration::from_secs(10)), RequeuePosition::Last)?;

        // // Execute main workflow with proper error handling
        // self.execute_main_workflow(&scoped, &chain).await
    }

    async fn on_success(
        &self,
        job: &BorrowedJob<Self::JobData>,
        _success_data: SuccessHookData<'_, Self::Output>,
        _tx: &mut TransactionContext<'_>,
    ) {
        self.soft_release_eoa_lock(&job.job.data).await;
    }

    async fn on_nack(
        &self,
        job: &BorrowedJob<Self::JobData>,
        _nack_data: NackHookData<'_, Self::ErrorData>,
        _tx: &mut TransactionContext<'_>,
    ) {
        self.soft_release_eoa_lock(&job.job.data).await;
    }

    #[tracing::instrument(name = "eoa_executor_worker_on_fail", skip_all, fields(eoa = ?job.job.data.eoa_address, chain_id = job.job.data.chain_id, job_id = ?job.job.id))]
    async fn on_fail(
        &self,
        job: &BorrowedJob<Self::JobData>,
        fail_data: FailHookData<'_, Self::ErrorData>,
        _tx: &mut TransactionContext<'_>,
    ) {
        if let EoaExecutorWorkerError::StoreError { inner_error, .. } = &fail_data.error {
            if let TransactionStoreError::LockLost { .. } = &inner_error {
                tracing::error!(
                    eoa = ?job.job.data.eoa_address,
                    chain_id = job.job.data.chain_id,
                    "Encountered lock lost store error, skipping soft release of EOA lock"
                );
                return;
            }
        } else {
            self.soft_release_eoa_lock(&job.job.data).await;
        }
    }
}

impl<CS> EoaExecutorJobHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    async fn soft_release_eoa_lock(&self, job_data: &EoaExecutorWorkerJobData) {
        let keys = EoaExecutorStoreKeys::new(
            job_data.eoa_address,
            job_data.chain_id,
            self.namespace.clone(),
        );

        let lock_key = keys.eoa_lock_key_name();
        let mut conn = self.redis.clone();
        if let Err(e) = conn.del::<&str, ()>(&lock_key).await {
            tracing::error!(
                eoa = ?job_data.eoa_address,
                chain_id = job_data.chain_id,
                error = ?e,
                "Failed to release EOA lock"
            );
        }
    }
}

pub struct EoaExecutorWorker<C: Chain> {
    pub store: AtomicEoaExecutorStore,
    pub chain: C,

    pub eoa: Address,
    pub chain_id: u64,
    pub noop_signing_credential: SigningCredential,

    pub max_inflight: u64,
    pub max_recycled_nonces: u64,

    pub webhook_queue: Arc<Queue<WebhookJobHandler>>,
    pub signer: Arc<EoaSigner>,
}

impl<C: Chain> EoaExecutorWorker<C> {
    /// Execute the main EOA worker workflow
    async fn execute_main_workflow(
        &self,
    ) -> JobResult<EoaExecutorWorkerResult, EoaExecutorWorkerError> {
        // 1. CRASH RECOVERY
        let recovered = self
            .recover_borrowed_state()
            .await
            .map_err(|e| {
                tracing::error!("Error in recover_borrowed_state: {}", e);
                e
            })
            .map_err(|e| e.handle())?;

        // 2. CONFIRM FLOW
        let confirmations_report = self
            .confirm_flow()
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, "Error in confirm flow");
                e
            })
            .map_err(|e| e.handle())?;

        // 3. SEND FLOW
        let sent = self
            .send_flow()
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, "Error in send_flow");
                e
            })
            .map_err(|e| e.handle())?;

        // 4. CHECK FOR REMAINING WORK
        let pending_count = self
            .store
            .get_pending_transactions_count()
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, "Error in peek_pending_transactions");
                e
            })
            .map_err(|e| Into::<EoaExecutorWorkerError>::into(e).handle())?;

        let borrowed_count = self
            .store
            .get_borrowed_transactions_count()
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, "Error in peek_borrowed_transactions");
                e
            })
            .map_err(|e| Into::<EoaExecutorWorkerError>::into(e).handle())?;

        let recycled_nonces_count = self
            .store
            .get_recycled_nonces_count()
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, "Error in peek_recycled_nonces");
                e
            })
            .map_err(|e| Into::<EoaExecutorWorkerError>::into(e).handle())?;

        let submitted_count = self
            .store
            .get_submitted_transactions_count()
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, "Error in get_submitted_transactions_count");
                e
            })
            .map_err(|e| Into::<EoaExecutorWorkerError>::into(e).handle())?;

        tracing::info!(
            recovered = recovered,
            confirmed = confirmations_report.moved_to_success,
            temp_failed = confirmations_report.moved_to_pending,
            replacements = confirmations_report.moved_to_pending,
            currently_submitted = submitted_count,
            currently_pending = pending_count,
            currently_borrowed = borrowed_count,
            currently_recycled = recycled_nonces_count,
        );

        Ok(EoaExecutorWorkerResult {
            recovered_transactions: recovered,
            confirmed_transactions: confirmations_report.moved_to_success as u32,
            failed_transactions: confirmations_report.moved_to_pending as u32,
            sent_transactions: sent,

            replaced_transactions: confirmations_report.moved_to_pending as u32,
            submitted_transactions: submitted_count as u32,
            pending_transactions: pending_count as u32,
            borrowed_transactions: borrowed_count as u32,
            recycled_nonces: recycled_nonces_count as u32,
        })
    }

    // ========== CRASH RECOVERY ==========
    #[tracing::instrument(skip_all)]
    async fn recover_borrowed_state(&self) -> Result<u32, EoaExecutorWorkerError> {
        let borrowed_transactions = self.store.peek_borrowed_transactions().await?;
        let mut borrowed_transactions = self.store.hydrate_all(borrowed_transactions).await?;

        if borrowed_transactions.is_empty() {
            return Ok(0);
        }

        tracing::warn!(
            "Recovering {} borrowed transactions. This indicates a worker crash or system issue",
            borrowed_transactions.len()
        );

        // Sort borrowed transactions by nonce to ensure proper ordering
        borrowed_transactions.sort_by_key(|tx| tx.signed_transaction.nonce());

        // Rebroadcast all transactions in parallel
        let rebroadcast_futures: Vec<_> = borrowed_transactions
            .iter()
            .map(|borrowed| {
                let tx_envelope = borrowed.signed_transaction.clone().into();
                let nonce = borrowed.signed_transaction.nonce();
                let transaction_id = borrowed.transaction_id.clone();

                tracing::info!(
                    transaction_id = ?transaction_id,
                    nonce = nonce,
                    "Recovering borrowed transaction"
                );

                async move {
                    let send_result = self.chain.provider().send_tx_envelope(tx_envelope).await;
                    (borrowed, send_result)
                }
            })
            .collect();

        let rebroadcast_results = futures::future::join_all(rebroadcast_futures).await;

        // Convert results to SubmissionResult for batch processing
        let submission_results: Vec<SubmissionResult> = rebroadcast_results
            .into_iter()
            .map(|(borrowed, send_result)| {
                SubmissionResult::from_send_result(
                    borrowed,
                    send_result,
                    SendContext::Rebroadcast,
                    &self.chain,
                )
            })
            .collect();

        // TODO: Implement post-processing analysis for balance threshold updates and nonce resets
        // Currently we lose the granular error handling that was in the individual atomic operations.
        // Consider:
        // 1. Analyzing submission_results for specific error patterns
        // 2. Calling update_balance_threshold if needed
        // 3. Detecting nonce reset conditions
        // 4. Or move this logic into the batch processor itself

        // Process all results in one batch operation
        let report = self
            .store
            .process_borrowed_transactions(submission_results, self.webhook_queue.clone())
            .await?;

        // TODO: Handle post-processing updates here if needed
        // For now, we skip the individual error analysis that was done in the old atomic approach

        tracing::info!(
            "Recovered {} transactions: {} submitted, {} recycled, {} failed",
            report.total_processed,
            report.moved_to_submitted,
            report.moved_to_pending,
            report.failed_transactions
        );

        Ok(report.total_processed as u32)
    }

    // ========== HEALTH ACCESSOR ==========

    /// Get EOA health, initializing it if it doesn't exist
    /// This method ensures the health data is always available for the worker
    async fn get_eoa_health(&self) -> Result<EoaHealth, EoaExecutorWorkerError> {
        let store_health = self.store.get_eoa_health().await?;

        match store_health {
            Some(health) => Ok(health),
            None => {
                // Initialize with fresh data from chain
                let balance = self
                    .chain
                    .provider()
                    .get_balance(self.eoa)
                    .await
                    .map_err(|e| {
                        let engine_error = e.to_engine_error(&self.chain);
                        EoaExecutorWorkerError::RpcError {
                            message: format!(
                                "Failed to get balance during initialization: {engine_error}"
                            ),
                            inner_error: engine_error,
                        }
                    })?;

                let now = current_timestamp_ms();

                let health = EoaHealth {
                    balance,
                    balance_threshold: U256::ZERO,
                    balance_fetched_at: now,
                    last_confirmation_at: now,
                    last_nonce_movement_at: now,
                    nonce_resets: Vec::new(),
                };

                // Save to store
                self.store.update_health_data(&health).await?;
                Ok(health)
            }
        }
    }

    #[tracing::instrument(skip_all, fields(eoa = ?self.eoa, chain_id = self.chain.chain_id()))]
    async fn update_balance_threshold(&self) -> Result<(), EoaExecutorWorkerError> {
        let mut health = self.get_eoa_health().await?;

        tracing::info!("Updating balance threshold");
        let balance_threshold = self
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

        health.balance_threshold = balance_threshold;
        self.store.update_health_data(&health).await?;
        Ok(())
    }

    async fn release_eoa_lock(self) -> Result<(), EoaExecutorWorkerError> {
        self.store.release_eoa_lock().await?;
        Ok(())
    }
}
