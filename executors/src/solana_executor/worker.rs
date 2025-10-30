use engine_core::{
    credentials::SigningCredential,
    error::{EngineError, SolanaRpcErrorToEngineError},
    execution_options::{solana::{SolanaPriorityFee, SolanaTransactionOptions}, WebhookOptions},
    signer::SolanaSigner,
};
use engine_solana_core::{transaction::{InstructionDataEncoding, SolanaTransaction}, SolanaInstructionData};
use serde::{Deserialize, Serialize};
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcSendTransactionConfig, RpcTransactionConfig},
};
use solana_commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::{
    EncodedTransactionWithStatusMeta, UiTransactionEncoding
};
use spl_memo_interface::instruction::build_memo;
use std::{sync::Arc, time::Duration};
use base64::Engine;
use tracing::{error, info, warn};
use twmq::{
    DurableExecution, FailHookData, NackHookData, Queue, SuccessHookData, UserCancellable,
    error::TwmqError,
    hooks::TransactionContext,
    job::{BorrowedJob, JobError, JobResult, RequeuePosition, ToJobError},
};

use crate::{
    solana_executor::{
        rpc_cache::SolanaRpcCache,
        storage::{LockError, SolanaTransactionAttempt, SolanaTransactionStorage, TransactionLock},
    },
    transaction_registry::TransactionRegistry,
    webhook::{
        WebhookJobHandler,
        envelope::{ExecutorStage, HasTransactionMetadata, HasWebhookOptions, WebhookCapable},
    },
};

const CONFIRMATION_RETRY_DELAY: Duration = Duration::from_millis(200);
const NETWORK_ERROR_RETRY_DELAY: Duration = Duration::from_secs(2);
const MAX_SEND_ATTEMPTS_WITHOUT_TRANSACTION: u32 = 500;

// ========== JOB DATA ==========
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SolanaExecutorJobData {
    pub transaction_id: String,
    pub transaction: SolanaTransactionOptions,
    pub signing_credential: SigningCredential,
    #[serde(default)]
    pub webhook_options: Vec<WebhookOptions>,
}

impl HasWebhookOptions for SolanaExecutorJobData {
    fn webhook_options(&self) -> Vec<WebhookOptions> {
        self.webhook_options.clone()
    }
}

impl HasTransactionMetadata for SolanaExecutorJobData {
    fn transaction_id(&self) -> String {
        self.transaction_id.clone()
    }
}

// ========== SUCCESS RESULT ==========
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SolanaExecutorResult {
    pub transaction_id: String,
    pub signature: String,
    pub signer_address: String,
    pub chain_id: String,
    pub submission_attempt_number: u32,
    pub slot: u64,
    pub block_time: Option<i64>,
    pub transaction: EncodedTransactionWithStatusMeta,
}

// ========== ERROR TYPES ==========
#[derive(Serialize, Deserialize, Debug, Clone, thiserror::Error)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "errorCode")]
pub enum SolanaExecutorError {
    #[error("Failed to build transaction: {inner_error}")]
    #[serde(rename_all = "camelCase")]
    TransactionBuildFailed { inner_error: String },

    #[error("Failed to sign transaction: {inner_error}")]
    #[serde(rename_all = "camelCase")]
    SigningFailed { inner_error: EngineError },

    #[error("Failed to send transaction: {inner_error}")]
    #[serde(rename_all = "camelCase")]
    SendFailed { inner_error: EngineError },

    #[error("Transaction failed on-chain: {reason}")]
    #[serde(rename_all = "camelCase")]
    TransactionFailed { reason: String },

    #[error("RPC error: {inner_error}")]
    #[serde(rename_all = "camelCase")]
    RpcError { inner_error: EngineError },

    #[error("Failed to get priority fees: {inner_error}")]
    #[serde(rename_all = "camelCase")]
    PriorityFeeError { inner_error: EngineError },

    #[error("Blockhash expired, retrying with new blockhash (resubmission {submission_attempt_number})")]
    #[serde(rename_all = "camelCase")]
    BlockhashExpired { submission_attempt_number: u32 },

    #[error("Max retries exceeded: {max_retries}")]
    #[serde(rename_all = "camelCase")]
    MaxRetriesExceeded { max_retries: u32 },

    #[error("Transaction sent successfully")]
    #[serde(rename_all = "camelCase")]
    TransactionSent {
        /// The signature of the sent transaction
        signature: String,
        /// Resubmission attempt number of this transaction
        submission_attempt_number: u32,
    },

    #[error("Transaction not yet confirmed")]
    #[serde(rename_all = "camelCase")]
    NotYetConfirmed {
        /// The signature of the transaction we're waiting for
        signature: String,
    },

    #[error("Failed to acquire lock: another worker is processing")]
    #[serde(rename_all = "camelCase")]
    LockHeldByAnotherWorker,

    #[error("Lost lock during execution")]
    LockLost,

    #[error("Internal error: {message}")]
    InternalError { message: String },

    #[error("Transaction cancelled by user")]
    UserCancelled,
}

impl From<LockError> for SolanaExecutorError {
    fn from(e: LockError) -> Self {
        match e {
            LockError::AlreadyLocked => SolanaExecutorError::LockHeldByAnotherWorker,
            LockError::RedisError(msg) => SolanaExecutorError::InternalError { message: msg },
        }
    }
}

impl From<TwmqError> for SolanaExecutorError {
    fn from(error: TwmqError) -> Self {
        SolanaExecutorError::InternalError {
            message: format!("Queue error: {error}"),
        }
    }
}

impl UserCancellable for SolanaExecutorError {
    fn user_cancelled() -> Self {
        SolanaExecutorError::UserCancelled
    }
}

impl SolanaExecutorError {
    /// Check if this is actually a successful send (not an error)
    pub fn is_send_success(&self) -> bool {
        matches!(self, SolanaExecutorError::TransactionSent { .. })
    }
    
    /// Get the signature if this error contains one
    pub fn signature(&self) -> Option<&str> {
        match self {
            SolanaExecutorError::TransactionSent { signature, .. } => Some(signature.as_str()),
            SolanaExecutorError::NotYetConfirmed { signature } => Some(signature.as_str()),
            _ => None,
        }
    }
}

// ========== HANDLER ==========
pub struct SolanaExecutorJobHandler {
    pub solana_signer: Arc<SolanaSigner>,
    pub rpc_cache: Arc<SolanaRpcCache>,
    pub storage: Arc<SolanaTransactionStorage>,
    pub webhook_queue: Arc<Queue<WebhookJobHandler>>,
    pub transaction_registry: Arc<TransactionRegistry>,
}

impl ExecutorStage for SolanaExecutorJobHandler {
    fn stage_name() -> &'static str {
        "solana_executor"
    }

    fn executor_name() -> &'static str {
        "solana_executor"
    }
}

impl WebhookCapable for SolanaExecutorJobHandler {
    fn webhook_queue(&self) -> &Arc<Queue<WebhookJobHandler>> {
        &self.webhook_queue
    }
}

impl DurableExecution for SolanaExecutorJobHandler {
    type Output = SolanaExecutorResult;
    type ErrorData = SolanaExecutorError;
    type JobData = SolanaExecutorJobData;

    #[tracing::instrument(
        skip(self, job), 
        fields(
            transaction_id = job.job.id,
            stage = Self::stage_name()
        )
    )]
    async fn process(
        &self,
        job: &BorrowedJob<Self::JobData>,
    ) -> JobResult<Self::Output, Self::ErrorData> {
        // let queued_at_ms = job.job.created_at * 1000;
        let data = &job.job.data;
        let transaction_id = &data.transaction_id;

        info!("Starting to process Solana transaction");

        // Try to acquire lock - NACK if another worker has it
        let lock = self
            .storage
            .try_acquire_lock(transaction_id)
            .await
            .map_err(|e| match e {
                LockError::AlreadyLocked => {
                    info!(transaction_id = %transaction_id, "Another worker holds lock, nacking");
                    SolanaExecutorError::LockHeldByAnotherWorker
                        .nack(Some(CONFIRMATION_RETRY_DELAY), RequeuePosition::Last)
                }
                LockError::RedisError(msg) => {
                    SolanaExecutorError::InternalError { message: msg }.fail()
                }
            })?;

        info!(transaction_id = %transaction_id, "Acquired lock");

        let rpc_client = self.rpc_cache.get_or_create(data.transaction.execution_options.chain_id).await;

        let result = self.execute_transaction(&rpc_client, data, &lock, job.job.attempts).await;

        if let Err(e) = lock.release().await {
            warn!(transaction_id = %transaction_id, error = ?e, "Failed to release lock");
        }

        result
    }



    async fn on_success(
        &self,
        job: &BorrowedJob<Self::JobData>,
        success_data: SuccessHookData<'_, Self::Output>,
        tx: &mut TransactionContext<'_>,
    ) {
        let transaction_id = &job.job.data.transaction_id;
        info!(
            transaction_id = %transaction_id,
            signature = %success_data.result.signature,
            slot = success_data.result.slot,
            "Transaction confirmed"
        );

        let _ = self.storage.delete_attempt(transaction_id).await;
        self.transaction_registry
            .add_remove_command(tx.pipeline(), transaction_id);

        if let Err(e) = self.queue_success_webhook(job, success_data, tx) {
            error!(
                transaction_id = %transaction_id,
                error = ?e,
                "Failed to queue success webhook"
            );
        }
    }

    async fn on_fail(
        &self,
        job: &BorrowedJob<Self::JobData>,
        fail_data: FailHookData<'_, Self::ErrorData>,
        tx: &mut TransactionContext<'_>,
    ) {
        let transaction_id = &job.job.data.transaction_id;
        error!(
            transaction_id = %transaction_id,
            error = ?fail_data.error,
            "Transaction permanently failed"
        );

        let _ = self.storage.delete_attempt(transaction_id).await;
        self.transaction_registry
            .add_remove_command(tx.pipeline(), transaction_id);

        if let Err(e) = self.queue_fail_webhook(job, fail_data, tx) {
            error!(
                transaction_id = %transaction_id,
                error = ?e,
                "Failed to queue fail webhook"
            );
        }
    }

    async fn on_nack(
        &self,
        job: &BorrowedJob<Self::JobData>,
        nack_data: NackHookData<'_, Self::ErrorData>,
        tx: &mut TransactionContext<'_>,
    ) {
        let transaction_id = &job.job.data.transaction_id;
        
        match nack_data.error {
            // Special case: TransactionSent is actually a success, send success webhook with stage="send"
            SolanaExecutorError::TransactionSent { signature, submission_attempt_number } => {
                info!(
                    transaction_id = %transaction_id,
                    signature = %signature,
                    submission_attempt_number = submission_attempt_number,
                    "Transaction sent to RPC"
                );

                #[derive(serde::Serialize, Clone)]
                #[serde(rename_all = "camelCase")]
                struct TransactionSentPayload {
                    signature: String,
                    submission_attempt_number: u32,
                }
                
                let payload = TransactionSentPayload {
                    signature: signature.clone(),
                    submission_attempt_number: *submission_attempt_number,
                };
                
                if let Err(e) = self.queue_webhook_with_custom_payload(
                    job,
                    payload,
                    crate::webhook::envelope::StageEvent::Success,
                    "send",
                    tx,
                ) {
                    error!(
                        transaction_id = %transaction_id,
                        error = ?e,
                        "Failed to queue send webhook"
                    );
                }
            }
            
            // Don't send webhook for NotYetConfirmed - silent retry
            SolanaExecutorError::NotYetConfirmed { .. } => {}
            
            // For all other errors (network errors, RPC errors, etc.), send nack webhook
            _ => {
                warn!(
                    transaction_id = %transaction_id,
                    error = ?nack_data.error,
                    "Retrying after error"
                );
                if let Err(e) = self.queue_nack_webhook(job, nack_data, tx) {
                    error!(
                        transaction_id = %transaction_id,
                        error = ?e,
                        "Failed to queue nack webhook"
                    );
                }
            }
        }
    }
}

impl SolanaExecutorJobHandler {
    /// Helper to convert Solana RPC errors with context
    fn to_engine_solana_error(&self, e: &solana_client::client_error::ClientError, chain_id: &str) -> EngineError {
        e.to_engine_solana_error(chain_id)
    }

    /// Get priority fee at a given percentile
    /// ALWAYS NACK on error - this is a network operation that can always be retried
    async fn get_percentile_compute_unit_price(
        &self,
        rpc_client: &RpcClient,
        writable_accounts: &[Pubkey],
        percentile: u8,
        chain_id: &str,
    ) -> JobResult<u64, SolanaExecutorError> {
        let mut fee_history = rpc_client
            .get_recent_prioritization_fees(writable_accounts)
            .await
            .map_err(|e| {
                warn!(
                    chain_id = %chain_id,
                    error = %e,
                    "Failed to get priority fees"
                );
                let engine_error = self.to_engine_solana_error(&e, chain_id,);
                SolanaExecutorError::PriorityFeeError {
                    inner_error: engine_error,
                }
                .nack(Some(NETWORK_ERROR_RETRY_DELAY), RequeuePosition::Last)
            })?;
        
        fee_history.sort_by_key(|a| a.prioritization_fee);
        let percentile_index = ((percentile as f64 / 100.0) * fee_history.len() as f64).round() as usize;
        
        let result = if percentile_index < fee_history.len() {
            fee_history[percentile_index].prioritization_fee
        } else {
            // Fallback to max if index out of bounds
            let fallback = fee_history.last().map(|f| f.prioritization_fee).unwrap_or(0);
            warn!(
                chain_id = %chain_id,
                percentile_index = percentile_index,
                history_len = fee_history.len(),
                "Priority fee percentile out of bounds"
            );
            fallback
        };

        Ok(result)
    }

    fn get_writable_accounts(instructions: &[SolanaInstructionData]) -> Vec<Pubkey> {
        instructions
            .iter()
            .flat_map(|inst| {
                inst.accounts
                    .iter()
                    .filter(|a| a.is_writable)
                    .map(|a| a.pubkey)
            })
            .collect()
    }

    async fn get_compute_unit_price(
        &self,
        priority_fee: &SolanaPriorityFee,
        instructions: &[SolanaInstructionData],
        rpc_client: &RpcClient,
        chain_id: &str,
    ) -> JobResult<u64, SolanaExecutorError> {
        let writable_accounts = Self::get_writable_accounts(instructions);
        
        match priority_fee {
            SolanaPriorityFee::Auto => {
                self.get_percentile_compute_unit_price(
                    rpc_client,
                    &writable_accounts,
                    75,
                    chain_id,
                )
                .await
            }
            SolanaPriorityFee::Manual { micro_lamports_per_unit } => {
                Ok(*micro_lamports_per_unit)
            }
            SolanaPriorityFee::Percentile { percentile } => {
                self.get_percentile_compute_unit_price(
                    rpc_client,
                    &writable_accounts,
                    *percentile,
                    chain_id,
                )
                .await
            }
        }
    }


    /// Main execution flow:
    /// 1. Fetch last attempt from storage
    /// 2. If attempt exists, check if confirmed
    /// 3. If confirmed, return success
    /// 4. If not confirmed, check if blockhash valid
    /// 5. If blockhash invalid or no attempt exists, send new transaction
    /// 6. Retry until limit
    async fn execute_transaction(
        &self,
        rpc_client: &RpcClient,
        job_data: &SolanaExecutorJobData,
        lock: &TransactionLock,
        job_attempt_number: u32,
    ) -> JobResult<SolanaExecutorResult, SolanaExecutorError> {
        let transaction_id = &job_data.transaction_id;
        let chain_id_str = &job_data.transaction.execution_options.chain_id;
        let signer_address = job_data.transaction.execution_options.signer_address;
        let commitment_level = job_data.transaction.execution_options.commitment.to_commitment_level();
        let commitment = CommitmentConfig { commitment: commitment_level };
        let max_retries = job_data.transaction.execution_options.max_blockhash_retries;

        // Verify we still hold the lock
        self.verify_lock(lock, transaction_id).await?;

        // Step 1: Fetch last attempt from storage
        let stored_attempt = self
            .storage
            .get_attempt(transaction_id, lock)
            .await
            .map_err(|e| self.handle_redis_error(e, transaction_id))?;

        if let Some(attempt) = &stored_attempt {
            // Step 2: Attempt exists, check confirmation status
            let signature = attempt.signature;
            let submission_attempt_number = attempt.submission_attempt_number;

            // Step 3: Check if transaction is confirmed
            match rpc_client.get_signature_status_with_commitment(&signature, commitment).await {
                Ok(Some(Ok(()))) => {
                    // Transaction confirmed! Fetch full transaction details
                    info!(
                        transaction_id = %transaction_id,
                        signature = %signature,
                        submission_attempt_number = submission_attempt_number,
                        "Transaction confirmed, fetching details"
                    );
                    
                    let transaction_details = rpc_client
                        .get_transaction_with_config(&signature, RpcTransactionConfig {
                            encoding: Some(UiTransactionEncoding::Json),
                            commitment: Some(commitment),
                            max_supported_transaction_version: Some(0),
                        })
                        .await
                        .map_err(|e| {
                            error!(
                                transaction_id = %transaction_id,
                                signature = %signature,
                                error = %e,
                                "Failed to fetch transaction details"
                            );
                            let engine_error = self.to_engine_solana_error(&e, chain_id_str.as_str());
                            SolanaExecutorError::RpcError {
                                inner_error: engine_error,
                            }
                            .nack(Some(NETWORK_ERROR_RETRY_DELAY), RequeuePosition::Last)
                        })?;

                    return Ok(SolanaExecutorResult {
                        transaction_id: transaction_id.clone(),
                        signature: signature.to_string(),
                        signer_address: signer_address.to_string(),
                        chain_id: chain_id_str.as_str().to_string(),
                        submission_attempt_number,
                        slot: transaction_details.slot,
                        block_time: transaction_details.block_time,
                        transaction: transaction_details.transaction,
                    });
                }
                Ok(Some(Err(tx_error))) => {
                    // Transaction failed on-chain - permanent failure
                    error!(
                        transaction_id = %transaction_id,
                        signature = %signature,
                        error = ?tx_error,
                        submission_attempt_number = submission_attempt_number,
                        "Transaction failed on-chain (permanent failure)"
                    );
                    let _ = self.storage.delete_attempt(transaction_id).await;
                    return Err(SolanaExecutorError::TransactionFailed {
                        reason: format!("{:?}", tx_error),
                    }
                    .fail());
                }
                Ok(None) => {
                    // Step 4: Not confirmed yet, check if blockhash is still valid
                    let time_since_sent = crate::metrics::current_timestamp_ms().saturating_sub(attempt.sent_at);
                    let min_wait_before_resubmit_ms = 30_000; // Wait at least 30 seconds before resubmitting

                    if time_since_sent < min_wait_before_resubmit_ms {
                        // Haven't waited long enough - keep checking
                        return Err(SolanaExecutorError::NotYetConfirmed {
                            signature: signature.to_string(),
                        }
                        .nack(Some(CONFIRMATION_RETRY_DELAY), RequeuePosition::Last));
                    }
                    
                    // Check blockhash validity
                    match rpc_client.is_blockhash_valid(&attempt.blockhash, CommitmentConfig { commitment: CommitmentLevel::Finalized }).await {
                        Ok(true) => {
                            // Blockhash still valid, not confirmed yet - retry
                            return Err(SolanaExecutorError::NotYetConfirmed {
                                signature: signature.to_string(),
                            }
                            .nack(Some(CONFIRMATION_RETRY_DELAY), RequeuePosition::Last));
                        }
                        Ok(false) => {
                            // Blockhash expired, need to resubmit
                            warn!(
                                transaction_id = %transaction_id,
                                signature = %signature,
                                submission_attempt_number = submission_attempt_number,
                                "Blockhash expired, resubmitting"
                            );

                            // Check if we've exceeded max retries
                            if submission_attempt_number > max_retries {
                                error!(
                                    transaction_id = %transaction_id,
                                    submission_attempt_number = submission_attempt_number,
                                    max_retries = max_retries,
                                    "Max retries exceeded"
                                );
                                let _ = self.storage.delete_attempt(transaction_id).await;
                                return Err(SolanaExecutorError::MaxRetriesExceeded {
                                    max_retries,
                                }
                                .fail());
                            }

                            // Delete old attempt and resubmit
                            self.storage
                                .delete_attempt(transaction_id)
                                .await
                                .map_err(|e| {
                                    error!(
                                        transaction_id = %transaction_id,
                                        error = %e,
                                        "Failed to delete old attempt"
                                    );
                                    SolanaExecutorError::InternalError {
                                        message: format!("Failed to delete old attempt: {e}"),
                                    }
                                    .fail()
                                })?;
                        }
                        Err(e) => {
                            // RPC error checking blockhash
                            warn!(
                                transaction_id = %transaction_id,
                                error = %e,
                                "RPC error checking blockhash"
                            );
                            let engine_error = self.to_engine_solana_error(&e, chain_id_str.as_str());
                            return Err(SolanaExecutorError::RpcError {
                                inner_error: engine_error,
                            }
                            .nack(Some(NETWORK_ERROR_RETRY_DELAY), RequeuePosition::Last));
                        }
                    }
                }
                Err(e) => {
                    // RPC error getting signature status
                    warn!(
                        transaction_id = %transaction_id,
                        error = %e,
                        "RPC error getting signature status"
                    );
                    let engine_error = self.to_engine_solana_error(&e, chain_id_str.as_str());
                    return Err(SolanaExecutorError::RpcError {
                        inner_error: engine_error,
                    }
                    .nack(Some(NETWORK_ERROR_RETRY_DELAY), RequeuePosition::Last));
                }
            }
        }

        // Step 5: No attempt exists or blockhash expired - send new transaction
        let submission_attempt_number = stored_attempt.as_ref().map(|a| a.submission_attempt_number + 1).unwrap_or(1);

        // Check job attempt limits
        if stored_attempt.is_none() && job_attempt_number > MAX_SEND_ATTEMPTS_WITHOUT_TRANSACTION {
            error!(
                transaction_id = %transaction_id,
                job_attempt_number = job_attempt_number,
                "Max send attempts exceeded"
            );
            return Err(SolanaExecutorError::MaxRetriesExceeded { 
                max_retries: MAX_SEND_ATTEMPTS_WITHOUT_TRANSACTION 
            }.fail());
        }
        
        if submission_attempt_number > max_retries + 1 {
            error!(
                transaction_id = %transaction_id,
                submission_attempt_number = submission_attempt_number,
                max_retries = max_retries,
                "Max resubmission retries exceeded"
            );
            return Err(SolanaExecutorError::MaxRetriesExceeded { max_retries }.fail());
        }

        info!(
            transaction_id = %transaction_id,
            submission_attempt_number = submission_attempt_number,
            "Sending transaction"
        );

        let (recent_blockhash, last_valid_height) = rpc_client
            .get_latest_blockhash_with_commitment(commitment)
            .await
            .map_err(|e| {
                error!(
                    transaction_id = %transaction_id,
                    error = %e,
                    "Failed to get blockhash"
                );
                let engine_error = self.to_engine_solana_error(&e, chain_id_str.as_str());
                SolanaExecutorError::RpcError {
                    inner_error: engine_error,
                }
                .nack(Some(NETWORK_ERROR_RETRY_DELAY), RequeuePosition::Last)
            })?;

        // Build transaction - handle execution options differently for instructions vs serialized
        let versioned_tx = match &job_data.transaction.input {
            engine_solana_core::transaction::SolanaTransactionInput::Instructions { instructions } => {
                // For instruction-based transactions: calculate priority fees and apply execution options
                let compute_unit_price = if let Some(priority_fee) = &job_data.transaction.execution_options.priority_fee {
                    Some(self.get_compute_unit_price(priority_fee, instructions, rpc_client, chain_id_str.as_str()).await?)
                } else {
                    None
                };
                
                // Add memo instruction with transaction_id for unique signatures
                // This ensures that even with the same blockhash, each resubmission has a unique signature
                let memo_data = format!("thirdweb-engine:{}", transaction_id);
                let memo_ix = build_memo(&spl_memo_interface::v3::id(), memo_data.as_bytes(), &[]);
                
                let mut instructions_with_memo = instructions.clone();
                let memo_data_base64 = base64::engine::general_purpose::STANDARD.encode(memo_data.as_bytes());
                instructions_with_memo.push(SolanaInstructionData {
                    program_id: memo_ix.program_id,
                    accounts: vec![],
                    data: memo_data_base64,
                    encoding: InstructionDataEncoding::Base64,
                });
                
                let solana_tx = SolanaTransaction {
                    input: engine_solana_core::transaction::SolanaTransactionInput::Instructions {
                        instructions: instructions_with_memo,
                    },
                    compute_unit_limit: job_data.transaction.execution_options.compute_unit_limit,
                    compute_unit_price,
                };

                solana_tx
                    .to_versioned_transaction(signer_address, recent_blockhash)
                    .map_err(|e| {
                        error!(
                            transaction_id = %transaction_id,
                            error = %e,
                            "Failed to build transaction from instructions"
                        );
                        SolanaExecutorError::TransactionBuildFailed {
                            inner_error: e.to_string(),
                        }
                        .fail()
                    })?
            }
            engine_solana_core::transaction::SolanaTransactionInput::Serialized { .. } => {
                // For serialized transactions: ignore execution options to avoid invalidating signatures
                let solana_tx = SolanaTransaction {
                    input: job_data.transaction.input.clone(),
                    compute_unit_limit: None,
                    compute_unit_price: None,
                };

                solana_tx
                    .to_versioned_transaction(signer_address, recent_blockhash)
                    .map_err(|e| {
                        error!(
                            transaction_id = %transaction_id,
                            error = %e,
                            "Failed to deserialize compiled transaction"
                        );
                        SolanaExecutorError::TransactionBuildFailed {
                            inner_error: e.to_string(),
                        }
                        .fail()
                    })?
            }
        };

        let signed_tx = self
            .solana_signer
            .sign_transaction(versioned_tx, signer_address, &job_data.signing_credential)
            .await
            .map_err(|e| {
                error!(
                    transaction_id = %transaction_id,
                    error = ?e,
                    "Failed to sign transaction"
                );
                SolanaExecutorError::SigningFailed { inner_error: e }.fail()
            })?;

        let signature = signed_tx.signatures[0];

        // Store attempt BEFORE sending to prevent duplicates
        let attempt = SolanaTransactionAttempt::new(
            signature,
            recent_blockhash,
            last_valid_height,
            submission_attempt_number,
        );

        let stored = self
            .storage
            .store_attempt_if_not_exists(transaction_id, &attempt, lock)
            .await
            .map_err(|e| self.handle_redis_error(e, transaction_id))?;

        if !stored {
            warn!(
                transaction_id = %transaction_id,
                "Attempt already stored (race condition)"
            );
            return Err(SolanaExecutorError::InternalError {
                message: "Attempt already stored".to_string(),
            }
            .nack(Some(CONFIRMATION_RETRY_DELAY), RequeuePosition::Last));
        }

        // Send transaction to RPC
        let config = RpcSendTransactionConfig {
            skip_preflight: false,
            preflight_commitment: Some(commitment_level),
            ..Default::default()
        };

        match rpc_client.send_transaction_with_config(&signed_tx, config).await {
            Ok(_) => {
                info!(
                    transaction_id = %transaction_id,
                    signature = %signature,
                    submission_attempt_number = submission_attempt_number,
                    "Transaction sent to RPC"
                );
                // Return TransactionSent "error" which will trigger a send success webhook
                Err(SolanaExecutorError::TransactionSent {
                    signature: signature.to_string(),
                    submission_attempt_number,
                }
                .nack(Some(CONFIRMATION_RETRY_DELAY), RequeuePosition::Last))
            }
            Err(e) => {
                error!(
                    transaction_id = %transaction_id,
                    signature = %signature,
                    error = %e,
                    "Failed to send transaction"
                );
                let engine_error = self.to_engine_solana_error(&e, chain_id_str.as_str());

                let is_retryable = is_send_error_retryable(&engine_error);
                if is_retryable {
                    // Network error or temporary issue - retry
                    Err(SolanaExecutorError::SendFailed {
                        inner_error: engine_error,
                    }
                    .nack(Some(NETWORK_ERROR_RETRY_DELAY), RequeuePosition::Last))
                } else {
                    // Permanent error - fail
                    let _ = self.storage.delete_attempt(transaction_id).await;
                    Err(SolanaExecutorError::SendFailed {
                        inner_error: engine_error,
                    }
                    .fail())
                }
            }
        }
    }

    async fn verify_lock(
        &self,
        lock: &TransactionLock,
        transaction_id: &str,
    ) -> JobResult<(), SolanaExecutorError> {
        match lock.still_held().await {
            Ok(true) => Ok(()),
            Ok(false) => {
                warn!(transaction_id = %transaction_id, "Lost lock");
                Err(SolanaExecutorError::LockLost
                    .nack(Some(CONFIRMATION_RETRY_DELAY), RequeuePosition::Last))
            }
            Err(e) => Err(SolanaExecutorError::InternalError {
                message: format!("Failed to check lock: {e}"),
            }
            .fail()),
        }
    }

    fn handle_redis_error(
        &self,
        error: twmq::redis::RedisError,
        transaction_id: &str,
    ) -> JobError<SolanaExecutorError> {
        if error.to_string().contains("lock lost") {
            warn!(transaction_id = %transaction_id, "Lock lost during Redis operation");
            SolanaExecutorError::LockLost.nack(Some(CONFIRMATION_RETRY_DELAY), RequeuePosition::Last)
        } else {
            SolanaExecutorError::InternalError {
                message: format!("Redis error: {error}"),
            }
            .fail()
        }
    }
}

/// Determines if a Solana send error should be retried
/// Network errors and temporary issues should be retried
/// Bad transactions (invalid signature, insufficient funds, etc.) should not be retried
fn is_send_error_retryable(error: &EngineError) -> bool {
    use engine_core::error::{SolanaRpcErrorKind, SolanaRpcResponseErrorData};
    
    match error {
        EngineError::SolanaRpcError { kind,  .. } => match kind {
            // Network/IO errors are always retryable
            SolanaRpcErrorKind::Io { .. } => true,
            SolanaRpcErrorKind::Reqwest { .. } => true,
            
            // RPC errors need more inspection
            SolanaRpcErrorKind::RpcError { data, message, .. } => {
                // Check if it's a preflight failure
                if let SolanaRpcResponseErrorData::SendTransactionPreflightFailure { .. } = data {
                    return false;
                }
                
                // Check message for permanent errors
                let msg_lower = message.to_lowercase();
                if msg_lower.contains("invalid signature")
                    || msg_lower.contains("signature verification failed")
                    || msg_lower.contains("insufficient funds")
                    || msg_lower.contains("account not found")
                    || msg_lower.contains("transaction already processed")
                    || msg_lower.contains("invalid account")
                {
                    return false;
                }
                
                true
            }
            
            // Transaction errors are permanent
            SolanaRpcErrorKind::TransactionError { .. } => false,
            
            // Signing errors are permanent
            SolanaRpcErrorKind::SigningError { .. } => false,
            
            // JSON/parse errors might be temporary
            SolanaRpcErrorKind::SerdeJson { .. } => true,
            
            // Custom errors - check message
            SolanaRpcErrorKind::Custom { message } => {
                !message.to_lowercase().contains("invalid")
            }
            
            // Unknown errors - be conservative and retry
            SolanaRpcErrorKind::Unknown { .. } => true,
        }
        // Other engine errors - be conservative and retry
        _ => true,
    }
}
