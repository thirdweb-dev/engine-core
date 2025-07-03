use alloy::consensus::{SignableTransaction, Transaction};
use alloy::network::TransactionBuilder;
use alloy::primitives::{Address, B256, Bytes, U256};
use alloy::providers::Provider;
use alloy::rpc::types::TransactionRequest as AlloyTransactionRequest;
use alloy::transports::{RpcError, TransportErrorKind};
use engine_core::credentials::SigningCredential;
use engine_core::{
    chain::{Chain, ChainService, RpcCredentials},
    error::{AlloyRpcErrorToEngineError, EngineError},
    execution_options::WebhookOptions,
    signer::{AccountSigner, EoaSigner, EoaSigningOptions},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use twmq::{
    FailHookData, NackHookData, Queue, SuccessHookData, UserCancellable,
    error::TwmqError,
    hooks::TransactionContext,
    job::{BorrowedJob, DelayOptions, JobResult, RequeuePosition, ToJobError, ToJobResult},
};

use crate::{
    transaction_registry::TransactionRegistry,
    webhook::{
        WebhookJobHandler,
        envelope::{ExecutorStage, HasTransactionMetadata, HasWebhookOptions, WebhookCapable},
    },
};

use super::{
    error_classifier::{EoaErrorMapper, EoaExecutionError, RecoveryStrategy},
    nonce_manager::NonceManager,
};

// --- Job Payload ---
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EoaSendJobData {
    pub transaction_id: String,
    pub chain_id: u64,
    pub from: Address,
    pub to: Option<Address>,
    pub value: U256,
    pub data: Bytes,
    pub webhook_options: Option<Vec<WebhookOptions>>,

    pub assigned_nonce: Option<u64>,

    pub gas_limit: Option<u64>,

    pub signing_credential: SigningCredential,
    pub rpc_credentials: RpcCredentials,
}

impl HasWebhookOptions for EoaSendJobData {
    fn webhook_options(&self) -> Option<Vec<WebhookOptions>> {
        self.webhook_options.clone()
    }
}

impl HasTransactionMetadata for EoaSendJobData {
    fn transaction_id(&self) -> String {
        self.transaction_id.clone()
    }
}

// --- Success Result ---
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EoaSendResult {
    pub transaction_hash: B256,
    pub nonce_used: u64,
    pub possibly_duplicate: Option<bool>,
}

// --- Error Types ---
#[derive(Serialize, Deserialize, Debug, Clone, thiserror::Error)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "errorCode")]
pub enum EoaSendError {
    #[error("Chain service error for chainId {chain_id}: {message}")]
    ChainServiceError { chain_id: u64, message: String },

    #[error("Transaction simulation failed: {message}")]
    SimulationFailed {
        message: String,
        revert_reason: Option<String>,
        revert_data: Option<Bytes>,
    },

    #[error("Gas estimation failed: {message}")]
    GasEstimationFailed { message: String },

    #[error("Fee estimation failed: {message}")]
    FeeEstimationFailed {
        message: String,
        inner_error: EngineError,
    },

    #[error("Nonce assignment failed: {reason}")]
    NonceAssignmentFailed { reason: String },

    #[error("Max in-flight transactions reached for EOA {eoa}: {current}/{max}")]
    MaxInFlightReached {
        eoa: Address,
        current: u32,
        max: u32,
    },

    #[error("Transaction send failed: {message}")]
    SendFailed {
        nonce_used: U256,
        message: String,
        possibly_sent: bool, // true for "nonce too low" errors
        should_retry: bool,
    },

    #[error("EOA health check failed: {reason}")]
    UnhealthyEoa {
        eoa: Address,
        reason: String,
        should_resync: bool,
    },

    #[error("Invalid RPC Credentials: {message}")]
    InvalidRpcCredentials { message: String },

    #[error("Internal error: {message}")]
    InternalError { message: String },

    #[error("Transaction cancelled by user")]
    UserCancelled,
}

impl From<TwmqError> for EoaSendError {
    fn from(error: TwmqError) -> Self {
        EoaSendError::InternalError {
            message: format!("Queue error: {}", error),
        }
    }
}

impl UserCancellable for EoaSendError {
    fn user_cancelled() -> Self {
        EoaSendError::UserCancelled
    }
}

impl EoaSendError {
    /// Returns true if the nonce might have been consumed (used) by this error
    pub fn possibly_sent(&self) -> bool {
        match self {
            EoaSendError::SendFailed { possibly_sent, .. } => *possibly_sent,
            _ => false,
        }
    }
}

// --- Handler ---
pub struct EoaSendHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    pub chain_service: Arc<CS>,
    pub nonce_manager: Arc<NonceManager>,
    pub webhook_queue: Arc<Queue<WebhookJobHandler>>,
    pub transaction_registry: Arc<TransactionRegistry>,
    pub eoa_signer: Arc<EoaSigner>,
    pub max_in_flight: u32,
}

impl<CS> ExecutorStage for EoaSendHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    fn executor_name() -> &'static str {
        "eoa"
    }

    fn stage_name() -> &'static str {
        "send"
    }
}

impl<CS> WebhookCapable for EoaSendHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    fn webhook_queue(&self) -> &Arc<Queue<WebhookJobHandler>> {
        &self.webhook_queue
    }
}

impl<CS> twmq::DurableExecution for EoaSendHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    type Output = EoaSendResult;
    type ErrorData = EoaSendError;
    type JobData = EoaSendJobData;

    #[tracing::instrument(skip(self, job), fields(transaction_id = job.job.id, stage = Self::stage_name(), executor = Self::executor_name()))]
    async fn process(
        &self,
        job: &BorrowedJob<Self::JobData>,
    ) -> JobResult<Self::Output, Self::ErrorData> {
        let job_data = &job.job.data;

        // 1. Get Chain
        let chain = self
            .chain_service
            .get_chain(job_data.chain_id)
            .map_err(|e| EoaSendError::ChainServiceError {
                chain_id: job_data.chain_id,
                message: format!("Failed to get chain instance: {}", e),
            })
            .map_err_fail()?;

        let chain_auth_headers = job_data
            .rpc_credentials
            .to_header_map()
            .map_err(|e| EoaSendError::InvalidRpcCredentials {
                message: e.to_string(),
            })
            .map_err_fail()?;

        let chain = chain.with_new_default_headers(chain_auth_headers);

        // 2. Build base transaction request
        let mut tx_request = AlloyTransactionRequest::default()
            .with_from(job_data.from)
            .with_value(job_data.value)
            .with_input(job_data.data.clone())
            .with_chain_id(job_data.chain_id);

        if let Some(to) = job_data.to {
            tx_request = tx_request.with_to(to);
        }

        tx_request = self
            .estimate_gas_fees(&chain, tx_request)
            .await
            .map_err_fail()?;

        // 4. Estimate gas limit if not provided (this also simulates)
        if let Some(gas_limit) = job_data.gas_limit {
            tx_request = tx_request.with_gas_limit(gas_limit);
        } else {
            match chain.provider().estimate_gas(tx_request).await {
                Ok(gas) => {
                    let gas_with_buffer = gas * 110 / 100; // Add 10% buffer
                    tx_request = tx_request.with_gas_limit(gas_with_buffer);
                }
                Err(rpc_error) => {
                    return self.handle_simulation_error(rpc_error, &chain).await;
                }
            }
        }

        // 5. Assign nonce (atomic operation with sync fallback)
        let assigned_nonce = if let Some(nonce) = job_data.assigned_nonce {
            // Retry with previously assigned nonce
            nonce
        } else {
            // First attempt - assign new nonce
            match self
                .nonce_manager
                .assign_nonce(job_data.from, job_data.chain_id)
                .await
            {
                Ok((nonce, _epoch)) => nonce,
                Err(super::nonce_manager::NonceManagerError::NeedsSync { .. }) => {
                    // Need to sync - try to acquire sync lock and sync
                    return self.handle_sync_required(job_data, &chain).await;
                }
                Err(e) => {
                    return self.handle_nonce_assignment_error(e).await;
                }
            }
        };

        // 6. Apply nonce to final transaction
        let final_tx = tx_request
            .with_nonce(assigned_nonce)
            .build_typed_tx()
            .map_err(|e| EoaSendError::InternalError {
                message: format!("Failed to build typed transaction: {}", json!(e)),
            })
            .map_err_fail()?;

        let gas_limit = final_tx.gas_limit();
        let max_fee_per_gas = final_tx.max_fee_per_gas();
        let max_priority_fee_per_gas = final_tx.max_priority_fee_per_gas();

        // 7. Sign the transaction
        let signing_options = EoaSigningOptions {
            from: job_data.from,
            chain_id: Some(job_data.chain_id),
        };

        let signature = self
            .eoa_signer
            .sign_transaction(
                signing_options,
                final_tx.clone(),
                job_data.signing_credential.clone(),
            )
            .await
            .map_err(|e| EoaSendError::InternalError {
                message: format!("Failed to sign transaction: {}", e),
            })
            .map_err_fail()?;

        // 8. Create signed transaction envelope
        let signed_tx = final_tx.into_signed(
            signature
                .parse()
                .map_err(|e| EoaSendError::InternalError {
                    message: format!("Failed to parse signature: {}", e),
                })
                .map_err_fail()?,
        );

        tracing::debug!(
            nonce = %assigned_nonce,
            gas_limit = %gas_limit,
            max_fee_per_gas = %max_fee_per_gas,
            max_priority_fee_per_gas = ?max_priority_fee_per_gas,
            "Sending signed transaction"
        );

        let pre_computed_hash = signed_tx.hash();

        // 9. Send transaction
        match chain.provider().send_tx_envelope(signed_tx.into()).await {
            Ok(pending_tx) => {
                let tx_hash = *pending_tx.tx_hash();

                tracing::info!(
                    transaction_hash = %tx_hash,
                    nonce = %assigned_nonce,
                    "Transaction sent successfully"
                );

                Ok(EoaSendResult {
                    transaction_hash: tx_hash,
                    nonce_used: assigned_nonce,
                    possibly_duplicate: None,
                })
            }
            Err(send_error) => {
                self.handle_send_error(send_error, assigned_nonce, &chain)
                    .await
            }
        }
    }

    async fn on_success(
        &self,
        job: &BorrowedJob<Self::JobData>,
        success_data: SuccessHookData<'_, Self::Output>,
        tx: &mut TransactionContext<'_>,
    ) {
        // 1. Record nonce assignment for tracking
        self.nonce_manager.add_nonce_assignment_command(
            tx.pipeline(),
            job.job.data.from,
            job.job.data.chain_id,
            success_data.result.nonce_used,
            &job.job.data.transaction_id,
            success_data.result.transaction_hash,
        );

        // 2. Update transaction registry: move from send to confirm queue
        self.transaction_registry.add_set_command(
            tx.pipeline(),
            &job.job.data.transaction_id,
            "eoa_confirm",
        );

        // 3. Queue confirmation job
        let confirm_job = self
            .confirm_queue
            .clone()
            .job(EoaConfirmationJobData {
                transaction_id: job.job.data.transaction_id.clone(),
                chain_id: job.job.data.chain_id,
                eoa_address: job.job.data.from,
                nonce: success_data.result.nonce_used,
                transaction_hash: success_data.result.transaction_hash,
                webhook_options: job.job.data.webhook_options.clone(),
                rpc_credentials: job.job.data.rpc_credentials.clone(),
            })
            .with_id(&job.job.data.transaction_id)
            .with_delay(DelayOptions {
                delay: Duration::from_secs(2),
                position: RequeuePosition::Last,
            });

        if let Err(e) = tx.queue_job(confirm_job) {
            tracing::error!(
                transaction_id = %job.job.data.transaction_id,
                error = %e,
                "Failed to queue confirmation job"
            );
        }

        if let Err(e) = self.queue_success_webhook(job, success_data, tx) {
            tracing::error!(
                transaction_id = %job.job.data.transaction_id,
                error = %e,
                "Failed to queue success webhook"
            );
        }
    }

    async fn on_nack(
        &self,
        job: &BorrowedJob<Self::JobData>,
        nack_data: NackHookData<'_, Self::ErrorData>,
        tx: &mut TransactionContext<'_>,
    ) {
        // Update health on nack (increment error count)
        if matches!(
            nack_data.error,
            EoaSendError::SimulationFailed { .. }
                | EoaSendError::SendFailed { .. }
                | EoaSendError::UnhealthyEoa { .. }
        ) {
            // TODO: Update health error counters
        }

        if let Err(e) = self.queue_nack_webhook(job, nack_data, tx) {
            tracing::error!(
                transaction_id = %job.job.data.transaction_id,
                error = %e,
                "Failed to queue nack webhook"
            );
        }
    }

    async fn on_fail(
        &self,
        job: &BorrowedJob<Self::JobData>,
        fail_data: FailHookData<'_, Self::ErrorData>,
        tx: &mut TransactionContext<'_>,
    ) {
        // Handle nonce recycling based on error type
        if let Some(nonce) = job.job.data.assigned_nonce {
            let should_recycle = match fail_data.error {
                EoaSendError::SendFailed { possibly_sent, .. } => !possibly_sent,
                EoaSendError::SimulationFailed { .. } => true,
                EoaSendError::GasEstimationFailed { .. } => true,
                EoaSendError::NonceAssignmentFailed { .. } => false, // Nonce wasn't assigned
                EoaSendError::MaxInFlightReached { .. } => false,    // Nonce wasn't assigned
                EoaSendError::UnhealthyEoa { should_resync, .. } => *should_resync,
                _ => false,
            };

            if should_recycle {
                tracing::debug!(
                    nonce = %nonce,
                    transaction_id = %job.job.data.transaction_id,
                    "Recycling nonce after permanent failure"
                );

                self.nonce_manager.add_recycle_nonce_command(
                    tx.pipeline(),
                    job.job.data.from,
                    job.job.data.chain_id,
                    nonce,
                );
            }
        }

        // Handle sync triggering for health issues
        if let EoaSendError::UnhealthyEoa {
            should_resync: true,
            ..
        } = fail_data.error
        {
            // TODO: Trigger sync operation
            tracing::warn!(
                eoa = %job.job.data.from,
                chain_id = job.job.data.chain_id,
                "EOA health issue detected - sync recommended"
            );
        }

        // Remove transaction from registry since it failed permanently
        self.transaction_registry
            .add_remove_command(tx.pipeline(), &job.job.data.transaction_id);

        if let Err(e) = self.queue_fail_webhook(job, fail_data, tx) {
            tracing::error!(
                transaction_id = %job.job.data.transaction_id,
                error = %e,
                "Failed to queue fail webhook"
            );
        }
    }
}

// --- Error Handling ---

impl<CS> EoaSendHandler<CS>
where
    CS: ChainService + Send + Sync + 'static,
{
    async fn handle_simulation_error(
        &self,
        rpc_error: RpcError<TransportErrorKind>,
        chain: &impl Chain,
    ) -> JobResult<EoaSendResult, EoaSendError> {
        // Check if this is a revert first
        if let RpcError::ErrorResp(error_payload) = &rpc_error {
            if error_payload.as_revert_data().is_some() {
                return Err(EoaSendError::SimulationFailed {
                    message: format!(
                        "Transaction reverted during simulation: {}",
                        error_payload.message
                    ),
                    revert_reason: Some(error_payload.message.clone()),
                    revert_data: None,
                }
                .fail());
            }
        }

        // Try to map actionable errors
        match EoaErrorMapper::map_send_error(&rpc_error, chain) {
            Ok(eoa_error) => {
                let strategy = EoaErrorMapper::get_recovery_strategy(&eoa_error);
                eoa_error.to_send_job_result(
                    &strategy,
                    || {
                        // This shouldn't happen for simulation errors, but handle gracefully
                        EoaSendResult {
                            transaction_hash: B256::ZERO,
                            nonce_used: U256::ZERO,
                            gas_limit: U256::ZERO,
                            max_fee_per_gas: U256::ZERO,
                            max_priority_fee_per_gas: U256::ZERO,
                            possibly_duplicate: None,
                        }
                    },
                    |reason| EoaSendError::SimulationFailed {
                        message: reason.clone(),
                        revert_reason: None,
                        revert_data: None,
                    },
                )
            }
            Err(engine_error) => {
                // Use existing engine error handling
                Err(EoaSendError::SimulationFailed {
                    message: engine_error.to_string(),
                    revert_reason: None,
                    revert_data: None,
                }
                .fail())
            }
        }
    }

    async fn handle_sync_required(
        &self,
        job_data: &EoaSendJobData,
        chain: &impl Chain,
    ) -> JobResult<EoaSendResult, EoaSendError> {
        tracing::info!(
            eoa = %job_data.from,
            chain_id = job_data.chain_id,
            "Sync required - attempting to sync nonce"
        );

        // Get current onchain nonce
        let onchain_nonce = match chain.provider().get_transaction_count(job_data.from).await {
            Ok(nonce) => nonce.to::<u64>(),
            Err(e) => {
                // Try to map actionable errors, otherwise use engine error handling
                match EoaErrorMapper::map_send_error(&e, chain) {
                    Ok(eoa_error) => {
                        let strategy = EoaErrorMapper::get_recovery_strategy(&eoa_error);
                        return eoa_error.to_send_job_result(
                            &strategy,
                            || EoaSendResult {
                                transaction_hash: B256::ZERO,
                                nonce_used: U256::ZERO,
                                gas_limit: U256::ZERO,
                                max_fee_per_gas: U256::ZERO,
                                max_priority_fee_per_gas: U256::ZERO,
                                possibly_duplicate: None,
                            },
                            |reason| EoaSendError::ChainServiceError {
                                chain_id: job_data.chain_id,
                                message: format!(
                                    "Failed to get onchain nonce for sync: {}",
                                    reason
                                ),
                            },
                        );
                    }
                    Err(engine_error) => {
                        // Use existing engine error handling
                        return Err(EoaSendError::ChainServiceError {
                            chain_id: job_data.chain_id,
                            message: format!(
                                "Failed to get onchain nonce for sync: {}",
                                engine_error
                            ),
                        }
                        .fail());
                    }
                }
            }
        };

        // Try to acquire sync lock and perform sync
        match self
            .nonce_manager
            .try_sync_nonce(job_data.from, job_data.chain_id, onchain_nonce)
            .await
        {
            Ok(true) => {
                // Successfully synced
                tracing::info!(
                    eoa = %job_data.from,
                    chain_id = job_data.chain_id,
                    onchain_nonce = %onchain_nonce,
                    "Successfully synced nonce"
                );

                // Delay and retry the job
                Err(EoaSendError::NonceAssignmentFailed {
                    reason: "Nonce synced - retrying".to_string(),
                }
                .nack(Some(Duration::from_millis(100)), RequeuePosition::Last))
            }
            Ok(false) => {
                // Another process is syncing
                tracing::debug!(
                    eoa = %job_data.from,
                    chain_id = job_data.chain_id,
                    "Another process is syncing - backing off"
                );

                Err(EoaSendError::NonceAssignmentFailed {
                    reason: "Sync in progress - backing off".to_string(),
                }
                .nack(Some(Duration::from_secs(2)), RequeuePosition::Last))
            }
            Err(e) => {
                tracing::error!(
                    eoa = %job_data.from,
                    chain_id = job_data.chain_id,
                    error = %e,
                    "Failed to sync nonce"
                );

                Err(EoaSendError::NonceAssignmentFailed {
                    reason: format!("Sync failed: {}", e),
                }
                .nack(Some(Duration::from_secs(10)), RequeuePosition::Last))
            }
        }
    }

    async fn handle_nonce_assignment_error(
        &self,
        nonce_error: super::nonce_manager::NonceManagerError,
    ) -> JobResult<EoaSendResult, EoaSendError> {
        use super::nonce_manager::NonceManagerError;

        match nonce_error {
            NonceManagerError::MaxInFlightReached { eoa, current, max } => {
                Err(EoaSendError::MaxInFlightReached { eoa, current, max }
                    .nack(Some(Duration::from_secs(10)), RequeuePosition::Last))
            }
            NonceManagerError::NeedsSync { eoa } => {
                // This shouldn't happen since we handle it above, but just in case
                Err(EoaSendError::NonceAssignmentFailed {
                    reason: format!("Unexpected needs sync for EOA {}", eoa),
                }
                .nack(Some(Duration::from_secs(30)), RequeuePosition::Last))
            }
            _ => Err(EoaSendError::NonceAssignmentFailed {
                reason: nonce_error.to_string(),
            }
            .fail()),
        }
    }

    async fn handle_send_error(
        &self,
        send_error: RpcError<TransportErrorKind>,
        nonce: u64,
        chain: &impl Chain,
    ) -> JobResult<EoaSendResult, EoaSendError> {
        // Try to map actionable errors, otherwise use engine error handling
        match EoaErrorMapper::map_send_error(&send_error, chain) {
            Ok(eoa_error) => {
                let strategy = EoaErrorMapper::get_recovery_strategy(&eoa_error);

                tracing::debug!(
                    nonce = %nonce,
                    error = ?eoa_error,
                    strategy = ?strategy,
                    "Mapped send error"
                );

                if strategy.queue_confirmation {
                    tracing::warn!(nonce = %nonce, message = %eoa_error.message(), "Transaction possibly sent - treating as success");

                    Ok(EoaSendResult {
                        transaction_hash: B256::ZERO, // Will be resolved in confirmation
                        nonce_used: nonce,
                        gas_limit: U256::ZERO,
                        max_fee_per_gas: U256::ZERO,
                        max_priority_fee_per_gas: U256::ZERO,
                        possibly_duplicate: Some(true),
                    })
                } else {
                    eoa_error.to_send_job_result(
                        &strategy,
                        || EoaSendResult {
                            transaction_hash: B256::ZERO,
                            nonce_used: nonce,
                            gas_limit: U256::ZERO,
                            max_fee_per_gas: U256::ZERO,
                            max_priority_fee_per_gas: U256::ZERO,
                            possibly_duplicate: None,
                        },
                        |reason| EoaSendError::SendFailed {
                            nonce_used: nonce,
                            message: reason,
                            possibly_sent: strategy.queue_confirmation,
                            should_retry: strategy.retryable,
                        },
                    )
                }
            }
            Err(engine_error) => {
                // Use existing engine error handling - not actionable
                tracing::debug!(
                    nonce = %nonce,
                    engine_error = ?engine_error,
                    "Using engine error handling for non-actionable error"
                );

                Err(EoaSendError::SendFailed {
                    nonce_used: nonce,
                    message: engine_error.to_string(),
                    possibly_sent: false,
                    should_retry: false,
                }
                .fail())
            }
        }
    }

    async fn estimate_gas_fees(
        &self,
        chain: &impl Chain,
        tx: AlloyTransactionRequest,
    ) -> Result<AlloyTransactionRequest, EoaSendError> {
        // Try EIP-1559 fees first, fall back to legacy if unsupported
        match chain.provider().estimate_eip1559_fees().await {
            Ok(eip1559_fees) => {
                tracing::debug!(
                    "Using EIP-1559 fees: max_fee={}, max_priority_fee={}",
                    eip1559_fees.max_fee_per_gas,
                    eip1559_fees.max_priority_fee_per_gas
                );
                Ok(tx
                    .with_max_fee_per_gas(eip1559_fees.max_fee_per_gas)
                    .with_max_priority_fee_per_gas(eip1559_fees.max_priority_fee_per_gas))
            }
            Err(eip1559_error) => {
                // Check if this is an "unsupported feature" error
                if let RpcError::UnsupportedFeature(_) = &eip1559_error {
                    tracing::debug!("EIP-1559 not supported, falling back to legacy gas price");

                    // Fall back to legacy gas price
                    match chain.provider().get_gas_price().await {
                        Ok(gas_price) => {
                            // For legacy transactions, use the gas price
                            tracing::debug!("Using legacy gas price: {}", gas_price);
                            Ok(tx.with_gas_price(gas_price))
                        }
                        Err(legacy_error) => Err(EoaSendError::FeeEstimationFailed {
                            message: format!("Failed to get legacy gas price: {}", legacy_error),
                            inner_error: legacy_error.to_engine_error(chain),
                        }),
                    }
                } else {
                    // Other EIP-1559 error
                    Err(EoaSendError::FeeEstimationFailed {
                        message: format!("Failed to estimate EIP-1559 fees: {}", eip1559_error),
                        inner_error: eip1559_error.to_engine_error(chain),
                    })
                }
            }
        }
    }
}
