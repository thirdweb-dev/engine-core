use std::time::Duration;

use alloy::{
    consensus::{
        SignableTransaction, Signed, TxEip4844Variant, TxEip4844WithSidecar, TypedTransaction,
    },
    network::{TransactionBuilder, TransactionBuilder7702},
    primitives::{Bytes, U256},
    providers::Provider,
    rpc::types::TransactionRequest as AlloyTransactionRequest,
    signers::Signature,
    transports::RpcError,
};
use engine_core::{
    chain::Chain,
    credentials::SigningCredential,
    error::AlloyRpcErrorToEngineError,
    signer::{AccountSigner, EoaSigningOptions},
    transaction::TransactionTypeData,
};

use crate::eoa::{
    store::{
        BorrowedTransaction, BorrowedTransactionData, PendingTransaction, SubmittedNoopTransaction,
    }, worker::{
        error::{is_retryable_preparation_error, is_unsupported_eip1559_error, EoaExecutorWorkerError}, EoaExecutorWorker
    }, EoaTransactionRequest
};

// Retry constants for preparation phase
const MAX_PREPARATION_RETRIES: u32 = 3;
const PREPARATION_RETRY_DELAY_MS: u64 = 100;

impl<C: Chain> EoaExecutorWorker<C> {
    pub async fn build_and_sign_single_transaction_with_retries(
        &self,
        pending_transaction: &PendingTransaction,
        nonce: u64,
    ) -> Result<BorrowedTransaction, EoaExecutorWorkerError> {
        let mut last_error = None;

        // Internal retry loop for retryable errors
        for attempt in 0..=MAX_PREPARATION_RETRIES {
            if attempt > 0 {
                // Simple exponential backoff
                let delay = PREPARATION_RETRY_DELAY_MS * (2_u64.pow(attempt - 1));
                tokio::time::sleep(Duration::from_millis(delay)).await;

                tracing::debug!(
                    transaction_id = ?pending_transaction.transaction_id,
                    attempt = attempt,
                    "Retrying transaction preparation"
                );
            }

            match self
                .build_and_sign_single_transaction(pending_transaction, nonce)
                .await
            {
                Ok(result) => return Ok(result),
                Err(error) => {
                    if is_retryable_preparation_error(&error) && attempt < MAX_PREPARATION_RETRIES {
                        tracing::warn!(
                            transaction_id = ?pending_transaction.transaction_id,
                            attempt = attempt,
                            error = ?error,
                            "Retryable error during transaction preparation, will retry"
                        );
                        last_error = Some(error);
                        continue;
                    } else {
                        // Either deterministic error or exceeded max retries
                        return Err(error);
                    }
                }
            }
        }

        // This should never be reached, but just in case
        Err(
            last_error.unwrap_or_else(|| EoaExecutorWorkerError::InternalError {
                message: "Unexpected error in retry loop".to_string(),
            }),
        )
    }

    pub async fn build_and_sign_single_transaction(
        &self,
        pending_transaction: &PendingTransaction,
        nonce: u64,
    ) -> Result<BorrowedTransaction, EoaExecutorWorkerError> {
        // Get transaction data
        let tx_data = pending_transaction.user_request.clone();
        // Build and sign transaction
        let signed_tx = self.build_and_sign_transaction(&tx_data, nonce).await?;

        Ok(BorrowedTransaction {
            data: BorrowedTransactionData {
                transaction_id: pending_transaction.transaction_id.clone(),
                hash: signed_tx.hash().to_string(),
                signed_transaction: signed_tx,
                borrowed_at: chrono::Utc::now().timestamp_millis().max(0) as u64,
                queued_at: pending_transaction.queued_at,
            },
            user_request: pending_transaction.user_request.clone(),
        })
    }

    pub async fn build_and_sign_noop_transaction(
        &self,
        nonce: u64,
    ) -> Result<Signed<TypedTransaction>, EoaExecutorWorkerError> {
        // Create a minimal transaction to consume the recycled nonce
        // Send 0 ETH to self with minimal gas

        // Build no-op transaction (send 0 to self)
        let tx_request = AlloyTransactionRequest::default()
            .with_from(self.eoa)
            .with_to(self.eoa) // Send to self
            .with_value(U256::ZERO) // Send 0 value
            .with_input(Bytes::new()) // No data
            .with_chain_id(self.chain.chain_id())
            .with_nonce(nonce)
            .with_gas_limit(21000); // Minimal gas for basic transfer

        let tx_request = self.estimate_gas_fees(tx_request).await?;
        let built_tx = tx_request.build_typed_tx().map_err(|e| {
            EoaExecutorWorkerError::TransactionBuildFailed {
                message: format!("Failed to build typed transaction for no-op: {e:?}"),
            }
        })?;

        let tx = self
            .sign_transaction(built_tx, &self.noop_signing_credential)
            .await?;

        Ok(tx)
    }

    pub async fn send_noop_transaction(
        &self,
        nonce: u64,
    ) -> Result<SubmittedNoopTransaction, EoaExecutorWorkerError> {
        let tx = self.build_and_sign_noop_transaction(nonce).await?;

        self.chain
            .provider()
            .send_tx_envelope(tx.into())
            .await
            .map_err(|e| EoaExecutorWorkerError::TransactionSendError {
                message: format!("Failed to send no-op transaction: {e:?}"),
                inner_error: e.to_engine_error(&self.chain),
            })
            .map(|pending| SubmittedNoopTransaction {
                nonce,
                transaction_hash: pending.tx_hash().to_string(),
            })
    }

    async fn estimate_gas_fees(
        &self,
        tx: AlloyTransactionRequest,
    ) -> Result<AlloyTransactionRequest, EoaExecutorWorkerError> {
        // Check what fees are missing and need to be estimated

        // If we have gas_price set, we're doing legacy - don't estimate EIP-1559
        if tx.gas_price.is_some() {
            return Ok(tx);
        }

        // If we have both EIP-1559 fees set, don't estimate
        if tx.max_fee_per_gas.is_some() && tx.max_priority_fee_per_gas.is_some() {
            return Ok(tx);
        }

        // Try EIP-1559 fees first, fall back to legacy if unsupported
        match self.chain.provider().estimate_eip1559_fees().await {
            Ok(eip1559_fees) => {
                tracing::debug!(
                    "Using EIP-1559 fees: max_fee={}, max_priority_fee={}",
                    eip1559_fees.max_fee_per_gas,
                    eip1559_fees.max_priority_fee_per_gas
                );

                let mut result = tx;
                // Only set fees that are missing
                if result.max_fee_per_gas.is_none() {
                    result = result.with_max_fee_per_gas(eip1559_fees.max_fee_per_gas);
                }
                if result.max_priority_fee_per_gas.is_none() {
                    result =
                        result.with_max_priority_fee_per_gas(eip1559_fees.max_priority_fee_per_gas);
                }

                Ok(result)
            }
            Err(eip1559_error) => {
                // Check if this is an "unsupported feature" error
                if is_unsupported_eip1559_error(&eip1559_error) {
                    tracing::debug!("EIP-1559 not supported, falling back to legacy gas price");

                    // Fall back to legacy gas price only if no gas price is set
                    if tx.authorization_list().is_none() {
                        match self.chain.provider().get_gas_price().await {
                            Ok(gas_price) => {
                                tracing::debug!("Using legacy gas price: {}", gas_price);
                                Ok(tx.with_gas_price(gas_price))
                            }
                            Err(legacy_error) => Err(EoaExecutorWorkerError::RpcError {
                                message: format!("Failed to get legacy gas price: {legacy_error}"),
                                inner_error: legacy_error.to_engine_error(&self.chain),
                            }),
                        }
                    } else {
                        Err(EoaExecutorWorkerError::TransactionBuildFailed {
                            message: "EIP7702 transactions not supported on chain".to_string(),
                        })
                    }
                } else {
                    // Other EIP-1559 error
                    Err(EoaExecutorWorkerError::RpcError {
                        message: format!("Failed to estimate EIP-1559 fees: {eip1559_error}"),
                        inner_error: eip1559_error.to_engine_error(&self.chain),
                    })
                }
            }
        }
    }

    pub async fn build_typed_transaction(
        &self,
        request: &EoaTransactionRequest,
        nonce: u64,
    ) -> Result<TypedTransaction, EoaExecutorWorkerError> {
        // Build transaction request from stored data
        let mut tx_request = AlloyTransactionRequest::default()
            .with_from(request.from)
            .with_value(request.value)
            .with_input(request.data.clone())
            .with_chain_id(request.chain_id)
            .with_nonce(nonce);

        if let Some(to) = request.to {
            tx_request = tx_request.with_to(to);
        }

        if let Some(gas_limit) = request.gas_limit {
            tx_request = tx_request.with_gas_limit(gas_limit);
        }

        // Handle gas fees - either from user settings or estimation
        tx_request = if let Some(type_data) = &request.transaction_type_data {
            // User provided gas settings - respect them first
            match type_data {
                TransactionTypeData::Eip1559(data) => {
                    let mut req = tx_request;
                    if let Some(max_fee) = data.max_fee_per_gas {
                        req = req.with_max_fee_per_gas(max_fee);
                    }
                    if let Some(max_priority) = data.max_priority_fee_per_gas {
                        req = req.with_max_priority_fee_per_gas(max_priority);
                    }

                    // if either not set, estimate the other one
                    if req.max_fee_per_gas.is_none() || req.max_priority_fee_per_gas.is_none() {
                        req = self.estimate_gas_fees(req).await?;
                    }

                    req
                }
                TransactionTypeData::Legacy(data) => {
                    if let Some(gas_price) = data.gas_price {
                        tx_request.with_gas_price(gas_price)
                    } else {
                        // User didn't provide gas price, estimate it
                        self.estimate_gas_fees(tx_request).await?
                    }
                }
                TransactionTypeData::Eip7702(data) => {
                    let mut req = tx_request;
                    if let Some(authorization_list) = &data.authorization_list {
                        req = req.with_authorization_list(authorization_list.clone());
                    }
                    if let Some(max_fee) = data.max_fee_per_gas {
                        req = req.with_max_fee_per_gas(max_fee);
                    }
                    if let Some(max_priority) = data.max_priority_fee_per_gas {
                        req = req.with_max_priority_fee_per_gas(max_priority);
                    }

                    // if either not set, estimate the other one
                    if req.max_fee_per_gas.is_none() || req.max_priority_fee_per_gas.is_none() {
                        req = self.estimate_gas_fees(req).await?;
                    }

                    req
                }
            }
        } else {
            // No user settings - estimate appropriate fees
            self.estimate_gas_fees(tx_request).await?
        };

        // Estimate gas if needed
        if tx_request.gas.is_none() {
            match self.chain.provider().estimate_gas(tx_request.clone()).await {
                Ok(gas_limit) => {
                    tx_request = tx_request.with_gas_limit(gas_limit * 110 / 100); // 10% buffer
                }
                Err(e) => {
                    // Check if this is a revert
                    if let RpcError::ErrorResp(error_payload) = &e {
                        if let Some(revert_data) = error_payload.as_revert_data() {
                            // This is a revert - the transaction is fundamentally broken
                            // This should fail the individual transaction, not the worker
                            return Err(EoaExecutorWorkerError::TransactionSimulationFailed {
                                message: format!(
                                    "Transaction reverted during gas estimation: {} (revert: {})",
                                    error_payload.message,
                                    hex::encode(&revert_data)
                                ),
                                inner_error: e.to_engine_error(&self.chain),
                            });
                        } else if error_payload.message.to_lowercase().contains("revert") {
                            // This is a revert - the transaction is fundamentally broken
                            // This should fail the individual transaction, not the worker
                            // We need this fallback case because some providers don't return revert data
                            return Err(EoaExecutorWorkerError::TransactionSimulationFailed {
                                message: format!(
                                    "Transaction reverted during gas estimation: {}",
                                    error_payload.message
                                ),
                                inner_error: e.to_engine_error(&self.chain),
                            });
                        } else if error_payload.message.to_lowercase().contains("oversized") {
                            // This is an oversized transaction - the transaction is fundamentally broken
                            // This should fail the individual transaction, not the worker
                            return Err(EoaExecutorWorkerError::TransactionSimulationFailed {
                                message: format!(
                                    "Transaction data is oversized during gas estimation: {}",
                                    error_payload.message
                                ),
                                inner_error: e.to_engine_error(&self.chain),
                            });
                        }
                    }

                    // Not a revert - could be RPC issue, this should nack the worker
                    let engine_error = e.to_engine_error(&self.chain);
                    return Err(EoaExecutorWorkerError::RpcError {
                        message: format!("Gas estimation failed: {engine_error}"),
                        inner_error: engine_error,
                    });
                }
            }
        }

        // Build typed transaction
        tx_request
            .build_typed_tx()
            .map_err(|e| EoaExecutorWorkerError::TransactionBuildFailed {
                message: format!("Failed to build typed transaction: {e:?}"),
            })
    }

    pub async fn sign_transaction(
        &self,
        typed_tx: TypedTransaction,
        credential: &SigningCredential,
    ) -> Result<Signed<TypedTransaction>, EoaExecutorWorkerError> {
        let signing_options = EoaSigningOptions {
            from: self.eoa,
            chain_id: Some(self.chain_id),
        };

        let signature = self
            .signer
            .sign_transaction(signing_options, &typed_tx, credential)
            .await
            .map_err(|engine_error| EoaExecutorWorkerError::SigningError {
                message: format!("Failed to sign transaction: {engine_error}"),
                inner_error: engine_error,
            })?;

        let signature = signature.parse::<Signature>().map_err(|e| {
            EoaExecutorWorkerError::SignatureParsingFailed {
                message: format!("Failed to parse signature: {e}"),
            }
        })?;

        Ok(typed_tx.into_signed(signature))
    }

    async fn build_and_sign_transaction(
        &self,
        request: &EoaTransactionRequest,
        nonce: u64,
    ) -> Result<Signed<TypedTransaction>, EoaExecutorWorkerError> {
        let typed_tx = self.build_typed_transaction(request, nonce).await?;
        
        // Inject KMS cache into the signing credential
        let credential_with_cache = self.inject_kms_cache_into_credential(request.signing_credential.clone());
        
        self.sign_transaction(typed_tx, &credential_with_cache)
            .await
    }

    pub fn apply_gas_bump_to_typed_transaction(
        &self,
        mut typed_tx: TypedTransaction,
        bump_multiplier: u32, // e.g., 120 for 20% increase
    ) -> TypedTransaction {
        match &mut typed_tx {
            TypedTransaction::Eip1559(tx) => {
                tx.max_fee_per_gas = tx.max_fee_per_gas * bump_multiplier as u128 / 100;
                tx.max_priority_fee_per_gas =
                    tx.max_priority_fee_per_gas * bump_multiplier as u128 / 100;
            }
            TypedTransaction::Legacy(tx) => {
                tx.gas_price = tx.gas_price * bump_multiplier as u128 / 100;
            }
            TypedTransaction::Eip2930(tx) => {
                tx.gas_price = tx.gas_price * bump_multiplier as u128 / 100;
            }
            TypedTransaction::Eip7702(tx) => {
                tx.max_fee_per_gas = tx.max_fee_per_gas * bump_multiplier as u128 / 100;
                tx.max_priority_fee_per_gas =
                    tx.max_priority_fee_per_gas * bump_multiplier as u128 / 100;
            }
            TypedTransaction::Eip4844(tx) => match tx {
                TxEip4844Variant::TxEip4844(tx) => {
                    tx.max_fee_per_gas = tx.max_fee_per_gas * bump_multiplier as u128 / 100;
                    tx.max_priority_fee_per_gas =
                        tx.max_priority_fee_per_gas * bump_multiplier as u128 / 100;
                }
                TxEip4844Variant::TxEip4844WithSidecar(TxEip4844WithSidecar { tx, .. }) => {
                    tx.max_fee_per_gas = tx.max_fee_per_gas * bump_multiplier as u128 / 100;
                    tx.max_priority_fee_per_gas =
                        tx.max_priority_fee_per_gas * bump_multiplier as u128 / 100;
                }
            },
        }
        typed_tx
    }
}
