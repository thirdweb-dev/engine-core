use std::sync::Arc;

use engine_core::{
    chain::RpcCredentials,
    credentials::SigningCredential,
    error::EngineError,
    execution_options::{
        BaseExecutionOptions, QueuedTransaction, SendTransactionRequest, SpecificExecutionOptions,
        WebhookOptions, aa::Erc4337ExecutionOptions,
    },
    transaction::InnerTransaction,
};
use engine_executors::{
    external_bundler::{
        confirm::UserOpConfirmationHandler,
        send::{ExternalBundlerSendHandler, ExternalBundlerSendJobData},
    },
    webhook::WebhookJobHandler,
    transaction_registry::TransactionRegistry,
};
use twmq::{Queue, error::TwmqError};

use crate::chains::ThirdwebChainService;

pub struct ExecutionRouter {
    pub webhook_queue: Arc<Queue<WebhookJobHandler>>,
    pub external_bundler_send_queue: Arc<Queue<ExternalBundlerSendHandler<ThirdwebChainService>>>,
    pub userop_confirm_queue: Arc<Queue<UserOpConfirmationHandler<ThirdwebChainService>>>,
    pub transaction_registry: Arc<TransactionRegistry>,
}

impl ExecutionRouter {
    pub async fn execute(
        &self,
        execution_request: SendTransactionRequest,
        rpc_credentials: RpcCredentials,
        signing_credential: engine_core::credentials::SigningCredential,
    ) -> Result<Vec<QueuedTransaction>, EngineError> {
        match execution_request.execution_options.specific {
            SpecificExecutionOptions::ERC4337(ref erc4337_execution_options) => {
                self.execute_external_bundler(
                    &execution_request.execution_options.base,
                    erc4337_execution_options,
                    &execution_request.webhook_options,
                    &execution_request.params,
                    rpc_credentials,
                    signing_credential,
                )
                .await?;

                let queued_transaction = QueuedTransaction {
                    id: execution_request
                        .execution_options
                        .base
                        .idempotency_key
                        .clone(),
                    batch_index: 0,
                    execution_params: execution_request.execution_options,
                    transaction_params: execution_request.params,
                };

                Ok(vec![queued_transaction])
            }

            SpecificExecutionOptions::Auto(auto_execution_options) => {
                todo!()
            }
        }
    }

    async fn execute_external_bundler(
        &self,
        base_execution_options: &BaseExecutionOptions,
        erc4337_execution_options: &Erc4337ExecutionOptions,
        webhook_options: &Option<Vec<WebhookOptions>>,
        transactions: &[InnerTransaction],
        rpc_credentials: RpcCredentials,
        signing_credential: SigningCredential,
    ) -> Result<(), TwmqError> {
        let job_data = ExternalBundlerSendJobData {
            transaction_id: base_execution_options.idempotency_key.clone(),
            chain_id: base_execution_options.chain_id,
            transactions: transactions.to_vec(),
            execution_options: erc4337_execution_options.clone(),
            signing_credential,
            webhook_options: webhook_options.clone(),
            rpc_credentials,
        };

        // Register transaction in registry first
        self.transaction_registry
            .set_transaction_queue(
                &base_execution_options.idempotency_key,
                "external_bundler_send",
            )
            .await
            .map_err(|e| TwmqError::Runtime(format!("Failed to register transaction: {}", e)))?;

        // Create job with transaction ID as the job ID for idempotency
        self.external_bundler_send_queue
            .clone()
            .job(job_data)
            .with_id(&base_execution_options.idempotency_key)
            .push()
            .await?;

        tracing::debug!(
            transaction_id = %base_execution_options.idempotency_key,
            queue = "external_bundler_send",
            "Job queued successfully"
        );

        Ok(())
    }
}
