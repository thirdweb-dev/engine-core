use std::sync::Arc;

use alloy::primitives::U256;
use engine_aa_core::smart_account::{DeterminedSmartAccount, SmartAccount, SmartAccountFromSalt};
use engine_core::{
    chain::{ChainService, RpcCredentials},
    credentials::SigningCredential,
    error::EngineError,
    execution_options::{
        BaseExecutionOptions, QueuedTransaction, SendTransactionRequest, SpecificExecutionOptions,
        WebhookOptions, aa::Erc4337ExecutionOptions, eip7702::Eip7702ExecutionOptions,
    },
    transaction::InnerTransaction,
};
use engine_executors::{
    eip7702_executor::{
        confirm::Eip7702ConfirmationHandler,
        send::{Eip7702SendHandler, Eip7702SendJobData},
    },
    external_bundler::{
        confirm::UserOpConfirmationHandler,
        send::{ExternalBundlerSendHandler, ExternalBundlerSendJobData},
    },
    transaction_registry::TransactionRegistry,
    webhook::WebhookJobHandler,
};
use twmq::{Queue, error::TwmqError};
use vault_sdk::VaultClient;
use vault_types::{
    RegexRule, Rule,
    enclave::auth::Auth,
    userop::{UserOperationV06Rules, UserOperationV07Rules},
};

use crate::chains::ThirdwebChainService;

pub struct ExecutionRouter {
    pub webhook_queue: Arc<Queue<WebhookJobHandler>>,
    pub external_bundler_send_queue: Arc<Queue<ExternalBundlerSendHandler<ThirdwebChainService>>>,
    pub userop_confirm_queue: Arc<Queue<UserOpConfirmationHandler<ThirdwebChainService>>>,
    pub eip7702_send_queue: Arc<Queue<Eip7702SendHandler<ThirdwebChainService>>>,
    pub eip7702_confirm_queue: Arc<Queue<Eip7702ConfirmationHandler<ThirdwebChainService>>>,
    pub transaction_registry: Arc<TransactionRegistry>,
    pub vault_client: Arc<VaultClient>,
    pub chains: Arc<ThirdwebChainService>,
}

impl ExecutionRouter {
    fn generate_random_nonce() -> U256 {
        use rand::Rng;
        let mut rng = rand::rng();
        let rand1 = rng.random::<u64>();
        let rand2 = rng.random::<u64>();
        let rand3 = rng.random::<u64>();

        U256::from_limbs([0, rand1, rand2, rand3])
    }

    /// Convert vault access tokens to signed tokens with ERC4337-specific restrictions
    async fn convert_vault_credential_for_erc4337(
        &self,
        signing_credential: SigningCredential,
        erc4337_options: &Erc4337ExecutionOptions,
        base_options: &BaseExecutionOptions,
        transactions: &[InnerTransaction],
    ) -> Result<(SigningCredential, Option<U256>), EngineError> {
        // Only convert vault access tokens
        let access_token = match &signing_credential {
            SigningCredential::Vault(Auth::AccessToken { access_token }) => access_token,
            _ => return Ok((signing_credential, None)),
        };

        // Skip if already a signed token
        if access_token.starts_with("vt_sat_") {
            return Ok((signing_credential, None));
        }

        // Generate nonce like TypeScript version
        let nonce_seed = Self::generate_random_nonce();
        let preallocated_nonce = nonce_seed << 64;

        // Get chain and encode calldata properly
        let chain = self.chains.get_chain(base_options.chain_id).map_err(|e| {
            EngineError::InternalError {
                message: format!("Failed to get chain: {}", e),
            }
        })?;

        // Parse account salt using the new helper method
        let salt_data = erc4337_options.get_salt_data()?;

        // Determine smart account address
        let smart_account = match erc4337_options.smart_account_address {
            Some(address) => DeterminedSmartAccount { address },
            None => {
                SmartAccountFromSalt {
                    admin_address: erc4337_options.signer_address,
                    chain: &chain,
                    factory_address: erc4337_options.entrypoint_details.factory_address,
                    salt_data: &salt_data,
                }
                .to_determined_smart_account()
                .await?
            }
        };

        // Encode calldata using smart account primitives
        let encoded_calldata = if transactions.len() == 1 {
            smart_account.encode_execute(&transactions[0])
        } else {
            smart_account.encode_execute_batch(transactions)
        };

        // Create rules for UserOp restrictions
        let nonce_rule = Rule::Regex(RegexRule {
            pattern: format!("^{}$", preallocated_nonce),
        });
        let calldata_rule = Rule::Regex(RegexRule {
            pattern: format!("(?i)^{}$", encoded_calldata),
        });

        let userop_v06_rules = UserOperationV06Rules {
            nonce: Some(nonce_rule.clone()),
            call_data: Some(calldata_rule.clone()),
            sender: None,
            init_code: None,
            call_gas_limit: None,
            verification_gas_limit: None,
            pre_verification_gas: None,
            max_fee_per_gas: None,
            max_priority_fee_per_gas: None,
            paymaster_and_data: None,
            chain_id: None,
            entrypoint: None,
        };

        let userop_v07_rules = UserOperationV07Rules {
            nonce: Some(nonce_rule),
            call_data: Some(calldata_rule),
            sender: None,
            call_gas_limit: None,
            verification_gas_limit: None,
            pre_verification_gas: None,
            max_fee_per_gas: None,
            max_priority_fee_per_gas: None,
            factory: None,
            factory_data: None,
            paymaster: None,
            paymaster_data: None,
            paymaster_verification_gas_limit: None,
            paymaster_post_op_gas_limit: None,
            chain_id: None,
            entrypoint: None,
        };

        // Use the vault client helper method
        let additional_policies = vec![VaultClient::create_eoa_sign_structured_message_policy(
            vec![erc4337_options.signer_address],
            None,
            Some(userop_v06_rules),
            Some(userop_v07_rules),
        )];

        // 24 hour expiry - use unix timestamp
        let expiry_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            + (24 * 60 * 60);

        let signed_token = self
            .vault_client
            .create_signed_access_token(access_token.clone(), additional_policies, expiry_timestamp)
            .map_err(|e| EngineError::VaultError {
                message: format!("Failed to create signed access token: {}", e),
            })?;

        let converted_credential = SigningCredential::Vault(Auth::AccessToken {
            access_token: signed_token,
        });

        Ok((converted_credential, Some(preallocated_nonce)))
    }

    pub async fn execute(
        &self,
        execution_request: SendTransactionRequest,
        rpc_credentials: RpcCredentials,
        signing_credential: engine_core::credentials::SigningCredential,
    ) -> Result<Vec<QueuedTransaction>, EngineError> {
        match execution_request.execution_options.specific {
            SpecificExecutionOptions::ERC4337(ref erc4337_execution_options) => {
                // Convert vault access tokens to signed tokens with proper nonce and calldata
                let (converted_credential, pregenerated_nonce) = self
                    .convert_vault_credential_for_erc4337(
                        signing_credential,
                        erc4337_execution_options,
                        &execution_request.execution_options.base,
                        &execution_request.params,
                    )
                    .await?;

                self.execute_external_bundler(
                    &execution_request.execution_options.base,
                    erc4337_execution_options,
                    &execution_request.webhook_options,
                    &execution_request.params,
                    rpc_credentials,
                    converted_credential,
                    pregenerated_nonce,
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

            SpecificExecutionOptions::EIP7702(ref eip7702_execution_options) => {
                self.execute_eip7702(
                    &execution_request.execution_options.base,
                    eip7702_execution_options,
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

            SpecificExecutionOptions::Auto(_auto_execution_options) => {
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
        pregenerated_nonce: Option<U256>,
    ) -> Result<(), TwmqError> {
        let job_data = ExternalBundlerSendJobData {
            transaction_id: base_execution_options.idempotency_key.clone(),
            chain_id: base_execution_options.chain_id,
            transactions: transactions.to_vec(),
            execution_options: erc4337_execution_options.clone(),
            signing_credential,
            webhook_options: webhook_options.clone(),
            rpc_credentials,
            pregenerated_nonce,
        };

        // Register transaction in registry first
        self.transaction_registry
            .set_transaction_queue(
                &base_execution_options.idempotency_key,
                "external_bundler_send",
            )
            .await
            .map_err(|e| TwmqError::Runtime {
                message: format!("Failed to register transaction: {}", e),
            })?;

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

    async fn execute_eip7702(
        &self,
        base_execution_options: &BaseExecutionOptions,
        eip7702_execution_options: &Eip7702ExecutionOptions,
        webhook_options: &Option<Vec<WebhookOptions>>,
        transactions: &[InnerTransaction],
        rpc_credentials: RpcCredentials,
        signing_credential: SigningCredential,
    ) -> Result<(), TwmqError> {
        let job_data = Eip7702SendJobData {
            transaction_id: base_execution_options.idempotency_key.clone(),
            chain_id: base_execution_options.chain_id,
            transactions: transactions.to_vec(),
            eoa_address: eip7702_execution_options.from,
            signing_credential,
            webhook_options: webhook_options.clone(),
            rpc_credentials,
            nonce: None, // Let the executor handle nonce generation
        };

        // Register transaction in registry first
        self.transaction_registry
            .set_transaction_queue(&base_execution_options.idempotency_key, "eip7702_send")
            .await
            .map_err(|e| TwmqError::Runtime {
                message: format!("Failed to register transaction: {}", e),
            })?;

        // Create job with transaction ID as the job ID for idempotency
        self.eip7702_send_queue
            .clone()
            .job(job_data)
            .with_id(&base_execution_options.idempotency_key)
            .push()
            .await?;

        tracing::debug!(
            transaction_id = %base_execution_options.idempotency_key,
            queue = "eip7702_send",
            "Job queued successfully"
        );

        Ok(())
    }
}
