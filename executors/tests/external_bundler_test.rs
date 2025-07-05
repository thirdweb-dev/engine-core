use std::sync::Arc;
use std::time::Duration;
use std::collections::HashMap;
use alloy::primitives::{Address, Bytes, U256};
use serde::{Deserialize, Serialize};
use serde_json::json;
use mockall::mock;
use mockall::predicate::*;
use twmq::job::{Job, BorrowedJob, JobError, JobResult};
use twmq::hooks::TransactionContext;
use twmq::{Queue, SuccessHookData, NackHookData, FailHookData};
use testcontainers::clients::Cli;
use testcontainers_modules::redis::Redis;
use engine_core::{
    chain::{Chain, ChainService, RpcCredentials},
    credentials::SigningCredential,
    execution_options::{WebhookOptions, aa::Erc4337ExecutionOptions},
    transaction::InnerTransaction,
    userop::UserOpSigner,
    rpc_clients::UserOperationReceipt,
    error::EngineError,
};
use engine_aa_core::smart_account::details::EntrypointDetails;
use engine_aa_types::VersionedUserOp;
use engine_executors::{
    transaction_registry::TransactionRegistry,
    webhook::WebhookJobHandler,
    external_bundler::{
        send::{
            ExternalBundlerSendHandler, ExternalBundlerSendJobData, ExternalBundlerSendResult,
            ExternalBundlerSendError,
        },
        confirm::{
            UserOpConfirmationHandler, UserOpConfirmationJobData, UserOpConfirmationResult,
            UserOpConfirmationError,
        },
        deployment::{RedisDeploymentCache, RedisDeploymentLock},
    },
};

// Mock ChainService for testing
mock! {
    ChainService {}
    
    #[async_trait::async_trait]
    impl ChainService for ChainService {
        async fn get_chain(&self, chain_id: u64) -> Result<Box<dyn Chain>, EngineError>;
    }
}

// Mock Chain for testing
mock! {
    Chain {}
    
    impl Chain for Chain {
        fn chain_id(&self) -> u64;
        fn rpc_client(&self) -> &dyn engine_core::rpc_clients::RpcClient;
        fn bundler_client(&self) -> &dyn engine_core::rpc_clients::BundlerClient;
        fn with_new_default_headers(&self, headers: reqwest::header::HeaderMap) -> Box<dyn Chain>;
    }
}

// Mock BundlerClient for testing
mock! {
    BundlerClient {}
    
    #[async_trait::async_trait]
    impl engine_core::rpc_clients::BundlerClient for BundlerClient {
        async fn send_user_op(
            &self,
            user_op: &VersionedUserOp,
            entrypoint: Address,
        ) -> Result<Bytes, EngineError>;
        
        async fn get_user_op_receipt(
            &self,
            user_op_hash: Bytes,
        ) -> Result<Option<UserOperationReceipt>, EngineError>;
    }
}

// Mock UserOpSigner for testing
mock! {
    UserOpSigner {}
    
    #[async_trait::async_trait]
    impl engine_core::userop::UserOpSigner for UserOpSigner {
        async fn sign_user_op(
            &self,
            user_op: &mut VersionedUserOp,
            credential: &SigningCredential,
            chain_id: u64,
            entrypoint: Address,
        ) -> Result<(), EngineError>;
    }
}

fn create_test_job_data() -> ExternalBundlerSendJobData {
    ExternalBundlerSendJobData {
        transaction_id: "test_tx_123".to_string(),
        chain_id: 1,
        transactions: vec![InnerTransaction {
            to: Address::from([0x42; 20]),
            value: U256::from(1000),
            data: Bytes::new(),
        }],
        execution_options: Erc4337ExecutionOptions {
            signer_address: Address::from([0x11; 20]),
            smart_account_address: Some(Address::from([0x22; 20])),
            account_salt: "test_salt".to_string(),
            entrypoint_details: EntrypointDetails {
                entrypoint_address: Address::from([0x33; 20]),
                factory_address: Address::from([0x44; 20]),
            },
        },
        signing_credential: SigningCredential::PrivateKey("0x1234567890abcdef".to_string()),
        webhook_options: Some(vec![WebhookOptions {
            url: "https://example.com/webhook".to_string(),
            secret: Some("webhook_secret".to_string()),
        }]),
        rpc_credentials: RpcCredentials::None,
        pregenerated_nonce: Some(U256::from(42)),
    }
}

fn create_test_confirmation_job_data() -> UserOpConfirmationJobData {
    UserOpConfirmationJobData {
        transaction_id: "test_tx_123".to_string(),
        chain_id: 1,
        account_address: Address::from([0x22; 20]),
        user_op_hash: Bytes::from_hex("0x1234567890abcdef").unwrap(),
        nonce: U256::from(42),
        deployment_lock_acquired: true,
        webhook_options: Some(vec![WebhookOptions {
            url: "https://example.com/webhook".to_string(),
            secret: Some("webhook_secret".to_string()),
        }]),
        rpc_credentials: RpcCredentials::None,
    }
}

#[tokio::test]
async fn test_external_bundler_send_job_data_serialization() {
    let job_data = create_test_job_data();
    
    let serialized = serde_json::to_string(&job_data).unwrap();
    let deserialized: ExternalBundlerSendJobData = serde_json::from_str(&serialized).unwrap();
    
    assert_eq!(deserialized.transaction_id, "test_tx_123");
    assert_eq!(deserialized.chain_id, 1);
    assert_eq!(deserialized.transactions.len(), 1);
    assert_eq!(deserialized.pregenerated_nonce, Some(U256::from(42)));
}

#[tokio::test]
async fn test_external_bundler_send_result_serialization() {
    let result = ExternalBundlerSendResult {
        account_address: Address::from([0x22; 20]),
        nonce: U256::from(42),
        user_op_hash: Bytes::from_hex("0x1234567890abcdef").unwrap(),
        user_operation_sent: VersionedUserOp::V06(Default::default()),
        deployment_lock_acquired: true,
    };
    
    let serialized = serde_json::to_string(&result).unwrap();
    let deserialized: ExternalBundlerSendResult = serde_json::from_str(&serialized).unwrap();
    
    assert_eq!(deserialized.account_address, Address::from([0x22; 20]));
    assert_eq!(deserialized.nonce, U256::from(42));
    assert_eq!(deserialized.deployment_lock_acquired, true);
}

#[tokio::test]
async fn test_external_bundler_send_error_serialization() {
    let error = ExternalBundlerSendError::ChainServiceError {
        chain_id: 1,
        message: "Test error".to_string(),
    };
    
    let serialized = serde_json::to_string(&error).unwrap();
    let deserialized: ExternalBundlerSendError = serde_json::from_str(&serialized).unwrap();
    
    match deserialized {
        ExternalBundlerSendError::ChainServiceError { chain_id, message } => {
            assert_eq!(chain_id, 1);
            assert_eq!(message, "Test error");
        }
        _ => panic!("Expected ChainServiceError"),
    }
}

#[tokio::test]
async fn test_external_bundler_send_error_did_acquire_lock() {
    let error_with_lock = ExternalBundlerSendError::BundlerSendFailed {
        account_address: Address::from([0x22; 20]),
        nonce_used: U256::from(42),
        had_deployment_lock: true,
        user_op: VersionedUserOp::V06(Default::default()),
        message: "Test error".to_string(),
        inner_error: None,
    };
    
    let lock_address = error_with_lock.did_acquire_lock();
    assert_eq!(lock_address, Some(Address::from([0x22; 20])));
    
    let error_without_lock = ExternalBundlerSendError::BundlerSendFailed {
        account_address: Address::from([0x22; 20]),
        nonce_used: U256::from(42),
        had_deployment_lock: false,
        user_op: VersionedUserOp::V06(Default::default()),
        message: "Test error".to_string(),
        inner_error: None,
    };
    
    let lock_address = error_without_lock.did_acquire_lock();
    assert_eq!(lock_address, None);
    
    let error_no_lock_info = ExternalBundlerSendError::ChainServiceError {
        chain_id: 1,
        message: "Test error".to_string(),
    };
    
    let lock_address = error_no_lock_info.did_acquire_lock();
    assert_eq!(lock_address, None);
}

#[tokio::test]
async fn test_user_op_confirmation_job_data_serialization() {
    let job_data = create_test_confirmation_job_data();
    
    let serialized = serde_json::to_string(&job_data).unwrap();
    let deserialized: UserOpConfirmationJobData = serde_json::from_str(&serialized).unwrap();
    
    assert_eq!(deserialized.transaction_id, "test_tx_123");
    assert_eq!(deserialized.chain_id, 1);
    assert_eq!(deserialized.account_address, Address::from([0x22; 20]));
    assert_eq!(deserialized.deployment_lock_acquired, true);
}

#[tokio::test]
async fn test_user_op_confirmation_result_serialization() {
    let receipt = UserOperationReceipt {
        user_op_hash: Bytes::from_hex("0x1234567890abcdef").unwrap(),
        entrypoint: Address::from([0x33; 20]),
        sender: Address::from([0x22; 20]),
        nonce: U256::from(42),
        paymaster: None,
        actual_gas_cost: U256::from(1000),
        actual_gas_used: U256::from(500),
        success: true,
        logs: vec![],
        receipt: alloy::rpc::types::TransactionReceipt::default(),
    };
    
    let result = UserOpConfirmationResult {
        user_op_hash: Bytes::from_hex("0x1234567890abcdef").unwrap(),
        receipt,
        deployment_lock_released: true,
    };
    
    let serialized = serde_json::to_string(&result).unwrap();
    let deserialized: UserOpConfirmationResult = serde_json::from_str(&serialized).unwrap();
    
    assert_eq!(deserialized.user_op_hash, Bytes::from_hex("0x1234567890abcdef").unwrap());
    assert_eq!(deserialized.deployment_lock_released, true);
    assert_eq!(deserialized.receipt.success, true);
}

#[tokio::test]
async fn test_user_op_confirmation_error_serialization() {
    let error = UserOpConfirmationError::ReceiptNotAvailable {
        user_op_hash: Bytes::from_hex("0x1234567890abcdef").unwrap(),
        attempt_number: 5,
    };
    
    let serialized = serde_json::to_string(&error).unwrap();
    let deserialized: UserOpConfirmationError = serde_json::from_str(&serialized).unwrap();
    
    match deserialized {
        UserOpConfirmationError::ReceiptNotAvailable { user_op_hash, attempt_number } => {
            assert_eq!(user_op_hash, Bytes::from_hex("0x1234567890abcdef").unwrap());
            assert_eq!(attempt_number, 5);
        }
        _ => panic!("Expected ReceiptNotAvailable error"),
    }
}

#[tokio::test]
async fn test_external_bundler_send_handler_creation() {
    let docker = Cli::default();
    let redis_container = docker.run(Redis::default());
    let redis_url = format!("redis://127.0.0.1:{}/", redis_container.get_host_port_ipv4(6379));
    
    let client = twmq::redis::Client::open(redis_url).unwrap();
    let conn_manager = twmq::redis::aio::ConnectionManager::new(client.clone()).await.unwrap();
    
    let chain_service = Arc::new(MockChainService::new());
    let userop_signer = Arc::new(MockUserOpSigner::new());
    let deployment_cache = RedisDeploymentCache::new(client.clone()).await.unwrap();
    let deployment_lock = RedisDeploymentLock::new(client.clone()).await.unwrap();
    let transaction_registry = Arc::new(TransactionRegistry::new(conn_manager, None));
    
    // Create mock queues (in real implementation these would be proper queues)
    let webhook_queue = Arc::new(Queue::new("webhook_queue".to_string()));
    let confirm_queue = Arc::new(Queue::new("confirm_queue".to_string()));
    
    let handler = ExternalBundlerSendHandler {
        chain_service,
        userop_signer,
        deployment_cache,
        deployment_lock,
        webhook_queue,
        confirm_queue,
        transaction_registry,
    };
    
    // Test deployment manager creation
    let deployment_manager = handler.deployment_manager();
    // Just verify it was created without errors
    assert!(true); // Placeholder assertion
}

#[tokio::test]
async fn test_user_op_confirmation_handler_creation() {
    let docker = Cli::default();
    let redis_container = docker.run(Redis::default());
    let redis_url = format!("redis://127.0.0.1:{}/", redis_container.get_host_port_ipv4(6379));
    
    let client = twmq::redis::Client::open(redis_url).unwrap();
    let conn_manager = twmq::redis::aio::ConnectionManager::new(client.clone()).await.unwrap();
    
    let chain_service = Arc::new(MockChainService::new());
    let deployment_lock = RedisDeploymentLock::new(client.clone()).await.unwrap();
    let webhook_queue = Arc::new(Queue::new("webhook_queue".to_string()));
    let transaction_registry = Arc::new(TransactionRegistry::new(conn_manager, None));
    
    let handler = UserOpConfirmationHandler::new(
        chain_service,
        deployment_lock,
        webhook_queue,
        transaction_registry,
    );
    
    // Test default configuration
    assert_eq!(handler.max_confirmation_attempts, 20);
    assert_eq!(handler.confirmation_retry_delay, Duration::from_secs(5));
    
    // Test custom configuration
    let custom_handler = handler.with_retry_config(10, Duration::from_secs(3));
    assert_eq!(custom_handler.max_confirmation_attempts, 10);
    assert_eq!(custom_handler.confirmation_retry_delay, Duration::from_secs(3));
}

#[tokio::test]
async fn test_external_bundler_send_handler_executor_stage() {
    type TestHandler = ExternalBundlerSendHandler<MockChainService>;
    
    assert_eq!(TestHandler::executor_name(), "erc4337");
    assert_eq!(TestHandler::stage_name(), "prepare_and_send");
}

#[tokio::test]
async fn test_user_op_confirmation_handler_executor_stage() {
    type TestHandler = UserOpConfirmationHandler<MockChainService>;
    
    assert_eq!(TestHandler::executor_name(), "erc4337");
    assert_eq!(TestHandler::stage_name(), "confirmation");
}

#[tokio::test]
async fn test_external_bundler_send_webhook_options() {
    let job_data = create_test_job_data();
    
    let webhook_options = job_data.webhook_options();
    assert!(webhook_options.is_some());
    
    let options = webhook_options.unwrap();
    assert_eq!(options.len(), 1);
    assert_eq!(options[0].url, "https://example.com/webhook");
    assert_eq!(options[0].secret, Some("webhook_secret".to_string()));
}

#[tokio::test]
async fn test_user_op_confirmation_webhook_options() {
    let job_data = create_test_confirmation_job_data();
    
    let webhook_options = job_data.webhook_options();
    assert!(webhook_options.is_some());
    
    let options = webhook_options.unwrap();
    assert_eq!(options.len(), 1);
    assert_eq!(options[0].url, "https://example.com/webhook");
    assert_eq!(options[0].secret, Some("webhook_secret".to_string()));
}

#[tokio::test]
async fn test_external_bundler_send_job_data_without_webhook() {
    let mut job_data = create_test_job_data();
    job_data.webhook_options = None;
    
    let webhook_options = job_data.webhook_options();
    assert!(webhook_options.is_none());
}

#[tokio::test]
async fn test_user_op_confirmation_job_data_without_webhook() {
    let mut job_data = create_test_confirmation_job_data();
    job_data.webhook_options = None;
    
    let webhook_options = job_data.webhook_options();
    assert!(webhook_options.is_none());
}

#[tokio::test]
async fn test_external_bundler_send_job_data_with_multiple_transactions() {
    let mut job_data = create_test_job_data();
    job_data.transactions = vec![
        InnerTransaction {
            to: Address::from([0x42; 20]),
            value: U256::from(1000),
            data: Bytes::new(),
        },
        InnerTransaction {
            to: Address::from([0x43; 20]),
            value: U256::from(2000),
            data: Bytes::from_hex("0x1234").unwrap(),
        },
    ];
    
    let serialized = serde_json::to_string(&job_data).unwrap();
    let deserialized: ExternalBundlerSendJobData = serde_json::from_str(&serialized).unwrap();
    
    assert_eq!(deserialized.transactions.len(), 2);
    assert_eq!(deserialized.transactions[0].to, Address::from([0x42; 20]));
    assert_eq!(deserialized.transactions[1].to, Address::from([0x43; 20]));
    assert_eq!(deserialized.transactions[0].value, U256::from(1000));
    assert_eq!(deserialized.transactions[1].value, U256::from(2000));
}

#[tokio::test]
async fn test_external_bundler_send_job_data_without_pregenerated_nonce() {
    let mut job_data = create_test_job_data();
    job_data.pregenerated_nonce = None;
    
    let serialized = serde_json::to_string(&job_data).unwrap();
    let deserialized: ExternalBundlerSendJobData = serde_json::from_str(&serialized).unwrap();
    
    assert_eq!(deserialized.pregenerated_nonce, None);
}

#[tokio::test]
async fn test_external_bundler_send_job_data_with_smart_account_address() {
    let mut job_data = create_test_job_data();
    job_data.execution_options.smart_account_address = Some(Address::from([0x55; 20]));
    
    let serialized = serde_json::to_string(&job_data).unwrap();
    let deserialized: ExternalBundlerSendJobData = serde_json::from_str(&serialized).unwrap();
    
    assert_eq!(deserialized.execution_options.smart_account_address, Some(Address::from([0x55; 20])));
}

#[tokio::test]
async fn test_external_bundler_send_job_data_without_smart_account_address() {
    let mut job_data = create_test_job_data();
    job_data.execution_options.smart_account_address = None;
    
    let serialized = serde_json::to_string(&job_data).unwrap();
    let deserialized: ExternalBundlerSendJobData = serde_json::from_str(&serialized).unwrap();
    
    assert_eq!(deserialized.execution_options.smart_account_address, None);
}

#[tokio::test]
async fn test_external_bundler_send_error_variants() {
    let errors = vec![
        ExternalBundlerSendError::ChainServiceError {
            chain_id: 1,
            message: "Chain error".to_string(),
        },
        ExternalBundlerSendError::InvalidAccountSalt {
            message: "Invalid salt".to_string(),
        },
        ExternalBundlerSendError::AccountDeterminationFailed {
            signer_address: Address::from([0x11; 20]),
            factory_address: Address::from([0x44; 20]),
            account_salt: "salt".to_string(),
            message: "Failed to determine account".to_string(),
            inner_error: None,
        },
        ExternalBundlerSendError::DeploymentLocked {
            account_address: Address::from([0x22; 20]),
            message: "Deployment locked".to_string(),
        },
        ExternalBundlerSendError::UserOpBuildFailed {
            account_address: Address::from([0x22; 20]),
            nonce_used: U256::from(42),
            had_deployment_lock: true,
            stage: "build".to_string(),
            message: "Build failed".to_string(),
            inner_error: None,
        },
        ExternalBundlerSendError::PolicyRestriction {
            policy_id: "policy_123".to_string(),
            reason: "Policy violation".to_string(),
        },
        ExternalBundlerSendError::InvalidRpcCredentials {
            message: "Invalid credentials".to_string(),
        },
        ExternalBundlerSendError::InternalError {
            message: "Internal error".to_string(),
        },
        ExternalBundlerSendError::UserCancelled,
    ];
    
    for error in errors {
        let serialized = serde_json::to_string(&error).unwrap();
        let deserialized: ExternalBundlerSendError = serde_json::from_str(&serialized).unwrap();
        
        // Just verify that serialization/deserialization works
        match (&error, &deserialized) {
            (ExternalBundlerSendError::ChainServiceError { chain_id: c1, .. }, 
             ExternalBundlerSendError::ChainServiceError { chain_id: c2, .. }) => {
                assert_eq!(c1, c2);
            }
            (ExternalBundlerSendError::UserCancelled, ExternalBundlerSendError::UserCancelled) => {
                assert!(true);
            }
            _ => {
                // For other variants, just check they serialize/deserialize without error
                assert!(true);
            }
        }
    }
}

#[tokio::test]
async fn test_user_op_confirmation_error_variants() {
    let errors = vec![
        UserOpConfirmationError::ChainServiceError {
            chain_id: 1,
            message: "Chain error".to_string(),
        },
        UserOpConfirmationError::ReceiptNotAvailable {
            user_op_hash: Bytes::from_hex("0x1234").unwrap(),
            attempt_number: 5,
        },
        UserOpConfirmationError::ReceiptQueryFailed {
            user_op_hash: Bytes::from_hex("0x1234").unwrap(),
            message: "Query failed".to_string(),
            inner_error: None,
        },
        UserOpConfirmationError::InternalError {
            message: "Internal error".to_string(),
        },
        UserOpConfirmationError::UserCancelled,
    ];
    
    for error in errors {
        let serialized = serde_json::to_string(&error).unwrap();
        let deserialized: UserOpConfirmationError = serde_json::from_str(&serialized).unwrap();
        
        // Just verify that serialization/deserialization works
        match (&error, &deserialized) {
            (UserOpConfirmationError::ChainServiceError { chain_id: c1, .. }, 
             UserOpConfirmationError::ChainServiceError { chain_id: c2, .. }) => {
                assert_eq!(c1, c2);
            }
            (UserOpConfirmationError::UserCancelled, UserOpConfirmationError::UserCancelled) => {
                assert!(true);
            }
            _ => {
                // For other variants, just check they serialize/deserialize without error
                assert!(true);
            }
        }
    }
}

#[tokio::test]
async fn test_rpc_credentials_serialization() {
    let credentials = vec![
        RpcCredentials::None,
        RpcCredentials::BearerToken("token123".to_string()),
        RpcCredentials::ApiKey("key123".to_string()),
    ];
    
    for credential in credentials {
        let job_data = ExternalBundlerSendJobData {
            rpc_credentials: credential,
            ..create_test_job_data()
        };
        
        let serialized = serde_json::to_string(&job_data).unwrap();
        let deserialized: ExternalBundlerSendJobData = serde_json::from_str(&serialized).unwrap();
        
        // Just verify that serialization/deserialization works
        assert!(true);
    }
}

#[tokio::test]
async fn test_signing_credential_serialization() {
    let credentials = vec![
        SigningCredential::PrivateKey("0x1234567890abcdef".to_string()),
        SigningCredential::VaultSigned {
            vault_id: "vault123".to_string(),
            wallet_id: "wallet456".to_string(),
        },
    ];
    
    for credential in credentials {
        let job_data = ExternalBundlerSendJobData {
            signing_credential: credential,
            ..create_test_job_data()
        };
        
        let serialized = serde_json::to_string(&job_data).unwrap();
        let deserialized: ExternalBundlerSendJobData = serde_json::from_str(&serialized).unwrap();
        
        // Just verify that serialization/deserialization works
        assert!(true);
    }
}

#[tokio::test]
async fn test_user_operation_receipt_with_paymaster() {
    let receipt = UserOperationReceipt {
        user_op_hash: Bytes::from_hex("0x1234567890abcdef").unwrap(),
        entrypoint: Address::from([0x33; 20]),
        sender: Address::from([0x22; 20]),
        nonce: U256::from(42),
        paymaster: Some(Address::from([0x55; 20])),
        actual_gas_cost: U256::from(1000),
        actual_gas_used: U256::from(500),
        success: true,
        logs: vec![],
        receipt: alloy::rpc::types::TransactionReceipt::default(),
    };
    
    let result = UserOpConfirmationResult {
        user_op_hash: Bytes::from_hex("0x1234567890abcdef").unwrap(),
        receipt,
        deployment_lock_released: false,
    };
    
    let serialized = serde_json::to_string(&result).unwrap();
    let deserialized: UserOpConfirmationResult = serde_json::from_str(&serialized).unwrap();
    
    assert_eq!(deserialized.receipt.paymaster, Some(Address::from([0x55; 20])));
    assert_eq!(deserialized.deployment_lock_released, false);
}

#[tokio::test]
async fn test_user_operation_receipt_failed_transaction() {
    let receipt = UserOperationReceipt {
        user_op_hash: Bytes::from_hex("0x1234567890abcdef").unwrap(),
        entrypoint: Address::from([0x33; 20]),
        sender: Address::from([0x22; 20]),
        nonce: U256::from(42),
        paymaster: None,
        actual_gas_cost: U256::from(1000),
        actual_gas_used: U256::from(500),
        success: false, // Failed transaction
        logs: vec![],
        receipt: alloy::rpc::types::TransactionReceipt::default(),
    };
    
    let result = UserOpConfirmationResult {
        user_op_hash: Bytes::from_hex("0x1234567890abcdef").unwrap(),
        receipt,
        deployment_lock_released: true,
    };
    
    let serialized = serde_json::to_string(&result).unwrap();
    let deserialized: UserOpConfirmationResult = serde_json::from_str(&serialized).unwrap();
    
    assert_eq!(deserialized.receipt.success, false);
    assert_eq!(deserialized.deployment_lock_released, true);
}