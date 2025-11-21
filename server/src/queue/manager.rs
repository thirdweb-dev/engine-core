// src/queue/manager.rs
use std::{sync::Arc, time::Duration};

use alloy::transports::http::reqwest;
use engine_core::credentials::KmsClientCache;
use engine_core::error::EngineError;
use engine_executors::{
    eip7702_executor::{confirm::Eip7702ConfirmationHandler, send::Eip7702SendHandler},
    eoa::{EoaExecutorJobHandler, authorization_cache::EoaAuthorizationCache},
    external_bundler::{
        confirm::UserOpConfirmationHandler,
        deployment::{RedisDeploymentCache, RedisDeploymentLock},
        send::ExternalBundlerSendHandler,
    },
    solana_executor::{
        rpc_cache::SolanaRpcCache, storage::SolanaTransactionStorage,
        worker::SolanaExecutorJobHandler,
    },
    transaction_registry::TransactionRegistry,
    webhook::{WebhookJobHandler, WebhookRetryConfig},
};
use twmq::{Queue, queue::QueueOptions, shutdown::ShutdownHandle};

use crate::{chains::ThirdwebChainService, config::QueueConfig};

pub struct QueueManager {
    pub webhook_queue: Arc<Queue<WebhookJobHandler>>,
    pub external_bundler_send_queue: Arc<Queue<ExternalBundlerSendHandler<ThirdwebChainService>>>,
    pub userop_confirm_queue: Arc<Queue<UserOpConfirmationHandler<ThirdwebChainService>>>,
    pub eoa_executor_queue: Arc<Queue<EoaExecutorJobHandler<ThirdwebChainService>>>,
    pub eip7702_send_queue: Arc<Queue<Eip7702SendHandler<ThirdwebChainService>>>,
    pub eip7702_confirm_queue: Arc<Queue<Eip7702ConfirmationHandler<ThirdwebChainService>>>,
    pub solana_executor_queue: Arc<Queue<SolanaExecutorJobHandler>>,
    pub transaction_registry: Arc<TransactionRegistry>,
}

fn get_queue_name_for_namespace(namespace: &Option<String>, name: &str) -> String {
    match namespace {
        Some(namespace) => format!("{namespace}_{name}"),
        None => name.to_owned(),
    }
}

const EXTERNAL_BUNDLER_SEND_QUEUE_NAME: &str = "external_bundler_send";
const USEROP_CONFIRM_QUEUE_NAME: &str = "userop_confirm";
const EIP7702_SEND_QUEUE_NAME: &str = "eip7702_send";
const EIP7702_CONFIRM_QUEUE_NAME: &str = "eip7702_confirm";
const WEBHOOK_QUEUE_NAME: &str = "webhook";
const EOA_EXECUTOR_QUEUE_NAME: &str = "eoa_executor";

impl QueueManager {
    pub async fn new(
        redis_client: twmq::redis::Client,
        queue_config: &QueueConfig,
        solana_config: &crate::config::SolanaConfig,
        chain_service: Arc<ThirdwebChainService>,
        userop_signer: Arc<engine_core::userop::UserOpSigner>,
        eoa_signer: Arc<engine_core::signer::EoaSigner>,
        authorization_cache: EoaAuthorizationCache,
        kms_client_cache: KmsClientCache,
    ) -> Result<Self, EngineError> {
        // Create transaction registry
        let transaction_registry = Arc::new(TransactionRegistry::new(
            redis_client.get_connection_manager().await?,
            queue_config.execution_namespace.clone(),
        ));

        // Create deployment cache and lock
        let deployment_cache = RedisDeploymentCache::new(redis_client.clone()).await?;
        let deployment_lock = RedisDeploymentLock::new(redis_client.clone()).await?;

        // Create queue options
        let base_queue_opts = QueueOptions {
            local_concurrency: queue_config.local_concurrency,
            polling_interval: Duration::from_millis(queue_config.polling_interval_ms),
            lease_duration: Duration::from_secs(queue_config.lease_duration_seconds),
            idempotency_mode: twmq::IdempotencyMode::Permanent,
            always_poll: false,
            max_success: 1000,
            max_failed: 1000,
        };

        let mut external_bundler_send_queue_opts = base_queue_opts.clone();
        external_bundler_send_queue_opts.local_concurrency =
            queue_config.external_bundler_send_workers;

        let mut userop_confirm_queue_opts = base_queue_opts.clone();
        userop_confirm_queue_opts.local_concurrency = queue_config.userop_confirm_workers;

        let mut eip7702_send_queue_opts = base_queue_opts.clone();
        eip7702_send_queue_opts.local_concurrency = queue_config.external_bundler_send_workers; // Reuse same config for now

        let mut eip7702_confirm_queue_opts = base_queue_opts.clone();
        eip7702_confirm_queue_opts.local_concurrency = queue_config.userop_confirm_workers; // Reuse same config for now

        let mut webhook_queue_opts = base_queue_opts.clone();
        webhook_queue_opts.local_concurrency = queue_config.webhook_workers;

        let mut eoa_executor_queue_opts = base_queue_opts.clone();
        eoa_executor_queue_opts.idempotency_mode = twmq::IdempotencyMode::Active;
        eoa_executor_queue_opts.local_concurrency = queue_config.eoa_executor_workers;

        // Create webhook queue
        let webhook_handler = WebhookJobHandler {
            // Specifc HTTP client config for high concurrency webhook handling
            http_client: reqwest::Client::builder()
                .pool_max_idle_per_host(50) // Allow more connections per host
                .pool_idle_timeout(Duration::from_secs(30)) // Shorter idle timeout
                .timeout(Duration::from_secs(10)) // Much shorter request timeout
                .connect_timeout(Duration::from_secs(5)) // Quick connection timeout
                .tcp_keepalive(Duration::from_secs(60)) // Keep connections alive
                .tcp_nodelay(true) // Reduce latency
                .build()
                .expect("Failed to create HTTP client"),
            retry_config: Arc::new(WebhookRetryConfig::default()),
        };

        let webhook_queue_name =
            get_queue_name_for_namespace(&queue_config.execution_namespace, WEBHOOK_QUEUE_NAME);

        let external_bundler_send_queue_name = get_queue_name_for_namespace(
            &queue_config.execution_namespace,
            EXTERNAL_BUNDLER_SEND_QUEUE_NAME,
        );

        let userop_confirm_queue_name = get_queue_name_for_namespace(
            &queue_config.execution_namespace,
            USEROP_CONFIRM_QUEUE_NAME,
        );

        let eip7702_send_queue_name = get_queue_name_for_namespace(
            &queue_config.execution_namespace,
            EIP7702_SEND_QUEUE_NAME,
        );

        let eip7702_confirm_queue_name = get_queue_name_for_namespace(
            &queue_config.execution_namespace,
            EIP7702_CONFIRM_QUEUE_NAME,
        );

        let eoa_executor_queue_name = get_queue_name_for_namespace(
            &queue_config.execution_namespace,
            EOA_EXECUTOR_QUEUE_NAME,
        );

        let webhook_queue = Queue::builder()
            .name(webhook_queue_name)
            .options(webhook_queue_opts)
            .handler(webhook_handler)
            .redis_client(redis_client.clone())
            .build()
            .await?
            .arc();

        // Create confirmation queues first (needed by send queues)
        let confirm_handler = UserOpConfirmationHandler::new(
            chain_service.clone(),
            deployment_lock.clone(),
            webhook_queue.clone(),
            transaction_registry.clone(),
        );

        let userop_confirm_queue = Queue::builder()
            .name(userop_confirm_queue_name)
            .options(userop_confirm_queue_opts)
            .handler(confirm_handler)
            .redis_client(redis_client.clone())
            .build()
            .await?
            .arc();

        // Create EIP-7702 confirmation queue
        let eip7702_confirm_handler = Eip7702ConfirmationHandler {
            chain_service: chain_service.clone(),
            webhook_queue: webhook_queue.clone(),
            transaction_registry: transaction_registry.clone(),
        };

        let eip7702_confirm_queue = Queue::builder()
            .name(eip7702_confirm_queue_name)
            .options(eip7702_confirm_queue_opts)
            .handler(eip7702_confirm_handler)
            .redis_client(redis_client.clone())
            .build()
            .await?
            .arc();

        let solana_signer = Arc::new(engine_core::signer::SolanaSigner::new(
            userop_signer.vault_client.clone(),
            userop_signer.iaw_client.clone(),
        ));

        // Create send queues
        let send_handler = ExternalBundlerSendHandler {
            chain_service: chain_service.clone(),
            userop_signer,
            deployment_cache,
            deployment_lock,
            webhook_queue: webhook_queue.clone(),
            confirm_queue: userop_confirm_queue.clone(),
            transaction_registry: transaction_registry.clone(),
        };

        let external_bundler_send_queue = Queue::builder()
            .name(external_bundler_send_queue_name)
            .options(external_bundler_send_queue_opts)
            .handler(send_handler)
            .redis_client(redis_client.clone())
            .build()
            .await?
            .arc();

        // Create delegation contract cache for EIP-7702
        let delegation_contract_cache =
            engine_executors::eip7702_executor::delegation_cache::DelegationContractCache::new(
                moka::future::Cache::builder()
                    .max_capacity(10000) // Large capacity since it's a single entry per chain
                    .time_to_live(Duration::from_secs(24 * 60 * 60)) // 24 hours as requested
                    .time_to_idle(Duration::from_secs(24 * 60 * 60)) // Also 24 hours for TTI
                    .build(),
            );

        // Create EIP-7702 send queue
        let eip7702_send_handler = Eip7702SendHandler {
            chain_service: chain_service.clone(),
            eoa_signer: eoa_signer.clone(),
            webhook_queue: webhook_queue.clone(),
            confirm_queue: eip7702_confirm_queue.clone(),
            transaction_registry: transaction_registry.clone(),
            delegation_contract_cache,
        };

        let eip7702_send_queue = Queue::builder()
            .name(eip7702_send_queue_name)
            .options(eip7702_send_queue_opts)
            .handler(eip7702_send_handler)
            .redis_client(redis_client.clone())
            .build()
            .await?
            .arc();

        // Create EOA executor queue
        let eoa_metrics = engine_executors::metrics::EoaMetrics::new(
            queue_config
                .monitoring
                .eoa_send_degradation_threshold_seconds,
            queue_config
                .monitoring
                .eoa_confirmation_degradation_threshold_seconds,
            queue_config.monitoring.eoa_stuck_threshold_seconds,
        );

        let eoa_executor_handler = EoaExecutorJobHandler {
            chain_service: chain_service.clone(),
            eoa_signer: eoa_signer.clone(),
            webhook_queue: webhook_queue.clone(),
            namespace: queue_config.execution_namespace.clone(),
            redis: redis_client.get_connection_manager().await?,
            authorization_cache,
            max_inflight: 100,
            max_recycled_nonces: 50,
            eoa_metrics,
            kms_client_cache,
        };

        let eoa_executor_queue = Queue::builder()
            .name(eoa_executor_queue_name)
            .options(eoa_executor_queue_opts)
            .handler(eoa_executor_handler)
            .redis_client(redis_client.clone())
            .build()
            .await?
            .arc();

        // Create Solana executor queue
        let solana_executor_queue_name =
            get_queue_name_for_namespace(&queue_config.execution_namespace, "solana_executor");
        let mut solana_executor_queue_opts = base_queue_opts.clone();
        solana_executor_queue_opts.local_concurrency = queue_config.solana_executor_workers;

        let solana_rpc_urls = engine_executors::solana_executor::rpc_cache::SolanaRpcUrls {
            devnet: solana_config.devnet.http_url.clone(),
            mainnet: solana_config.mainnet.http_url.clone(),
            local: solana_config.local.http_url.clone(),
        };
        let solana_rpc_cache = Arc::new(SolanaRpcCache::new(solana_rpc_urls));
        let solana_storage = SolanaTransactionStorage::new(
            redis_client.get_connection_manager().await?,
            queue_config.execution_namespace.clone(),
        );
        let solana_executor_handler = SolanaExecutorJobHandler {
            solana_signer,
            rpc_cache: solana_rpc_cache,
            storage: Arc::new(solana_storage),
            webhook_queue: webhook_queue.clone(),
            transaction_registry: transaction_registry.clone(),
        };

        let solana_executor_queue = Queue::builder()
            .name(solana_executor_queue_name)
            .options(solana_executor_queue_opts)
            .handler(solana_executor_handler)
            .redis_client(redis_client.clone())
            .build()
            .await?
            .arc();

        Ok(Self {
            webhook_queue,
            external_bundler_send_queue,
            userop_confirm_queue,
            eoa_executor_queue,
            eip7702_send_queue,
            eip7702_confirm_queue,
            solana_executor_queue,
            transaction_registry,
        })
    }

    /// Start all workers
    pub fn start_workers(&self, queue_config: &QueueConfig) -> ShutdownHandle {
        tracing::info!("Starting queue workers...");

        // Start webhook workers
        tracing::info!("Starting webhook worker");
        let webhook_worker = self.webhook_queue.work();

        // Start ERC-4337 send workers
        tracing::info!("Starting external bundler send worker");
        let external_bundler_send_worker = self.external_bundler_send_queue.work();

        // Start ERC-4337 confirmation workers
        tracing::info!("Starting external bundler confirmation worker");
        let userop_confirm_worker = self.userop_confirm_queue.work();

        // Start EOA executor workers
        tracing::info!("Starting EOA executor worker");
        let eoa_executor_worker = self.eoa_executor_queue.work();

        // Start EIP-7702 send workers
        tracing::info!("Starting EIP-7702 send worker");
        let eip7702_send_worker = self.eip7702_send_queue.work();

        // Start EIP-7702 confirmation workers
        tracing::info!("Starting EIP-7702 confirmation worker");
        let eip7702_confirm_worker = self.eip7702_confirm_queue.work();

        // Start Solana executor workers
        tracing::info!("Starting Solana executor worker");
        let solana_executor_worker = self.solana_executor_queue.work();

        tracing::info!(
            "Started {} webhook workers, {} send workers, {} confirm workers, {} eoa workers, {} EIP-7702 send workers, {} EIP-7702 confirm workers, {} solana workers",
            queue_config.webhook_workers,
            queue_config.external_bundler_send_workers,
            queue_config.userop_confirm_workers,
            queue_config.eoa_executor_workers,
            queue_config.external_bundler_send_workers, // Reusing same config for now
            queue_config.userop_confirm_workers,        // Reusing same config for now
            queue_config.solana_executor_workers,
        );

        ShutdownHandle::with_worker(webhook_worker)
            .and_worker(external_bundler_send_worker)
            .and_worker(userop_confirm_worker)
            .and_worker(eoa_executor_worker)
            .and_worker(eip7702_send_worker)
            .and_worker(eip7702_confirm_worker)
            .and_worker(solana_executor_worker)
    }

    /// Get queue statistics for monitoring
    pub async fn get_stats(&self) -> Result<QueueStats, Box<dyn std::error::Error + Send + Sync>> {
        use twmq::job::JobStatus;

        let webhook_stats = QueueStatistics {
            pending: self.webhook_queue.count(JobStatus::Pending).await?,
            active: self.webhook_queue.count(JobStatus::Active).await?,
            delayed: self.webhook_queue.count(JobStatus::Delayed).await?,
            success: self.webhook_queue.count(JobStatus::Success).await?,
            failed: self.webhook_queue.count(JobStatus::Failed).await?,
        };

        let send_stats = QueueStatistics {
            pending: self
                .external_bundler_send_queue
                .count(JobStatus::Pending)
                .await?,
            active: self
                .external_bundler_send_queue
                .count(JobStatus::Active)
                .await?,
            delayed: self
                .external_bundler_send_queue
                .count(JobStatus::Delayed)
                .await?,
            success: self
                .external_bundler_send_queue
                .count(JobStatus::Success)
                .await?,
            failed: self
                .external_bundler_send_queue
                .count(JobStatus::Failed)
                .await?,
        };

        let confirm_stats = QueueStatistics {
            pending: self.userop_confirm_queue.count(JobStatus::Pending).await?,
            active: self.userop_confirm_queue.count(JobStatus::Active).await?,
            delayed: self.userop_confirm_queue.count(JobStatus::Delayed).await?,
            success: self.userop_confirm_queue.count(JobStatus::Success).await?,
            failed: self.userop_confirm_queue.count(JobStatus::Failed).await?,
        };

        let eoa_executor_stats = QueueStatistics {
            pending: self.eoa_executor_queue.count(JobStatus::Pending).await?,
            active: self.eoa_executor_queue.count(JobStatus::Active).await?,
            delayed: self.eoa_executor_queue.count(JobStatus::Delayed).await?,
            success: self.eoa_executor_queue.count(JobStatus::Success).await?,
            failed: self.eoa_executor_queue.count(JobStatus::Failed).await?,
        };

        let eip7702_send_stats = QueueStatistics {
            pending: self.eip7702_send_queue.count(JobStatus::Pending).await?,
            active: self.eip7702_send_queue.count(JobStatus::Active).await?,
            delayed: self.eip7702_send_queue.count(JobStatus::Delayed).await?,
            success: self.eip7702_send_queue.count(JobStatus::Success).await?,
            failed: self.eip7702_send_queue.count(JobStatus::Failed).await?,
        };

        let eip7702_confirm_stats = QueueStatistics {
            pending: self.eip7702_confirm_queue.count(JobStatus::Pending).await?,
            active: self.eip7702_confirm_queue.count(JobStatus::Active).await?,
            delayed: self.eip7702_confirm_queue.count(JobStatus::Delayed).await?,
            success: self.eip7702_confirm_queue.count(JobStatus::Success).await?,
            failed: self.eip7702_confirm_queue.count(JobStatus::Failed).await?,
        };

        Ok(QueueStats {
            webhook: webhook_stats,
            external_bundler_send: send_stats,
            userop_confirm: confirm_stats,
            eoa_executor: eoa_executor_stats,
            eip7702_send: eip7702_send_stats,
            eip7702_confirm: eip7702_confirm_stats,
        })
    }
}

#[derive(Debug, serde::Serialize)]
pub struct QueueStats {
    pub webhook: QueueStatistics,
    pub external_bundler_send: QueueStatistics,
    pub userop_confirm: QueueStatistics,
    pub eoa_executor: QueueStatistics,
    pub eip7702_send: QueueStatistics,
    pub eip7702_confirm: QueueStatistics,
}

#[derive(Debug, serde::Serialize)]
pub struct QueueStatistics {
    pub pending: usize,
    pub active: usize,
    pub delayed: usize,
    pub success: usize,
    pub failed: usize,
}
