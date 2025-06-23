// src/queue/manager.rs
use std::{sync::Arc, time::Duration};

use alloy::transports::http::reqwest;
use engine_core::error::EngineError;
use engine_executors::{
    external_bundler::{
        confirm::UserOpConfirmationHandler,
        deployment::{RedisDeploymentCache, RedisDeploymentLock},
        send::ExternalBundlerSendHandler,
    },
    webhook::{WebhookJobHandler, WebhookRetryConfig},
    transaction_registry::TransactionRegistry,
};
use twmq::{Queue, queue::QueueOptions, shutdown::ShutdownHandle};

use crate::{
    chains::ThirdwebChainService,
    config::{QueueConfig, RedisConfig},
};

pub struct QueueManager {
    pub webhook_queue: Arc<Queue<WebhookJobHandler>>,
    pub external_bundler_send_queue: Arc<Queue<ExternalBundlerSendHandler<ThirdwebChainService>>>,
    pub userop_confirm_queue: Arc<Queue<UserOpConfirmationHandler<ThirdwebChainService>>>,
    pub transaction_registry: Arc<TransactionRegistry>,
}

fn get_queue_name_for_namespace(namespace: &Option<String>, name: &str) -> String {
    match namespace {
        Some(namespace) => format!("{}_{}", namespace, name),
        None => name.to_owned(),
    }
}

const EXTERNAL_BUNDLER_SEND_QUEUE_NAME: &str = "external_bundler_send";
const USEROP_CONFIRM_QUEUE_NAME: &str = "userop_confirm";
const WEBHOOK_QUEUE_NAME: &str = "webhook";

impl QueueManager {
    pub async fn new(
        redis_config: &RedisConfig,
        queue_config: &QueueConfig,
        chain_service: Arc<ThirdwebChainService>,
        userop_signer: Arc<engine_core::userop::UserOpSigner>,
    ) -> Result<Self, EngineError> {
        // Create Redis clients
        let redis_client = twmq::redis::Client::open(redis_config.url.as_str())?;
        
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
            always_poll: false,
            max_success: 1000,
            max_failed: 1000,
        };

        let mut external_bundler_send_queue_opts = base_queue_opts.clone();
        external_bundler_send_queue_opts.local_concurrency =
            queue_config.external_bundler_send_workers;

        let mut userop_confirm_queue_opts = base_queue_opts.clone();
        userop_confirm_queue_opts.local_concurrency = queue_config.userop_confirm_workers;

        let mut webhook_queue_opts = base_queue_opts.clone();
        webhook_queue_opts.local_concurrency = queue_config.webhook_workers;

        // Create webhook queue
        let webhook_handler = WebhookJobHandler {
            http_client: reqwest::Client::new(),
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

        let webhook_queue = Queue::builder()
            .name(webhook_queue_name)
            .options(webhook_queue_opts)
            .handler(webhook_handler)
            .redis_client(redis_client.clone())
            .build()
            .await?
            .arc();

        // Create confirmation queue first (needed by send queue)
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

        // Create send queue
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

        Ok(Self {
            webhook_queue,
            external_bundler_send_queue,
            userop_confirm_queue,
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

        tracing::info!(
            "Started {} webhook workers, {} send workers, {} confirm workers",
            queue_config.webhook_workers,
            queue_config.external_bundler_send_workers,
            queue_config.userop_confirm_workers
        );

        ShutdownHandle::with_worker(webhook_worker)
            .and_worker(external_bundler_send_worker)
            .and_worker(userop_confirm_worker)
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

        Ok(QueueStats {
            webhook: webhook_stats,
            external_bundler_send: send_stats,
            userop_confirm: confirm_stats,
        })
    }
}

#[derive(Debug, serde::Serialize)]
pub struct QueueStats {
    pub webhook: QueueStatistics,
    pub external_bundler_send: QueueStatistics,
    pub userop_confirm: QueueStatistics,
}

#[derive(Debug, serde::Serialize)]
pub struct QueueStatistics {
    pub pending: usize,
    pub active: usize,
    pub delayed: usize,
    pub success: usize,
    pub failed: usize,
}
