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
};
use twmq::{Queue, queue::QueueOptions};

use crate::{
    chains::ThirdwebChainService,
    config::{QueueConfig, RedisConfig},
};

pub struct QueueManager {
    pub webhook_queue: Arc<Queue<WebhookJobHandler>>,
    pub erc4337_send_queue: Arc<Queue<ExternalBundlerSendHandler<ThirdwebChainService>>>,
    pub erc4337_confirm_queue: Arc<Queue<UserOpConfirmationHandler<ThirdwebChainService>>>,
}

impl QueueManager {
    pub async fn new(
        redis_config: &RedisConfig,
        queue_config: &QueueConfig,
        chain_service: Arc<ThirdwebChainService>,
        userop_signer: Arc<engine_core::userop::UserOpSigner>,
    ) -> Result<Self, EngineError> {
        // Create Redis clients
        let redis_client = twmq::redis::Client::open(redis_config.url.as_str())?;

        // Create deployment cache and lock
        let deployment_cache = RedisDeploymentCache::new(redis_client.clone()).await?;
        let deployment_lock = RedisDeploymentLock::new(redis_client.clone()).await?;

        // Create queue options
        let queue_opts = QueueOptions {
            local_concurrency: queue_config.local_concurrency,
            polling_interval: Duration::from_millis(queue_config.polling_interval_ms),
            lease_duration: Duration::from_secs(queue_config.lease_duration_seconds),
            always_poll: false,
            max_success: 1000,
            max_failed: 1000,
        };

        // Create webhook queue
        let webhook_handler = WebhookJobHandler {
            http_client: reqwest::Client::new(),
            retry_config: Arc::new(WebhookRetryConfig::default()),
        };

        let webhook_queue = Arc::new(
            Queue::new(
                &redis_config.url,
                "webhook",
                Some(queue_opts.clone()),
                webhook_handler,
            )
            .await?,
        );

        // Create confirmation queue first (needed by send queue)
        let confirm_handler = UserOpConfirmationHandler::new(
            chain_service.clone(),
            deployment_lock.clone(),
            webhook_queue.clone(),
        );

        let erc4337_confirm_queue = Arc::new(
            Queue::new(
                &redis_config.url,
                "erc4337_confirm",
                Some(queue_opts.clone()),
                confirm_handler,
            )
            .await?,
        );

        // Create send queue
        let send_handler = ExternalBundlerSendHandler {
            chain_service: chain_service.clone(),
            userop_signer,
            deployment_cache,
            deployment_lock,
            webhook_queue: webhook_queue.clone(),
            confirm_queue: erc4337_confirm_queue.clone(),
        };

        let erc4337_send_queue = Arc::new(
            Queue::new(
                &redis_config.url,
                "erc4337_send",
                Some(queue_opts),
                send_handler,
            )
            .await?,
        );

        Ok(Self {
            webhook_queue,
            erc4337_send_queue,
            erc4337_confirm_queue,
        })
    }

    /// Start all workers
    pub async fn start_workers(&self, queue_config: &QueueConfig) -> Result<(), EngineError> {
        tracing::info!("Starting queue workers...");

        // Start webhook workers
        for i in 0..queue_config.webhook_workers {
            let queue = self.webhook_queue.clone();
            tokio::spawn(async move {
                tracing::info!("Starting webhook worker {}", i);
                if let Err(e) = queue.work().await {
                    tracing::error!("Webhook worker {} failed: {}", i, e);
                }
            });
        }

        // Start ERC-4337 send workers
        for i in 0..queue_config.erc4337_send_workers {
            let queue = self.erc4337_send_queue.clone();
            tokio::spawn(async move {
                tracing::info!("Starting ERC-4337 send worker {}", i);
                if let Err(e) = queue.work().await {
                    tracing::error!("ERC-4337 send worker {} failed: {}", i, e);
                }
            });
        }

        // Start ERC-4337 confirmation workers
        for i in 0..queue_config.erc4337_confirm_workers {
            let queue = self.erc4337_confirm_queue.clone();
            tokio::spawn(async move {
                tracing::info!("Starting ERC-4337 confirm worker {}", i);
                if let Err(e) = queue.work().await {
                    tracing::error!("ERC-4337 confirm worker {} failed: {}", i, e);
                }
            });
        }

        tracing::info!(
            "Started {} webhook workers, {} send workers, {} confirm workers",
            queue_config.webhook_workers,
            queue_config.erc4337_send_workers,
            queue_config.erc4337_confirm_workers
        );

        Ok(())
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
            pending: self.erc4337_send_queue.count(JobStatus::Pending).await?,
            active: self.erc4337_send_queue.count(JobStatus::Active).await?,
            delayed: self.erc4337_send_queue.count(JobStatus::Delayed).await?,
            success: self.erc4337_send_queue.count(JobStatus::Success).await?,
            failed: self.erc4337_send_queue.count(JobStatus::Failed).await?,
        };

        let confirm_stats = QueueStatistics {
            pending: self.erc4337_confirm_queue.count(JobStatus::Pending).await?,
            active: self.erc4337_confirm_queue.count(JobStatus::Active).await?,
            delayed: self.erc4337_confirm_queue.count(JobStatus::Delayed).await?,
            success: self.erc4337_confirm_queue.count(JobStatus::Success).await?,
            failed: self.erc4337_confirm_queue.count(JobStatus::Failed).await?,
        };

        Ok(QueueStats {
            webhook: webhook_stats,
            erc4337_send: send_stats,
            erc4337_confirm: confirm_stats,
        })
    }
}

#[derive(Debug, serde::Serialize)]
pub struct QueueStats {
    pub webhook: QueueStatistics,
    pub erc4337_send: QueueStatistics,
    pub erc4337_confirm: QueueStatistics,
}

#[derive(Debug, serde::Serialize)]
pub struct QueueStatistics {
    pub pending: usize,
    pub active: usize,
    pub delayed: usize,
    pub success: usize,
    pub failed: usize,
}
