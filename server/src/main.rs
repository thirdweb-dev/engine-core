use std::sync::Arc;

use engine_core::{signer::EoaSigner, userop::UserOpSigner};
use thirdweb_core::{abi::ThirdwebAbiServiceBuilder, auth::ThirdwebAuth, iaw::IAWClient};
use thirdweb_engine::{
    chains::ThirdwebChainService,
    config,
    execution_router::ExecutionRouter,
    http::server::{EngineServer, EngineServerState},
    queue::manager::QueueManager,
};
use tracing_subscriber::{filter::EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = config::get_config();

    let subscriber = tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            // Default to info level if RUST_LOG environment variable is not set
            "thirdweb_engine=debug,tower_http=debug,axum=debug,twmq=debug,engine_executors=debug,thirdweb_core=debug"
                .into()
        }));

    match config.server.log_format {
        config::LogFormat::Json => subscriber
            .with(tracing_subscriber::fmt::layer().json())
            .init(),
        config::LogFormat::Pretty => subscriber.with(tracing_subscriber::fmt::layer()).init(),
    }

    let vault_client = vault_sdk::VaultClient::builder(config.thirdweb.urls.vault)
        .build()
        .await?;

    tracing::info!("Vault client initialized");

    let chains = Arc::new(ThirdwebChainService {
        secret_key: config.thirdweb.secret.clone(),
        client_id: config.thirdweb.client_id.clone(),
        bundler_base_url: config.thirdweb.urls.bundler,
        paymaster_base_url: config.thirdweb.urls.paymaster,
        rpc_base_url: config.thirdweb.urls.rpc,
    });

    let iaw_client = IAWClient::new(&config.thirdweb.urls.iaw_service)?;
    tracing::info!("IAW client initialized");

    let signer = Arc::new(UserOpSigner {
        vault_client: vault_client.clone(),
        iaw_client: iaw_client.clone(),
    });
    let eoa_signer = Arc::new(EoaSigner::new(vault_client.clone(), iaw_client));
    let redis_client = twmq::redis::Client::open(config.redis.url.as_str())?;

    let queue_manager = QueueManager::new(
        redis_client.clone(),
        &config.queue,
        chains.clone(),
        signer.clone(),
        eoa_signer.clone(),
    )
    .await?;

    tracing::info!("Queue manager initialized");

    // Start queue workers
    tracing::info!("Starting queue workers...");
    let all_workers = queue_manager.start_workers(&config.queue);

    let abi_service = ThirdwebAbiServiceBuilder::new(
        &config.thirdweb.urls.abi_service,
        ThirdwebAuth::SecretKey(config.thirdweb.secret.clone()),
    )?
    .build()?;

    let execution_router = ExecutionRouter {
        namespace: config.queue.execution_namespace.clone(),
        redis: redis_client.get_connection_manager().await?,
        webhook_queue: queue_manager.webhook_queue.clone(),
        external_bundler_send_queue: queue_manager.external_bundler_send_queue.clone(),
        userop_confirm_queue: queue_manager.userop_confirm_queue.clone(),
        eoa_executor_queue: queue_manager.eoa_executor_queue.clone(),
        eip7702_send_queue: queue_manager.eip7702_send_queue.clone(),
        eip7702_confirm_queue: queue_manager.eip7702_confirm_queue.clone(),
        transaction_registry: queue_manager.transaction_registry.clone(),
        vault_client: Arc::new(vault_client.clone()),
        chains: chains.clone(),
    };

    let mut server = EngineServer::new(EngineServerState {
        userop_signer: signer.clone(),
        eoa_signer: eoa_signer.clone(),
        abi_service: Arc::new(abi_service),
        vault_client: Arc::new(vault_client),
        chains,
        execution_router: Arc::new(execution_router),
        queue_manager: Arc::new(queue_manager),
    })
    .await;

    let address = format!("{}:{}", config.server.host, config.server.port);
    let listener = tokio::net::TcpListener::bind(&address).await?;

    server.start(listener)?;

    tracing::info!("Servers started, waiting for shutdown signal");
    if let Err(e) = tokio::signal::ctrl_c().await {
        tracing::warn!("Failed to listen for Ctrl+C: {}", e);
    }
    tracing::info!("Shutdown signal received");

    // Orchestrate shutdown with minimal wrapping since internal methods handle their own instrumentation
    tracing::info!("Starting coordinated shutdown");

    if let Err(e) = server.shutdown().await {
        tracing::error!("Error during coordinated shutdown: {}", e);
    } else {
        tracing::info!("All servers shut down successfully");
    }

    if let Err(e) = all_workers.shutdown().await {
        tracing::error!("Error during coordinated shutdown: {}", e);
    } else {
        tracing::info!("All workers shut down successfully");
    }

    Ok(())
}
