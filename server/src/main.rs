use std::sync::Arc;

use engine_core::userop::UserOpSigner;
use thirdweb_engine::{
    chains::ThirdwebChainService,
    config,
    http::server::{EngineServer, EngineServerState},
};
use tracing_subscriber::{filter::EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            // Default to info level if RUST_LOG environment variable is not set
            "thirdweb_engine=debug,tower_http=debug,axum=debug".into()
        }))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = config::get_config();

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

    let signer = UserOpSigner { vault_client };

    let mut server = EngineServer::new(EngineServerState {
        signer: Arc::new(signer),
        chains,
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

    Ok(())
}
