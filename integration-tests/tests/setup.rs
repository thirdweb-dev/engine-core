use anyhow::{Context, Result};
use config::{Config, File};
use engine_core::{
    credentials::KmsClientCache,
    signer::{EoaSigner, SolanaSigner},
    userop::UserOpSigner,
};
use engine_executors::{
    eoa::authorization_cache::EoaAuthorizationCache,
    metrics::{ExecutorMetrics, initialize_metrics},
    solana_executor::rpc_cache::{SolanaRpcCache, SolanaRpcUrls},
};
use serde::Deserialize;
use std::{env, sync::Arc, time::Duration};
use thirdweb_core::{abi::ThirdwebAbiServiceBuilder, auth::ThirdwebAuth, iaw::IAWClient};
use thirdweb_engine::{
    EngineServer,
    EngineServerState,
    QueueConfig,
    QueueManager,
    RedisConfig as ServerRedisConfig,
    SolanaConfig,
    ThirdwebChainService,
    // Import config types instead of duplicating them
    ThirdwebConfig,
};
use tokio::net::TcpListener;
use tracing::info;

/// Test configuration structure matching test_base.yaml
/// Reuses types from server config to avoid duplication
#[derive(Debug, Clone, Deserialize)]
pub struct TestConfig {
    pub vault: VaultConfig,
    pub redis: ServerRedisConfig,
    pub thirdweb: ThirdwebConfig,
    pub solana: SolanaConfig,
    pub queue: QueueConfig,
}

/// Vault-specific configuration for tests
#[derive(Debug, Clone, Deserialize)]
pub struct VaultConfig {
    /// Admin key for signing transactions
    pub admin_key: Option<String>,
    /// Pre-existing Solana wallet to use for tests (REQUIRED)
    pub test_solana_wallet: Option<String>,
}

/// Load test configuration from YAML files
pub fn load_test_config() -> Result<TestConfig> {
    let base_path = env::current_dir().context("Failed to determine current directory")?;
    let config_dir = base_path.join("configuration");

    // Detect environment (default to local)
    let environment = env::var("TEST_ENVIRONMENT").unwrap_or_else(|_| "local".to_string());

    info!(
        "Loading test configuration for environment: {}",
        environment
    );

    let config = Config::builder()
        .add_source(File::from(config_dir.join("test_base.yaml")))
        .add_source(
            File::from(config_dir.join(format!("test_{}.yaml", environment))).required(false), // Optional, falls back to base if not present
        )
        // Allow environment variable overrides with TEST_ prefix
        .add_source(config::Environment::with_prefix("TEST").separator("__"))
        .build()
        .context("Failed to build test configuration")?;

    let test_config = config
        .try_deserialize::<TestConfig>()
        .context("Failed to deserialize test configuration")?;

    info!("Test configuration loaded successfully");
    Ok(test_config)
}

/// Find an available port
fn find_available_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

/// Test environment for integration tests
#[allow(dead_code)]
pub struct TestEnvironment {
    /// Server port
    pub server_port: u16,

    /// Engine server instance
    engine_server: Arc<EngineServer>,

    /// Vault client for creating test wallets
    pub vault_client: Arc<vault_sdk::VaultClient>,

    /// Server URL for HTTP requests
    pub server_url: String,

    /// Test configuration
    pub config: TestConfig,
}

impl TestEnvironment {
    /// Create a new test environment with isolated components
    pub async fn new(test_name: &str) -> Result<Self> {
        init_logging();

        info!("Setting up test environment for: {}", test_name);

        // Load configuration from YAML files
        let config = load_test_config()?;

        // Validate required configuration
        if config.vault.admin_key.is_none() {
            anyhow::bail!(
                "vault.admin_key is required in test configuration. \
                Please set it in integration-tests/configuration/test_local.yaml"
            );
        }

        if config.vault.test_solana_wallet.is_none() {
            anyhow::bail!(
                "vault.test_solana_wallet is required in test configuration. \
                This should be a Solana wallet address that exists in your Vault. \
                Please set it in integration-tests/configuration/test_local.yaml"
            );
        }

        info!("Configuration loaded and validated");

        // Create vault client
        let vault_client = vault_sdk::VaultClient::builder(config.thirdweb.urls.vault.clone())
            .build()
            .await
            .context("Failed to create Vault client")?;

        info!("Vault client initialized: {}", config.thirdweb.urls.vault);

        // Setup components using config
        let chains = Arc::new(ThirdwebChainService {
            secret_key: config.thirdweb.secret.clone(),
            client_id: config.thirdweb.client_id.clone(),
            bundler_base_url: config.thirdweb.urls.bundler.clone(),
            paymaster_base_url: config.thirdweb.urls.paymaster.clone(),
            rpc_base_url: config.thirdweb.urls.rpc.clone(),
        });

        let iaw_client = IAWClient::new(&config.thirdweb.urls.iaw_service)
            .context("Failed to create IAW client")?;

        let kms_client_cache: KmsClientCache = moka::future::Cache::builder()
            .max_capacity(100)
            .time_to_live(Duration::from_secs(60 * 60))
            .time_to_idle(Duration::from_secs(60 * 30))
            .build();

        let signer = Arc::new(UserOpSigner {
            vault_client: vault_client.clone(),
            iaw_client: iaw_client.clone(),
        });

        let eoa_signer = Arc::new(EoaSigner::new(vault_client.clone(), iaw_client.clone()));
        let solana_signer = Arc::new(SolanaSigner::new(vault_client.clone(), iaw_client));

        // Setup Redis
        let initial_nodes: Vec<&str> = config
            .redis
            .url
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();
        let redis_client = twmq::redis::cluster::ClusterClient::new(initial_nodes)
            .context("Failed to connect to Valkey Cluster")?;

        let authorization_cache = EoaAuthorizationCache::new(
            moka::future::Cache::builder()
                .max_capacity(1024 * 1024 * 1024)
                .time_to_live(Duration::from_secs(60 * 5))
                .time_to_idle(Duration::from_secs(60))
                .build(),
        );

        // Create Solana RPC cache with configured URLs
        let solana_rpc_urls = SolanaRpcUrls {
            devnet: config.solana.devnet.http_url.clone(),
            mainnet: config.solana.mainnet.http_url.clone(),
            local: config.solana.local.http_url.clone(),
        };
        let solana_rpc_cache = Arc::new(SolanaRpcCache::new(solana_rpc_urls));

        info!("Solana RPC cache initialized");

        // Convert test config to engine queue config
        let queue_config = thirdweb_engine::QueueConfig {
            webhook_workers: config.queue.webhook_workers,
            external_bundler_send_workers: config.queue.external_bundler_send_workers,
            userop_confirm_workers: config.queue.userop_confirm_workers,
            eoa_executor_workers: config.queue.eoa_executor_workers,
            solana_executor_workers: config.queue.solana_executor_workers,
            execution_namespace: Some(format!("test_{}", test_name)),
            local_concurrency: config.queue.local_concurrency,
            polling_interval_ms: config.queue.polling_interval_ms,
            lease_duration_seconds: config.queue.lease_duration_seconds,
            monitoring: thirdweb_engine::config::MonitoringConfig {
                eoa_send_degradation_threshold_seconds: config
                    .queue
                    .monitoring
                    .eoa_send_degradation_threshold_seconds,
                eoa_confirmation_degradation_threshold_seconds: config
                    .queue
                    .monitoring
                    .eoa_confirmation_degradation_threshold_seconds,
                eoa_stuck_threshold_seconds: config.queue.monitoring.eoa_stuck_threshold_seconds,
            },
        };

        let solana_config = thirdweb_engine::SolanaConfig {
            devnet: thirdweb_engine::config::SolanRpcConfigData {
                http_url: config.solana.devnet.http_url.clone(),
                ws_url: config.solana.devnet.ws_url.clone(),
            },
            mainnet: thirdweb_engine::config::SolanRpcConfigData {
                http_url: config.solana.mainnet.http_url.clone(),
                ws_url: config.solana.mainnet.ws_url.clone(),
            },
            local: thirdweb_engine::config::SolanRpcConfigData {
                http_url: config.solana.local.http_url.clone(),
                ws_url: config.solana.local.ws_url.clone(),
            },
        };

        let queue_manager = QueueManager::new(
            redis_client.clone(),
            &queue_config,
            &solana_config,
            chains.clone(),
            signer.clone(),
            eoa_signer.clone(),
            authorization_cache.clone(),
            kms_client_cache.clone(),
        )
        .await
        .context("Failed to create queue manager")?;

        info!("Queue manager initialized");

        let abi_service = ThirdwebAbiServiceBuilder::new(
            &config.thirdweb.urls.abi_service,
            ThirdwebAuth::SecretKey(config.thirdweb.secret.clone()),
        )
        .context("Failed to create ABI service builder")?
        .build()
        .context("Failed to build ABI service")?;

        let execution_router = thirdweb_engine::ExecutionRouter {
            namespace: queue_config.execution_namespace.clone(),
            redis: redis_client.get_async_connection().await?,
            authorization_cache,
            webhook_queue: queue_manager.webhook_queue.clone(),
            external_bundler_send_queue: queue_manager.external_bundler_send_queue.clone(),
            userop_confirm_queue: queue_manager.userop_confirm_queue.clone(),
            eoa_executor_queue: queue_manager.eoa_executor_queue.clone(),
            eip7702_send_queue: queue_manager.eip7702_send_queue.clone(),
            eip7702_confirm_queue: queue_manager.eip7702_confirm_queue.clone(),
            solana_executor_queue: queue_manager.solana_executor_queue.clone(),
            transaction_registry: queue_manager.transaction_registry.clone(),
            vault_client: Arc::new(vault_client.clone()),
            chains: chains.clone(),
        };

        // Initialize metrics registry and executor metrics
        let metrics_registry = Arc::new(prometheus::Registry::new());
        let executor_metrics =
            ExecutorMetrics::new(&metrics_registry).expect("Failed to create executor metrics");
        initialize_metrics(executor_metrics);

        info!("Executor metrics initialized");

        // Create engine server
        let mut engine_server = EngineServer::new(EngineServerState {
            userop_signer: signer.clone(),
            eoa_signer: eoa_signer.clone(),
            solana_signer: solana_signer.clone(),
            solana_rpc_cache: solana_rpc_cache.clone(),
            abi_service: Arc::new(abi_service),
            vault_client: Arc::new(vault_client.clone()),
            chains,
            execution_router: Arc::new(execution_router),
            queue_manager: Arc::new(queue_manager),
            diagnostic_access_password: None,
            metrics_registry,
            kms_client_cache: kms_client_cache.clone(),
        })
        .await;

        // Find available port and start server
        let server_port = find_available_port();
        let server_addr = format!("127.0.0.1:{}", server_port);
        let listener = TcpListener::bind(&server_addr)
            .await
            .context("Failed to bind HTTP server to address")?;

        info!("Starting HTTP server on port {}", server_port);

        engine_server
            .start(listener)
            .context("Failed to start HTTP server")?;

        let server_url = format!("http://127.0.0.1:{}", server_port);

        // Wait for server to be ready
        tokio::time::sleep(Duration::from_millis(500)).await;

        info!("Test environment ready at {}", server_url);

        Ok(Self {
            server_port,
            engine_server: Arc::new(engine_server),
            vault_client: Arc::new(vault_client),
            server_url,
            config,
        })
    }

    /// Get a reference to the vault client
    pub fn vault_client(&self) -> &Arc<vault_sdk::VaultClient> {
        &self.vault_client
    }

    /// Get the server URL
    pub fn server_url(&self) -> &str {
        &self.server_url
    }

    /// Get the vault admin key from configuration
    pub fn vault_admin_key(&self) -> &str {
        self.config
            .vault
            .admin_key
            .as_ref()
            .expect("vault.admin_key should be validated in new()")
    }

    /// Get the test Solana wallet address from configuration
    pub fn test_solana_wallet(&self) -> &str {
        self.config
            .vault
            .test_solana_wallet
            .as_ref()
            .expect("vault.test_solana_wallet should be validated in new()")
    }
}

impl Drop for TestEnvironment {
    fn drop(&mut self) {
        info!("Cleaning up test environment");
        // Components are automatically cleaned up when their Arc references are dropped
    }
}

/// Initialize logging exactly once at the beginning of testing
pub fn init_logging() {
    use once_cell::sync::Lazy;

    static INIT_LOGGER: Lazy<()> = Lazy::new(|| {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::from_default_env()
                    .add_directive("engine_integration_tests=debug".parse().unwrap())
                    .add_directive("thirdweb_engine=debug".parse().unwrap())
                    .add_directive("engine_core=debug".parse().unwrap()),
            )
            .with_test_writer()
            .try_init()
            .ok(); // Ignore errors if already initialized
    });

    // Force the lazy evaluation
    Lazy::force(&INIT_LOGGER);
}
