use std::env;

use config::{Config, File};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct EngineConfig {
    pub server: ServerConfig,
    pub thirdweb: ThirdwebConfig,
    pub queue: QueueConfig,
    pub redis: RedisConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueueConfig {
    pub webhook_workers: usize,

    pub external_bundler_send_workers: usize,
    pub userop_confirm_workers: usize,
    pub eoa_executor_workers: usize,

    pub execution_namespace: Option<String>,

    pub local_concurrency: usize,
    pub polling_interval_ms: u64,
    pub lease_duration_seconds: u64,
    
    #[serde(default)]
    pub monitoring: MonitoringConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct MonitoringConfig {
    pub eoa_send_degradation_threshold_seconds: u64,
    pub eoa_confirmation_degradation_threshold_seconds: u64,
    pub eoa_stuck_threshold_seconds: u64,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            eoa_send_degradation_threshold_seconds: 10, // 10 seconds
            eoa_confirmation_degradation_threshold_seconds: 120, // 2 minutes
            eoa_stuck_threshold_seconds: 600, // 10 minutes
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RedisConfig {
    pub url: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub log_format: LogFormat,
    pub diagnostic_access_password: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ThirdwebConfig {
    pub secret: String,
    pub client_id: String,
    pub urls: ThirdwebUrls,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ThirdwebUrls {
    pub rpc: String,
    pub bundler: String,
    pub vault: String,
    pub paymaster: String,
    pub abi_service: String,
    pub iaw_service: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Json,
    Pretty,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: 3000,
            host: "0.0.0.0".into(),
            log_format: LogFormat::Pretty,
            diagnostic_access_password: None,
        }
    }
}

/// EngineConfig is cached, it only loads once
pub fn get_config() -> EngineConfig {
    let base_path = env::current_dir().expect("Failed to determine the current directory");
    let configuration_directory = base_path.join("configuration");

    // Detect the running environment
    let environment: Environment = env::var("APP_ENVIRONMENT")
        .unwrap_or_else(|_| "local".into())
        .try_into()
        .expect("Failed to parse APP_ENVIRONMENT");

    let environment_filename = format!("server_{}.yaml", environment.as_str());

    // Load configuration from files
    let config = Config::builder()
        .add_source(File::from(configuration_directory.join("server_base.yaml")))
        .add_source(File::from(
            configuration_directory.join(environment_filename),
        ))
        .add_source(config::Environment::with_prefix("app").separator("__"))
        .build()
        .unwrap_or_else(|e| {
            eprintln!("Configuration error: {e}");
            panic!("Failed to build configuration");
        });

    // Deserialize the configuration
    config.try_deserialize::<EngineConfig>()
        .unwrap_or_else(|e| {
            eprintln!("Configuration error: {e}");
            eprintln!("Make sure all required fields are set correctly in your configuration files or environment variables.");
            panic!("Failed to deserialize configuration");
        })
}

/// The possible runtime environment for our application.
pub enum Environment {
    Local,
    Development,
    Production,
}

impl Environment {
    pub fn as_str(&self) -> &'static str {
        match self {
            Environment::Local => "local",
            Environment::Development => "development",
            Environment::Production => "production",
        }
    }
}

impl TryFrom<String> for Environment {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "local" => Ok(Self::Local),
            "development" => Ok(Self::Development),
            "production" => Ok(Self::Production),
            other => Err(format!(
                "{other} is not a supported environment. Use either `local`, `development`, or `production`."
            )),
        }
    }
}
