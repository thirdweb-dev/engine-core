use std::env;

use config::{Config, File};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct EngineConfig {
    pub server: ServerConfig,
    pub thirdweb: ThirdwebConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
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
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: 3000,
            host: "0.0.0.0".into(),
        }
    }
}

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
            eprintln!("Configuration error: {}", e);
            panic!("Failed to build configuration");
        });

    // Deserialize the configuration
    config.try_deserialize::<EngineConfig>()
        .unwrap_or_else(|e| {
            eprintln!("Configuration error: {}", e);
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
                "{} is not a supported environment. Use either `local`, `development`, or `production`.",
                other
            )),
        }
    }
}
