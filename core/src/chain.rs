use crate::rpc_clients::{BundlerClient, PaymasterClient, transport::SharedClientTransportBuilder};
use alloy::{
    providers::{ProviderBuilder, RootProvider},
    rpc::client::RpcClient,
    transports::http::reqwest::{
        ClientBuilder as HttpClientBuilder, Url,
        header::{HeaderMap, HeaderValue},
    },
};
use serde::{Deserialize, Serialize};
use thirdweb_core::auth::ThirdwebAuth;

use crate::error::EngineError;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RpcCredentials {
    Thirdweb(ThirdwebAuth),
}

impl RpcCredentials {
    pub fn to_header_map(&self) -> Result<HeaderMap, EngineError> {
        let header_map = match self {
            RpcCredentials::Thirdweb(creds) => creds.to_header_map()?,
        };

        Ok(header_map)
    }
}

pub trait Chain: Send + Sync {
    fn chain_id(&self) -> u64;
    fn rpc_url(&self) -> Url;
    fn bundler_url(&self) -> Url;
    fn paymaster_url(&self) -> Url;

    fn provider(&self) -> &RootProvider;
    fn bundler_client(&self) -> &BundlerClient;
    fn paymaster_client(&self) -> &PaymasterClient;

    fn bundler_client_with_headers(&self, headers: HeaderMap) -> BundlerClient;
    fn paymaster_client_with_headers(&self, headers: HeaderMap) -> PaymasterClient;

    fn with_new_default_headers(&self, headers: HeaderMap) -> Self;
}

pub struct ThirdwebChainConfig<'a> {
    pub secret_key: &'a str,
    pub chain_id: u64,
    pub rpc_base_url: &'a str,
    pub bundler_base_url: &'a str,
    pub paymaster_base_url: &'a str,
    pub client_id: &'a str,
}

#[derive(Clone)]
pub struct ThirdwebChain {
    transport_builder: SharedClientTransportBuilder,

    chain_id: u64,
    rpc_url: Url,
    bundler_url: Url,
    paymaster_url: Url,

    /// Default clients (these also use the shared connection pool)
    pub bundler_client: BundlerClient,
    pub paymaster_client: PaymasterClient,
    pub provider: RootProvider,

    pub sensitive_bundler_client: BundlerClient,
    pub sensitive_paymaster_client: PaymasterClient,
}

impl Chain for ThirdwebChain {
    fn chain_id(&self) -> u64 {
        self.chain_id
    }

    fn rpc_url(&self) -> Url {
        self.rpc_url.clone()
    }

    fn bundler_url(&self) -> Url {
        self.bundler_url.clone()
    }

    fn paymaster_url(&self) -> Url {
        self.paymaster_url.clone()
    }

    fn provider(&self) -> &RootProvider {
        &self.provider
    }

    fn bundler_client(&self) -> &BundlerClient {
        &self.bundler_client
    }

    fn paymaster_client(&self) -> &PaymasterClient {
        &self.paymaster_client
    }

    fn bundler_client_with_headers(&self, headers: HeaderMap) -> BundlerClient {
        let transport = self
            .transport_builder
            .with_headers(self.bundler_url.clone(), headers);
        let rpc_client = RpcClient::builder().transport(transport, false);
        BundlerClient { inner: rpc_client }
    }

    fn paymaster_client_with_headers(&self, headers: HeaderMap) -> PaymasterClient {
        let transport = self
            .transport_builder
            .with_headers(self.paymaster_url.clone(), headers);
        let rpc_client = RpcClient::builder().transport(transport, false);
        PaymasterClient { inner: rpc_client }
    }

    fn with_new_default_headers(&self, headers: HeaderMap) -> Self {
        let mut new_self = self.to_owned();
        new_self.bundler_client = self.bundler_client_with_headers(headers.clone());
        new_self.paymaster_client = self.paymaster_client_with_headers(headers.clone());

        new_self
    }
}

impl ThirdwebChainConfig<'_> {
    pub fn to_chain(&self) -> Result<ThirdwebChain, EngineError> {
        // Special handling for chain ID 31337 (local anvil)
        let (rpc_url, bundler_url, paymaster_url) = if self.chain_id == 31337 {
            // For local anvil, use localhost URLs
            let local_rpc_url = "http://127.0.0.1:8545";
            let rpc_url = Url::parse(local_rpc_url).map_err(|e| EngineError::RpcConfigError {
                message: format!("Failed to parse local anvil RPC URL: {e}"),
            })?;

            // For bundler and paymaster, use the same local RPC URL
            // since anvil doesn't have separate bundler/paymaster services
            let bundler_url = rpc_url.clone();
            let paymaster_url = rpc_url.clone();

            (rpc_url, bundler_url, paymaster_url)
        } else {
            // Standard URL construction for other chains
            let rpc_url = Url::parse(&format!(
                "https://{chain_id}.{base_url}/{client_id}",
                chain_id = self.chain_id,
                base_url = self.rpc_base_url,
                client_id = self.client_id,
            ))
            .map_err(|e| EngineError::RpcConfigError {
                message: format!("Failed to parse RPC URL: {e}"),
            })?;

            let bundler_url = Url::parse(&format!(
                "https://{chain_id}.{base_url}/v2",
                chain_id = self.chain_id,
                base_url = self.bundler_base_url,
            ))
            .map_err(|e| EngineError::RpcConfigError {
                message: format!("Failed to parse Bundler URL: {e}"),
            })?;

            let paymaster_url = Url::parse(&format!(
                "https://{chain_id}.{base_url}/v2",
                chain_id = self.chain_id,
                base_url = self.paymaster_base_url,
            ))
            .map_err(|e| EngineError::RpcConfigError {
                message: format!("Failed to parse Paymaster URL: {e}"),
            })?;

            (rpc_url, bundler_url, paymaster_url)
        };

        let mut sensitive_headers = HeaderMap::new();

        // Only add auth headers for non-local chains
        if self.chain_id != 31337 {
            sensitive_headers.insert(
                "x-client-id",
                HeaderValue::from_str(self.client_id).map_err(|e| EngineError::RpcConfigError {
                    message: format!("Unserialisable client-id used: {e}"),
                })?,
            );

            sensitive_headers.insert(
                "x-secret-key",
                HeaderValue::from_str(self.secret_key).map_err(|e| {
                    EngineError::RpcConfigError {
                        message: format!("Unserialisable secret-key used: {e}"),
                    }
                })?,
            );
        }

        let reqwest_client =
            HttpClientBuilder::new()
                .build()
                .map_err(|e| EngineError::RpcConfigError {
                    message: format!("Failed to build HTTP client: {e}"),
                })?;

        let transport_builder = SharedClientTransportBuilder::new(reqwest_client.clone());

        let paymaster_transport = transport_builder.default_transport(paymaster_url.clone());
        let bundler_transport = transport_builder.default_transport(bundler_url.clone());

        let sensitive_bundler_transport = if self.chain_id == 31337 {
            // For local anvil, use the same transport as non-sensitive
            transport_builder.default_transport(bundler_url.clone())
        } else {
            transport_builder.with_headers(bundler_url.clone(), sensitive_headers.clone())
        };

        let sensitive_paymaster_transport = if self.chain_id == 31337 {
            // For local anvil, use the same transport as non-sensitive
            transport_builder.default_transport(paymaster_url.clone())
        } else {
            transport_builder.with_headers(paymaster_url.clone(), sensitive_headers)
        };

        let paymaster_rpc_client = RpcClient::builder().transport(paymaster_transport, false);
        let bundler_rpc_client = RpcClient::builder().transport(bundler_transport, false);

        let sensitive_bundler_rpc_client =
            RpcClient::builder().transport(sensitive_bundler_transport, false);
        let sensitive_paymaster_rpc_client =
            RpcClient::builder().transport(sensitive_paymaster_transport, false);

        Ok(ThirdwebChain {
            transport_builder,

            chain_id: self.chain_id,
            rpc_url: rpc_url.clone(),

            bundler_client: BundlerClient {
                inner: bundler_rpc_client,
            },
            paymaster_client: PaymasterClient {
                inner: paymaster_rpc_client,
            },

            sensitive_bundler_client: BundlerClient {
                inner: sensitive_bundler_rpc_client,
            },

            sensitive_paymaster_client: PaymasterClient {
                inner: sensitive_paymaster_rpc_client,
            },

            provider: ProviderBuilder::new()
                .disable_recommended_fillers()
                .connect_http(rpc_url),

            bundler_url,
            paymaster_url,
        })
    }
}

pub trait ChainService {
    fn get_chain(&self, chain_id: u64) -> Result<impl Chain, EngineError>;
}
