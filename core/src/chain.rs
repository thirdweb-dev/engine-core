use crate::rpc_clients::{BundlerClient, PaymasterClient};
use alloy::{
    providers::{ProviderBuilder, RootProvider},
    transports::http::{
        Http,
        reqwest::{
            ClientBuilder as HttpClientBuilder, Url,
            header::{HeaderMap, HeaderValue},
        },
    },
};

use crate::error::EngineError;

pub trait Chain {
    fn chain_id(&self) -> u64;
    fn rpc_url(&self) -> Url;
    fn bundler_url(&self) -> Url;
    fn paymaster_url(&self) -> Url;

    fn provider(&self) -> &RootProvider;
    fn bundler_client(&self) -> &BundlerClient;
    fn paymaster_client(&self) -> &PaymasterClient;
}

pub struct ThirdwebChainConfig<'a> {
    pub secret_key: &'a str,
    pub chain_id: u64,
    pub rpc_base_url: &'a str,
    pub bundler_base_url: &'a str,
    pub paymaster_base_url: &'a str,
    pub client_id: &'a str,
}

pub struct ThirdwebChain {
    chain_id: u64,
    rpc_url: Url,
    bundler_url: Url,
    paymaster_url: Url,

    pub bundler_client: BundlerClient,
    pub paymaster_client: PaymasterClient,
    pub provider: RootProvider,
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
}

impl<'a> ThirdwebChainConfig<'a> {
    pub fn to_chain(&self) -> Result<ThirdwebChain, EngineError> {
        let rpc_url = Url::parse(&format!(
            "https://{chain_id}.{base_url}/{client_id}",
            chain_id = self.chain_id,
            base_url = self.rpc_base_url,
            client_id = self.client_id,
        ))
        .map_err(|e| EngineError::RpcConfigError {
            message: format!("Failed to parse RPC URL: {}", e.to_string()),
        })?;

        let bundler_url = Url::parse(&format!(
            "https://{chain_id}.{base_url}/v2",
            chain_id = self.chain_id,
            base_url = self.bundler_base_url,
        ))
        .map_err(|e| EngineError::RpcConfigError {
            message: format!("Failed to parse Bundler URL: {}", e.to_string()),
        })?;

        let paymaster_url = Url::parse(&format!(
            "https://{chain_id}.{base_url}/v2",
            chain_id = self.chain_id,
            base_url = self.paymaster_base_url,
        ))
        .map_err(|e| EngineError::RpcConfigError {
            message: format!("Failed to parse Paymaster URL: {}", e.to_string()),
        })?;

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-client-id",
            HeaderValue::from_str(&self.client_id).map_err(|e| EngineError::RpcConfigError {
                message: format!("Unserialisable client-id used: {e}"),
            })?,
        );

        headers.insert(
            "x-secret-key",
            HeaderValue::from_str(&self.secret_key).map_err(|e| EngineError::RpcConfigError {
                message: format!("Unserialisable secret-key used: {e}"),
            })?,
        );

        let reqwest_client = HttpClientBuilder::new()
            .default_headers(headers)
            .build()
            .map_err(|e| EngineError::RpcConfigError {
                message: format!("Failed to build HTTP client: {e}"),
            })?;

        let paymaster_transport = Http::with_client(reqwest_client.clone(), paymaster_url.clone());
        let bundler_transport = Http::with_client(reqwest_client, bundler_url.clone());

        Ok(ThirdwebChain {
            chain_id: self.chain_id,
            rpc_url: rpc_url.clone(),
            bundler_client: BundlerClient::new(bundler_transport),
            paymaster_client: PaymasterClient::new(paymaster_transport),
            provider: ProviderBuilder::new()
                .disable_recommended_fillers()
                .connect_http(rpc_url)
                .into(),

            bundler_url,
            paymaster_url,
        })
    }
}
