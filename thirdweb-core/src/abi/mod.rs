use std::sync::Arc;

use alloy::{
    json_abi::JsonAbi,
    primitives::{Address, ChainId},
};
use reqwest::Url;

use crate::{
    auth::ThirdwebAuth,
    error::{SerializableReqwestError, ThirdwebError},
};

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct ThirdwebContractOptions {
    pub address: Address,
    pub chain_id: ChainId,
}

pub struct ThirdwebAbiService {
    pub url: Url,
    pub default_headers: reqwest::header::HeaderMap,
    pub client: reqwest::Client,
    pub cache: moka::future::Cache<ThirdwebContractOptions, JsonAbi>,
}

pub struct ThirdwebAbiServiceBuilder {
    pub url: Url,
    pub auth: ThirdwebAuth,
    pub cache_ttl: std::time::Duration,
    pub cache_capacity: u64,
}

impl ThirdwebAbiServiceBuilder {
    pub fn new(url: &str, auth: ThirdwebAuth) -> Result<Self, ThirdwebError> {
        Ok(Self {
            url: Url::parse(url).map_err(|e| ThirdwebError::url(url.to_owned(), e))?,
            auth,
            cache_ttl: std::time::Duration::from_secs(60 * 60),
            cache_capacity: 100,
        })
    }

    pub fn with_cache_ttl(mut self, cache_ttl: std::time::Duration) -> Self {
        self.cache_ttl = cache_ttl;
        self
    }

    pub fn with_cache_capacity(mut self, cache_capacity: u64) -> Self {
        self.cache_capacity = cache_capacity;
        self
    }

    /// Builds the ThirdwebAbiService
    pub fn build(self) -> Result<ThirdwebAbiService, ThirdwebError> {
        let client = reqwest::Client::builder()
            .build()
            .map_err(ThirdwebError::http_client_backend)?;

        let cache = moka::future::Cache::builder()
            .max_capacity(self.cache_capacity)
            .time_to_live(self.cache_ttl)
            .build();

        Ok(ThirdwebAbiService {
            client,
            default_headers: reqwest::header::HeaderMap::new(),
            cache,
            url: self.url,
        })
    }
}

impl ThirdwebAbiService {
    pub async fn get_abi(
        &self,
        contract: ThirdwebContractOptions,
    ) -> Result<JsonAbi, Arc<ThirdwebError>> {
        self.get_abi_with_headers(contract, self.default_headers.clone())
            .await
    }

    pub async fn get_abi_with_auth(
        &self,
        contract: ThirdwebContractOptions,
        auth: ThirdwebAuth,
    ) -> Result<JsonAbi, Arc<ThirdwebError>> {
        let headers = auth.to_header_map()?;
        self.get_abi_with_headers(contract, headers).await
    }

    pub async fn get_abi_with_headers(
        &self,
        contract: ThirdwebContractOptions,
        headers: reqwest::header::HeaderMap,
    ) -> Result<JsonAbi, Arc<ThirdwebError>> {
        tracing::debug!("Fetching ABI for contract {}", contract.address);
        self.cache
            .try_get_with(contract.clone(), async {
                let url = self
                    .url
                    .join(&format!("{}/{}", contract.chain_id, contract.address))
                    .map_err(|e| ThirdwebError::url(contract.address.to_string(), e))?;

                let response = self
                    .client
                    .get(url)
                    .headers(headers)
                    .send()
                    .await
                    .map_err(|e| {
                        tracing::error!("Failed to fetch ABI: {}", e);
                        SerializableReqwestError::from(e)
                    })?;

                Ok(response.json::<JsonAbi>().await.map_err(|e| {
                    tracing::error!("Failed to fetch ABI: {}", e);
                    SerializableReqwestError::from(e)
                })?)
            })
            .await
    }

    pub async fn get_abi_from_cache(&self, contract: ThirdwebContractOptions) -> Option<JsonAbi> {
        self.cache.get(&contract).await
    }
}
