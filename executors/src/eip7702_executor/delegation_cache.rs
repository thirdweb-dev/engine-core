use std::{ops::Deref, sync::Arc};

use alloy::primitives::Address;
use engine_core::{
    chain::Chain,
    error::{AlloyRpcErrorToEngineError, EngineError},
    rpc_clients::TwGetDelegationContractResponse,
};
use moka::future::Cache;

/// Cache key for delegation contract - uses chain_id as the key since each chain has one delegation contract
#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub struct DelegationContractCacheKey {
    chain_id: u64,
}

/// Cache for delegation contract addresses to avoid repeated RPC calls
#[derive(Clone)]
pub struct DelegationContractCache {
    pub inner: moka::future::Cache<DelegationContractCacheKey, Address>,
}

impl DelegationContractCache {
    /// Create a new delegation contract cache with the provided moka cache
    pub fn new(cache: Cache<DelegationContractCacheKey, Address>) -> Self {
        Self { inner: cache }
    }

    /// Get the delegation contract address for a chain, fetching it if not cached
    pub async fn get_delegation_contract<C: Chain>(
        &self,
        chain: &C,
    ) -> Result<Address, EngineError> {
        let cache_key = DelegationContractCacheKey {
            chain_id: chain.chain_id(),
        };

        // Use try_get_with for SWR behavior - this will fetch if not cached or expired
        let result = self
            .inner
            .try_get_with(cache_key, async {
                tracing::debug!(
                    chain_id = chain.chain_id(),
                    "Fetching delegation contract from bundler"
                );

                let TwGetDelegationContractResponse {
                    delegation_contract,
                } = chain
                    .bundler_client()
                    .tw_get_delegation_contract()
                    .await
                    .map_err(|e| e.to_engine_bundler_error(chain))?;

                tracing::debug!(
                    chain_id = chain.chain_id(),
                    delegation_contract = ?delegation_contract,
                    "Successfully fetched and cached delegation contract"
                );

                Ok(delegation_contract)
            })
            .await
            .map_err(|e: Arc<EngineError>| e.deref().clone())?;

        Ok(result)
    }
}
