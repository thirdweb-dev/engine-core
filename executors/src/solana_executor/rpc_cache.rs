use engine_core::execution_options::solana::SolanaChainId;
use moka::future::Cache;
use solana_client::nonblocking::rpc_client::RpcClient;
use std::{sync::Arc, time::Duration};
use tracing::info;

/// Cache key for RPC clients
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct RpcCacheKey {
    pub chain_id: SolanaChainId,
    pub rpc_url: String,
}

/// Solana RPC client cache with connection pooling
///
/// This cache maintains RpcClient instances per Solana cluster,
/// with the underlying ConnectionCache providing TCP connection reuse
#[derive(Clone)]
pub struct SolanaRpcCache {
    cache: Cache<RpcCacheKey, Arc<RpcClient>>,
    urls: SolanaRpcUrls,
}

#[derive(Clone)]
pub struct SolanaRpcUrls {
    pub devnet: String,
    pub mainnet: String,
    pub local: String,
}

impl SolanaRpcCache {
    /// Create a new RPC cache with specified TTL and max capacity
    pub fn new(urls: SolanaRpcUrls) -> Self {
        let cache = Cache::new(3);

        Self { cache, urls }
    }

    /// Get or create an RPC client for the given cluster
    pub async fn get_or_create(&self, chain_id: SolanaChainId) -> Arc<RpcClient> {
        let rpc_url = match chain_id {
            SolanaChainId::SolanaDevnet => self.urls.devnet.clone(),
            SolanaChainId::SolanaMainnet => self.urls.mainnet.clone(),
            SolanaChainId::SolanaLocal => self.urls.local.clone(),
        };

        let key = RpcCacheKey {
            chain_id: chain_id.clone(),
            rpc_url: rpc_url.clone(),
        };

        self.cache
            .get_with(key.clone(), async move {
                info!(
                    chain_id = ?chain_id,
                    rpc_url = ?rpc_url,
                    "Creating new Solana RPC client with connection cache"
                );

                // Create RPC client
                Arc::new(RpcClient::new(rpc_url.clone()))
            })
            .await
    }

    /// Get the number of cached clients
    pub fn len(&self) -> u64 {
        self.cache.entry_count()
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
