pub mod eip7702_executor;
pub mod eoa;
pub mod external_bundler;
pub mod metrics;
pub mod transaction_registry;
pub mod webhook;

use alloy::{rpc::json_rpc::{RpcSend, RpcRecv}, providers::Provider};

/// Extension trait for RpcWithBlock to automatically select block tag based on flashblocks support
pub trait FlashblocksSupport {
    fn with_flashblocks_support(self, chain_id: u64) -> Self;
}

impl<Params, Resp, Output, Map> FlashblocksSupport for alloy::providers::RpcWithBlock<Params, Resp, Output, Map>
where
    Params: RpcSend,
    Resp: RpcRecv,
    Map: Fn(Resp) -> Output + Clone,
{
    fn with_flashblocks_support(self, chain_id: u64) -> Self {
        match chain_id {
            8453 | 84532 => self.pending(), // Base Mainnet | Base Sepolia
            _ => self,
        }
    }
}

/// Result of fetching transaction counts with flashblocks awareness
#[derive(Debug, Clone)]
pub struct TransactionCounts {
    /// Latest confirmed transaction count (always from "latest" block)
    pub latest: u64,
    /// Preconfirmed transaction count (from "pending" block for flashblocks chains, same as latest for others)
    pub preconfirmed: u64,
}

/// Extension trait for Provider to fetch transaction counts with flashblocks awareness
pub trait FlashblocksTransactionCount {
    fn get_transaction_counts_with_flashblocks_support(
        &self,
        address: alloy::primitives::Address,
        chain_id: u64,
    ) -> impl Future<Output = Result<TransactionCounts, alloy::transports::TransportError>>;
}

impl<T> FlashblocksTransactionCount for T
where
    T: Provider,
{
    async fn get_transaction_counts_with_flashblocks_support(
        &self,
        address: alloy::primitives::Address,
        chain_id: u64,
    ) -> Result<TransactionCounts, alloy::transports::TransportError> {
        match chain_id {
            8453 | 84532 => {
                // For flashblocks chains, fetch both latest and pending in parallel
                let (latest_result, preconfirmed_result) = tokio::try_join!(
                    self.get_transaction_count(address),
                    self.get_transaction_count(address).pending()
                )?;
                Ok(TransactionCounts {
                    latest: latest_result,
                    preconfirmed: preconfirmed_result,
                })
            }
            _ => {
                // For non-flashblocks chains, fetch once and use same value for both
                let count = self.get_transaction_count(address).await?;
                Ok(TransactionCounts {
                    latest: count,
                    preconfirmed: count,
                })
            }
        }
    }
}
