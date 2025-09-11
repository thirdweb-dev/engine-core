pub mod eip7702_executor;
pub mod eoa;
pub mod external_bundler;
pub mod metrics;
pub mod transaction_registry;
pub mod webhook;

use alloy::rpc::json_rpc::{RpcSend, RpcRecv};

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
