use engine_core::{
    chain::{ChainService, ThirdwebChain, ThirdwebChainConfig},
    error::EngineError,
};

pub struct ThirdwebChainService {
    pub client_id: String,
    pub secret_key: String,
    pub rpc_base_url: String,
    pub bundler_base_url: String,
    pub paymaster_base_url: String,
}

#[allow(refining_impl_trait)]
impl ChainService for ThirdwebChainService {
    fn get_chain(&self, chain_id: u64) -> Result<ThirdwebChain, EngineError> {
        ThirdwebChainConfig {
            chain_id,
            rpc_base_url: &self.rpc_base_url,
            bundler_base_url: &self.bundler_base_url,
            paymaster_base_url: &self.paymaster_base_url,
            client_id: &self.client_id,
            secret_key: &self.secret_key,
        }
        .to_chain()
    }
}
