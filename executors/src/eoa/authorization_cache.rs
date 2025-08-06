use std::ops::Deref;

use alloy::primitives::Address;
use engine_core::{chain::Chain, error::EngineError};
use engine_eip7702_core::delegated_account::DelegatedAccount;
use moka::future::Cache;

#[derive(Hash, Eq, PartialEq)]
pub struct AuthorizationCacheKey {
    eoa_address: Address,
    chain_id: u64,
}

#[derive(Clone)]
pub struct EoaAuthorizationCache {
    pub inner: moka::future::Cache<AuthorizationCacheKey, bool>,
}

impl EoaAuthorizationCache {
    pub fn new(cache: Cache<AuthorizationCacheKey, bool>) -> Self {
        Self { inner: cache }
    }

    pub async fn is_minimal_account<C: Chain>(
        &self,
        delegated_account: &DelegatedAccount<C>,
    ) -> Result<bool, EngineError> {
        self.inner
            .try_get_with(
                AuthorizationCacheKey {
                    eoa_address: delegated_account.eoa_address,
                    chain_id: delegated_account.chain.chain_id(),
                },
                delegated_account.is_minimal_account(),
            )
            .await
            .map_err(|e| e.deref().clone())
    }
}
