use alloy::{
    primitives::{Address, Bytes},
    providers::Provider,
    sol,
    sol_types::SolCall,
};
use engine_core::{
    chain::Chain,
    error::{AlloyRpcErrorToEngineError, EngineError},
    transaction::InnerTransaction,
};

use crate::account_factory::{AccountFactory, get_account_factory};

sol! {
    function execute(address _target, uint256 _value, bytes _calldata);
}

sol! {
    function executeBatch(address[] _target, uint256[] _value, bytes[] _calldata);
}

pub trait SmartAccount {
    fn address(&self) -> &Address;

    /// Check if the account is deployed
    #[allow(async_fn_in_trait)]
    async fn is_deployed(&self, chain: &impl Chain) -> Result<bool, EngineError> {
        let code = chain
            .provider()
            .get_code_at(self.address().clone())
            .await
            .map_err(|t| t.to_engine_error(chain))?;

        Ok(code.len() > 0)
    }

    /// Encode a transaction call to the account
    fn encode_execute(&self, tx: &InnerTransaction) -> Bytes {
        executeCall {
            _target: tx.to,
            _value: tx.value,
            _calldata: tx.data.clone(),
        }
        .abi_encode()
        .into()
    }

    /// Encode a batch transaction call to the account
    fn encode_execute_batch(&self, batch: &Vec<InnerTransaction>) -> Bytes {
        executeBatchCall {
            _target: batch.iter().map(|tx| tx.to).collect(),
            _value: batch.iter().map(|tx| tx.value).collect(),
            _calldata: batch.iter().map(|tx| tx.data.clone()).collect(),
        }
        .abi_encode()
        .into()
    }
}

pub struct SmartAccountFromSalt<'a, C: Chain> {
    pub salt_data: &'a Bytes,
    pub factory_address: Address,
    pub admin_address: Address,
    pub chain: &'a C,
}

#[derive(Clone, Debug)]
pub struct DeterminedSmartAccount {
    pub address: Address,
}

impl<C: Chain> SmartAccountFromSalt<'_, C> {
    pub async fn to_determined_smart_account(self) -> Result<DeterminedSmartAccount, EngineError> {
        let factory = get_account_factory(self.chain, self.factory_address, None);
        let address = factory
            .predict_address(&self.admin_address, &self.salt_data)
            .await?;

        Ok(DeterminedSmartAccount { address })
    }
}

impl SmartAccount for DeterminedSmartAccount {
    fn address(&self) -> &Address {
        &self.address
    }
}
