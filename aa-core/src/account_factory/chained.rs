use alloy::{
    primitives::{Address, Bytes},
    sol,
};
use engine_core::{
    chain::Chain,
    error::{ContractErrorToEngineError, EngineError},
};

use super::AccountFactory;

pub struct ChainedAccountFactory<'a, C: Chain> {
    pub chain: &'a C,
    pub factory_address: Address,
}

sol! {
    #[sol(rpc)]
    contract AccountFactoryContract {
        function getAddress(address _adminSigner, bytes _data) view returns (address);
    }
}

impl<C: Chain> AccountFactory for ChainedAccountFactory<'_, C> {
    fn factory_address(&self) -> &Address {
        &self.factory_address
    }

    async fn predict_address(
        &self,
        signer: &Address,
        salt_data: &Bytes,
    ) -> Result<Address, EngineError> {
        let account_factory_contract =
            AccountFactoryContract::new(self.factory_address, self.chain.provider().clone());

        let predicted_address = account_factory_contract
            .getAddress(signer.to_owned(), salt_data.to_owned())
            .call()
            .await
            .map_err(|e| e.to_engine_error(self.chain.chain_id(), Some(self.factory_address)))?;

        Ok(predicted_address)
    }
}
