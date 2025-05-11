use alloy::{
    primitives::{Address, Bytes},
    sol,
    sol_types::SolCall,
};

mod default;
mod utils;

pub mod chained;
use chained::ChainedAccountFactory;
pub use default::*;
use engine_core::{
    chain::Chain,
    constants::{DEFAULT_FACTORY_ADDRESS_V0_6, DEFAULT_FACTORY_ADDRESS_V0_7},
    error::EngineError,
};

/// Interface for smart account factory implementations
pub trait SyncAccountFactory {
    /// Predicts the smart account address for a given signer
    fn predict_address_sync(&self, signer: &Address, salt_data: &Bytes) -> Address;
}

sol! {
    function createAccount(address admin, bytes salt) returns (address);
}

pub trait AccountFactory {
    fn factory_address(&self) -> &Address;

    fn predict_address(
        &self,
        signer: &Address,
        salt_data: &Bytes,
    ) -> impl Future<Output = Result<Address, EngineError>>;

    fn init_calldata(&self, signer: Address, salt_data: Bytes) -> Vec<u8> {
        createAccountCall {
            admin: signer,
            salt: salt_data,
        }
        .abi_encode()
    }
}

/// A factory that can use either implementation based on the provided addresses
pub enum SmartAccountFactory<'a, C: Chain> {
    Default(DefaultAccountFactory),
    Chained(ChainedAccountFactory<'a, C>),
}

// Implement the AccountFactory trait for our enum
impl<'a, C: Chain> AccountFactory for SmartAccountFactory<'a, C> {
    fn factory_address(&self) -> &Address {
        match self {
            Self::Default(factory) => &factory.factory_address,
            Self::Chained(factory) => &factory.factory_address,
        }
    }
    // Assuming your AccountFactory trait has this signature:
    async fn predict_address(
        &self,
        signer: &Address,
        salt_data: &Bytes,
    ) -> Result<Address, EngineError> {
        match self {
            Self::Default(factory) => factory.predict_address(signer, salt_data).await,
            Self::Chained(factory) => factory.predict_address(signer, salt_data).await,
        }
    }
}

/// Get the appropriate account factory based on the factory address
pub fn get_account_factory<'a, C: Chain>(
    chain: &'a C,
    factory_address: Address,
    implementation_address: Option<Address>,
) -> SmartAccountFactory<'a, C> {
    // Check if the factory address matches default v0.6
    if factory_address == DEFAULT_FACTORY_ADDRESS_V0_6 {
        SmartAccountFactory::Default(DefaultAccountFactory::v0_6())
    }
    // Check if the factory address matches default v0.7
    else if factory_address == DEFAULT_FACTORY_ADDRESS_V0_7 {
        SmartAccountFactory::Default(DefaultAccountFactory::v0_7())
    }
    // For custom factory addresses with known implementation
    else if let Some(implementation) = implementation_address {
        SmartAccountFactory::Default(DefaultAccountFactory::with_addresses(
            factory_address,
            implementation,
        ))
    }
    // For custom factory addresses with unknown implementation
    else {
        SmartAccountFactory::Chained(ChainedAccountFactory {
            chain,
            factory_address,
        })
    }
}
