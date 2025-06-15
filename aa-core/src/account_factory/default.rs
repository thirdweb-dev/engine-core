use alloy::primitives::{Address, Bytes};
use engine_core::{
    constants::{
        DEFAULT_FACTORY_ADDRESS_V0_6, DEFAULT_FACTORY_ADDRESS_V0_7,
        DEFAULT_IMPLEMENTATION_ADDRESS_V0_6, DEFAULT_IMPLEMENTATION_ADDRESS_V0_7,
    },
    error::EngineError,
};

use super::{AccountFactory, SyncAccountFactory, utils};

pub struct DefaultAccountFactory {
    pub factory_address: Address,
    pub implementation_address: Address,
}

impl DefaultAccountFactory {
    /// Create new factory with default address
    pub fn v0_6() -> Self {
        Self {
            factory_address: DEFAULT_FACTORY_ADDRESS_V0_6,
            implementation_address: DEFAULT_IMPLEMENTATION_ADDRESS_V0_6,
        }
    }

    /// Create with custom factory address
    pub fn v0_7() -> Self {
        Self {
            factory_address: DEFAULT_FACTORY_ADDRESS_V0_7,
            implementation_address: DEFAULT_IMPLEMENTATION_ADDRESS_V0_7,
        }
    }

    /// Create with custom factory and implementation addresses
    pub fn with_addresses(factory_address: Address, implementation_address: Address) -> Self {
        Self {
            factory_address,
            implementation_address,
        }
    }
}

impl SyncAccountFactory for DefaultAccountFactory {
    fn predict_address_sync(&self, signer: &Address, salt_data: &Bytes) -> Address {
        let salt = utils::generate_salt(signer, salt_data);
        utils::predict_deterministic_address(
            self.implementation_address,
            salt,
            self.factory_address,
        )
    }
}

// Add unified trait implementation for the sync type
impl AccountFactory for DefaultAccountFactory {
    fn factory_address(&self) -> &Address {
        &self.factory_address
    }

    fn predict_address(
        &self,
        signer: &Address,
        salt_data: &Bytes,
    ) -> impl Future<Output = Result<Address, EngineError>> {
        // Use the sync implementation but return as a ready future
        let address = SyncAccountFactory::predict_address_sync(self, signer, salt_data);
        std::future::ready(Ok(address))
    }

    fn implementation_address(&self) -> impl Future<Output = Result<Address, EngineError>> {
        // Use the sync implementation but return as a ready future
        std::future::ready(Ok(self.implementation_address))
    }
}

#[cfg(test)]
mod tests {

    use alloy::hex::FromHex;
    use alloy::primitives::{Bytes, address};

    use super::*;

    #[test]
    fn test_v07_address_prediction() {
        // Create factory with default addresses
        let factory = DefaultAccountFactory::v0_7();

        // Test inputs
        let signer = address!("0xbe2D2B388635D33b0C9C6d60dE9853716e4b51A3");
        let empty_salt = Bytes::from_hex("0x").unwrap();

        // Expected output
        let expected_address = address!("0xDA15403AF9690C74f30eCC9cCa686fCAD2C897f8");

        // Predict address
        let predicted_address = factory.predict_address_sync(&signer, &empty_salt);

        // Verify result
        assert_eq!(predicted_address, expected_address);
    }

    #[test]
    fn test_v06_address_prediction() {
        // Create factory with default addresses
        let factory = DefaultAccountFactory::v0_6();

        // Test inputs
        let signer = address!("0xbe2D2B388635D33b0C9C6d60dE9853716e4b51A3");
        let empty_salt = Bytes::from_hex("0x").unwrap();

        // Expected output
        let expected_address = address!("0xB7E052ec0BC8B741Ce7cA7B7dFBaECb4B234ffBE");

        // Predict address
        let predicted_address = factory.predict_address_sync(&signer, &empty_salt);

        // Verify result
        assert_eq!(predicted_address, expected_address);
    }
}
