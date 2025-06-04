use crate::{
    constants::{DEFAULT_FACTORY_ADDRESS_V0_6, ENTRYPOINT_ADDRESS_V0_6},
    defs::AddressDef,
};
use alloy::primitives::Address;
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};

use crate::constants::{DEFAULT_FACTORY_ADDRESS_V0_7, ENTRYPOINT_ADDRESS_V0_7};

#[derive(Deserialize, Serialize, Debug, JsonSchema, Clone, Copy)]
pub enum EntrypointVersion {
    #[serde(rename = "0.6")]
    V0_6,
    #[serde(rename = "0.7")]
    V0_7,
}

#[derive(Serialize, Debug, Clone)]
pub struct EntrypointAndFactoryDetails {
    #[serde(rename = "entrypointAddress")]
    pub entrypoint_address: Address,

    #[serde(rename = "entrypointVersion")]
    pub version: EntrypointVersion,

    #[serde(rename = "factoryAddress")]
    pub factory_address: Address,
}

/// # ERC-4337 Execution Options
/// This struct allows flexible configuration of ERC-4337 execution options,
/// with intelligent defaults and inferences based on provided values.
///
/// ## Field Inference
/// When fields are omitted, the system uses the following inference rules:
///
/// 1. **Version Inference**:
///    - If `entrypointVersion` is provided, it's used directly
///    - Otherwise, tries to infer from `entrypointAddress` (if provided)
///    - If that fails, tries to infer from `factoryAddress` (if provided)
///    - Defaults to version 0.7 if no inference is possible
///
/// 2. **Entrypoint Address Inference**:
///    - If provided explicitly, it's used as-is
///    - Otherwise, uses the default address corresponding to the inferred version:
///      - V0.6: 0x5FF137D4b0FDCD49DcA30c7CF57E578a026d2789
///      - V0.7: 0x0576a174D229E3cFA37253523E645A78A0C91B57
///
/// 3. **Factory Address Inference**:
///    - If provided explicitly, it's used as-is
///    - Otherwise, uses the default factory corresponding to the inferred version:
///      - V0.6: [DEFAULT_FACTORY_ADDRESS_V0_6]
///      - V0.7: [DEFAULT_FACTORY_ADDRESS_V0_7]
///
/// 4. **Account Salt**:
///    - If provided explicitly, it's used as-is
///    - Otherwise, defaults to "0x" (commonly used as the defauult "null" salt for smart accounts)
///
/// 5. **Smart Account Address**:
///    - If provided explicitly, it's used as-is
///    - Otherwise, it's read from the smart account factory
///
/// All optional fields can be omitted for a minimal configuration using version 0.7 defaults.
#[derive(Deserialize, Serialize, Debug, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Erc4337ExecutionOptions {
    #[schemars(with = "AddressDef")]
    pub signer_address: Address,

    #[serde(flatten)]
    #[schemars(with = "EntrypointAndFactoryDetailsDeserHelper")]
    pub entrypoint_details: EntrypointAndFactoryDetails,

    #[serde(default = "default_account_salt")]
    pub account_salt: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option::<AddressDef>")]
    pub smart_account_address: Option<Address>,
}

pub fn default_factory_address() -> Address {
    DEFAULT_FACTORY_ADDRESS_V0_7
}

pub fn default_entrypoint_address() -> Address {
    ENTRYPOINT_ADDRESS_V0_7
}

pub fn default_account_salt() -> String {
    "0x".to_string()
}
#[derive(Deserialize, JsonSchema)]
struct EntrypointAndFactoryDetailsDeserHelper {
    /// # Entrypoint Contract Address
    /// The address of the ERC-4337 entrypoint contract.
    ///
    /// If omitted, defaults to the standard address for the specified/inferred version.
    ///
    /// Known addresses:
    ///
    /// - V0.6: 0x5FF137D4b0FDCD49DcA30c7CF57E578a026d2789
    ///
    /// - V0.7: 0x0000000071727De22E5E9d8BAf0edAc6f37da032
    #[serde(rename = "entrypointAddress")]
    #[schemars(with = "Option<AddressDef>")]
    entrypoint_address: Option<Address>,

    /// # Entrypoint Version
    /// The version of the ERC-4337 standard to use.
    ///
    /// If omitted, the version will be inferred from the entrypoint address,
    /// then from factory address, or defaults to V0.7.
    #[serde(rename = "entrypointVersion")]
    version: Option<EntrypointVersion>,

    /// # Account Factory Address
    /// The address of the smart account factory contract.
    /// If omitted, defaults to the thirweb default account factory for the specified/inferred version.
    ///
    /// Known addresses:
    ///
    /// - V0.6: 0x85e23b94e7F5E9cC1fF78BCe78cfb15B81f0DF00
    ///
    /// - V0.7: 0x4bE0ddfebcA9A5A4a617dee4DeCe99E7c862dceb
    #[serde(rename = "factoryAddress")]
    #[schemars(with = "Option<AddressDef>")]
    factory_address: Option<Address>,
}

impl EntrypointAndFactoryDetails {
    // Helper function to get default entrypoint address for a version
    fn default_entrypoint_for_version(version: EntrypointVersion) -> Address {
        match version {
            EntrypointVersion::V0_6 => ENTRYPOINT_ADDRESS_V0_6,
            EntrypointVersion::V0_7 => ENTRYPOINT_ADDRESS_V0_7,
        }
    }

    // Helper function to get default factory address for a version
    fn default_factory_for_version(version: EntrypointVersion) -> Address {
        match version {
            EntrypointVersion::V0_6 => DEFAULT_FACTORY_ADDRESS_V0_6,
            EntrypointVersion::V0_7 => DEFAULT_FACTORY_ADDRESS_V0_7,
        }
    }

    // Helper to infer version from entrypoint address
    fn infer_version_from_entrypoint(address: &Address) -> Option<EntrypointVersion> {
        if *address == ENTRYPOINT_ADDRESS_V0_6 {
            Some(EntrypointVersion::V0_6)
        } else if *address == ENTRYPOINT_ADDRESS_V0_7 {
            Some(EntrypointVersion::V0_7)
        } else {
            None
        }
    }

    // Helper to infer version from factory address
    fn infer_version_from_factory(address: &Address) -> Option<EntrypointVersion> {
        if *address == DEFAULT_FACTORY_ADDRESS_V0_6 {
            Some(EntrypointVersion::V0_6)
        } else if *address == DEFAULT_FACTORY_ADDRESS_V0_7 {
            Some(EntrypointVersion::V0_7)
        } else {
            None
        }
    }
}

// Define how to convert from the helper to the real struct
impl From<EntrypointAndFactoryDetailsDeserHelper> for EntrypointAndFactoryDetails {
    fn from(helper: EntrypointAndFactoryDetailsDeserHelper) -> Self {
        // Default version to use if we can't infer anything better
        let default_version = EntrypointVersion::V0_7;

        // If version is explicitly provided, use it
        let version = match helper.version {
            Some(v) => v,
            None => {
                // Try to infer version from entrypoint address
                if let Some(entrypoint) = helper.entrypoint_address {
                    if let Some(v) = Self::infer_version_from_entrypoint(&entrypoint) {
                        v
                    } else {
                        // Try to infer from factory address
                        helper
                            .factory_address
                            .as_ref()
                            .and_then(Self::infer_version_from_factory)
                            .unwrap_or(default_version)
                    }
                } else {
                    // Try to infer from factory address
                    helper
                        .factory_address
                        .as_ref()
                        .and_then(Self::infer_version_from_factory)
                        .unwrap_or(default_version)
                }
            }
        };

        // Use provided entrypoint address or default for the version
        let entrypoint_address = helper
            .entrypoint_address
            .unwrap_or_else(|| Self::default_entrypoint_for_version(version));

        // Use provided factory address or default for the version
        let factory_address = helper
            .factory_address
            .unwrap_or_else(|| Self::default_factory_for_version(version));

        EntrypointAndFactoryDetails {
            entrypoint_address,
            version,
            factory_address,
        }
    }
}

// Add the custom deserializer
impl<'de> Deserialize<'de> for EntrypointAndFactoryDetails {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        EntrypointAndFactoryDetailsDeserHelper::deserialize(deserializer).map(Into::into)
    }
}
