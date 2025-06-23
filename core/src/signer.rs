use std::option;

use alloy::{
    dyn_abi::TypedData,
    primitives::{Address, ChainId},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use vault_sdk::VaultClient;
use vault_types::enclave::encrypted::eoa::MessageFormat;

use crate::{
    credentials::SigningCredential,
    defs::AddressDef,
    error::EngineError,
    execution_options::aa::{EntrypointAndFactoryDetails, EntrypointAndFactoryDetailsDeserHelper},
};

/// EOA signing options
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct EoaSigningOptions {
    /// The EOA address to sign with
    #[schemars(with = "AddressDef")]
    #[schema(value_type = AddressDef)]
    pub from: Address,
    /// Optional chain ID for the signature
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chain_id: Option<ChainId>,
}

/// Smart Account signing options
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SmartAccountSigningOptions {
    /// The smart account address (if deployed)
    #[schemars(with = "Option<AddressDef>")]
    #[schema(value_type = Option<AddressDef>)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub smart_account_address: Option<Address>,

    /// The EOA that controls the smart account
    #[schemars(with = "AddressDef")]
    #[schema(value_type = AddressDef)]
    pub signer_address: Address,

    /// Entrypoint and factory configuration
    #[serde(flatten)]
    #[schemars(with = "EntrypointAndFactoryDetailsDeserHelper")]
    #[schema(value_type = EntrypointAndFactoryDetailsDeserHelper)]
    pub entrypoint_details: EntrypointAndFactoryDetails,

    /// Account salt for deterministic addresses
    #[serde(default = "default_account_salt")]
    pub account_salt: String,

    /// Chain ID for smart account operations
    pub chain_id: ChainId,
}

/// Configuration options for signing operations
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum SigningOptions {
    /// Standard EOA (Externally Owned Account) signing
    #[serde(rename = "eoa")]
    #[schema(title = "EOA Signing Options")]
    Eoa(EoaSigningOptions),
    /// Smart Account signing with advanced signature patterns
    #[serde(rename = "smart_account")]
    #[schema(title = "Smart Account Signing Options")]
    SmartAccount(SmartAccountSigningOptions),
}

/// Account signer trait using impl Future pattern like TWMQ
pub trait AccountSigner {
    type SigningOptions;
    /// Sign a message
    fn sign_message(
        &self,
        options: Self::SigningOptions,
        message: &str,
        format: MessageFormat,
        credentials: SigningCredential,
    ) -> impl std::future::Future<Output = Result<String, EngineError>> + Send;

    /// Sign typed data
    fn sign_typed_data(
        &self,
        options: Self::SigningOptions,
        typed_data: &TypedData,
        credentials: SigningCredential,
    ) -> impl std::future::Future<Output = Result<String, EngineError>> + Send;
}

/// EOA signer implementation
#[derive(Clone)]
pub struct EoaSigner {
    pub vault_client: VaultClient,
}

impl EoaSigner {
    /// Create a new EOA signer
    pub fn new(vault_client: VaultClient) -> Self {
        Self { vault_client }
    }
}

impl AccountSigner for EoaSigner {
    type SigningOptions = EoaSigningOptions;

    async fn sign_message(
        &self,
        options: EoaSigningOptions,
        message: &str,
        format: MessageFormat,
        credentials: SigningCredential,
    ) -> Result<String, EngineError> {
        match credentials {
            SigningCredential::Vault(auth_method) => {
                let vault_result = self
                    .vault_client
                    .sign_message(
                        auth_method.clone(),
                        message.to_string(),
                        options.from,
                        options.chain_id,
                        Some(format),
                    )
                    .await
                    .map_err(|e| {
                        tracing::error!("Error signing message with EOA: {:?}", e);
                        EngineError::VaultError {
                            message: e.to_string(),
                        }
                    })?;

                Ok(vault_result.signature)
            }
        }
    }

    async fn sign_typed_data(
        &self,
        options: EoaSigningOptions,
        typed_data: &TypedData,
        credentials: SigningCredential,
    ) -> Result<String, EngineError> {
        match &credentials {
            SigningCredential::Vault(auth_method) => {
                let vault_result = self
                    .vault_client
                    .sign_typed_data(auth_method.clone(), typed_data.clone(), options.from)
                    .await
                    .map_err(|e| {
                        tracing::error!("Error signing typed data with EOA: {:?}", e);
                        EngineError::VaultError {
                            message: e.to_string(),
                        }
                    })?;

                Ok(vault_result.signature)
            }
        }
    }
}

/// Parameters for signing a message (used in routes)
pub struct MessageSignerParams {
    pub credentials: SigningCredential,
    pub message: String,
    pub format: MessageFormat,
    pub signing_options: SigningOptions,
}

/// Parameters for signing typed data (used in routes)
pub struct TypedDataSignerParams {
    pub credentials: SigningCredential,
    pub typed_data: TypedData,
    pub signing_options: SigningOptions,
}

fn default_account_salt() -> String {
    "0x".to_string()
}
