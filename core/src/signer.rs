use alloy::{
    consensus::TypedTransaction,
    dyn_abi::TypedData,
    eips::eip7702::SignedAuthorization,
    hex::{self, FromHex},
    network::TxSigner,
    primitives::{Address, Bytes, ChainId, U256},
    rpc::types::Authorization,
    signers::{Signer, SignerSync},
};
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, PickFirst, serde_as};
use thirdweb_core::iaw::IAWClient;
use vault_sdk::VaultClient;
use vault_types::enclave::encrypted::eoa::MessageFormat;

use crate::{
    credentials::SigningCredential,
    defs::AddressDef,
    error::{EngineError, SerialisableAwsSdkError, SerialisableAwsSignerError},
    execution_options::aa::{EntrypointAndFactoryDetails, EntrypointAndFactoryDetailsDeserHelper},
};

/// EOA signing options
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct EoaSigningOptions {
    /// The EOA address to sign with
    #[schema(value_type = AddressDef)]
    pub from: Address,
    /// Optional chain ID for the signature
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde_as(as = "Option<PickFirst<(_, DisplayFromStr)>>")]
    pub chain_id: Option<ChainId>,
}

/// ### ERC4337 (Smart Account) Signing Options
/// This struct allows flexible configuration of ERC-4337 signing options,
/// with intelligent defaults and inferences based on provided values.
///
/// ### Field Inference
/// When fields are omitted, the system uses the following inference rules:
///
/// 1. Version Inference:
///     - If `entrypointVersion` is provided, it's used directly
///     - Otherwise, tries to infer from `entrypointAddress` (if provided)
///     - If that fails, tries to infer from `factoryAddress` (if provided)
///     - Defaults to version 0.7 if no inference is possible
///
/// 2. Entrypoint Address Inference:
///    - If provided explicitly, it's used as-is
///    - Otherwise, uses the default address corresponding to the inferred version:
///      - V0.6: 0x5FF137D4b0FDCD49DcA30c7CF57E578a026d2789
///      - V0.7: 0x0576a174D229E3cFA37253523E645A78A0C91B57
///
/// 3. Factory Address Inference:
///    - If provided explicitly, it's used as-is
///    - Otherwise, uses the default factory corresponding to the inferred version:
///      - V0.6: 0x85e23b94e7F5E9cC1fF78BCe78cfb15B81f0DF00 [DEFAULT_FACTORY_ADDRESS_V0_6]
///      - V0.7: 0x4bE0ddfebcA9A5A4a617dee4DeCe99E7c862dceb [DEFAULT_FACTORY_ADDRESS_V0_7]
///
/// 4. Account Salt:
///    - If provided explicitly, it's used as-is
///    - Otherwise, defaults to "0x" (commonly used as the defauult "null" salt for smart accounts)
///
/// 5. Smart Account Address:
///    - If provided explicitly, it's used as-is
///    - Otherwise, it's read from the smart account factory
///
/// All optional fields can be omitted for a minimal configuration using version 0.7 defaults.
///
/// The most minimal usage only requires `signerAddress` + `chainId`
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Erc4337SigningOptions {
    /// The smart account address (if deployed)
    #[schema(value_type = Option<AddressDef>)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub smart_account_address: Option<Address>,

    /// The EOA that controls the smart account
    #[schema(value_type = AddressDef)]
    pub signer_address: Address,

    #[serde(flatten)]
    #[schema(value_type = EntrypointAndFactoryDetailsDeserHelper)]
    pub entrypoint_details: EntrypointAndFactoryDetails,

    /// Account salt for deterministic addresses
    #[serde(default = "default_account_salt")]
    pub account_salt: String,

    /// Chain ID for smart account operations
    #[serde_as(as = "PickFirst<(_, DisplayFromStr)>")]
    pub chain_id: ChainId,
}

impl Erc4337SigningOptions {
    /// Parse account salt into Bytes, handling both hex and plain string formats
    pub fn get_salt_data(&self) -> Result<Bytes, EngineError> {
        if self.account_salt.starts_with("0x") {
            Bytes::from_hex(&self.account_salt).map_err(|e| EngineError::ValidationError {
                message: format!("Failed to parse hex salt: {}", e),
            })
        } else {
            let hex_string = alloy::hex::encode(&self.account_salt);
            Bytes::from_hex(hex_string).map_err(|e| EngineError::ValidationError {
                message: format!("Failed to encode salt as hex: {}", e),
            })
        }
    }
}

/// Configuration options for signing operations
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum SigningOptions {
    #[serde(rename = "EOA")]
    #[schema(title = "EOA Signing Options")]
    Eoa(EoaSigningOptions),

    /// ### ERC4337 (Smart Account) Signing Options
    /// This struct allows flexible configuration of ERC-4337 signing options,
    /// with intelligent defaults and inferences based on provided values.
    ///
    /// ### Field Inference
    /// When fields are omitted, the system uses the following inference rules:
    ///
    /// 1. Version Inference:
    ///     - If `entrypointVersion` is provided, it's used directly
    ///     - Otherwise, tries to infer from `entrypointAddress` (if provided)
    ///     - If that fails, tries to infer from `factoryAddress` (if provided)
    ///     - Defaults to version 0.7 if no inference is possible
    ///
    /// 2. Entrypoint Address Inference:
    ///    - If provided explicitly, it's used as-is
    ///    - Otherwise, uses the default address corresponding to the inferred version:
    ///      - V0.6: 0x5FF137D4b0FDCD49DcA30c7CF57E578a026d2789
    ///      - V0.7: 0x0576a174D229E3cFA37253523E645A78A0C91B57
    ///
    /// 3. Factory Address Inference:
    ///    - If provided explicitly, it's used as-is
    ///    - Otherwise, uses the default factory corresponding to the inferred version:
    ///      - V0.6: 0x85e23b94e7F5E9cC1fF78BCe78cfb15B81f0DF00 [DEFAULT_FACTORY_ADDRESS_V0_6]
    ///      - V0.7: 0x4bE0ddfebcA9A5A4a617dee4DeCe99E7c862dceb [DEFAULT_FACTORY_ADDRESS_V0_7]
    ///
    /// 4. Account Salt:
    ///    - If provided explicitly, it's used as-is
    ///    - Otherwise, defaults to "0x" (commonly used as the defauult "null" salt for smart accounts)
    ///
    /// 5. Smart Account Address:
    ///    - If provided explicitly, it's used as-is
    ///    - Otherwise, it's read from the smart account factory
    ///
    /// All optional fields can be omitted for a minimal configuration using version 0.7 defaults.
    ///
    /// The most minimal usage only requires `signerAddress` + `chainId`
    #[serde(rename = "ERC4337")]
    #[schema(title = "ERC4337 Signing Options")]
    ERC4337(Erc4337SigningOptions),
}

pub trait AccountSigner {
    /// Sign a message
    fn sign_message(
        &self,
        options: EoaSigningOptions,
        message: &str,
        format: MessageFormat,
        credentials: &SigningCredential,
    ) -> impl std::future::Future<Output = Result<String, EngineError>> + Send;

    /// Sign typed data
    fn sign_typed_data(
        &self,
        options: EoaSigningOptions,
        typed_data: &TypedData,
        credentials: &SigningCredential,
    ) -> impl std::future::Future<Output = Result<String, EngineError>> + Send;

    /// Sign a transaction
    fn sign_transaction(
        &self,
        options: EoaSigningOptions,
        transaction: &TypedTransaction,
        credentials: &SigningCredential,
    ) -> impl std::future::Future<Output = Result<String, EngineError>> + Send;

    /// Sign EIP-7702 authorization
    fn sign_authorization(
        &self,
        options: EoaSigningOptions,
        chain_id: u64,
        address: Address,
        nonce: u64,
        credentials: &SigningCredential,
    ) -> impl std::future::Future<Output = Result<SignedAuthorization, EngineError>> + Send;
}

/// EOA signer implementation
#[derive(Clone)]
pub struct EoaSigner {
    pub vault_client: VaultClient,
    pub iaw_client: IAWClient,
}

impl EoaSigner {
    /// Create a new EOA signer
    pub fn new(vault_client: VaultClient, iaw_client: IAWClient) -> Self {
        Self {
            vault_client,
            iaw_client,
        }
    }
}

impl AccountSigner for EoaSigner {
    async fn sign_message(
        &self,
        options: EoaSigningOptions,
        message: &str,
        format: MessageFormat,
        credentials: &SigningCredential,
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
                        tracing::error!("Error signing message with EOA (Vault): {:?}", e);
                        e
                    })?;

                Ok(vault_result.signature)
            }
            SigningCredential::Iaw {
                auth_token,
                thirdweb_auth,
            } => {
                // Convert MessageFormat to IAW MessageFormat
                let iaw_format = match format {
                    MessageFormat::Text => thirdweb_core::iaw::MessageFormat::Text,
                    MessageFormat::Hex => thirdweb_core::iaw::MessageFormat::Hex,
                };

                let iaw_result = self
                    .iaw_client
                    .sign_message(
                        auth_token,
                        thirdweb_auth,
                        message,
                        options.from,
                        options.chain_id,
                        Some(iaw_format),
                    )
                    .await
                    .map_err(|e| {
                        tracing::error!("Error signing message with EOA (IAW): {:?}", e);
                        EngineError::from(e)
                    })?;

                Ok(iaw_result.signature)
            }
            SigningCredential::AwsKms(creds) => {
                let signer = creds.get_signer(options.chain_id).await?;
                let message = match format {
                    MessageFormat::Text => message.to_string().into_bytes(),
                    MessageFormat::Hex => {
                        hex::decode(message).map_err(|_| EngineError::ValidationError {
                            message: "Invalid hex string".to_string(),
                        })?
                    }
                };

                // TODO: create serialisable error for @alloy-signer::error::Error
                let signature = signer.sign_message(&message).await.map_err(|e| {
                    tracing::error!("Error signing message with EOA (AWS KMS): {:?}", e);
                    EngineError::AwsKmsSignerError {
                        error: SerialisableAwsSignerError::Sign {
                            aws_sdk_error: SerialisableAwsSdkError::Other {
                                message: e.to_string(),
                            },
                        },
                    }
                })?;
                Ok(signature.to_string())
            }
            SigningCredential::PrivateKey(signer) => {
                let message_bytes = match format {
                    MessageFormat::Text => message.to_string().into_bytes(),
                    MessageFormat::Hex => {
                        alloy::hex::decode(message).map_err(|_| EngineError::ValidationError {
                            message: "Invalid hex string".to_string(),
                        })?
                    }
                };

                let signature = signer.sign_message(&message_bytes).await.map_err(|e| {
                    tracing::error!("Error signing message with EOA (PrivateKey): {:?}", e);
                    EngineError::ValidationError {
                        message: format!("Failed to sign message: {}", e),
                    }
                })?;
                Ok(signature.to_string())
            }
        }
    }

    async fn sign_typed_data(
        &self,
        options: EoaSigningOptions,
        typed_data: &TypedData,
        credentials: &SigningCredential,
    ) -> Result<String, EngineError> {
        match &credentials {
            SigningCredential::Vault(auth_method) => {
                let vault_result = self
                    .vault_client
                    .sign_typed_data(auth_method.clone(), typed_data.clone(), options.from)
                    .await
                    .map_err(|e| {
                        tracing::error!("Error signing typed data with EOA (Vault): {:?}", e);
                        e
                    })?;

                Ok(vault_result.signature)
            }
            SigningCredential::Iaw {
                auth_token,
                thirdweb_auth,
            } => {
                let iaw_result = self
                    .iaw_client
                    .sign_typed_data(auth_token, thirdweb_auth, typed_data, options.from)
                    .await
                    .map_err(|e| {
                        tracing::error!("Error signing typed data with EOA (IAW): {:?}", e);
                        EngineError::from(e)
                    })?;

                Ok(iaw_result.signature)
            }

            SigningCredential::AwsKms(creds) => {
                let signer = creds.get_signer(options.chain_id).await?;

                // TODO: create serialisable error for @alloy-signer::error::Error
                let signature = signer
                    .sign_dynamic_typed_data(typed_data)
                    .await
                    .map_err(|e| {
                        tracing::error!("Error signing message with EOA (AWS KMS): {:?}", e);
                        EngineError::AwsKmsSignerError {
                            error: SerialisableAwsSignerError::Sign {
                                aws_sdk_error: SerialisableAwsSdkError::Other {
                                    message: e.to_string(),
                                },
                            },
                        }
                    })?;
                Ok(signature.to_string())
            }
            SigningCredential::PrivateKey(signer) => {
                let signature = signer
                    .sign_dynamic_typed_data(typed_data)
                    .await
                    .map_err(|e| {
                        tracing::error!("Error signing typed data with EOA (PrivateKey): {:?}", e);
                        EngineError::ValidationError {
                            message: format!("Failed to sign typed data: {}", e),
                        }
                    })?;
                Ok(signature.to_string())
            }
        }
    }

    async fn sign_transaction(
        &self,
        options: EoaSigningOptions,
        transaction: &TypedTransaction,
        credentials: &SigningCredential,
    ) -> Result<String, EngineError> {
        match credentials {
            SigningCredential::Vault(auth_method) => {
                let vault_result = self
                    .vault_client
                    .sign_transaction(auth_method.clone(), transaction.clone(), options.from)
                    .await
                    .map_err(|e| {
                        tracing::error!("Error signing transaction with EOA (Vault): {:?}", e);
                        e
                    })?;

                Ok(vault_result.signature)
            }
            SigningCredential::Iaw {
                auth_token,
                thirdweb_auth,
            } => {
                let iaw_result = self
                    .iaw_client
                    .sign_transaction(auth_token, thirdweb_auth, transaction)
                    .await
                    .map_err(|e| {
                        tracing::error!("Error signing transaction with EOA (IAW): {:?}", e);
                        EngineError::from(e)
                    })?;

                Ok(iaw_result.signature)
            }
            SigningCredential::AwsKms(creds) => {
                let signer = creds.get_signer(options.chain_id).await?;
                let mut transaction = transaction.clone();

                // TODO: create serialisable error for @alloy-signer::error::Error
                let signature = signer
                    .sign_transaction(&mut transaction)
                    .await
                    .map_err(|e| {
                        tracing::error!("Error signing message with EOA (AWS KMS): {:?}", e);
                        EngineError::AwsKmsSignerError {
                            error: SerialisableAwsSignerError::Sign {
                                aws_sdk_error: SerialisableAwsSdkError::Other {
                                    message: e.to_string(),
                                },
                            },
                        }
                    })?;
                Ok(signature.to_string())
            }
            SigningCredential::PrivateKey(signer) => {
                let mut transaction = transaction.clone();
                let signature = signer
                    .sign_transaction(&mut transaction)
                    .await
                    .map_err(|e| {
                        tracing::error!("Error signing transaction with EOA (PrivateKey): {:?}", e);
                        EngineError::ValidationError {
                            message: format!("Failed to sign transaction: {}", e),
                        }
                    })?;
                Ok(signature.to_string())
            }
        }
    }

    async fn sign_authorization(
        &self,
        options: EoaSigningOptions,
        chain_id: u64,
        address: Address,
        nonce: u64,
        credentials: &SigningCredential,
    ) -> Result<SignedAuthorization, EngineError> {
        // Create the Authorization struct that both clients expect
        let authorization = Authorization {
            chain_id: U256::from(chain_id),
            address,
            nonce,
        };
        match credentials {
            SigningCredential::Vault(auth_method) => {
                let vault_result = self
                    .vault_client
                    .sign_authorization(auth_method.clone(), options.from, authorization)
                    .await
                    .map_err(|e| {
                        tracing::error!("Error signing authorization with EOA (Vault): {:?}", e);
                        e
                    })?;

                // Return the signed authorization as Authorization
                Ok(vault_result.signed_authorization)
            }
            SigningCredential::Iaw {
                auth_token,
                thirdweb_auth,
            } => {
                let iaw_result = self
                    .iaw_client
                    .sign_authorization(auth_token, thirdweb_auth, options.from, &authorization)
                    .await
                    .map_err(|e| {
                        tracing::error!("Error signing authorization with EOA (IAW): {:?}", e);
                        e
                    })?;

                // Return the signed authorization as Authorization
                Ok(iaw_result.signed_authorization)
            }
            SigningCredential::AwsKms(creds) => {
                let signer = creds.get_signer(options.chain_id).await?;
                let authorization_hash = authorization.signature_hash();

                let signature = signer.sign_hash(&authorization_hash).await.map_err(|e| {
                    tracing::error!("Error signing authorization with EOA (AWS KMS): {:?}", e);
                    EngineError::AwsKmsSignerError {
                        error: SerialisableAwsSignerError::Sign {
                            aws_sdk_error: SerialisableAwsSdkError::Other {
                                message: e.to_string(),
                            },
                        },
                    }
                })?;

                Ok(authorization.into_signed(signature))
            }
            SigningCredential::PrivateKey(signer) => {
                let authorization_hash = authorization.signature_hash();
                let signature = signer.sign_hash_sync(&authorization_hash).map_err(|e| {
                    tracing::error!("Error signing authorization with EOA (PrivateKey): {:?}", e);
                    EngineError::ValidationError {
                        message: format!("Failed to sign authorization: {}", e),
                    }
                })?;

                Ok(authorization.into_signed(signature))
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
