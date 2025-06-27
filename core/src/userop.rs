use alloy::{
    hex::FromHex,
    primitives::{Address, Bytes, ChainId},
};
use thirdweb_core::iaw::IAWClient;
use types_core::UserOpVersion;
use vault_sdk::VaultClient;
use vault_types::{
    enclave::encrypted::eoa::StructuredMessageInput,
    userop::{UserOperationV06Input, UserOperationV07Input},
};

use crate::{credentials::SigningCredential, error::EngineError};

#[derive(Clone)]
pub struct UserOpSigner {
    pub vault_client: VaultClient,
    pub iaw_client: IAWClient,
}

pub struct UserOpSignerParams {
    pub credentials: SigningCredential,
    pub entrypoint: Address,
    pub userop: UserOpVersion,
    pub signer_address: Address,
    pub chain_id: ChainId,
}

fn userop_to_vault_input(userop: &UserOpVersion, entrypoint: Address) -> StructuredMessageInput {
    match userop {
        UserOpVersion::V0_6(userop) => {
            StructuredMessageInput::UserOperationV06Input(UserOperationV06Input {
                call_data: userop.call_data.clone(),
                init_code: userop.init_code.clone(),
                nonce: userop.nonce,
                pre_verification_gas: userop.pre_verification_gas,
                max_fee_per_gas: userop.max_fee_per_gas,
                verification_gas_limit: userop.verification_gas_limit,
                sender: userop.sender,
                paymaster_and_data: userop.paymaster_and_data.clone(),
                signature: userop.signature.clone(),
                call_gas_limit: userop.call_gas_limit,
                max_priority_fee_per_gas: userop.max_priority_fee_per_gas,
                entrypoint,
            })
        }
        UserOpVersion::V0_7(userop) => {
            StructuredMessageInput::UserOperationV07Input(UserOperationV07Input {
                call_data: userop.call_data.clone(),
                nonce: userop.nonce,
                pre_verification_gas: userop.pre_verification_gas,
                max_fee_per_gas: userop.max_fee_per_gas,
                verification_gas_limit: userop.verification_gas_limit,
                sender: userop.sender,
                paymaster_data: userop.paymaster_data.clone().unwrap_or_default(),
                factory: userop.factory.unwrap_or_default(),
                factory_data: userop.factory_data.clone().unwrap_or_default(),
                paymaster_post_op_gas_limit: userop
                    .paymaster_post_op_gas_limit
                    .unwrap_or_default(),
                paymaster_verification_gas_limit: userop
                    .paymaster_verification_gas_limit
                    .unwrap_or_default(),
                signature: userop.signature.clone(),
                call_gas_limit: userop.call_gas_limit,
                max_priority_fee_per_gas: userop.max_priority_fee_per_gas,
                paymaster: userop.paymaster.unwrap_or_default(),
                entrypoint,
            })
        }
    }
}

impl UserOpSigner {
    pub async fn sign(&self, params: UserOpSignerParams) -> Result<Bytes, EngineError> {
        match &params.credentials {
            SigningCredential::Vault(auth_method) => {
                let vault_result = self
                    .vault_client
                    .sign_structured_message(
                        auth_method.clone(),
                        params.signer_address,
                        userop_to_vault_input(&params.userop, params.entrypoint),
                        Some(params.chain_id),
                    )
                    .await
                    .map_err(|e| {
                        tracing::error!("Error signing userop: {:?}", e);
                        e
                    })?;

                Ok(Bytes::from_hex(vault_result.signature).map_err(|_| {
                    EngineError::VaultError {
                        message: "Bad signature received from vault".to_string(),
                    }
                })?)
            }
            SigningCredential::Iaw { auth_token, thirdweb_auth } => {
                let result = self.iaw_client.sign_userop(
                    auth_token.clone(),
                    thirdweb_auth.clone(),
                    params.userop,
                    params.entrypoint,
                    params.signer_address,
                    params.chain_id,
                ).await.map_err(|e| EngineError::ValidationError {
                    message: format!("Failed to sign userop: {}", e),
                })?;
                
                Ok(Bytes::from_hex(&result.signature).map_err(|_| {
                    EngineError::ValidationError {
                        message: "Bad signature received from IAW".to_string(),
                    }
                })?)
            }
        }
    }
}
