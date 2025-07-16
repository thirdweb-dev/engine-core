use alloy::{
    core::sol_types::SolValue,
    primitives::{Address, B256, Bytes, ChainId, U256, keccak256},
    rpc::types::{PackedUserOperation, UserOperation},
};
use serde::{Deserialize, Serialize};

/// UserOp version enum
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum VersionedUserOp {
    V0_6(UserOperation),
    V0_7(PackedUserOperation),
}

/// Error type for UserOp operations
#[derive(
    Debug,
    Clone,
    thiserror::Error,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
    utoipa::ToSchema,
)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum UserOpError {
    #[error("Unexpected error: {0}")]
    UnexpectedError(String),
}

/// Compute UserOperation v0.6 hash
pub fn compute_user_op_v06_hash(
    op: &UserOperation,
    entrypoint: Address,
    chain_id: ChainId,
) -> Result<B256, UserOpError> {
    // Hash the byte fields first
    let init_code_hash = keccak256(&op.init_code);
    let call_data_hash = keccak256(&op.call_data);
    let paymaster_and_data_hash = keccak256(&op.paymaster_and_data);

    // Create the inner tuple (WITHOUT signature!)
    let inner_tuple = (
        op.sender,
        op.nonce,
        init_code_hash,
        call_data_hash,
        op.call_gas_limit,
        op.verification_gas_limit,
        op.pre_verification_gas,
        op.max_fee_per_gas,
        op.max_priority_fee_per_gas,
        paymaster_and_data_hash,
    );

    // ABI encode and hash the inner tuple
    let inner_encoded = inner_tuple.abi_encode();
    let inner_hash = keccak256(&inner_encoded);

    // Create the outer tuple
    let outer_tuple = (inner_hash, entrypoint, U256::from(chain_id));

    // ABI encode and hash the outer tuple
    let outer_encoded = outer_tuple.abi_encode();
    let final_hash = keccak256(&outer_encoded);
    Ok(final_hash)
}

/// Compute UserOperation v0.7 hash
pub fn compute_user_op_v07_hash(
    op: &PackedUserOperation,
    entrypoint: Address,
    chain_id: ChainId,
) -> Result<B256, UserOpError> {
    // Construct initCode from factory and factoryData
    let init_code: Bytes = if let Some(factory) = op.factory {
        if factory != Address::ZERO {
            [
                &factory[..],
                &op.factory_data.clone().unwrap_or_default()[..],
            ]
            .concat()
            .into()
        } else {
            op.factory_data.clone().unwrap_or_default()
        }
    } else {
        Bytes::default()
    };

    // Construct accountGasLimits
    let vgl_u128: u128 = op.verification_gas_limit.try_into().map_err(|_| {
        UserOpError::UnexpectedError("verification_gas_limit too large".to_string())
    })?;
    let cgl_u128: u128 = op
        .call_gas_limit
        .try_into()
        .map_err(|_| UserOpError::UnexpectedError("call_gas_limit too large".to_string()))?;

    let mut account_gas_limits_bytes = [0u8; 32];
    account_gas_limits_bytes[0..16].copy_from_slice(&vgl_u128.to_be_bytes());
    account_gas_limits_bytes[16..32].copy_from_slice(&cgl_u128.to_be_bytes());
    let account_gas_limits = B256::from(account_gas_limits_bytes);

    // Construct gasFees
    let mpfpg_u128: u128 = op.max_priority_fee_per_gas.try_into().map_err(|_| {
        UserOpError::UnexpectedError("max_priority_fee_per_gas too large".to_string())
    })?;
    let mfpg_u128: u128 = op
        .max_fee_per_gas
        .try_into()
        .map_err(|_| UserOpError::UnexpectedError("max_fee_per_gas too large".to_string()))?;

    let mut gas_fees_bytes = [0u8; 32];
    gas_fees_bytes[0..16].copy_from_slice(&mpfpg_u128.to_be_bytes());
    gas_fees_bytes[16..32].copy_from_slice(&mfpg_u128.to_be_bytes());
    let gas_fees = B256::from(gas_fees_bytes);

    // Construct paymasterAndData
    let paymaster_and_data: Bytes = if let Some(paymaster) = op.paymaster {
        if paymaster != Address::ZERO {
            let pm_vgl_u128: u128 = op
                .paymaster_verification_gas_limit
                .unwrap_or_default()
                .try_into()
                .map_err(|_| {
                    UserOpError::UnexpectedError(
                        "paymaster_verification_gas_limit too large".to_string(),
                    )
                })?;
            let pm_pogl_u128: u128 = op
                .paymaster_post_op_gas_limit
                .unwrap_or_default()
                .try_into()
                .map_err(|_| {
                    UserOpError::UnexpectedError(
                        "paymaster_post_op_gas_limit too large".to_string(),
                    )
                })?;
            [
                &paymaster[..],
                &pm_vgl_u128.to_be_bytes()[..],
                &pm_pogl_u128.to_be_bytes()[..],
                &op.paymaster_data.clone().unwrap_or_default()[..],
            ]
            .concat()
            .into()
        } else {
            op.paymaster_data.clone().unwrap_or_default()
        }
    } else {
        Bytes::default()
    };

    // Hash the byte fields
    let init_code_hash = keccak256(&init_code);
    let call_data_hash = keccak256(&op.call_data);
    let paymaster_and_data_hash = keccak256(&paymaster_and_data);

    // Create the inner tuple
    let inner_tuple = (
        op.sender,
        op.nonce,
        init_code_hash,
        call_data_hash,
        account_gas_limits,
        op.pre_verification_gas,
        gas_fees,
        paymaster_and_data_hash,
    );

    // ABI encode and hash the inner tuple
    let inner_encoded = inner_tuple.abi_encode();
    let inner_hash = keccak256(&inner_encoded);

    // Create the outer tuple
    let outer_tuple = (inner_hash, entrypoint, U256::from(chain_id));

    // ABI encode and hash the outer tuple
    let outer_encoded = outer_tuple.abi_encode();
    let final_hash = keccak256(&outer_encoded);
    Ok(final_hash)
}
