use alloy::primitives::{Address, Bytes, U256};
use alloy::rpc::client::RpcClient;
use alloy::rpc::types::UserOperationReceipt;
use alloy::transports::{IntoBoxTransport, TransportResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::userop::UserOpVersion;

// Gas buffer added for managed account factories (matches TypeScript)
pub const MANAGED_ACCOUNT_GAS_BUFFER: U256 = U256::from_limbs([21_000, 0, 0, 0]);

/// A JSON-RPC client for interacting with an ERC-4337 bundler and paymaster
#[derive(Debug, Clone)]
pub struct BundlerClient {
    inner: RpcClient,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UseropGasEstimation {
    pub call_gas_limit: U256,
    pub verification_gas_limit: U256,
    pub pre_verification_gas: U256,
    #[serde(alias = "paymasterVerificationGas")]
    pub paymaster_verification_gas_limit: U256,
    #[serde(alias = "paymasterPostOpGas")]
    pub paymaster_post_op_gas_limit: Option<U256>,
}

impl BundlerClient {
    /// Create a new bundler client with the given transport
    pub fn new(transport: impl IntoBoxTransport) -> Self {
        let client = RpcClient::builder().transport(transport, false);

        Self { inner: client }
    }

    /// Get a user operation receipt by hash
    pub async fn get_user_op_receipt(
        &self,
        user_op_hash: Bytes,
    ) -> TransportResult<Option<UserOperationReceipt>> {
        self.inner
            .request("eth_getUserOperationReceipt", [user_op_hash])
            .await
    }

    /// Estimate the gas for a user operation
    pub async fn estimate_user_op_gas(
        &self,
        user_op: &UserOpVersion,
        entrypoint: Address,
        state_overrides: Option<HashMap<String, HashMap<String, String>>>,
    ) -> TransportResult<UseropGasEstimation> {
        let state_overrides = state_overrides.unwrap_or_default();

        // Convert the result and apply gas buffer to match TypeScript implementation
        let result: UseropGasEstimation = self
            .inner
            .request(
                "eth_estimateUserOperationGas",
                (user_op, entrypoint, state_overrides),
            )
            .await?;

        Ok(UseropGasEstimation {
            call_gas_limit: result.call_gas_limit + MANAGED_ACCOUNT_GAS_BUFFER,
            ..result
        })
    }

    pub async fn send_user_op(
        &self,
        user_op: &UserOpVersion,
        entrypoint: Address,
    ) -> TransportResult<Bytes> {
        let result: Bytes = self
            .inner
            .request("eth_sendUserOperation", (user_op, entrypoint))
            .await?;

        Ok(result)
    }
}
