use alloy::eips::eip1559::Eip1559Estimation;
use alloy::primitives::{Address, Bytes, U256};
use alloy::rpc::client::RpcClient;
use alloy::rpc::types::{PackedUserOperation, UserOperation};
use alloy::transports::{IntoBoxTransport, TransportResult};
use serde::{Deserialize, Serialize};

/// Paymaster result for v0.6
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PaymasterResultV06 {
    pub pre_verification_gas: Option<U256>,
    pub verification_gas_limit: Option<U256>,
    pub call_gas_limit: Option<U256>,
    pub paymaster_and_data: Bytes,
}

/// Paymaster result for v0.7
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PaymasterResultV07 {
    pub pre_verification_gas: Option<U256>,
    pub verification_gas_limit: Option<U256>,
    pub call_gas_limit: Option<U256>,
    pub paymaster: Address,
    pub paymaster_data: Bytes,
    pub paymaster_verification_gas_limit: Option<U256>,
    pub paymaster_post_op_gas_limit: Option<U256>,
}

/// A JSON-RPC client for interacting with an ERC-4337 bundler and paymaster
#[derive(Debug, Clone)]
pub struct PaymasterClient {
    pub inner: RpcClient,
}

impl PaymasterClient {
    /// Create a new bundler client with the given transport
    pub fn new(transport: impl IntoBoxTransport) -> Self {
        let client = RpcClient::builder().transport(transport, false);
        client.next_id();
        Self { inner: client }
    }

    /// Get the user operation gas fees
    pub async fn get_user_op_gas_fees(&self) -> TransportResult<Eip1559Estimation> {
        self.inner
            .request("thirdweb_getUserOperationGasPrice", ())
            .await
    }

    /// Get paymaster and data for a user operation
    pub async fn get_user_op_paymaster_and_data_v0_6(
        &self,
        userop: &UserOperation,
        entrypoint: Address,
    ) -> TransportResult<PaymasterResultV06> {
        self.inner
            .request("pm_sponsorUserOperation", (userop, entrypoint))
            .await
    }

    /// Get paymaster and data for a user operation
    pub async fn get_user_op_paymaster_and_data_v0_7(
        &self,
        userop: &PackedUserOperation,
        entrypoint: Address,
    ) -> TransportResult<PaymasterResultV07> {
        self.inner
            .request("pm_sponsorUserOperation", (userop, entrypoint))
            .await
    }
}
