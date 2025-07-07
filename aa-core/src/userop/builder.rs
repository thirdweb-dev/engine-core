use std::sync::Arc;

use alloy::{
    hex,
    primitives::{Address, Bytes, U256},
    rpc::types::{PackedUserOperation, UserOperation},
};
use engine_aa_types::VersionedUserOp;
use engine_core::{
    chain::Chain,
    credentials::SigningCredential,
    error::{AlloyRpcErrorToEngineError, EngineError},
    execution_options::aa::{EntrypointAndFactoryDetails, EntrypointVersion},
    userop::{UserOpSigner, UserOpSignerParams},
};

pub struct UserOpBuilderConfig<'a, C: Chain> {
    pub account_address: Address,
    pub signer_address: Address,
    pub entrypoint_and_factory: EntrypointAndFactoryDetails,
    pub call_data: Bytes,
    pub init_call_data: Vec<u8>,
    pub is_deployed: bool,
    pub nonce: U256,
    pub credential: SigningCredential,
    pub chain: &'a C,
    pub signer: Arc<UserOpSigner>,
}

pub struct UserOpBuilder<'a, C: Chain> {
    config: UserOpBuilderConfig<'a, C>,
}

const DUMMY_SIGNATURE: [u8; 65] = hex!(
    "0xfffffffffffffffffffffffffffffff0000000000000000000000000000000007aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1c"
);

impl<'a, C: Chain> UserOpBuilder<'a, C> {
    pub fn new(config: UserOpBuilderConfig<'a, C>) -> Self {
        Self { config }
    }

    pub async fn build(self) -> Result<VersionedUserOp, EngineError> {
        let mut userop = match self.config.entrypoint_and_factory.version {
            EntrypointVersion::V0_6 => UserOpBuilderV0_6::new(&self.config).build().await?,
            EntrypointVersion::V0_7 => UserOpBuilderV0_7::new(&self.config).build().await?,
        };

        tracing::debug!("UserOp built, proceeding with signing");

        let signature = self
            .config
            .signer
            .sign(UserOpSignerParams {
                credentials: self.config.credential.clone(),
                entrypoint: self.config.entrypoint_and_factory.entrypoint_address,
                userop: userop.clone(),
                signer_address: self.config.signer_address,
                chain_id: self.config.chain.chain_id(),
            })
            .await?;

        match &mut userop {
            VersionedUserOp::V0_6(userop) => {
                userop.signature = signature;
            }
            VersionedUserOp::V0_7(userop) => {
                userop.signature = signature;
            }
        }

        tracing::debug!("UserOp signed succcessfully");

        Ok(userop)
    }
}

struct UserOpBuilderV0_6<'a, C: Chain> {
    userop: UserOperation,
    entrypoint: Address,
    chain: &'a C,
}

impl<'a, C: Chain> UserOpBuilderV0_6<'a, C> {
    pub fn new(config: &UserOpBuilderConfig<'a, C>) -> Self {
        let initcode: Bytes = if config.is_deployed {
            Bytes::default()
        } else {
            let mut initcode: Vec<u8> = config
                .entrypoint_and_factory
                .factory_address
                .into_array()
                .to_vec();

            initcode.extend_from_slice(config.init_call_data.as_slice());
            Bytes::from(initcode)
        };
        Self {
            userop: UserOperation {
                sender: config.account_address,
                nonce: config.nonce,
                init_code: initcode,
                call_data: config.call_data.clone(),
                call_gas_limit: U256::ZERO,
                verification_gas_limit: U256::ZERO,
                pre_verification_gas: U256::ZERO,
                max_fee_per_gas: U256::ZERO,
                max_priority_fee_per_gas: U256::ZERO,
                paymaster_and_data: Bytes::default(),
                signature: Bytes::from(DUMMY_SIGNATURE),
            },
            entrypoint: config.entrypoint_and_factory.entrypoint_address,
            chain: config.chain,
        }
    }

    async fn build(mut self) -> Result<VersionedUserOp, EngineError> {
        // let prices = self
        //     .chain
        //     .provider()
        //     .estimate_eip1559_fees()
        //     .await
        //     .map_err(|err| err.to_engine_error(self.chain))?;

        // TODO: modularize this so only used with thirdweb paymaster
        let prices = self
            .chain
            .paymaster_client()
            .get_user_op_gas_fees()
            .await
            .map_err(|e| e.to_engine_error(self.chain))?;

        tracing::debug!("Gas prices determined");

        self.userop.max_fee_per_gas = U256::from(prices.max_fee_per_gas);
        self.userop.max_priority_fee_per_gas = U256::from(prices.max_priority_fee_per_gas);

        let pm_response = self
            .chain
            .paymaster_client()
            .get_user_op_paymaster_and_data_v0_6(&self.userop, self.entrypoint)
            .await
            .map_err(|err| err.to_engine_paymaster_error(self.chain))?;

        tracing::debug!("v6 Userop paymaster and data determined");

        self.userop.paymaster_and_data = pm_response.paymaster_and_data;

        let (call_gas_limit, verification_gas_limit, pre_verification_gas) = match (
            pm_response.call_gas_limit,
            pm_response.verification_gas_limit,
            pm_response.pre_verification_gas,
        ) {
            (Some(call_gas_limit), Some(verification_gas_limit), Some(pre_verification_gas)) => {
                (call_gas_limit, verification_gas_limit, pre_verification_gas)
            }
            _ => {
                tracing::debug!("No paymaster provided gas limits, getting from bundler");

                let bundler_response = self
                    .chain
                    .bundler_client()
                    .estimate_user_op_gas(
                        &VersionedUserOp::V0_6(self.userop.clone()),
                        self.entrypoint,
                        None,
                    )
                    .await
                    .map_err(|err| err.to_engine_bundler_error(self.chain))?;

                (
                    bundler_response.call_gas_limit,
                    bundler_response.verification_gas_limit,
                    bundler_response.pre_verification_gas,
                )
            }
        };

        self.userop.call_gas_limit = call_gas_limit;
        self.userop.verification_gas_limit = verification_gas_limit;
        self.userop.pre_verification_gas = pre_verification_gas;

        Ok(VersionedUserOp::V0_6(self.userop))
    }
}

// New V0.7 Builder Implementation
struct UserOpBuilderV0_7<'a, C: Chain> {
    userop: PackedUserOperation,
    entrypoint: Address,
    chain: &'a C,
}

impl<'a, C: Chain> UserOpBuilderV0_7<'a, C> {
    pub fn new(config: &UserOpBuilderConfig<'a, C>) -> Self {
        let (factory, factory_data) = if !config.is_deployed {
            // If not deployed, set factory and factory data
            (
                Some(config.entrypoint_and_factory.factory_address),
                Some(Bytes::from(config.init_call_data.clone())),
            )
        } else {
            // If deployed, use None for factory and empty bytes for factory data
            (None, None)
        };

        Self {
            userop: PackedUserOperation {
                sender: config.account_address,
                nonce: config.nonce,
                factory,
                factory_data,
                call_data: config.call_data.clone(),
                call_gas_limit: U256::ZERO,
                verification_gas_limit: U256::ZERO,
                pre_verification_gas: U256::ZERO,
                max_fee_per_gas: U256::ZERO,
                max_priority_fee_per_gas: U256::ZERO,
                paymaster: None,
                paymaster_data: None,
                paymaster_verification_gas_limit: None,
                paymaster_post_op_gas_limit: None,
                signature: Bytes::from(DUMMY_SIGNATURE),
            },
            entrypoint: config.entrypoint_and_factory.entrypoint_address,
            chain: config.chain,
        }
    }

    async fn build(mut self) -> Result<VersionedUserOp, EngineError> {
        // Get gas prices, same as v0.6
        // let prices = self
        //     .chain
        //     .provider()
        //     .estimate_eip1559_fees()
        //     .await
        //     .map_err(|err| err.to_engine_error(self.chain))?;

        // TODO: modularize this so only used with thirdweb paymaster
        let prices = self
            .chain
            .paymaster_client()
            .get_user_op_gas_fees()
            .await
            .map_err(|e| e.to_engine_error(self.chain))?;

        tracing::info!("Gas prices determined");

        self.userop.max_fee_per_gas = U256::from(prices.max_fee_per_gas);
        self.userop.max_priority_fee_per_gas = U256::from(prices.max_priority_fee_per_gas);

        // Get paymaster data
        let pm_response = self
            .chain
            .paymaster_client()
            .get_user_op_paymaster_and_data_v0_7(&self.userop, self.entrypoint)
            .await
            .map_err(|err| err.to_engine_paymaster_error(self.chain))?;

        tracing::debug!("v7 Userop paymaster and data determined");

        // Apply paymaster data
        self.userop.paymaster = Some(pm_response.paymaster);
        self.userop.paymaster_data = Some(pm_response.paymaster_data);

        // Determine gas limits - either from paymaster or from bundler
        let (
            call_gas_limit,
            verification_gas_limit,
            pre_verification_gas,
            paymaster_verification_gas_limit,
            paymaster_post_op_gas_limit,
        ) = match (
            pm_response.call_gas_limit,
            pm_response.verification_gas_limit,
            pm_response.pre_verification_gas,
            pm_response.paymaster_verification_gas_limit,
            pm_response.paymaster_post_op_gas_limit,
        ) {
            (Some(call), Some(verification), Some(pre), Some(pm_verification), Some(pm_post)) => {
                (call, verification, pre, pm_verification, pm_post)
            }
            _ => {
                // If paymaster didn't provide all gas limits, get them from the bundler
                tracing::debug!("No paymaster provided gas limits, getting from bundler");

                let bundler_response = self
                    .chain
                    .bundler_client()
                    .estimate_user_op_gas(
                        &VersionedUserOp::V0_7(self.userop.clone()),
                        self.entrypoint,
                        None,
                    )
                    .await
                    .map_err(|err| err.to_engine_bundler_error(self.chain))?;

                (
                    bundler_response.call_gas_limit,
                    bundler_response.verification_gas_limit,
                    bundler_response.pre_verification_gas,
                    bundler_response.paymaster_verification_gas_limit,
                    bundler_response
                        .paymaster_post_op_gas_limit
                        .unwrap_or_default(),
                )
            }
        };

        tracing::debug!("Gas limits determined");

        // Set gas limits
        self.userop.call_gas_limit = call_gas_limit;
        self.userop.verification_gas_limit = verification_gas_limit;
        self.userop.pre_verification_gas = pre_verification_gas;
        self.userop.paymaster_verification_gas_limit = Some(paymaster_verification_gas_limit);
        self.userop.paymaster_post_op_gas_limit = Some(paymaster_post_op_gas_limit);

        Ok(VersionedUserOp::V0_7(self.userop))
    }
}
