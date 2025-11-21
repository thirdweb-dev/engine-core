use alloy::{
    primitives::{Address, FixedBytes},
    providers::Provider,
};
use engine_core::{
    chain::Chain,
    credentials::SigningCredential,
    error::{AlloyRpcErrorToEngineError, EngineError},
    signer::{AccountSigner, EoaSigningOptions},
};
use rand::Rng;

use crate::constants::{EIP_7702_DELEGATION_CODE_LENGTH, EIP_7702_DELEGATION_PREFIX};

/// Represents an EOA address that can have EIP-7702 delegation, associated with a specific chain
#[derive(Clone, Debug)]
pub struct DelegatedAccount<C: Chain> {
    /// The EOA address that may have delegation
    pub eoa_address: Address,
    /// The chain this account operates on
    pub chain: C,
}

impl<C: Chain> DelegatedAccount<C> {
    /// Create a new delegated account from an EOA address and chain
    pub fn new(eoa_address: Address, chain: C) -> Self {
        Self { eoa_address, chain }
    }

    /// Check if the EOA has EIP-7702 delegation to the minimal account implementation
    pub async fn is_minimal_account(
        &self,
        delegation_contract: Option<Address>,
    ) -> Result<bool, EngineError> {
        // Get the bytecode at the EOA address using eth_getCode
        let code = self
            .chain
            .provider()
            .get_code_at(self.eoa_address)
            .await
            .map_err(|e| e.to_engine_error(self.chain()))?;

        tracing::debug!(
            eoa_address = ?self.eoa_address,
            code_length = code.len(),
            code_hex = ?alloy::hex::encode(&code),
            "Checking EIP-7702 delegation"
        );

        // Check if code exists and starts with EIP-7702 delegation prefix "0xef0100"
        if code.len() < EIP_7702_DELEGATION_CODE_LENGTH
            || !code.starts_with(&EIP_7702_DELEGATION_PREFIX)
        {
            tracing::debug!(
                eoa_address = ?self.eoa_address,
                has_delegation = false,
                reason = "Code too short or doesn't start with EIP-7702 prefix",
                "EIP-7702 delegation check result"
            );
            return Ok(false);
        }

        // Extract the target address from bytes 3-23 (20 bytes for address)
        // EIP-7702 format: 0xef0100 + 20 bytes address

        let target_bytes = &code[3..23];
        let target_address = Address::from_slice(target_bytes);

        // Compare with the minimal account implementation address
        let is_delegated = match delegation_contract {
            Some(delegation_contract) => target_address == delegation_contract,
            None => true,
        };

        tracing::debug!(
            eoa_address = ?self.eoa_address,
            target_address = ?target_address,
            minimal_account_address = ?delegation_contract,
            has_delegation = is_delegated,
            "EIP-7702 delegation check result"
        );

        Ok(is_delegated)
    }

    /// Get the EOA address
    pub fn address(&self) -> Address {
        self.eoa_address
    }

    /// Get the current nonce for the EOA
    pub async fn get_nonce(&self) -> Result<u64, EngineError> {
        self.chain
            .provider()
            .get_transaction_count(self.eoa_address)
            .await
            .map_err(|e| e.to_engine_error(self.chain()))
    }

    /// Get a reference to the chain
    pub fn chain(&self) -> &C {
        &self.chain
    }

    /// Sign authorization for EIP-7702 delegation (automatically fetches nonce)
    pub async fn sign_authorization<S: AccountSigner>(
        &self,
        eoa_signer: &S,
        credentials: &SigningCredential,
        delegation_contract: Address,
    ) -> Result<alloy::eips::eip7702::SignedAuthorization, EngineError> {
        let nonce = self.get_nonce().await?;

        let signing_options = EoaSigningOptions {
            from: self.eoa_address,
            chain_id: Some(self.chain.chain_id()),
        };

        eoa_signer
            .sign_authorization(
                signing_options,
                self.chain.chain_id(),
                delegation_contract,
                nonce,
                credentials,
            )
            .await
    }

    /// Generate a random UID for wrapped calls
    pub fn generate_random_uid() -> FixedBytes<32> {
        let mut rng = rand::rng();
        let mut bytes = [0u8; 32];
        rng.fill(&mut bytes);
        FixedBytes::from(bytes)
    }
}
