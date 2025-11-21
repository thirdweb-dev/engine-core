use alloy::{
    dyn_abi::TypedData,
    primitives::{Address, U256},
    sol,
    sol_types::{SolCall, eip712_domain},
};
use engine_core::{
    chain::Chain,
    credentials::SigningCredential,
    error::{AlloyRpcErrorToEngineError, EngineError},
    signer::{AccountSigner, EoaSigner, EoaSigningOptions},
    transaction::InnerTransaction,
};
use serde_json::Value;
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::delegated_account::DelegatedAccount;

sol!(
    #[derive(serde::Serialize, serde::Deserialize)]
    struct Call {
        address target;
        uint256 value;
        bytes data;
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    struct WrappedCalls {
        Call[] calls;
        bytes32 uid;
    }

    function execute(Call[] calldata calls) external payable;
    function executeWithSig(WrappedCalls calldata wrappedCalls, bytes calldata signature) external payable;

    #[derive(Serialize_repr, Deserialize_repr)]
    enum LimitType {
        Unlimited,
        Lifetime,
        Allowance,
    }

    #[derive(Serialize_repr, Deserialize_repr)]
    enum Condition {
        Unconstrained,
        Equal,
        Greater,
        Less,
        GreaterOrEqual,
        LessOrEqual,
        NotEqual,
    }

    #[derive(serde::Serialize)]
    struct UsageLimit {
        LimitType limitType;
        uint256 limit; // ignored if limitType == Unlimited
        uint256 period; // ignored if limitType != Allowance
    }

    #[derive(serde::Serialize)]
    struct Constraint {
        Condition condition;
        uint64 index;
        bytes32 refValue;
        UsageLimit limit;
    }

    #[derive(serde::Serialize)]
    struct CallSpec {
        address target;
        bytes4 selector;
        uint256 maxValuePerUse;
        UsageLimit valueLimit;
        Constraint[] constraints;
    }

    #[derive(serde::Serialize)]
    struct TransferSpec {
        address target;
        uint256 maxValuePerUse;
        UsageLimit valueLimit;
    }

    #[derive(serde::Serialize)]
    struct SessionSpec {
        address signer;
        bool isWildcard;
        uint256 expiresAt;
        CallSpec[] callPolicies;
        TransferSpec[] transferPolicies;
        bytes32 uid;
    }

    function createSessionWithSig(SessionSpec calldata sessionSpec, bytes calldata signature) external;
);

/// A transaction for a minimal account that supports signing and execution via bundler
pub struct MinimalAccountTransaction<C: Chain> {
    /// The delegated account this transaction belongs to
    account: DelegatedAccount<C>,
    /// The raw transactions to be wrapped
    wrapped_calls: WrappedCalls,
    /// Authorization if needed for delegation setup
    authorization: Option<alloy::eips::eip7702::SignedAuthorization>,
}

impl<C: Chain> DelegatedAccount<C> {
    /// Create a transaction for a session key address to execute on a target account
    /// The session key address is the signer for this transaction, ie, they will sign the wrapped calls
    /// The thirdweb executor is only responsible for calling executeWithSig
    /// The flow is:
    /// thirdweb executor -> executeWithSig(wrapped_calls) on the session key address
    /// session key address -> execute(wrapped_calls.calls) on the target account
    pub fn session_key_transaction(
        self,
        target_account: Address,
        transactions: &[InnerTransaction],
    ) -> MinimalAccountTransaction<C> {
        // First take all the inner transactions, and convert them to calls
        // These are all the calls that the session key address wants to make on the target account
        let inner_calls = transactions
            .iter()
            .map(|tx| Call {
                target: tx.to.unwrap_or_default(),
                value: tx.value,
                data: tx.data.clone(),
            })
            .collect();

        // then get the calldata for calling the execute function (on the target account) with these calls
        let outer_call = executeCall { calls: inner_calls };

        // the session key address wants to call the execute function on the target account with these calls
        let session_key_call = Call {
            target: target_account,
            value: U256::ZERO,
            data: outer_call.abi_encode().into(),
        };

        // but the session key address still wants the "executor" (thirdweb bundler) to sponsor the transaction
        // so the session key call is wrapped in a WrappedCalls struct
        // the session key address is the signer for this transaction, ie, they will sign the wrapped calls
        // the thirdweb executor is only responsible for calling executeWithSig
        // the flow is:
        // thirdweb executor -> executeWithSig(wrapped_calls) on the session key address
        // session key address -> execute(wrapped_calls.calls) on the target account
        let wrapped_calls = WrappedCalls {
            calls: vec![session_key_call],
            uid: Self::generate_random_uid(),
        };

        MinimalAccountTransaction {
            account: self,
            wrapped_calls,
            authorization: None,
        }
    }

    pub fn owner_transaction(
        self,
        transactions: &[InnerTransaction],
    ) -> MinimalAccountTransaction<C> {
        let inner_calls = transactions
            .iter()
            .map(|tx| Call {
                target: tx.to.unwrap_or_default(),
                value: tx.value,
                data: tx.data.clone(),
            })
            .collect();

        let wrapped_calls = WrappedCalls {
            calls: inner_calls,
            uid: Self::generate_random_uid(),
        };

        MinimalAccountTransaction {
            account: self,
            wrapped_calls,
            authorization: None,
        }
    }
}

impl<C: Chain> MinimalAccountTransaction<C> {
    /// Set the authorization for delegation setup
    pub fn set_authorization(&mut self, authorization: alloy::eips::eip7702::SignedAuthorization) {
        self.authorization = Some(authorization);
    }

    pub async fn add_authorization_if_needed<S: AccountSigner>(
        mut self,
        signer: &S,
        credentials: &SigningCredential,
        delegation_contract: Address,
    ) -> Result<Self, EngineError> {
        if self
            .account
            .is_minimal_account(Some(delegation_contract))
            .await?
        {
            return Ok(self);
        }

        let authorization = self
            .account
            .sign_authorization(signer, credentials, delegation_contract)
            .await?;
        self.authorization = Some(authorization);
        Ok(self)
    }

    /// Build the transaction data as JSON for bundler execution with automatic signing
    pub async fn build<S: AccountSigner>(
        &self,
        signer: &S,
        credentials: &SigningCredential,
    ) -> Result<(Value, String), EngineError> {
        let signature = self.sign_wrapped_calls(signer, credentials).await?;

        // Serialize wrapped calls to JSON
        let wrapped_calls_json = serde_json::to_value(&self.wrapped_calls).map_err(|e| {
            EngineError::ValidationError {
                message: format!("Failed to serialize wrapped calls: {e}"),
            }
        })?;

        Ok((wrapped_calls_json, signature))
    }

    pub fn calldata_for_self_execution(&self) -> Vec<u8> {
        let calls = self.wrapped_calls.calls.clone();

        let execute_call = executeCall { calls };

        execute_call.abi_encode()
    }

    /// Execute the transaction directly via bundler client
    /// This builds the transaction and calls tw_execute on the bundler
    pub async fn execute(
        &self,
        eoa_signer: &EoaSigner,
        credentials: &SigningCredential,
    ) -> Result<String, EngineError> {
        let (wrapped_calls_json, signature) = self.build(eoa_signer, credentials).await?;

        self.account
            .chain()
            .bundler_client()
            .tw_execute(
                self.account.address(),
                &wrapped_calls_json,
                &signature,
                self.authorization.as_ref(),
            )
            .await
            .map_err(|e| e.to_engine_bundler_error(self.account.chain()))
    }

    /// Get the account this transaction belongs to
    pub fn account(&self) -> &DelegatedAccount<C> {
        &self.account
    }

    /// Get the authorization if set  
    pub fn authorization(&self) -> Option<&alloy::eips::eip7702::SignedAuthorization> {
        self.authorization.as_ref()
    }

    pub async fn sign_wrapped_calls<S: AccountSigner>(
        &self,
        signer: &S,
        credentials: &SigningCredential,
    ) -> Result<String, EngineError> {
        let typed_data = self.create_wrapped_calls_typed_data();
        self.sign_typed_data(signer, credentials, &typed_data).await
    }

    /// Sign typed data with EOA signer
    async fn sign_typed_data<S: AccountSigner>(
        &self,
        signer: &S,
        credentials: &SigningCredential,
        typed_data: &TypedData,
    ) -> Result<String, EngineError> {
        let signing_options = EoaSigningOptions {
            from: self.account.address(),
            chain_id: Some(self.account.chain().chain_id()),
        };

        signer
            .sign_typed_data(signing_options, typed_data, credentials)
            .await
    }

    /// Create typed data for signing wrapped calls using Alloy's native types
    fn create_wrapped_calls_typed_data(&self) -> TypedData {
        let domain = eip712_domain! {
            name: "MinimalAccount",
            version: "1",
            chain_id: self.account.chain().chain_id(),
            verifying_contract: self.account.address(),
        };

        // Use Alloy's native TypedData creation from struct
        TypedData::from_struct(&self.wrapped_calls, Some(domain))
    }
}
