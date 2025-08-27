use std::time::Duration;

use alloy::{
    consensus::{SignableTransaction, TypedTransaction},
    eips::eip7702::SignedAuthorization,
    network::{EthereumWallet, TransactionBuilder, TransactionBuilder7702, TxSigner},
    primitives::{Address, Bytes, U256},
    providers::{
        DynProvider, Identity, Provider, ProviderBuilder, RootProvider,
        ext::AnvilApi,
        fillers::{
            BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller,
            WalletFiller,
        },
        layers::AnvilProvider,
    },
    rpc::types::{TransactionReceipt, TransactionRequest},
    signers::Signer,
    sol,
    sol_types::SolCall,
};
use engine_core::{
    chain::Chain,
    credentials::SigningCredential,
    error::EngineError,
    signer::{AccountSigner, EoaSigningOptions},
    transaction::InnerTransaction,
};
use engine_eip7702_core::{
    delegated_account::DelegatedAccount,
    transaction::{CallSpec, LimitType, SessionSpec, WrappedCalls},
};
use serde_json::Value;
use tokio::time::sleep;

// Mock ERC20 contract
sol! {
  #[allow(missing_docs)]
  #[sol(rpc, bytecode=include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/bytecode/erc20.hex")))]
  contract MockERC20 {
      mapping(address => uint256) public balanceOf;
      mapping(address => mapping(address => uint256)) public allowance;
      uint256 public totalSupply;
      string public name;
      string public symbol;
      uint8 public decimals;

      event Transfer(address indexed from, address indexed to, uint256 value);
      event Approval(address indexed owner, address indexed spender, uint256 value);

      constructor(string memory _name, string memory _symbol) {
          name = _name;
          symbol = _symbol;
          decimals = 18;
      }

      function mint(uint256 amount) public {
          balanceOf[msg.sender] += amount;
          totalSupply += amount;
          emit Transfer(address(0), msg.sender, amount);
      }

      function mintTo(address to, uint256 amount) public {
          balanceOf[to] += amount;
          totalSupply += amount;
          emit Transfer(address(0), to, amount);
      }

      function transfer(address to, uint256 amount) public returns (bool) {
          require(balanceOf[msg.sender] >= amount, "Insufficient balance");
          balanceOf[msg.sender] -= amount;
          balanceOf[to] += amount;
          emit Transfer(msg.sender, to, amount);
          return true;
      }

      function approve(address spender, uint256 amount) public returns (bool) {
          allowance[msg.sender][spender] = amount;
          emit Approval(msg.sender, spender, amount);
          return true;
      }

      function transferFrom(address from, address to, uint256 amount) public returns (bool) {
          require(balanceOf[from] >= amount, "Insufficient balance");
          require(allowance[from][msg.sender] >= amount, "Insufficient allowance");

          balanceOf[from] -= amount;
          balanceOf[to] += amount;
          allowance[from][msg.sender] -= amount;

          emit Transfer(from, to, amount);
          return true;
      }

      function burn(uint256 amount) public {
          require(balanceOf[msg.sender] >= amount, "Insufficient balance");
          balanceOf[msg.sender] -= amount;
          totalSupply -= amount;
          emit Transfer(msg.sender, address(0), amount);
      }
  }
}

// Test chain implementation
#[derive(Clone)]
struct TestChain {
    chain_id: u64,
    provider: alloy::providers::DynProvider,
    rpc_url: alloy::transports::http::reqwest::Url,
}

impl Chain for TestChain {
    fn chain_id(&self) -> u64 {
        self.chain_id
    }

    fn rpc_url(&self) -> alloy::transports::http::reqwest::Url {
        self.rpc_url.clone()
    }

    fn bundler_url(&self) -> alloy::transports::http::reqwest::Url {
        // For tests, we'll just use the same URL
        self.rpc_url.clone()
    }

    fn paymaster_url(&self) -> alloy::transports::http::reqwest::Url {
        // For tests, we'll just use the same URL
        self.rpc_url.clone()
    }

    fn provider(&self) -> &alloy::providers::RootProvider {
        self.provider.root()
    }

    fn bundler_client(&self) -> &engine_core::rpc_clients::BundlerClient {
        // For tests, we don't need real bundler client
        panic!("bundler_client not implemented for test chain")
    }

    fn paymaster_client(&self) -> &engine_core::rpc_clients::PaymasterClient {
        // For tests, we don't need real paymaster client
        panic!("paymaster_client not implemented for test chain")
    }

    fn bundler_client_with_headers(
        &self,
        _headers: alloy::transports::http::reqwest::header::HeaderMap,
    ) -> engine_core::rpc_clients::BundlerClient {
        panic!("bundler_client_with_headers not implemented for test chain")
    }

    fn paymaster_client_with_headers(
        &self,
        _headers: alloy::transports::http::reqwest::header::HeaderMap,
    ) -> engine_core::rpc_clients::PaymasterClient {
        panic!("paymaster_client_with_headers not implemented for test chain")
    }

    fn with_new_default_headers(
        &self,
        _headers: alloy::transports::http::reqwest::header::HeaderMap,
    ) -> Self {
        self.clone()
    }
}

// Mock EoaSigner for tests
struct MockEoaSigner;

impl AccountSigner for MockEoaSigner {
    async fn sign_message(
        &self,
        _options: EoaSigningOptions,
        _message: &str,
        _format: vault_types::enclave::encrypted::eoa::MessageFormat,
        credentials: &SigningCredential,
    ) -> Result<String, EngineError> {
        match credentials {
            SigningCredential::PrivateKey(signer) => {
                let message_bytes = _message.as_bytes();
                let signature = signer.sign_message(message_bytes).await.map_err(|e| {
                    EngineError::ValidationError {
                        message: format!("Failed to sign message: {e}"),
                    }
                })?;
                Ok(signature.to_string())
            }
            _ => Err(EngineError::ValidationError {
                message: "Only PrivateKey credentials supported in mock".to_string(),
            }),
        }
    }

    async fn sign_typed_data(
        &self,
        _options: EoaSigningOptions,
        typed_data: &alloy::dyn_abi::TypedData,
        credentials: &SigningCredential,
    ) -> Result<String, EngineError> {
        match credentials {
            SigningCredential::PrivateKey(signer) => {
                let signature = signer
                    .sign_dynamic_typed_data(typed_data)
                    .await
                    .map_err(|e| EngineError::ValidationError {
                        message: format!("Failed to sign typed data: {e}"),
                    })?;
                Ok(signature.to_string())
            }
            _ => Err(EngineError::ValidationError {
                message: "Only PrivateKey credentials supported in mock".to_string(),
            }),
        }
    }

    async fn sign_transaction(
        &self,
        _options: EoaSigningOptions,
        transaction: &TypedTransaction,
        credentials: &SigningCredential,
    ) -> Result<String, EngineError> {
        match credentials {
            SigningCredential::PrivateKey(signer) => {
                let mut tx = transaction.clone();
                let signature = signer.sign_transaction(&mut tx).await.map_err(|e| {
                    EngineError::ValidationError {
                        message: format!("Failed to sign transaction: {e}"),
                    }
                })?;
                Ok(signature.to_string())
            }
            _ => Err(EngineError::ValidationError {
                message: "Only PrivateKey credentials supported in mock".to_string(),
            }),
        }
    }

    async fn sign_authorization(
        &self,
        _options: EoaSigningOptions,
        chain_id: u64,
        address: Address,
        nonce: u64,
        credentials: &SigningCredential,
    ) -> Result<SignedAuthorization, EngineError> {
        match credentials {
            SigningCredential::PrivateKey(signer) => {
                let authorization = alloy::rpc::types::Authorization {
                    chain_id: U256::from(chain_id),
                    address,
                    nonce,
                };
                let authorization_hash = authorization.signature_hash();
                let signature = signer.sign_hash(&authorization_hash).await.map_err(|e| {
                    EngineError::ValidationError {
                        message: format!("Failed to sign authorization: {e}"),
                    }
                })?;
                Ok(authorization.into_signed(signature))
            }
            _ => Err(EngineError::ValidationError {
                message: "Only PrivateKey credentials supported in mock".to_string(),
            }),
        }
    }
}

struct TestSetup {
    chain: TestChain,

    anvil_provider: FillProvider<
        JoinFill<
            JoinFill<
                Identity,
                JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
            >,
            WalletFiller<EthereumWallet>,
        >,
        AnvilProvider<RootProvider>,
    >,
    mock_erc20_contract: crate::MockERC20::MockERC20Instance<DynProvider>,

    executor_credentials: SigningCredential,
    developer_credentials: SigningCredential,
    user_credentials: SigningCredential,

    executor_address: Address,
    developer_address: Address,
    user_address: Address,

    signer: MockEoaSigner,
    delegation_contract: Option<Address>,
}

const ANVIL_PORT: u16 = 8545;

impl TestSetup {
    async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        // let wallet = anvil.wallet().unwrap();
        let provider = ProviderBuilder::new().connect_anvil_with_wallet_and_config(|anvil| {
            anvil
                .port(ANVIL_PORT)
                .prague()
                .chain_id(31337)
                .block_time(1)
        })?;
        // .wallet(wallet)
        // .connect_http(anvil.endpoint_url());

        // Deploy the contract directly with sol! macro
        let contract = MockERC20::deploy(
            provider.clone().erased(),
            "TestToken".to_string(),
            "TEST".to_string(),
        )
        .await?;

        println!("✅ Deployed MockERC20 at: {}", contract.address());
        let chain = TestChain {
            chain_id: 31337, // Anvil default chain ID
            provider: provider.clone().erased(),
            rpc_url: "http://localhost:8545".parse()?,
        };

        // Create random credentials for our test EOAs
        let executor_credentials = SigningCredential::random_local();
        let developer_credentials = SigningCredential::random_local();
        let user_credentials = SigningCredential::random_local();

        // Extract addresses from the credentials
        let executor_address = match &executor_credentials {
            SigningCredential::PrivateKey(signer) => signer.address(),
            _ => panic!("Expected PrivateKey credential"),
        };
        let developer_address = match &developer_credentials {
            SigningCredential::PrivateKey(signer) => signer.address(),
            _ => panic!("Expected PrivateKey credential"),
        };
        let user_address = match &user_credentials {
            SigningCredential::PrivateKey(signer) => signer.address(),
            _ => panic!("Expected PrivateKey credential"),
        };

        // Create a mock EoaSigner for tests - we'll implement the trait ourselves
        let signer = MockEoaSigner;

        // Fund the test accounts
        Self::fund_account(&chain, executor_address).await?;

        Ok(TestSetup {
            chain,
            executor_credentials,
            developer_credentials,
            user_credentials,
            executor_address,
            developer_address,
            user_address,
            signer,
            mock_erc20_contract: contract,
            anvil_provider: provider,
            delegation_contract: None,
        })
    }

    async fn fund_account(
        chain: &TestChain,
        address: Address,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Use anvil_setBalance to fund the account
        let balance = U256::from(100).pow(U256::from(18));
        let _: () = chain
            .provider()
            .client()
            .request("anvil_setBalance", (address, format!("0x{balance:x}")))
            .await?;

        Ok(())
    }

    async fn fetch_and_set_bytecode(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Fetch bytecode from Base Sepolia
        let base_sepolia_url = "https://84532.rpc.thirdweb.com".parse()?;
        let base_sepolia_provider = ProviderBuilder::new().connect_http(base_sepolia_url);

        let delegation_contract_response = self
            .chain
            .bundler_client()
            .tw_get_delegation_contract()
            .await?;

        // Store the delegation contract address for later use
        self.delegation_contract = Some(delegation_contract_response.delegation_contract);

        let bytecode = base_sepolia_provider
            .get_code_at(delegation_contract_response.delegation_contract)
            .await?;
        // Set bytecode on our Anvil chain
        let _: () = self
            .anvil_provider
            .anvil_set_code(delegation_contract_response.delegation_contract, bytecode)
            .await?;

        println!(
            "Set bytecode for minimal account implementation at {:?}",
            delegation_contract_response.delegation_contract
        );

        Ok(())
    }

    async fn executor_broadcasts_authorization_for_account(
        &self,
        target_address: Address, // the account being delegated
        authorization: SignedAuthorization,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Executor pays gas to delegate someone else's account
        let tx_request = TransactionRequest::default()
            .with_from(self.executor_address)
            .with_chain_id(self.chain.chain_id())
            .with_to(target_address) // Self-transaction on target to trigger delegation
            .with_value(U256::ZERO)
            .with_authorization_list(vec![authorization])
            .with_gas_limit(100000)
            .with_max_fee_per_gas(20_000_000_000u128) // 20 gwei
            .with_max_priority_fee_per_gas(1_000_000_000u128); // 1 gwei

        // Get the current nonce for executor
        let nonce = self
            .chain
            .provider()
            .get_transaction_count(self.executor_address)
            .await?;
        let tx_request = tx_request.with_nonce(nonce);

        let gas_fees = self.chain.provider().estimate_eip1559_fees().await?;

        let tx_request = tx_request
            .with_nonce(nonce)
            .with_max_fee_per_gas(gas_fees.max_fee_per_gas)
            .with_max_priority_fee_per_gas(gas_fees.max_priority_fee_per_gas);

        // Convert to TypedTransaction for signing
        let mut typed_tx: TypedTransaction = tx_request.build_typed_tx().unwrap();

        // Executor signs the transaction
        let signature_str = match &self.executor_credentials {
            SigningCredential::PrivateKey(signer) => {
                let sig = signer.sign_transaction(&mut typed_tx).await?;
                sig.to_string()
            }
            _ => panic!("Expected PrivateKey credential for tests"),
        };

        // Parse signature and create signed transaction
        let signature: alloy::primitives::Signature = signature_str.parse()?;
        let signed_tx = typed_tx.into_signed(signature);

        // Send the transaction
        let pending = self
            .chain
            .provider()
            .send_tx_envelope(signed_tx.into())
            .await?;
        let _receipt = pending.get_receipt().await?;

        Ok(())
    }

    async fn execute_wrapped_calls_via_delegated_account(
        &self,
        target_address: Address,
        wrapped_calls: &Value,
        signature: &str,
        authorization: Option<&SignedAuthorization>,
    ) -> Result<TransactionReceipt, Box<dyn std::error::Error>> {
        use engine_eip7702_core::transaction::executeWithSigCall;

        // Parse the wrapped calls back to the proper format
        let wrapped_calls: WrappedCalls = serde_json::from_value(wrapped_calls.clone())?;

        // Create executeWithSig call data
        let execute_call = executeWithSigCall {
            wrappedCalls: wrapped_calls,
            signature: signature.parse::<Bytes>()?,
        };

        let call_data = execute_call.abi_encode();

        let tx_request = TransactionRequest::default()
            .with_from(self.executor_address)
            .with_chain_id(self.chain.chain_id())
            .with_to(target_address)
            .with_value(U256::ZERO)
            .with_input(call_data)
            .with_gas_limit(500000);

        // Add authorization if provided (for initial delegation)
        let tx_request = if let Some(auth) = authorization {
            tx_request.with_authorization_list(vec![auth.clone()])
        } else {
            tx_request
        };

        let nonce = self
            .chain
            .provider()
            .get_transaction_count(self.executor_address)
            .await?;

        let gas_fees = self.chain.provider().estimate_eip1559_fees().await?;

        let tx_request = tx_request
            .with_nonce(nonce)
            .with_max_fee_per_gas(gas_fees.max_fee_per_gas)
            .with_max_priority_fee_per_gas(gas_fees.max_priority_fee_per_gas);

        let mut typed_tx: TypedTransaction = tx_request.build_typed_tx().unwrap();

        let signature_str = match &self.executor_credentials {
            SigningCredential::PrivateKey(signer) => {
                let sig = signer.sign_transaction(&mut typed_tx).await?;
                sig.to_string()
            }
            _ => panic!("Expected PrivateKey credential for tests"),
        };

        let signature: alloy::primitives::Signature = signature_str.parse()?;
        let signed_tx = typed_tx.into_signed(signature);

        let pending = self
            .anvil_provider
            .send_tx_envelope(signed_tx.into())
            .await?;
        let receipt = pending.get_receipt().await?;

        Ok(receipt)
    }

    fn create_session_spec_for_granter_and_grantee(&self) -> SessionSpec {
        use engine_eip7702_core::transaction::UsageLimit;
        use std::time::{SystemTime, UNIX_EPOCH};

        let expires_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600; // 1 hour from now

        SessionSpec {
            signer: self.developer_address, // developer (grantee) is the signer who can use this session
            isWildcard: false,
            expiresAt: U256::from(expires_at),
            callPolicies: vec![CallSpec {
                target: self.mock_erc20_contract.address().to_owned(),
                selector: MockERC20::mintCall::SELECTOR.into(),
                maxValuePerUse: U256::ZERO,
                valueLimit: UsageLimit {
                    limitType: LimitType::Unlimited,
                    limit: U256::ZERO,
                    period: U256::ZERO,
                },
                constraints: vec![],
            }],
            transferPolicies: vec![],
            uid: [0u8; 32].into(),
        }
    }

    async fn user_signs_session_spec(
        &self,
        session_spec: &SessionSpec,
    ) -> Result<String, Box<dyn std::error::Error>> {
        use alloy::dyn_abi::TypedData;
        use alloy::sol_types::eip712_domain;

        let domain = eip712_domain! {
            name: "MinimalAccount",
            version: "1",
            chain_id: self.chain.chain_id(),
            verifying_contract: self.user_address,
        };

        let typed_data = TypedData::from_struct(session_spec, Some(domain));

        let signature = self
            .signer
            .sign_typed_data(
                EoaSigningOptions {
                    from: self.user_address,
                    chain_id: Some(self.chain.chain_id()),
                },
                &typed_data,
                &self.user_credentials,
            )
            .await?;

        Ok(signature)
    }

    async fn executor_establishes_session_on_granter_account(
        &self,
        session_spec: &SessionSpec,
        granter_session_signature: &str,
    ) -> Result<TransactionReceipt, Box<dyn std::error::Error>> {
        use engine_eip7702_core::transaction::createSessionWithSigCall;

        let create_session_call = createSessionWithSigCall {
            sessionSpec: session_spec.clone(),
            signature: granter_session_signature.parse::<Bytes>()?,
        };

        let call_data = create_session_call.abi_encode();

        let tx_request = TransactionRequest::default()
            .with_from(self.executor_address)
            .with_chain_id(self.chain.chain_id())
            .with_to(self.user_address) // call createSessionWithSig on granter's delegated account
            .with_value(U256::ZERO)
            .with_input(call_data)
            .with_gas_limit(500000);

        let nonce = self
            .chain
            .provider()
            .get_transaction_count(self.executor_address)
            .await?;
        let tx_request = tx_request.with_nonce(nonce);

        let gas_fees = self.chain.provider().estimate_eip1559_fees().await?;

        let tx_request = tx_request
            .with_nonce(nonce)
            .with_max_fee_per_gas(gas_fees.max_fee_per_gas)
            .with_max_priority_fee_per_gas(gas_fees.max_priority_fee_per_gas);

        let mut typed_tx: TypedTransaction = tx_request.build_typed_tx().unwrap();

        let signature = match &self.executor_credentials {
            SigningCredential::PrivateKey(signer) => signer.sign_transaction(&mut typed_tx).await?,
            _ => panic!("Expected PrivateKey credential for tests"),
        };

        let signed_tx = typed_tx.into_signed(signature);

        let pending = self
            .chain
            .provider()
            .send_tx_envelope(signed_tx.into())
            .await?;
        let receipt = pending.get_receipt().await?;

        Ok(receipt)
    }
}

#[tokio::test]
async fn test_eip7702_integration() -> Result<(), Box<dyn std::error::Error>> {
    // Set up test environment
    let mut setup = TestSetup::new().await?;

    // Step 1: Fetch and set bytecode from Base Sepolia
    setup.fetch_and_set_bytecode().await?;
    // Step 2: Create delegated accounts for each EOA
    let developer_account = DelegatedAccount::new(setup.developer_address, setup.chain.clone());
    let user_account = DelegatedAccount::new(setup.user_address, setup.chain.clone());

    // Step 3: Test is_minimal_account - all should be false initially
    assert!(
        !developer_account.is_minimal_account().await?,
        "Developer should not be minimal account initially"
    );
    assert!(
        !user_account.is_minimal_account().await?,
        "User should not be minimal account initially"
    );
    println!("✓ All accounts are not minimal accounts initially");

    // Step 7: Test owner_transaction - developer mints tokens to themselves
    let mint_call_data = MockERC20::mintCall {
        amount: U256::from(1000),
    }
    .abi_encode();

    let erc20_address = setup.mock_erc20_contract.address().to_owned();

    let mint_transaction = InnerTransaction {
        to: Some(erc20_address),
        data: mint_call_data.into(),
        value: U256::ZERO,
        gas_limit: None,
        transaction_type_data: None,
    };

    let developer_tx = developer_account
        .clone()
        .owner_transaction(&[mint_transaction])
        .add_authorization_if_needed(
            &setup.signer, 
            &setup.developer_credentials,
            setup.delegation_contract.expect("Delegation contract should be set")
        )
        .await?;

    let (wrapped_calls_json, signature) = developer_tx
        .build(&setup.signer, &setup.developer_credentials)
        .await?;

    // Execute the wrapped calls via executeWithSig on the delegated developer account
    setup
        .execute_wrapped_calls_via_delegated_account(
            setup.developer_address,
            &wrapped_calls_json,
            &signature,
            developer_tx.authorization(),
        )
        .await?;

    // Check developer's balance - this should now reflect the executed wrapped calls
    let developer_balance = setup
        .mock_erc20_contract
        .balanceOf(setup.developer_address)
        .call()
        .await?;
    assert_eq!(
        developer_balance,
        U256::from(1000),
        "Developer should have 1000 tokens"
    );
    println!(
        "✓ Developer successfully minted tokens using owner_transaction via EIP-7702 delegation"
    );

    assert!(
        developer_account.is_minimal_account().await?,
        "Developer should be minimal account after minting"
    );

    // Step 8: Delegate user account (session key granter)
    // User signs authorization but executor broadcasts it (user has no funds)
    let user_authorization = user_account
        .sign_authorization(
            &setup.signer, 
            &setup.user_credentials,
            setup.delegation_contract.expect("Delegation contract should be set")
        )
        .await?;

    // Executor broadcasts the user's delegation transaction
    setup
        .executor_broadcasts_authorization_for_account(setup.user_address, user_authorization)
        .await?;

    assert!(
        user_account.is_minimal_account().await?,
        "User (session key granter) should be minimal account after delegation"
    );
    println!("✓ User (session key granter) is now a minimal account (delegated by executor)");

    // Step 9: Developer is already delegated via add_authorization_if_needed in owner_transaction
    assert!(
        developer_account.is_minimal_account().await?,
        "Developer (session key grantee) should already be minimal account from earlier delegation"
    );
    println!("✓ Developer (session key grantee) was already delegated in previous step");

    // Step 10: User (granter) creates and signs session spec allowing developer (grantee) to mint tokens
    let session_spec = setup.create_session_spec_for_granter_and_grantee();

    // User (granter) signs the session spec (EIP-712 signature)
    let granter_session_signature = setup.user_signs_session_spec(&session_spec).await?;

    // Executor calls createSessionWithSig on the granter's (user's) delegated account to establish the session
    let _receipt = setup
        .executor_establishes_session_on_granter_account(&session_spec, &granter_session_signature)
        .await?;

    println!(
        "✓ Session key established: developer (grantee) can now act on behalf of user (granter)"
    );

    // Step 11: Developer (grantee) creates transaction to mint tokens for user (granter)
    let mint_for_granter_transaction = InnerTransaction {
        to: Some(erc20_address),
        data: MockERC20::mintCall {
            amount: U256::from(500),
        }
        .abi_encode()
        .into(),
        value: U256::ZERO,
        gas_limit: Some(100000),
        transaction_type_data: None,
    };

    // Developer (grantee) creates session_key_transaction to act on behalf of user (granter)
    let grantee_session_tx = developer_account.clone().session_key_transaction(
        setup.user_address, // granter's account (target)
        &[mint_for_granter_transaction],
    );

    let (grantee_wrapped_calls, grantee_signature) = grantee_session_tx
        .build(&setup.signer, &setup.developer_credentials)
        .await?;

    // Executor executes the session: calls executeWithSig on grantee's account
    // which then calls execute on granter's account
    setup
        .execute_wrapped_calls_via_delegated_account(
            setup.developer_address, // grantee's account (where executeWithSig is called)
            &grantee_wrapped_calls,
            &grantee_signature,
            None, // No authorization needed for grantee account (already delegated)
        )
        .await?;

    sleep(Duration::from_secs(1)).await;

    // Check granter's (user's) balance increased from session key transaction
    let granter_balance = setup
        .mock_erc20_contract
        .balanceOf(setup.user_address)
        .call()
        .await?;
    assert_eq!(
        granter_balance,
        U256::from(500),
        "Granter (user) should have 500 tokens from session key transaction"
    );
    println!(
        "✓ Session key transaction successful: grantee (developer) minted tokens for granter (user)"
    );

    println!("✓ All EIP-7702 integration tests passed!");

    Ok(())
}
