use alloy::{
    hex::{self, FromHex},
    primitives::{Address, Bytes, U256},
};
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json, Result},
};
use engine_aa_core::{
    account_factory::{AccountFactory, get_account_factory},
    smart_account::{DeterminedSmartAccount, SmartAccount, SmartAccountFromSalt},
    userop::builder::{UserOpBuilder, UserOpBuilderConfig},
};
use engine_core::{
    credentials::SigningCredential, execution_options::aa::Erc4337ExecutionOptions,
    transaction::InnerTransaction, userop::UserOpVersion,
};
use rand::Rng;
use serde::{Deserialize, Serialize};
use vault_types::enclave::auth::Auth;

use crate::{
    chains::ChainService,
    http::{error::EngineResult, server::EngineServerState},
};

// Request model for the test route
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateUserOpRequest {
    // Chain identifier
    chain_id: u64,

    // ERC-4337 execution options
    #[serde(flatten)]
    execution_options: Erc4337ExecutionOptions,

    // Inner transactions to execute
    transactions: Vec<InnerTransaction>,

    // Authentication for signing
    #[serde(rename = "auth")]
    credential: Auth,
}

// Response model
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateUserOpResponse {
    user_op: UserOpVersion,
    account_address: Address,
}

/// Test route that creates a signed UserOp from execution options and transactions
#[axum::debug_handler]
pub async fn create_user_op(
    State(state): State<EngineServerState>,
    Json(request): Json<CreateUserOpRequest>,
) -> Result<impl IntoResponse> {
    // Get nonce for the account
    let nonce = {
        let mut rng = rand::rng();
        let limb1: u64 = rng.random();
        let limb2: u64 = rng.random();
        let limb3: u64 = rng.random();
        U256::from_limbs([0, limb1, limb2, limb3])
    };

    // Get the chain service for the specified chain ID
    let chain = state.chains.get_chain(request.chain_id).map_err(|err| {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("Chain not found: {}", err) })),
        )
    })?;

    // Encode transactions into call data using the appropriate encoder
    let salt_data = if request.execution_options.account_salt.starts_with("0x") {
        Bytes::from_hex(request.execution_options.account_salt).map_err(|err| {
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": format!("Failed to parse account salt: {}", err) })),
            )
        })?
    } else {
        let vec = hex::encode(request.execution_options.account_salt);
        Bytes::from_hex(vec).expect("unreachable")
    };

    // Determine if the account is deployed
    let smart_account = match request.execution_options.smart_account_address {
        Some(address) => DeterminedSmartAccount { address },
        None => SmartAccountFromSalt {
            admin_address: request.execution_options.signer_address,
            chain: &chain,
            factory_address: request.execution_options.entrypoint_details.factory_address,
            salt_data: &salt_data,
        }
        .to_determined_smart_account()
        .await
        .api_error()?,
    };
    tracing::info!("Smarto account determined");

    let calldata = if request.transactions.len() == 1 {
        smart_account.encode_execute(&request.transactions[0])
    } else {
        smart_account.encode_execute_batch(&request.transactions)
    };

    let is_deployed = smart_account.is_deployed(&chain).await.api_error()?;

    tracing::info!("Deployment status determined");

    let init_call_data = get_account_factory(
        &chain,
        request.execution_options.entrypoint_details.factory_address,
        None,
    )
    .init_calldata(request.execution_options.signer_address, salt_data);

    // Convert Auth to SigningCredential
    let signing_credential = SigningCredential::Vault(request.credential);

    // Create UserOpBuilder configuration
    let builder_config = UserOpBuilderConfig {
        account_address: smart_account.address,
        signer_address: request.execution_options.signer_address,
        entrypoint_and_factory: request.execution_options.entrypoint_details,
        call_data: calldata,
        init_call_data,
        is_deployed,
        nonce,
        credential: signing_credential,
        chain: &chain,
        signer: state.signer.clone(),
    };

    // Build and sign the UserOp
    let user_op = UserOpBuilder::new(builder_config)
        .build()
        .await
        .api_error()?;

    // Prepare the response
    let response = CreateUserOpResponse {
        user_op,
        account_address: smart_account.address,
    };

    Ok((StatusCode::OK, Json(response)))
}
