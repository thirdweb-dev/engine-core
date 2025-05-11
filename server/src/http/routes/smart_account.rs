use alloy::primitives::{Address, Bytes};
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json, Result},
};
use engine_aa_core::smart_account::{SmartAccount, SmartAccountFromSalt};
use engine_core::constants::{DEFAULT_FACTORY_ADDRESS_V0_6, DEFAULT_FACTORY_ADDRESS_V0_7};
use serde::{Deserialize, Serialize};

use crate::{
    chains::ChainService,
    http::{error::EngineResult, server::EngineServerState},
};

// Request model
#[derive(Deserialize)]
pub struct SmartAccountStatusRequest {
    signer_address: Address,
    chain_id: u64,
}

// Response models
#[derive(Serialize)]
struct SmartAccountStatusResponse {
    signer_address: Address,
    chain_id: u64,
    v6_account: AccountDetails,
    v7_account: AccountDetails,
}

#[derive(Serialize)]
struct AccountDetails {
    address: Address,
    is_deployed: bool,
}

pub async fn smart_account_status(
    State(state): State<EngineServerState>,
    Json(request): Json<SmartAccountStatusRequest>,
) -> Result<impl IntoResponse> {
    // Get the chain for the specified chain ID
    let chain = state.chains.get_chain(request.chain_id).map_err(|err| {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("Chain not found: {}", err) })),
        )
    })?;

    // Empty salt data for this example
    let empty_salt = Bytes::default();

    // Create the smart account wrappers
    let v6_account = SmartAccountFromSalt {
        admin_address: request.signer_address,
        chain: &chain,
        factory_address: DEFAULT_FACTORY_ADDRESS_V0_6,
        salt_data: &empty_salt,
    }
    .to_determined_smart_account()
    .await
    .api_error()?;

    let v7_account = SmartAccountFromSalt {
        admin_address: request.signer_address,
        chain: &chain,
        factory_address: DEFAULT_FACTORY_ADDRESS_V0_7,
        salt_data: &empty_salt,
    }
    .to_determined_smart_account()
    .await
    .api_error()?;

    // Check deployment status
    let v6_is_deployed = v6_account.is_deployed(&chain).await.api_error()?;
    let v7_is_deployed = v7_account.is_deployed(&chain).await.api_error()?;

    // Prepare the response
    let response = SmartAccountStatusResponse {
        signer_address: request.signer_address,
        chain_id: request.chain_id,
        v6_account: AccountDetails {
            address: v6_account.address().clone(),
            is_deployed: v6_is_deployed,
        },
        v7_account: AccountDetails {
            address: v7_account.address().clone(),
            is_deployed: v7_is_deployed,
        },
    };

    Ok((StatusCode::OK, Json(response)))
}
