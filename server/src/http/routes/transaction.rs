use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    Json as AxumJson,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Json},
};
use engine_core::{
    chain::RpcCredentials,
    execution_options::{
        ExecutionOptions, ExecutorType, QueuedNotification, SpecificExecutionOptions,
        TransactionRequest, TransactionResponse,
    },
};
use engine_executors::{
    external_bundler::send::ExternalBundlerSendJobData, webhook::WebhookJobPayload,
};
use thirdweb_core::auth::ThirdwebAuth;

use crate::http::{error::ApiEngineError, server::EngineServerState};

/// Extract RPC credentials from headers
pub fn extract_rpc_credentials(headers: &HeaderMap) -> Result<RpcCredentials, ApiEngineError> {
    // try secret key first
    let secret_key = headers
        .get("x-thirdweb-secret-key")
        .and_then(|v| v.to_str().ok());

    if let Some(secret_key) = secret_key {
        return Ok(RpcCredentials::Thirdweb(ThirdwebAuth::SecretKey(
            secret_key.to_string(),
        )));
    }

    // if not, try client id and service key
    let client_id = headers
        .get("x-thirdweb-client-id")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            ApiEngineError(engine_core::error::EngineError::ValidationError {
                message: "Missing x-client-id header".to_string(),
            })
        })?;

    let service_key = headers
        .get("x-thirdweb-service-key")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            ApiEngineError(engine_core::error::EngineError::ValidationError {
                message: "Missing x-service-key header".to_string(),
            })
        })?;

    Ok(RpcCredentials::Thirdweb(ThirdwebAuth::ClientIdServiceKey(
        thirdweb_core::auth::ThirdwebClientIdAndServiceKey {
            client_id: client_id.to_string(),
            service_key: service_key.to_string(),
        },
    )))
}

/// Extract signing credential from headers
fn extract_signing_credential(
    headers: &HeaderMap,
) -> Result<engine_core::credentials::SigningCredential, ApiEngineError> {
    let vault_access_token = headers
        .get("x-vault-access-token")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            ApiEngineError(engine_core::error::EngineError::ValidationError {
                message: "Missing x-vault-access-token header".to_string(),
            })
        })?;

    Ok(engine_core::credentials::SigningCredential::Vault(
        vault_types::enclave::auth::Auth::AccessToken {
            access_token: vault_access_token.to_string(),
        },
    ))
}

/// Main transaction endpoint that queues jobs based on execution options
#[axum::debug_handler]
pub async fn write_transaction(
    State(state): State<EngineServerState>,
    headers: HeaderMap,
    AxumJson(request): AxumJson<TransactionRequest>,
) -> Result<impl IntoResponse, ApiEngineError> {
    // Extract RPC credentials from headers
    let rpc_credentials = extract_rpc_credentials(&headers)?;
    let signing_credential = extract_signing_credential(&headers)?;

    let transaction_id = request.execution_options.transaction_id().to_string();
    let executor_type = request.execution_options.executor_type();

    tracing::info!(
        transaction_id = %transaction_id,
        executor_type = ?executor_type,
        chain_id = %request.execution_options.chain_id(),
        "Processing transaction request"
    );

    // Queue the job based on execution options
    match &request.execution_options.specific {
        SpecificExecutionOptions::ERC4337(erc4337_options) => {
            queue_erc4337_job(
                &state,
                &request,
                rpc_credentials,
                signing_credential,
                &transaction_id,
            )
            .await
            .map_err(|e| {
                tracing::error!(
                    transaction_id = %transaction_id,
                    error = %e,
                    "Failed to queue ERC-4337 job"
                );
                ApiEngineError(engine_core::error::EngineError::InternalError(format!(
                    "Failed to queue transaction: {}",
                    e
                )))
            })?;
        }
    }

    // Send immediate queued notification webhook
    if let Some(webhook_options) = &request.webhook_options {
        if let Err(e) = send_queued_notification(
            &state,
            &transaction_id,
            executor_type.clone(),
            request.execution_options.clone(),
            &webhook_options.url,
        )
        .await
        {
            tracing::warn!(
                transaction_id = %transaction_id,
                webhook_url = %&webhook_options.url,
                error = %e,
                "Failed to queue notification webhook, continuing..."
            );
        }
    }

    tracing::info!(
        transaction_id = %transaction_id,
        executor_type = ?executor_type,
        "Transaction queued successfully"
    );

    Ok((
        StatusCode::ACCEPTED,
        Json(TransactionResponse { transaction_id }),
    ))
}

/// Queue an ERC-4337 job
async fn queue_erc4337_job(
    state: &EngineServerState,
    request: &TransactionRequest,
    rpc_credentials: RpcCredentials,
    signing_credential: engine_core::credentials::SigningCredential,
    transaction_id: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let SpecificExecutionOptions::ERC4337(erc4337_options) = &request.execution_options.specific;

    let job_data = ExternalBundlerSendJobData {
        transaction_id: transaction_id.to_string(),
        chain_id: request.execution_options.base.chain_id,
        transactions: request.params.clone(),
        execution_options: erc4337_options.clone(),
        signing_credential: signing_credential,
        webhook_options: request.webhook_options.clone(),
        rpc_credentials,
    };

    // Create job with transaction ID as the job ID for idempotency
    state
        .erc4337_send_queue
        .clone()
        .job(job_data)
        .with_id(transaction_id)
        .push()
        .await?;

    tracing::debug!(
        transaction_id = %transaction_id,
        queue = "erc4337_send",
        "Job queued successfully"
    );

    Ok(())
}

/// Send immediate queued notification webhook
async fn send_queued_notification(
    state: &EngineServerState,
    transaction_id: &str,
    executor_type: ExecutorType,
    execution_options: ExecutionOptions,
    webhook_url: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let notification = QueuedNotification {
        transaction_id: transaction_id.to_string(),
        executor_type,
        execution_options,
        timestamp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        queue_name: match executor_type {
            ExecutorType::Erc4337 => "erc4337_send".to_string(),
        },
    };

    let webhook_payload = WebhookJobPayload {
        url: webhook_url.to_string(),
        body: serde_json::to_string(&notification)?,
        headers: Some(
            [
                ("Content-Type".to_string(), "application/json".to_string()),
                ("X-Event-Type".to_string(), "transaction.queued".to_string()),
                ("X-Transaction-Id".to_string(), transaction_id.to_string()),
            ]
            .into_iter()
            .collect(),
        ),
        hmac_secret: None, // TODO: Add HMAC support if needed
        http_method: Some("POST".to_string()),
    };

    let webhook_job = state.webhook_queue.clone().job(webhook_payload);
    let mut webhook_job_with_id = webhook_job;
    webhook_job_with_id.options.id = format!("{}_queued_notification", transaction_id);

    state
        .webhook_queue
        .push(webhook_job_with_id.options)
        .await?;

    tracing::info!(
        transaction_id = %transaction_id,
        webhook_url = %webhook_url,
        "Queued notification webhook scheduled"
    );

    Ok(())
}
