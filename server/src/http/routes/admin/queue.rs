use axum::{
    debug_handler,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Json},
};
use serde::Serialize;
use utoipa::ToSchema;

use crate::http::{error::ApiEngineError, server::EngineServerState, types::SuccessResponse};

// ===== TYPES =====

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct EmptyIdempotencySetResponse {
    pub queue_name: String,
    pub message: String,
}

// ===== ROUTE HANDLER =====

#[utoipa::path(
    post,
    operation_id = "emptyQueueIdempotencySet",
    path = "/admin/queue/{queue_name}/empty-idempotency-set",
    tag = "Admin",
    responses(
        (status = 200, description = "Successfully emptied idempotency set", body = SuccessResponse<EmptyIdempotencySetResponse>, content_type = "application/json"),
    ),
    params(
        ("queue_name" = String, Path, description = "Queue name - one of: webhook, external_bundler_send, userop_confirm, eoa_executor, eip7702_send, eip7702_confirm"),
    )
)]
/// Empty Queue Idempotency Set
///
/// Empty the idempotency set for a specific queue. This removes all job IDs from the deduplication set,
/// allowing duplicate jobs to be submitted again.
#[debug_handler]
pub async fn empty_queue_idempotency_set(
    State(state): State<EngineServerState>,
    Path(queue_name): Path<String>,
) -> Result<impl IntoResponse, ApiEngineError> {
    tracing::info!(
        queue_name = queue_name,
        "Processing empty idempotency set request"
    );

    // Map queue name to the appropriate queue and empty its idempotency set
    let result = match queue_name.as_str() {
        "webhook" => state.queue_manager.webhook_queue.empty_dedupe_set().await,
        "external_bundler_send" => {
            state
                .queue_manager
                .external_bundler_send_queue
                .empty_dedupe_set()
                .await
        }
        "userop_confirm" => {
            state
                .queue_manager
                .userop_confirm_queue
                .empty_dedupe_set()
                .await
        }
        "eoa_executor" => {
            state
                .queue_manager
                .eoa_executor_queue
                .empty_dedupe_set()
                .await
        }
        "eip7702_send" => {
            state
                .queue_manager
                .eip7702_send_queue
                .empty_dedupe_set()
                .await
        }
        "eip7702_confirm" => {
            state
                .queue_manager
                .eip7702_confirm_queue
                .empty_dedupe_set()
                .await
        }
        _ => {
            return Err(ApiEngineError(
                engine_core::error::EngineError::ValidationError {
                    message: format!(
                        "Invalid queue name '{queue_name}'. Valid options are: webhook, external_bundler_send, userop_confirm, eoa_executor, eip7702_send, eip7702_confirm"
                    ),
                },
            ));
        }
    };

    // Handle the result
    match result {
        Ok(()) => {
            tracing::info!(
                queue_name = queue_name,
                "Successfully emptied idempotency set"
            );

            Ok((
                StatusCode::OK,
                Json(SuccessResponse::new(EmptyIdempotencySetResponse {
                    queue_name: queue_name.clone(),
                    message: format!(
                        "Successfully emptied idempotency set for queue '{queue_name}'"
                    ),
                })),
            ))
        }
        Err(e) => {
            tracing::error!(
                queue_name = queue_name,
                error = ?e,
                "Failed to empty idempotency set"
            );

            Err(ApiEngineError(
                engine_core::error::EngineError::InternalError {
                    message: format!(
                        "Failed to empty idempotency set for queue '{queue_name}': {e}"
                    ),
                },
            ))
        }
    }
}
