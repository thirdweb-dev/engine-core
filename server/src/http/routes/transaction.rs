// Transaction Management Operations

use axum::{
    debug_handler,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Json},
};
use serde::Serialize;
use twmq::{CancelResult as TwmqCancelResult, error::TwmqError};
use utoipa::ToSchema;

use crate::http::{error::ApiEngineError, server::EngineServerState, types::SuccessResponse};

// ===== TYPES =====

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TransactionCancelResponse {
    pub transaction_id: String,
    pub result: CancelResult,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CancelResult {
    CancelledImmediately,
    CancellationPending,
    CannotCancel { reason: String },
    NotFound,
}

// ===== ROUTE HANDLER =====

#[utoipa::path(
    post,
    operation_id = "cancelTransaction",
    path = "/transactions/{id}/cancel",
    tag = "Transactions",
    responses(
        (status = 200, description = "Transaction cancellation result", body = TransactionCancelResponse, content_type = "application/json"),
    ),
    params(
        ("id" = String, Path, description = "Transaction ID to cancel"),
    )
)]
/// Cancel Transaction
///
/// Attempt to cancel a queued transaction. Transactions that have been sent and are waiting for mine cannot be cancelled.
#[debug_handler]
pub async fn cancel_transaction(
    State(state): State<EngineServerState>,
    Path(transaction_id): Path<String>,
) -> Result<impl IntoResponse, ApiEngineError> {
    tracing::info!(
        transaction_id = %transaction_id,
        "Processing transaction cancellation request"
    );

    // Check which queue the transaction is in
    let queue_name = state
        .queue_manager
        .transaction_registry
        .get_transaction_queue(&transaction_id)
        .await
        .map_err(|e| ApiEngineError(e.into()))?;

    let result = match queue_name.as_deref() {
        Some("external_bundler_send") => {
            // Transaction is in send queue - try to cancel
            match state
                .queue_manager
                .external_bundler_send_queue
                .cancel_job(&transaction_id)
                .await
                .map_err(|e| ApiEngineError(TwmqError::into(e)))?
            {
                TwmqCancelResult::CancelledImmediately => {
                    // Remove from registry since it's cancelled
                    state
                        .queue_manager
                        .transaction_registry
                        .remove_transaction(&transaction_id)
                        .await
                        .map_err(|e| ApiEngineError(e.into()))?;

                    tracing::info!(
                        transaction_id = %transaction_id,
                        "Transaction cancelled immediately"
                    );

                    CancelResult::CancelledImmediately
                }
                TwmqCancelResult::CancellationPending => {
                    tracing::info!(
                        transaction_id = %transaction_id,
                        "Transaction cancellation pending"
                    );

                    CancelResult::CancellationPending
                }
                TwmqCancelResult::NotFound => {
                    tracing::warn!(
                        transaction_id = %transaction_id,
                        "Transaction not found in send queue"
                    );

                    CancelResult::NotFound
                }
            }
        }
        Some("userop_confirm") => {
            tracing::info!(
                transaction_id = %transaction_id,
                "Cannot cancel transaction - already sent and waiting for mine"
            );

            CancelResult::CannotCancel {
                reason: "Transaction has been sent and is waiting for mine".to_string(),
            }
        }
        Some(other_queue) => {
            tracing::warn!(
                transaction_id = %transaction_id,
                queue = %other_queue,
                "Transaction in unsupported queue for cancellation"
            );

            CancelResult::CannotCancel {
                reason: format!("Transaction in unsupported queue: {}", other_queue),
            }
        }
        None => {
            tracing::warn!(
                transaction_id = %transaction_id,
                "Transaction not found in registry"
            );

            CancelResult::NotFound
        }
    };

    let response = TransactionCancelResponse {
        transaction_id,
        result,
    };

    Ok((StatusCode::OK, Json(SuccessResponse::new(response))))
}
