use std::sync::Arc;

use alloy::{consensus::Transaction, primitives::Address};
use axum::{
    Router, debug_handler,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
};
use engine_core::{credentials::SigningCredential, error::EngineError};
use engine_executors::eoa::{
    EoaExecutorWorkerJobData,
    store::{EoaExecutorStore, EoaHealth, TransactionData},
};
use serde::{Deserialize, Serialize};
use twmq::{Queue, redis::AsyncCommands};

use crate::chains::ThirdwebChainService;
use crate::http::{
    error::ApiEngineError, extractors::DiagnosticAuthExtractor, server::EngineServerState,
    types::SuccessResponse,
};

// ===== TYPES =====

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EoaStateResponse {
    pub eoa: String,
    pub chain_id: u64,
    pub cached_nonce: Option<u64>,
    pub optimistic_nonce: Option<u64>,
    pub pending_count: u64,
    pub submitted_count: u64,
    pub borrowed_count: u64,
    pub recycled_nonces_count: u64,
    pub recycled_nonces: Vec<u64>,
    pub health: Option<EoaHealth>,
    pub manual_reset_scheduled: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionDetailResponse {
    pub transaction_data: Option<TransactionData>,
    pub attempts_count: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingTransactionResponse {
    pub transaction_id: String,
    pub queued_at: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingTransactionsResponse {
    pub transactions: Vec<PendingTransactionResponse>,
    pub total_count: u64,
    pub has_more: bool,
    pub offset: u64,
    pub limit: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubmittedTransactionResponse {
    pub nonce: u64,
    pub transaction_hash: String,
    pub transaction_id: String,
    pub queued_at: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BorrowedTransactionResponse {
    pub transaction_id: String,
    pub nonce: u64,
    pub queued_at: u64,
    pub borrowed_at: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManualResetResponse {
    pub job_enqueued: bool,
}

#[derive(Debug, Deserialize)]
pub struct PaginationQuery {
    pub offset: Option<u64>,
    pub limit: Option<u64>,
}

// ===== ROUTE HANDLERS =====

/// Get EOA State
///
/// Get comprehensive state information for an EOA on a specific chain, including
/// all transaction counts, nonces, and recycled nonces information.
#[debug_handler]
pub async fn get_eoa_state(
    _auth: DiagnosticAuthExtractor,
    State(state): State<EngineServerState>,
    Path(eoa_chain): Path<String>,
) -> Result<impl IntoResponse, ApiEngineError> {
    let (eoa, chain_id) = parse_eoa_chain(&eoa_chain)?;
    let eoa_address: Address = eoa.parse().map_err(|_| {
        ApiEngineError(engine_core::error::EngineError::ValidationError {
            message: "Invalid EOA address format".to_string(),
        })
    })?;

    // Get Redis connection from the EOA executor queue
    let eoa_queue = &state.queue_manager.eoa_executor_queue;
    let redis_conn = eoa_queue.redis.clone();

    // Get namespace from the config
    let namespace = state
        .queue_manager
        .eoa_executor_queue
        .handler
        .namespace
        .clone();
    let store = EoaExecutorStore::new(redis_conn, namespace, eoa_address, chain_id);

    // Get all the state information using store methods
    let cached_nonce = store.get_cached_transaction_count().await.ok();
    let optimistic_nonce = store.get_optimistic_transaction_count().await.ok();
    let pending_count = store.get_pending_transactions_count().await.map_err(|e| {
        ApiEngineError(engine_core::error::EngineError::InternalError {
            message: format!("Failed to get pending count: {e}"),
        })
    })?;
    let submitted_count = store
        .get_submitted_transactions_count()
        .await
        .map_err(|e| {
            ApiEngineError(engine_core::error::EngineError::InternalError {
                message: format!("Failed to get submitted count: {e}"),
            })
        })?;
    let borrowed_count = store.get_borrowed_transactions_count().await.map_err(|e| {
        ApiEngineError(engine_core::error::EngineError::InternalError {
            message: format!("Failed to get borrowed count: {e}"),
        })
    })?;

    // Get recycled nonces using store methods
    let recycled_nonces = store.get_recycled_nonces().await.map_err(|e| {
        ApiEngineError(engine_core::error::EngineError::InternalError {
            message: format!("Failed to get recycled nonces: {e}"),
        })
    })?;
    let recycled_nonces_count = recycled_nonces.len() as u64;

    let health = store.get_eoa_health().await.map_err(|e| {
        ApiEngineError(engine_core::error::EngineError::InternalError {
            message: format!("Failed to get EOA health: {e}"),
        })
    })?;

    let manual_reset_scheduled = store.is_manual_reset_scheduled().await.map_err(|e| {
        ApiEngineError(engine_core::error::EngineError::InternalError {
            message: format!("Failed to check if manual reset is scheduled: {e}"),
        })
    })?;

    let response = EoaStateResponse {
        eoa: eoa_address.to_string(),
        chain_id,
        cached_nonce,
        optimistic_nonce,
        pending_count,
        submitted_count,
        borrowed_count,
        recycled_nonces_count,
        recycled_nonces,
        manual_reset_scheduled,
        health,
    };

    Ok((StatusCode::OK, Json(SuccessResponse::new(response))))
}

/// Get Transaction Detail
///
/// Get fully hydrated transaction details including all attempts and user request data.
/// Note: This endpoint requires the transaction to exist for the given EOA:chain combination.
#[debug_handler]
pub async fn get_transaction_detail(
    _auth: DiagnosticAuthExtractor,
    State(state): State<EngineServerState>,
    Path((eoa_chain, transaction_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiEngineError> {
    let (eoa, chain_id) = parse_eoa_chain(&eoa_chain)?;
    let eoa_address: Address = eoa.parse().map_err(|_| {
        ApiEngineError(engine_core::error::EngineError::ValidationError {
            message: "Invalid EOA address format".to_string(),
        })
    })?;

    // Get Redis connection from the EOA executor queue
    let eoa_queue = &state.queue_manager.eoa_executor_queue;
    let redis_conn = eoa_queue.handler.redis.clone();

    // Get namespace from the config
    let namespace = eoa_queue.handler.namespace.clone();
    let store = EoaExecutorStore::new(redis_conn, namespace, eoa_address, chain_id);

    // Get transaction data using store method
    let transaction_data = store
        .get_transaction_data(&transaction_id)
        .await
        .map_err(|e| {
            ApiEngineError(engine_core::error::EngineError::InternalError {
                message: format!("Failed to get transaction data: {e}"),
            })
        })?;

    // Get attempts count using store method
    let attempts_count = store
        .get_transaction_attempts_count(&transaction_id)
        .await
        .map_err(|e| {
            ApiEngineError(engine_core::error::EngineError::InternalError {
                message: format!("Failed to get attempts count: {e}"),
            })
        })?;

    let response = TransactionDetailResponse {
        transaction_data,
        attempts_count,
    };

    Ok((StatusCode::OK, Json(SuccessResponse::new(response))))
}

/// Get Pending Transactions
///
/// Get paginated list of pending transactions for an EOA (raw data from Redis).
#[debug_handler]
pub async fn get_pending_transactions(
    _auth: DiagnosticAuthExtractor,
    State(state): State<EngineServerState>,
    Path(eoa_chain): Path<String>,
    Query(pagination): Query<PaginationQuery>,
) -> Result<impl IntoResponse, ApiEngineError> {
    let (eoa, chain_id) = parse_eoa_chain(&eoa_chain)?;
    let eoa_address: Address = eoa.parse().map_err(|_| {
        ApiEngineError(engine_core::error::EngineError::ValidationError {
            message: "Invalid EOA address format".to_string(),
        })
    })?;

    // Get Redis connection from the EOA executor queue
    let eoa_queue = &state.queue_manager.eoa_executor_queue;
    let redis_conn = eoa_queue.handler.redis.clone();

    // Get namespace from the config
    let namespace = eoa_queue.handler.namespace.clone();

    let store = EoaExecutorStore::new(redis_conn, namespace, eoa_address, chain_id);
    let offset = pagination.offset.unwrap_or(0);
    let limit = pagination.limit.unwrap_or(1000).min(1000); // Cap at 100

    // Use store methods to get pending transactions with pagination
    let pending_txs = store
        .peek_pending_transactions_paginated(offset, limit)
        .await
        .map_err(|e| {
            ApiEngineError(engine_core::error::EngineError::InternalError {
                message: format!("Failed to get pending transactions: {e}"),
            })
        })?;

    let total_count = store.get_pending_transactions_count().await.map_err(|e| {
        ApiEngineError(engine_core::error::EngineError::InternalError {
            message: format!("Failed to get pending count: {e}"),
        })
    })?;

    let transactions: Vec<PendingTransactionResponse> = pending_txs
        .into_iter()
        .map(|tx| PendingTransactionResponse {
            transaction_id: tx.transaction_id,
            queued_at: tx.queued_at,
        })
        .collect();
    let has_more = (offset + (transactions.len() as u64)) < total_count;

    let response = PendingTransactionsResponse {
        transactions,
        total_count,
        has_more,
        offset,
        limit,
    };

    Ok((StatusCode::OK, Json(SuccessResponse::new(response))))
}

/// Get Submitted Transactions
///
/// Get all submitted transactions for an EOA (raw data from Redis).
#[debug_handler]
pub async fn get_submitted_transactions(
    _auth: DiagnosticAuthExtractor,
    State(state): State<EngineServerState>,
    Path(eoa_chain): Path<String>,
) -> Result<impl IntoResponse, ApiEngineError> {
    let (eoa, chain_id) = parse_eoa_chain(&eoa_chain)?;
    let eoa_address: Address = eoa.parse().map_err(|_| {
        ApiEngineError(engine_core::error::EngineError::ValidationError {
            message: "Invalid EOA address format".to_string(),
        })
    })?;

    // Get Redis connection from the EOA executor queue
    let eoa_queue = &state.queue_manager.eoa_executor_queue;
    let redis_conn = eoa_queue.handler.redis.clone();

    // Get namespace from the config
    let namespace = eoa_queue.handler.namespace.clone();

    let store = EoaExecutorStore::new(redis_conn, namespace, eoa_address, chain_id);

    // Use store method to get submitted transactions
    let submitted_txs = store.get_all_submitted_transactions().await.map_err(|e| {
        ApiEngineError(engine_core::error::EngineError::InternalError {
            message: format!("Failed to get submitted transactions: {e}"),
        })
    })?;

    let transactions: Vec<SubmittedTransactionResponse> = submitted_txs
        .into_iter()
        .map(|tx| SubmittedTransactionResponse {
            nonce: tx.nonce,
            transaction_hash: tx.transaction_hash,
            transaction_id: tx.transaction_id,
            queued_at: tx.queued_at,
        })
        .collect();

    Ok((StatusCode::OK, Json(SuccessResponse::new(transactions))))
}

/// Get Borrowed Transactions
///
/// Get all borrowed transactions for an EOA (raw data from Redis).
#[debug_handler]
pub async fn get_borrowed_transactions(
    _auth: DiagnosticAuthExtractor,
    State(state): State<EngineServerState>,
    Path(eoa_chain): Path<String>,
) -> Result<impl IntoResponse, ApiEngineError> {
    let (eoa, chain_id) = parse_eoa_chain(&eoa_chain)?;
    let eoa_address: Address = eoa.parse().map_err(|_| {
        ApiEngineError(engine_core::error::EngineError::ValidationError {
            message: "Invalid EOA address format".to_string(),
        })
    })?;

    // Get Redis connection from the EOA executor queue
    let eoa_queue = &state.queue_manager.eoa_executor_queue;
    let redis_conn = eoa_queue.handler.redis.clone();

    // Get namespace from the config
    let namespace = eoa_queue.handler.namespace.clone();

    let store = EoaExecutorStore::new(redis_conn, namespace, eoa_address, chain_id);

    // Use store method to get borrowed transactions
    let borrowed_txs = store.peek_borrowed_transactions().await.map_err(|e| {
        ApiEngineError(engine_core::error::EngineError::InternalError {
            message: format!("Failed to get borrowed transactions: {e}"),
        })
    })?;

    let transactions: Vec<BorrowedTransactionResponse> = borrowed_txs
        .into_iter()
        .map(|tx| BorrowedTransactionResponse {
            transaction_id: tx.transaction_id,
            nonce: tx.signed_transaction.nonce(),
            queued_at: tx.queued_at,
            borrowed_at: tx.borrowed_at,
        })
        .collect();

    Ok((StatusCode::OK, Json(SuccessResponse::new(transactions))))
}

/// Schedule a manual reset for the EOA

#[debug_handler]
pub async fn schedule_manual_reset(
    _auth: DiagnosticAuthExtractor,
    State(state): State<EngineServerState>,
    Path(eoa_chain): Path<String>,
) -> Result<impl IntoResponse, ApiEngineError> {
    let (eoa, chain_id) = parse_eoa_chain(&eoa_chain)?;
    let eoa_address: Address = eoa.parse().map_err(|_| {
        ApiEngineError(engine_core::error::EngineError::ValidationError {
            message: "Invalid EOA address format".to_string(),
        })
    })?;

    let eoa_queue = &state.queue_manager.eoa_executor_queue;
    let redis_conn = eoa_queue.handler.redis.clone();

    let namespace = eoa_queue.handler.namespace.clone();

    let store = EoaExecutorStore::new(redis_conn, namespace, eoa_address, chain_id);

    store.schedule_manual_reset().await.map_err(|e| {
        ApiEngineError(engine_core::error::EngineError::InternalError {
            message: format!("Failed to schedule manual reset: {e}"),
        })
    })?;

    let job_enqueued = ensure_eoa_executor_job(
        &state.queue_manager.eoa_executor_queue,
        &store,
        eoa_address,
        chain_id,
    )
    .await?;

    tracing::info!(
        eoa = ?eoa_address,
        chain_id,
        job_enqueued,
        "Manual reset scheduled via admin endpoint"
    );

    Ok((
        StatusCode::OK,
        Json(SuccessResponse::new(ManualResetResponse { job_enqueued })),
    ))
}

// ===== HELPER FUNCTIONS =====

/// Parse eoa:chain_id format
fn parse_eoa_chain(eoa_chain: &str) -> Result<(String, u64), ApiEngineError> {
    let parts: Vec<&str> = eoa_chain.split(':').collect();
    if parts.len() != 2 {
        return Err(ApiEngineError(
            engine_core::error::EngineError::ValidationError {
                message: "Invalid format. Expected 'address:chain_id'".to_string(),
            },
        ));
    }

    let eoa = parts[0].to_string();
    let chain_id = parts[1].parse::<u64>().map_err(|_| {
        ApiEngineError(engine_core::error::EngineError::ValidationError {
            message: "Invalid chain_id format".to_string(),
        })
    })?;

    Ok((eoa, chain_id))
}

async fn ensure_eoa_executor_job(
    queue: &Arc<Queue<engine_executors::eoa::EoaExecutorJobHandler<ThirdwebChainService>>>,
    store: &EoaExecutorStore,
    eoa: Address,
    chain_id: u64,
) -> Result<bool, ApiEngineError> {
    let job_id = format!("eoa_{}_{}", eoa, chain_id);

    if job_exists_in_queue(queue, &job_id).await? {
        return Ok(false);
    }

    let signing_credential = find_signing_credential(store).await?;

    let job_data = EoaExecutorWorkerJobData {
        eoa_address: eoa,
        chain_id,
        noop_signing_credential: signing_credential,
    };

    queue
        .clone()
        .job(job_data)
        .with_id(&job_id)
        .push()
        .await
        .map_err(|e| {
            ApiEngineError(EngineError::InternalError {
                message: format!("Failed to enqueue EOA executor job: {e}"),
            })
        })?;

    Ok(true)
}

async fn job_exists_in_queue(
    queue: &Arc<Queue<engine_executors::eoa::EoaExecutorJobHandler<ThirdwebChainService>>>,
    job_id: &str,
) -> Result<bool, ApiEngineError> {
    let mut conn = queue.redis.clone();
    let dedupe_key = queue.dedupe_set_name();

    conn.sismember(&dedupe_key, job_id).await.map_err(|e| {
        ApiEngineError(EngineError::InternalError {
            message: format!("Failed to query queue dedupe set: {e}"),
        })
    })
}

async fn find_signing_credential(
    store: &EoaExecutorStore,
) -> Result<SigningCredential, ApiEngineError> {
    if let Some(pending) = store
        .peek_pending_transactions(1)
        .await
        .map_err(|e| {
            ApiEngineError(EngineError::InternalError {
                message: format!("Failed to inspect pending transactions: {e}"),
            })
        })?
        .into_iter()
        .next()
    {
        return Ok(pending.user_request.signing_credential.clone());
    }

    let borrowed_transactions = store.peek_borrowed_transactions().await.map_err(|e| {
        ApiEngineError(EngineError::InternalError {
            message: format!("Failed to inspect borrowed transactions: {e}"),
        })
    })?;

    for borrowed in borrowed_transactions {
        if let Some(tx_data) = store
            .get_transaction_data(&borrowed.transaction_id)
            .await
            .map_err(|e| {
                ApiEngineError(EngineError::InternalError {
                    message: format!("Failed to hydrate transaction data: {e}"),
                })
            })?
        {
            return Ok(tx_data.user_request.signing_credential.clone());
        }
    }

    Err(ApiEngineError(EngineError::ValidationError {
        message: "Unable to determine signing credential for this EOA; no pending or borrowed transactions found".to_string(),
    }))
}

pub fn eoa_diagnostics_router() -> Router<EngineServerState> {
    // Add hidden admin diagnostic routes (not included in OpenAPI)
    Router::new()
        .route(
            "/admin/executors/eoa/{eoa_chain}/state",
            axum::routing::get(get_eoa_state),
        )
        .route(
            "/admin/executors/eoa/{eoa_chain}/transaction/{transaction_id}",
            axum::routing::get(get_transaction_detail),
        )
        .route(
            "/admin/executors/eoa/{eoa_chain}/pending",
            axum::routing::get(get_pending_transactions),
        )
        .route(
            "/admin/executors/eoa/{eoa_chain}/submitted",
            axum::routing::get(get_submitted_transactions),
        )
        .route(
            "/admin/executors/eoa/{eoa_chain}/borrowed",
            axum::routing::get(get_borrowed_transactions),
        )
        .route(
            "/admin/executors/eoa/{eoa_chain}/reset",
            axum::routing::post(schedule_manual_reset),
        )
}
