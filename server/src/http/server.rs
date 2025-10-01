use std::sync::Arc;

use axum::{Json, Router, routing::get};
use engine_core::{signer::EoaSigner, userop::UserOpSigner, credentials::KmsClientCache};
use serde_json::json;
use thirdweb_core::abi::ThirdwebAbiService;
use tokio::{sync::watch, task::JoinHandle};
use utoipa::OpenApi;
use utoipa_axum::{router::OpenApiRouter, routes};
use utoipa_scalar::{Scalar, Servable};
use vault_sdk::VaultClient;

use crate::{
    chains::ThirdwebChainService, execution_router::ExecutionRouter,
    http::routes::admin::eoa_diagnostics::eoa_diagnostics_router, queue::manager::QueueManager,
};
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};

#[derive(Clone)]
pub struct EngineServerState {
    pub chains: Arc<ThirdwebChainService>,
    pub userop_signer: Arc<UserOpSigner>,
    pub eoa_signer: Arc<EoaSigner>,
    pub abi_service: Arc<ThirdwebAbiService>,
    pub vault_client: Arc<VaultClient>,

    pub execution_router: Arc<ExecutionRouter>,
    pub queue_manager: Arc<QueueManager>,

    pub diagnostic_access_password: Option<String>,
    pub metrics_registry: Arc<prometheus::Registry>,
    pub kms_client_cache: KmsClientCache,
}

pub struct EngineServer {
    handle: Option<JoinHandle<Result<(), std::io::Error>>>,
    shutdown_tx: Option<watch::Sender<bool>>,
    app: Router,
}

const SCALAR_HTML: &str = include_str!("../../res/scalar.html");

impl EngineServer {
    pub async fn new(state: EngineServerState) -> Self {
        #[derive(OpenApi)]
        struct ApiDoc;

        let cors = CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any)
            .allow_credentials(false);

        let v1_router = OpenApiRouter::new()
            .routes(routes!(crate::http::routes::contract_write::write_contract,))
            .routes(routes!(
                crate::http::routes::contract_encode::encode_contract
            ))
            .routes(routes!(crate::http::routes::contract_read::read_contract,))
            .routes(routes!(
                crate::http::routes::transaction_write::write_transaction
            ))
            .routes(routes!(
                crate::http::routes::transaction::cancel_transaction
            ))
            .routes(routes!(crate::http::routes::sign_message::sign_message))
            .routes(routes!(
                crate::http::routes::sign_typed_data::sign_typed_data
            ))
            .routes(routes!(
                crate::http::routes::admin::queue::empty_queue_idempotency_set
            ))
            .layer(cors)
            .layer(TraceLayer::new_for_http())
            .with_state(state.clone());

        let eoa_diagnostics_router = eoa_diagnostics_router().with_state(state.clone());

        // Create metrics router
        let metrics_router = Router::new()
            .route(
                "/metrics",
                get(crate::http::routes::admin::metrics::get_metrics),
            )
            .with_state(state.metrics_registry.clone());

        let (router, api) = OpenApiRouter::with_openapi(ApiDoc::openapi())
            .nest("/v1", v1_router)
            .split_for_parts();

        // Merge the hidden diagnostic routes after OpenAPI split
        let router = router.merge(eoa_diagnostics_router).merge(metrics_router);

        let api_clone = api.clone();
        let router = router
            .merge(Scalar::with_url("/reference", api).custom_html(SCALAR_HTML))
            // health endpoint with 200 and JSON response {}
            .route("/health", get(|| async { Json(json!({"status": "ok"})) }))
            .route("/api.json", get(|| async { Json(api_clone) }));

        Self {
            handle: None,
            shutdown_tx: None,
            app: router,
        }
    }

    pub fn start(&mut self, listener: tokio::net::TcpListener) -> Result<(), std::io::Error> {
        // Create a shutdown channel
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let app = self.app.clone();

        // Start the HTTP server in a background task
        let handle = tokio::spawn(async move {
            tracing::info!("HTTP server starting on {}", listener.local_addr().unwrap());

            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let mut rx = shutdown_rx;
                    while !*rx.borrow() {
                        if rx.changed().await.is_err() {
                            break;
                        }
                    }
                    tracing::info!("HTTP server shutting down");
                })
                .await
        });

        self.handle = Some(handle);
        self.shutdown_tx = Some(shutdown_tx);

        Ok(())
    }

    pub async fn shutdown(&mut self) -> Result<(), std::io::Error> {
        if let Some(tx) = self.shutdown_tx.take() {
            if tx.send(true).is_err() {
                tracing::error!("Failed to send shutdown signal to HTTP server");
            }
        }

        if let Some(handle) = self.handle.take() {
            match handle.await {
                Ok(result) => {
                    if let Err(e) = result {
                        tracing::error!("HTTP server error during shutdown: {}", e);
                        return Err(e);
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to join HTTP server task: {}", e);
                    return Err(std::io::Error::other(format!("Task join error: {e}")));
                }
            }
        }

        Ok(())
    }
}
