use std::sync::Arc;

use axum::{Json, Router, routing::get};
use engine_core::{signer::EoaSigner, userop::UserOpSigner};
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

        let eoa_diagnostics_router = eoa_diagnostics_router().with_state(state);

        let (router, api) = OpenApiRouter::with_openapi(ApiDoc::openapi())
            .nest("/v1", v1_router)
            .split_for_parts();

        // Merge the hidden diagnostic routes after OpenAPI split
        let router = router.merge(eoa_diagnostics_router);

        let api_clone = api.clone();
        let router = router
            .merge(Scalar::with_url("/reference", api).custom_html(SCALAR_HTML))
            .route("/api.json", get(|| async { Json(api_clone) }));

        // let v1_router = ApiRouter::new()
        //     // generate Scalar API References using the openapi spec route
        //     .api_route(
        //         "/read/contract",
        //         post_with(read_contract, read_contract_docs),
        //     )
        //     .api_route(
        //         "/encode/contract",
        //         post_with(encode_contract, encode_contract_docs),
        //     )
        //     .api_route(
        //         "/write/contract",
        //         post_with(write_contract, write_contract_docs),
        //     )
        //     .api_route(
        //         "/write/transaction",
        //         post_with(write_transaction, write_transaction_docs),
        //     )
        //     // We'll serve our generated document here.
        //     .route("/api.json", get(serve_api))
        //     // .route("/smart-account/status", post(smart_account_status))
        //     // .route("/userop/create", post(create_user_op))
        //     // .route("/test", post(read_contract))
        //     .layer(cors)
        //     .layer(TraceLayer::new_for_http())
        //     // Generate the documentation.
        //     .route("/reference", Scalar::new("/v1/api.json").axum_route())
        //     .with_state(state);

        // let router = ApiRouter::new()
        //     .nest_api_service("/v1", v1_router)
        //     .finish_api(&mut api)
        //     // Expose the documentation to the handlers.
        //     .layer(Extension(Arc::new(api)));
        // // Add more routes here

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
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Task join error: {}", e),
                    ));
                }
            }
        }

        Ok(())
    }
}
