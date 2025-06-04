use std::sync::Arc;

use aide::{
    axum::{ApiRouter, IntoApiResponse, routing::post_with},
    openapi::{Info, OpenApi},
    scalar::Scalar,
};
use axum::{Extension, Json, Router, routing::get};
use engine_core::userop::UserOpSigner;
use thirdweb_core::abi::ThirdwebAbiService;
use tokio::{sync::watch, task::JoinHandle};

use crate::{
    chains::ThirdwebChainService,
    execution_router::ExecutionRouter,
    http::routes::{
        contract_encode::{encode_contract, encode_contract_docs},
        contract_read::{read_contract, read_contract_docs},
        contract_write::{write_contract, write_contract_docs},
        transaction_write::{write_transaction, write_transaction_docs},
    },
};
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};

#[derive(Clone)]
pub struct EngineServerState {
    pub chains: Arc<ThirdwebChainService>,
    pub signer: Arc<UserOpSigner>,
    pub abi_service: Arc<ThirdwebAbiService>,

    pub execution_router: Arc<ExecutionRouter>,
}

pub struct EngineServer {
    handle: Option<JoinHandle<Result<(), std::io::Error>>>,
    shutdown_tx: Option<watch::Sender<bool>>,
    app: Router,
}

// Note that this clones the document on each request.
// To be more efficient, we could wrap it into an Arc,
// or even store it as a serialized string.
async fn serve_api(Extension(api): Extension<Arc<OpenApi>>) -> impl IntoApiResponse {
    Json(api)
}

impl EngineServer {
    pub async fn new(state: EngineServerState) -> Self {
        aide::generate::on_error(|error| {
            println!("{error}");
        });

        aide::generate::extract_schemas(true);

        let mut api = OpenApi {
            info: Info {
                description: Some("Engine Core API".to_string()),
                title: "Engine Core API".to_string(),
                ..Info::default()
            },
            components: None,

            ..OpenApi::default()
        };

        let cors = CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any)
            .allow_credentials(false);

        let v1_router = ApiRouter::new()
            // generate Scalar API References using the openapi spec route
            .api_route(
                "/read/contract",
                post_with(read_contract, read_contract_docs),
            )
            .api_route(
                "/encode/contract",
                post_with(encode_contract, encode_contract_docs),
            )
            .api_route(
                "/write/contract",
                post_with(write_contract, write_contract_docs),
            )
            .api_route(
                "/write/transaction",
                post_with(write_transaction, write_transaction_docs),
            )
            // We'll serve our generated document here.
            .route("/api.json", get(serve_api))
            // .route("/smart-account/status", post(smart_account_status))
            // .route("/userop/create", post(create_user_op))
            // .route("/test", post(read_contract))
            .layer(cors)
            .layer(TraceLayer::new_for_http())
            // Generate the documentation.
            .route("/scalar", Scalar::new("/v1/api.json").axum_route())
            .with_state(state);

        let router = ApiRouter::new()
            .nest_api_service("/v1", v1_router)
            .finish_api(&mut api)
            // Expose the documentation to the handlers.
            .layer(Extension(Arc::new(api)));
        // Add more routes here

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
