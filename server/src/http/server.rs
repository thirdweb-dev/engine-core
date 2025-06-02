use std::sync::Arc;

use axum::{Router, routing::post};
use engine_core::userop::UserOpSigner;
use engine_executors::{
    external_bundler::{confirm::UserOpConfirmationHandler, send::ExternalBundlerSendHandler},
    webhook::WebhookJobHandler,
};
use thirdweb_core::abi::ThirdwebAbiService;
use tokio::{sync::watch, task::JoinHandle};
use twmq::Queue;

use crate::chains::ThirdwebChainService;
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};

use super::routes::{
    create_userop::create_user_op, encode_method::read_contract,
    smart_account::smart_account_status,
};

#[derive(Clone)]
pub struct EngineServerState {
    pub chains: Arc<ThirdwebChainService>,
    pub signer: Arc<UserOpSigner>,
    pub abi_service: Arc<ThirdwebAbiService>,

    pub webhook_queue: Arc<Queue<WebhookJobHandler>>,
    pub erc4337_send_queue: Arc<Queue<ExternalBundlerSendHandler<ThirdwebChainService>>>,
    pub erc4337_confirm_queue: Arc<Queue<UserOpConfirmationHandler<ThirdwebChainService>>>,
}

pub struct EngineServer {
    handle: Option<JoinHandle<Result<(), std::io::Error>>>,
    shutdown_tx: Option<watch::Sender<bool>>,
    app: Router,
}

impl EngineServer {
    pub async fn new(state: EngineServerState) -> Self {
        let cors = CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any)
            .allow_credentials(false);

        let router = Router::new()
            .route(
                "/write/transaction",
                post(crate::http::routes::transaction::write_transaction),
            )
            .route("/smart-account/status", post(smart_account_status))
            .route("/userop/create", post(create_user_op))
            .route("/test", post(read_contract))
            .layer(cors)
            .layer(TraceLayer::new_for_http())
            // Add more routes here
            .with_state(state);

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
