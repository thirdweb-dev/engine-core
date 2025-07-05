use std::sync::Arc;
use std::net::SocketAddr;

use axum::{Json, Router, routing::get};
use engine_core::{signer::EoaSigner, userop::UserOpSigner};
use thirdweb_core::abi::ThirdwebAbiService;
use tokio::{sync::watch, task::JoinHandle};
use utoipa::OpenApi;
use utoipa_axum::{router::OpenApiRouter, routes};
use utoipa_scalar::{Scalar, Servable};
use vault_sdk::VaultClient;
use hyper::server::conn::http2;
use hyper_util::rt::TokioIo;
use tokio_rustls::{rustls, TlsAcceptor};
use rustls::ServerConfig;
use std::io::BufReader;
use std::fs::File;
use rustls_pemfile::{certs, pkcs8_private_keys};
use tower::Service;

use crate::{
    chains::ThirdwebChainService, execution_router::ExecutionRouter, queue::manager::QueueManager,
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
}

pub struct EngineServer {
    handle: Option<JoinHandle<Result<(), std::io::Error>>>,
    shutdown_tx: Option<watch::Sender<bool>>,
    app: Router,
    tls_config: Option<Arc<ServerConfig>>,
}

#[derive(Clone)]
pub struct TlsConfig {
    pub cert_path: String,
    pub key_path: String,
}

const SCALAR_HTML: &str = include_str!("../../res/scalar.html");

impl EngineServer {
    pub async fn new(state: EngineServerState) -> Self {
        Self::new_with_tls(state, None).await
    }

    pub async fn new_with_tls(state: EngineServerState, tls_config: Option<TlsConfig>) -> Self {
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
            .layer(cors)
            .layer(TraceLayer::new_for_http())
            .with_state(state);

        let (router, api) = OpenApiRouter::with_openapi(ApiDoc::openapi())
            .nest("/v1", v1_router)
            .split_for_parts();

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

        // Load TLS configuration if provided
        let tls_server_config = if let Some(tls_config) = tls_config {
            Some(Self::load_tls_config(&tls_config.cert_path, &tls_config.key_path).await.unwrap())
        } else {
            None
        };

        Self {
            handle: None,
            shutdown_tx: None,
            app: router,
            tls_config: tls_server_config,
        }
    }

    async fn load_tls_config(cert_path: &str, key_path: &str) -> Result<Arc<ServerConfig>, std::io::Error> {
        let cert_file = File::open(cert_path)?;
        let key_file = File::open(key_path)?;
        
        let mut cert_reader = BufReader::new(cert_file);
        let mut key_reader = BufReader::new(key_file);
        
        let cert_chain = certs(&mut cert_reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        
        let mut keys = pkcs8_private_keys(&mut key_reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        
        if keys.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "No private keys found",
            ));
        }
        
        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, keys.remove(0))
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        
        Ok(Arc::new(config))
    }

    pub fn start(&mut self, listener: tokio::net::TcpListener) -> Result<(), std::io::Error> {
        // Create a shutdown channel
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let app = self.app.clone();
        let tls_config = self.tls_config.clone();

        // Start the HTTP server in a background task
        let handle = tokio::spawn(async move {
            let addr = listener.local_addr().unwrap();
            
            if let Some(tls_config) = tls_config {
                tracing::info!("HTTPS/2 server starting on {}", addr);
                Self::serve_https_h2(listener, app, tls_config, shutdown_rx).await
            } else {
                tracing::info!("HTTP server starting on {} (HTTP/1.1 only)", addr);
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
            }
        });

        self.handle = Some(handle);
        self.shutdown_tx = Some(shutdown_tx);

        Ok(())
    }

    async fn serve_https_h2(
        listener: tokio::net::TcpListener,
        app: Router,
        tls_config: Arc<ServerConfig>,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Result<(), std::io::Error> {
        let tls_acceptor = TlsAcceptor::from(tls_config);
        let mut shutdown_rx = shutdown_rx;
        
        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, addr)) => {
                            let tls_acceptor = tls_acceptor.clone();
                            let app = app.clone();
                            
                            tokio::spawn(async move {
                                if let Err(e) = Self::handle_connection(stream, addr, tls_acceptor, app).await {
                                    tracing::error!("Error handling connection from {}: {}", addr, e);
                                }
                            });
                        }
                        Err(e) => {
                            tracing::error!("Error accepting connection: {}", e);
                        }
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        tracing::info!("HTTPS/2 server shutting down");
                        break;
                    }
                }
            }
        }
        
        Ok(())
    }

    async fn handle_connection(
        stream: tokio::net::TcpStream,
        addr: SocketAddr,
        tls_acceptor: TlsAcceptor,
        app: Router,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let tls_stream = tls_acceptor.accept(stream).await?;
        let io = TokioIo::new(tls_stream);
        
        let mut app = app.clone();
        let service = tower::service_fn(move |req| {
            let mut app = app.clone();
            async move { app.call(req).await }
        });
        
        if let Err(e) = http2::Builder::new(hyper_util::rt::TokioExecutor::new())
            .serve_connection(io, service)
            .await
        {
            tracing::error!("Error serving HTTP/2 connection from {}: {}", addr, e);
        }
        
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
