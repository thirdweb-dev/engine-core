use alloy::{
    rpc::json_rpc::{RequestPacket, ResponsePacket},
    transports::{
        TransportError, TransportErrorKind, TransportFut, TransportResult, http::reqwest,
    },
};
use std::task;
use tower::Service;
use tracing::{Instrument, debug, debug_span, trace};

/// A transport that uses a shared reqwest client but injects custom headers per request
#[derive(Clone, Debug)]
pub struct HeaderInjectingTransport {
    /// The shared reqwest client (this is where connection pooling happens)
    client: reqwest::Client,
    /// The URL to send requests to
    url: reqwest::Url,
    /// Headers to inject into every request made by this transport
    custom_headers: reqwest::header::HeaderMap,
}

impl HeaderInjectingTransport {
    /// Create a new transport with a shared client and custom headers
    pub fn new(
        client: reqwest::Client,
        url: reqwest::Url,
        headers: reqwest::header::HeaderMap,
    ) -> Self {
        Self {
            client,
            url,
            custom_headers: headers,
        }
    }

    /// Create a transport with authentication headers
    pub fn with_auth(
        client: reqwest::Client,
        url: reqwest::Url,
        client_id: &str,
        secret_key: &str,
    ) -> Result<Self, reqwest::header::InvalidHeaderValue> {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("x-client-id", client_id.parse()?);
        headers.insert("x-secret-key", secret_key.parse()?);

        Ok(Self::new(client, url, headers))
    }

    /// The core request handling - similar to alloy's reqwest transport but with header injection
    async fn do_request(self, req: RequestPacket) -> TransportResult<ResponsePacket> {
        // Build the HTTP request with custom headers
        let mut request_builder = self.client.post(self.url).json(&req); // This serializes the JSON-RPC request

        // Inject our custom headers
        for (name, value) in &self.custom_headers {
            request_builder = request_builder.header(name, value);
        }

        // Send the request using the shared client
        let resp = request_builder
            .send()
            .await
            .map_err(TransportErrorKind::custom)?;

        let status = resp.status();
        debug!(?status, "received response from server");

        // Get response body
        let body = resp.bytes().await.map_err(TransportErrorKind::custom)?;
        debug!(bytes = body.len(), "retrieved response body");
        trace!(body = ?String::from_utf8_lossy(&body), "response body");

        // Check for HTTP errors
        if !status.is_success() {
            return Err(TransportErrorKind::http_error(
                status.as_u16(),
                String::from_utf8_lossy(&body).into_owned(),
            ));
        }

        // Deserialize JSON-RPC response
        serde_json::from_slice(&body)
            .map_err(|err| TransportError::deser_err(err, String::from_utf8_lossy(&body)))
    }
}

// This is the key implementation - makes it work with alloy's RpcClient
impl Service<RequestPacket> for HeaderInjectingTransport {
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;

    #[inline]
    fn poll_ready(&mut self, _cx: &mut task::Context<'_>) -> task::Poll<Result<(), Self::Error>> {
        // reqwest always returns ready
        task::Poll::Ready(Ok(()))
    }

    #[inline]
    fn call(&mut self, req: RequestPacket) -> Self::Future {
        let this = self.clone(); // Clone is cheap - just clones the Arc inside Client
        let span = debug_span!("HeaderInjectingTransport", url = ?this.url);
        Box::pin(this.do_request(req).instrument(span))
    }
}

/// Builder for creating transports with different header configurations
/// All transports created by this builder share the same connection pool
#[derive(Clone, Debug)]
pub struct SharedClientTransportBuilder {
    /// The shared reqwest client - this is where connection pooling magic happens
    shared_client: reqwest::Client,
}

impl SharedClientTransportBuilder {
    /// Create a new builder with a shared client
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            shared_client: client,
        }
    }

    /// Create a transport with custom headers
    pub fn with_headers(
        &self,
        url: reqwest::Url,
        headers: reqwest::header::HeaderMap,
    ) -> HeaderInjectingTransport {
        HeaderInjectingTransport::new(self.shared_client.clone(), url, headers)
    }

    /// Create a transport with no additional headers (uses client's defaults)
    pub fn default_transport(&self, url: reqwest::Url) -> HeaderInjectingTransport {
        HeaderInjectingTransport::new(
            self.shared_client.clone(),
            url,
            reqwest::header::HeaderMap::new(),
        )
    }
}

// // Updated ThirdwebChain implementation
// use crate::error::EngineError;
// use crate::rpc_clients::{BundlerClient, PaymasterClient};
// use alloy::{
//     providers::{ProviderBuilder, RootProvider},
//     rpc::client::RpcClient,
// };

// pub struct ThirdwebChain {
//     /// Builder for creating transports that share the same connection pool
//     transport_builder: SharedClientTransportBuilder,

//     chain_id: u64,
//     rpc_url: Url,
//     bundler_url: Url,
//     paymaster_url: Url,

//     /// Default clients (these also use the shared connection pool)
//     pub bundler_client: BundlerClient,
//     pub paymaster_client: PaymasterClient,
//     pub provider: RootProvider,
// }

// impl ThirdwebChain {
//     /// Create a bundler client with custom authentication
//     /// This is the method you were asking about!
//     pub fn create_bundler_client_with_auth(
//         &self,
//         client_id: &str,
//         secret_key: &str,
//     ) -> Result<BundlerClient, reqwest::header::InvalidHeaderValue> {
//         // Create a transport with custom auth headers
//         // This transport shares the connection pool with all other transports
//         let transport =
//             self.transport_builder
//                 .with_auth(self.bundler_url.clone(), client_id, secret_key)?;

//         // Create RPC client using the custom transport
//         let rpc_client = RpcClient::builder().transport(transport, false);

//         Ok(BundlerClient { inner: rpc_client })
//     }

//     /// Create a paymaster client with custom authentication
//     pub fn create_paymaster_client_with_auth(
//         &self,
//         client_id: &str,
//         secret_key: &str,
//     ) -> Result<PaymasterClient, reqwest::header::InvalidHeaderValue> {
//         let transport =
//             self.transport_builder
//                 .with_auth(self.paymaster_url.clone(), client_id, secret_key)?;

//         let rpc_client = RpcClient::builder().transport(transport, false);
//         Ok(PaymasterClient { inner: rpc_client })
//     }

//     /// Create clients with completely custom headers
//     pub fn create_bundler_client_with_headers(&self, headers: HeaderMap) -> BundlerClient {
//         let transport = self
//             .transport_builder
//             .with_headers(self.bundler_url.clone(), headers);
//         let rpc_client = RpcClient::builder().transport(transport, false);
//         BundlerClient { inner: rpc_client }
//     }
// }

// impl<'a> ThirdwebChainConfig<'a> {
//     pub fn to_chain(&self) -> Result<ThirdwebChain, EngineError> {
//         let rpc_url = Url::parse(&format!(
//             "https://{chain_id}.{base_url}/{client_id}",
//             chain_id = self.chain_id,
//             base_url = self.rpc_base_url,
//             client_id = self.client_id,
//         ))
//         .map_err(|e| EngineError::RpcConfigError {
//             message: format!("Failed to parse RPC URL: {}", e.to_string()),
//         })?;

//         let bundler_url = Url::parse(&format!(
//             "https://{chain_id}.{base_url}/v2",
//             chain_id = self.chain_id,
//             base_url = self.bundler_base_url,
//         ))
//         .map_err(|e| EngineError::RpcConfigError {
//             message: format!("Failed to parse Bundler URL: {}", e.to_string()),
//         })?;

//         let paymaster_url = Url::parse(&format!(
//             "https://{chain_id}.{base_url}/v2",
//             chain_id = self.chain_id,
//             base_url = self.paymaster_base_url,
//         ))
//         .map_err(|e| EngineError::RpcConfigError {
//             message: format!("Failed to parse Paymaster URL: {}", e.to_string()),
//         })?;

//         // Create default headers for the shared client
//         let mut default_headers = HeaderMap::new();
//         default_headers.insert(
//             "x-client-id",
//             reqwest::header::HeaderValue::from_str(&self.client_id).map_err(|e| {
//                 EngineError::RpcConfigError {
//                     message: format!("Invalid client-id: {e}"),
//                 }
//             })?,
//         );
//         default_headers.insert(
//             "x-secret-key",
//             reqwest::header::HeaderValue::from_str(&self.secret_key).map_err(|e| {
//                 EngineError::RpcConfigError {
//                     message: format!("Invalid secret-key: {e}"),
//                 }
//             })?,
//         );

//         // Create the single shared reqwest client - this is the connection pool
//         let shared_client = reqwest::ClientBuilder::new()
//             .default_headers(default_headers)
//             .build()
//             .map_err(|e| EngineError::RpcConfigError {
//                 message: format!("Failed to build HTTP client: {e}"),
//             })?;

//         // Create the transport builder that will create transports sharing this client
//         let transport_builder = SharedClientTransportBuilder::new(shared_client);

//         // Create default transports for the default clients
//         let default_bundler_transport = transport_builder.default_transport(bundler_url.clone());
//         let default_paymaster_transport =
//             transport_builder.default_transport(paymaster_url.clone());

//         // Create default RPC clients
//         let bundler_rpc_client = RpcClient::builder().transport(default_bundler_transport, false);
//         let paymaster_rpc_client =
//             RpcClient::builder().transport(default_paymaster_transport, false);

//         Ok(ThirdwebChain {
//             transport_builder,
//             chain_id: self.chain_id,
//             rpc_url: rpc_url.clone(),
//             bundler_url,
//             paymaster_url,
//             bundler_client: BundlerClient {
//                 inner: bundler_rpc_client,
//             },
//             paymaster_client: PaymasterClient {
//                 inner: paymaster_rpc_client,
//             },
//             provider: ProviderBuilder::new()
//                 .disable_recommended_fillers()
//                 .connect_http(rpc_url)
//                 .into(),
//         })
//     }
// }

// // Usage examples showing proper connection sharing
// impl ThirdwebChainService {
//     pub async fn send_user_op_with_custom_auth(
//         &self,
//         chain_id: u64,
//         user_op: &crate::userop::UserOpVersion,
//         entrypoint: alloy_primitives::Address,
//         client_id: &str,
//         secret_key: &str,
//     ) -> Result<alloy_primitives::Bytes, EngineError> {
//         let chain = self.get_chain(chain_id)?;

//         // This creates a new transport that shares the connection pool
//         let custom_bundler = chain
//             .create_bundler_client_with_auth(client_id, secret_key)
//             .map_err(|e| EngineError::RpcConfigError {
//                 message: format!("Failed to create client: {e}"),
//             })?;

//         // When this is called:
//         // 1. alloy serializes user_op + entrypoint to JSON-RPC
//         // 2. RpcClient calls our HeaderInjectingTransport.call(RequestPacket)
//         // 3. Our transport.do_request() is called
//         // 4. We build reqwest POST with custom headers + JSON body
//         // 5. shared_client.post().headers().json().send() - reuses connections!
//         // 6. Response comes back through alloy's deserialization
//         custom_bundler
//             .send_user_op(user_op, entrypoint)
//             .await
//             .map_err(|e| EngineError::RpcConfigError {
//                 message: format!("Request failed: {e}"),
//             })
//     }
// }
