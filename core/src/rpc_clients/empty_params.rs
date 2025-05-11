use alloy::rpc::client::RpcClient;
use alloy::transports::{BoxTransport, Transport, TransportErrorKind, TransportResult};
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::{Layer, Service};

// Layer that transforms JSON before sending
#[derive(Debug, Clone, Default)]
pub struct JsonTransformerLayer;

impl<S> Layer<S> for JsonTransformerLayer {
    type Service = JsonTransformerService<S>;

    fn layer(&self, service: S) -> Self::Service {
        JsonTransformerService { inner: service }
    }
}

// Service that transforms JSON before sending
#[derive(Debug, Clone)]
pub struct JsonTransformerService<S> {
    inner: S,
}

impl<S> Service<Value> for JsonTransformerService<S>
where
    S: Service<Value, Response = Value, Error = TransportErrorKind>,
{
    type Response = Value;
    type Error = TransportErrorKind;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Value) -> Self::Future {
        // Ensure params exists as an array in the JSON
        let modified_request = if let Value::Object(mut obj) = request {
            if !obj.contains_key("params") {
                obj.insert("params".to_string(), Value::Array(vec![]));
            } else if obj["params"].is_null() {
                obj.insert("params".to_string(), Value::Array(vec![]));
            }
            Value::Object(obj)
        } else {
            request
        };

        self.inner.call(modified_request)
    }
}
