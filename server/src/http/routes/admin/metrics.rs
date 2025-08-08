use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use prometheus::{Encoder, TextEncoder};
use std::sync::Arc;

/// Prometheus metrics endpoint
/// 
/// Returns metrics in Prometheus text format for all executors
pub async fn get_metrics(
    State(registry): State<Arc<prometheus::Registry>>,
) -> Result<Response, MetricsError> {
    let encoder = TextEncoder::new();
    let metric_families = registry.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer)
        .map_err(|e| MetricsError::EncodingFailed(e.to_string()))?;
    
    let metrics_output = String::from_utf8(buffer)
        .map_err(|e| MetricsError::Utf8Error(e.to_string()))?;
    
    Ok((
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        metrics_output,
    ).into_response())
}

#[derive(Debug, thiserror::Error)]
pub enum MetricsError {
    #[error("Failed to encode metrics: {0}")]
    EncodingFailed(String),
    
    #[error("UTF-8 conversion error: {0}")]
    Utf8Error(String),
}

impl IntoResponse for MetricsError {
    fn into_response(self) -> Response {
        let error_message = self.to_string();
        tracing::error!("Metrics error: {}", error_message);
        
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            [("content-type", "text/plain")],
            format!("Metrics export failed: {}", error_message),
        ).into_response()
    }
}
