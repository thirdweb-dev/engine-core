use axum::{Json, http::StatusCode, response::IntoResponse};
use engine_core::error::{ContractInteractionErrorKind, EngineError, RpcErrorKind};
use serde_json::json;

// Extension trait that lets you pair an error with a status code
/// Extension trait for EngineError to add HTTP response conversion
pub struct ApiEngineError(pub EngineError);

// 2. Allow automatic conversion from EngineError
impl From<EngineError> for ApiEngineError {
    fn from(error: EngineError) -> Self {
        ApiEngineError(error)
    }
}

impl IntoResponse for ApiEngineError {
    fn into_response(self) -> axum::response::Response {
        let code = self.status_code();

        self.with_status(code)
    }
}

impl ApiEngineError {
    fn with_status(self, status: StatusCode) -> axum::response::Response {
        (
            status,
            Json(json!({
                "error": {
                    "message": self.0.to_string(),
                    "details": self.0
                }
            })),
        )
            .into_response()
    }

    fn status_code(&self) -> StatusCode {
        match &self.0 {
            EngineError::RpcError { kind, .. } => match kind {
                RpcErrorKind::NullResp => StatusCode::BAD_GATEWAY,
                RpcErrorKind::ErrorResp(_) => StatusCode::BAD_GATEWAY,
                RpcErrorKind::UnsupportedFeature(_) => StatusCode::NOT_IMPLEMENTED,
                RpcErrorKind::TransportHttpError { status, .. } => {
                    StatusCode::from_u16(*status).unwrap_or(StatusCode::BAD_GATEWAY)
                }
                _ => StatusCode::SERVICE_UNAVAILABLE,
            },
            EngineError::RpcConfigError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            EngineError::ContractInteractionError { kind, .. } => match kind {
                ContractInteractionErrorKind::UnknownFunction(_)
                | ContractInteractionErrorKind::UnknownSelector(_)
                | ContractInteractionErrorKind::AbiError(_) => StatusCode::BAD_REQUEST,

                ContractInteractionErrorKind::ZeroData { .. } => StatusCode::NOT_FOUND,

                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            EngineError::VaultError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            EngineError::BundlerError { .. } => StatusCode::BAD_REQUEST,
            EngineError::PaymasterError { .. } => StatusCode::BAD_REQUEST,
            EngineError::ValidationError { .. } => StatusCode::BAD_REQUEST,
            EngineError::InternalError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            EngineError::ThirdwebError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

// 5. Result extension trait for more ergonomic usage
pub trait EngineResult<T, E> {
    fn api_error(self) -> Result<T, ApiEngineError>;
}

impl<T, E: Into<EngineError>> EngineResult<T, E> for Result<T, E> {
    fn api_error(self) -> Result<T, ApiEngineError> {
        self.map_err(|e| ApiEngineError(e.into()))
    }
}
