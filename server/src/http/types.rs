use aide::OperationIo;
use engine_core::error::EngineError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, OperationIo, utoipa::ToSchema)]
pub struct SuccessResponse<T> {
    pub result: T,
}

impl<T> SuccessResponse<T> {
    pub fn new(result: T) -> Self {
        Self { result }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ErrorResponse {
    pub error: ErrorResponseInner,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct ErrorResponseInner {
    pub message: String,
    pub details: EngineError,
}
