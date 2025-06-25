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

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ErrorResponse<E = EngineError> {
    pub error: ErrorResponseInner<E>,
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ErrorResponseInner<E = EngineError> {
    pub message: String,
    pub details: E,
}

/// Result of a single contract encode operation
///
/// Each result can either be successful (containing the encoded transaction data)
/// or failed (containing detailed error information).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
#[serde(untagged)]
pub enum BatchResultItem<S, E = EngineError> {
    #[schema(title = "BatchItemSuccess")]
    Success { result: S },

    #[schema(title = "BatchItemFailure")]
    Failure { error: E },
}
/// Collection of results from multiple contract encode operations
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
pub struct BatchResults<S, E = EngineError> {
    /// Array of results, one for each input contract call
    pub results: Vec<BatchResultItem<S, E>>,
}

impl<S, E> BatchResultItem<S, E> {
    /// Create a successful encode result item
    pub fn success(success_item: S) -> Self {
        BatchResultItem::Success {
            result: success_item,
        }
    }

    /// Create a failed encode result item
    pub fn failure(error: E) -> Self {
        BatchResultItem::Failure { error }
    }
}
