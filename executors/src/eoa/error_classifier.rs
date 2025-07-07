use alloy::transports::{RpcError, TransportErrorKind};
use engine_core::{
    chain::Chain,
    error::{AlloyRpcErrorToEngineError, EngineError, RpcErrorKind},
};
use std::time::Duration;
use twmq::job::RequeuePosition;

/// Domain-specific EOA execution errors mapped from RPC errors
#[derive(Debug, Clone)]
pub enum EoaExecutionError {
    /// Nonce too low - transaction might already be in mempool
    NonceTooLow { message: String },

    /// Nonce too high - indicates nonce gap or desync
    NonceTooHigh { message: String },

    /// Transaction already known in mempool
    AlreadyKnown { message: String },

    /// Replacement transaction underpriced
    ReplacementUnderpriced { message: String },

    /// Insufficient funds for transaction
    InsufficientFunds { message: String },

    /// Gas-related error (limit, estimation, etc.)
    GasError { message: String },

    /// Transaction pool is full or has limits
    PoolLimitExceeded { message: String },

    /// Account does not exist or invalid
    AccountError { message: String },

    /// Network/connectivity issues - use existing handling
    RpcError {
        message: String,
        inner_error: Option<EngineError>,
    },
}

/// Recovery strategy for an EOA execution error
#[derive(Debug, Clone, PartialEq)]
pub struct RecoveryStrategy {
    /// Should we queue confirmation job
    pub queue_confirmation: bool,
    /// Should we recycle the nonce
    pub recycle_nonce: bool,
    /// Should we trigger a resync
    pub needs_resync: bool,
    /// Is this error retryable
    pub retryable: bool,
    /// Retry delay if retryable
    pub retry_delay: Option<Duration>,
}

/// Maps RPC errors to domain-specific EOA errors and determines recovery strategies
pub struct EoaErrorMapper;

impl EoaErrorMapper {
    /// Map an RPC error from transaction sending - only handle actionable errors
    pub fn map_send_error<C: Chain>(
        error: &RpcError<TransportErrorKind>,
        chain: &C,
    ) -> Result<EoaExecutionError, EngineError> {
        match error {
            RpcError::ErrorResp(error_payload) => Ok(Self::map_ethereum_error(
                error_payload.code,
                &error_payload.message,
            )),
            _ => {
                // Use existing engine error handling for non-actionable errors
                Err(error.to_engine_error(chain))
            }
        }
    }

    /// Map Ethereum-specific errors that we need to act on
    fn map_ethereum_error(code: i64, message: &str) -> EoaExecutionError {
        let msg_lower = message.to_lowercase();

        match code {
            -32000 => {
                // Only handle the specific ethereum errors we care about
                if msg_lower.contains("nonce too low") {
                    EoaExecutionError::NonceTooLow {
                        message: message.to_string(),
                    }
                } else if msg_lower.contains("nonce too high") {
                    EoaExecutionError::NonceTooHigh {
                        message: message.to_string(),
                    }
                } else if msg_lower.contains("already known") || msg_lower.contains("duplicate") {
                    EoaExecutionError::AlreadyKnown {
                        message: message.to_string(),
                    }
                } else if msg_lower.contains("replacement") && msg_lower.contains("underpriced") {
                    EoaExecutionError::ReplacementUnderpriced {
                        message: message.to_string(),
                    }
                } else if msg_lower.contains("insufficient funds") {
                    EoaExecutionError::InsufficientFunds {
                        message: message.to_string(),
                    }
                } else if msg_lower.contains("gas") {
                    EoaExecutionError::GasError {
                        message: message.to_string(),
                    }
                } else if msg_lower.contains("txpool") || msg_lower.contains("pool limit") {
                    EoaExecutionError::PoolLimitExceeded {
                        message: message.to_string(),
                    }
                } else if msg_lower.contains("account") {
                    EoaExecutionError::AccountError {
                        message: message.to_string(),
                    }
                } else {
                    // Not an actionable error - let engine error handle it
                    EoaExecutionError::RpcError {
                        message: message.to_string(),
                        inner_error: Some(EngineError::InternalError {
                            message: message.to_string(),
                        }),
                    }
                }
            }
            _ => {
                // Not an actionable error code
                EoaExecutionError::RpcError {
                    message: format!("RPC error code {}: {}", code, message),
                    inner_error: Some(EngineError::InternalError {
                        message: message.to_string(),
                    }),
                }
            }
        }
    }

    /// Determine recovery strategy for an EOA execution error
    pub fn get_recovery_strategy(error: &EoaExecutionError) -> RecoveryStrategy {
        match error {
            EoaExecutionError::NonceTooLow { .. } => RecoveryStrategy {
                queue_confirmation: true,
                recycle_nonce: false,
                needs_resync: false,
                retryable: false,
                retry_delay: None,
            },

            EoaExecutionError::NonceTooHigh { .. } => RecoveryStrategy {
                queue_confirmation: false,
                recycle_nonce: true,
                needs_resync: true,
                retryable: true,
                retry_delay: Some(Duration::from_secs(10)),
            },

            EoaExecutionError::AlreadyKnown { .. } => RecoveryStrategy {
                queue_confirmation: true,
                recycle_nonce: false,
                needs_resync: false,
                retryable: false,
                retry_delay: None,
            },

            EoaExecutionError::ReplacementUnderpriced { .. } => RecoveryStrategy {
                queue_confirmation: true,
                recycle_nonce: false,
                needs_resync: false,
                retryable: true,
                retry_delay: Some(Duration::from_secs(10)),
            },

            EoaExecutionError::InsufficientFunds { .. } => RecoveryStrategy {
                queue_confirmation: false,
                recycle_nonce: true,
                needs_resync: false,
                retryable: true,
                retry_delay: Some(Duration::from_secs(60)),
            },

            EoaExecutionError::GasError { .. } => RecoveryStrategy {
                queue_confirmation: false,
                recycle_nonce: true,
                needs_resync: false,
                retryable: true,
                retry_delay: Some(Duration::from_secs(30)),
            },

            EoaExecutionError::PoolLimitExceeded { .. } => RecoveryStrategy {
                queue_confirmation: false,
                recycle_nonce: true,
                needs_resync: false,
                retryable: true,
                retry_delay: Some(Duration::from_secs(30)),
            },

            EoaExecutionError::AccountError { .. } => RecoveryStrategy {
                queue_confirmation: false,
                recycle_nonce: true,
                needs_resync: false,
                retryable: false,
                retry_delay: None,
            },

            EoaExecutionError::RpcError { .. } => {
                // This should not be used - let engine error handle it
                RecoveryStrategy {
                    queue_confirmation: false,
                    recycle_nonce: false,
                    needs_resync: false,
                    retryable: false,
                    retry_delay: None,
                }
            }
        }
    }
}

/// Helper for converting mapped errors and recovery strategies to job results
impl EoaExecutionError {
    /// Get the message for this error
    pub fn message(&self) -> &str {
        match self {
            EoaExecutionError::NonceTooLow { message }
            | EoaExecutionError::NonceTooHigh { message }
            | EoaExecutionError::AlreadyKnown { message }
            | EoaExecutionError::ReplacementUnderpriced { message }
            | EoaExecutionError::InsufficientFunds { message }
            | EoaExecutionError::GasError { message }
            | EoaExecutionError::PoolLimitExceeded { message }
            | EoaExecutionError::AccountError { message }
            | EoaExecutionError::RpcError { message, .. } => message,
        }
    }

    /// Convert to appropriate job result for send operations
    pub fn to_send_job_result<T, E>(
        &self,
        strategy: &RecoveryStrategy,
        success_factory: impl FnOnce() -> T,
        error_factory: impl FnOnce(String) -> E,
    ) -> twmq::job::JobResult<T, E> {
        use twmq::job::{ToJobError, ToJobResult};

        if strategy.queue_confirmation {
            // Treat as success since we need to check confirmation
            Ok(success_factory())
        } else if strategy.retryable {
            if let Some(delay) = strategy.retry_delay {
                Err(error_factory(self.message().to_string())
                    .nack(Some(delay), RequeuePosition::Last))
            } else {
                Err(error_factory(self.message().to_string())
                    .nack(Some(Duration::from_secs(5)), RequeuePosition::Last))
            }
        } else {
            // Permanent failure
            Err(error_factory(self.message().to_string()).fail())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nonce_too_low_mapping() {
        let error = EoaErrorMapper::map_ethereum_error(-32000, "nonce too low");
        let strategy = EoaErrorMapper::get_recovery_strategy(&error);

        match error {
            EoaExecutionError::NonceTooLow { .. } => {}
            _ => panic!("Expected NonceTooLow error"),
        }

        assert!(strategy.queue_confirmation);
        assert!(!strategy.recycle_nonce);
    }

    #[test]
    fn test_insufficient_funds_mapping() {
        let error = EoaErrorMapper::map_ethereum_error(
            -32000,
            "insufficient funds for gas * price + value",
        );
        let strategy = EoaErrorMapper::get_recovery_strategy(&error);

        match error {
            EoaExecutionError::InsufficientFunds { .. } => {}
            _ => panic!("Expected InsufficientFunds error"),
        }

        assert!(!strategy.queue_confirmation);
        assert!(strategy.recycle_nonce);
        assert!(strategy.retryable);
    }

    #[test]
    fn test_already_known_mapping() {
        let error = EoaErrorMapper::map_ethereum_error(-32000, "already known");
        let strategy = EoaErrorMapper::get_recovery_strategy(&error);

        match error {
            EoaExecutionError::AlreadyKnown { .. } => {}
            _ => panic!("Expected AlreadyKnown error"),
        }

        assert!(strategy.queue_confirmation);
        assert!(!strategy.recycle_nonce);
    }
}
