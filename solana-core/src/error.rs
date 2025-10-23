use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors that can occur during Solana program instruction preparation
#[derive(Debug, Error, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SolanaProgramError {
    #[error("Program not found: {identifier}")]
    ProgramNotFound { identifier: String },

    #[error("Instruction '{instruction}' not found in program {program}")]
    InstructionNotFound {
        program: String,
        instruction: String,
    },

    #[error("Failed to parse IDL: {error}")]
    IdlParseError { error: String },

    #[error("Failed to fetch IDL for program {program}: {error}")]
    IdlFetchError { program: String, error: String },

    #[error("Missing required account: {name}. Hint: {hint}")]
    MissingAccount { name: String, hint: String },

    #[error("Failed to derive PDA for account '{account}': {error}")]
    PdaDerivationError { account: String, error: String },

    #[error("Invalid argument '{arg}': {error}")]
    InvalidArgument { arg: String, error: String },

    #[error("Failed to encode instruction data: {error}")]
    EncodingError { error: String },

    #[error("Account constraint violation for '{account}': {constraint}")]
    ConstraintViolation { account: String, constraint: String },

    #[error("Invalid public key '{value}': {error}")]
    InvalidPubkey { value: String, error: String },

    #[error("Serialization error: {message}")]
    SerializationError { message: String },

    #[error("Unsupported IDL type: {type_name}")]
    UnsupportedType { type_name: String },

    #[error("Invalid seed for PDA derivation: {error}")]
    InvalidSeed { error: String },
}

pub type Result<T> = std::result::Result<T, SolanaProgramError>;

