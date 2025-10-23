/// Runtime IDL types for Solana programs
///
/// This module wraps Anchor IDL types and provides a unified interface
/// for both Anchor (Borsh) and native Solana (bincode) programs.
use anchor_lang::idl::types::{Idl, IdlInstruction};
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;

/// Program IDL with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProgramIdl {
    /// The underlying IDL
    #[serde(flatten)]
    pub idl: Idl,

    /// Serialization format used by this program
    #[serde(skip_serializing_if = "Option::is_none")]
    pub serialization: Option<SerializationFormat>,
}

/// Serialization format for instruction data
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SerializationFormat {
    /// Anchor programs use Borsh with 8-byte discriminators
    Borsh,
    /// Native Solana programs use bincode
    #[default]
    Bincode,
}

impl ProgramIdl {
    /// Parse IDL from JSON
    pub fn from_json(json: &str) -> crate::error::Result<Self> {
        serde_json::from_str(json).map_err(|e| crate::error::SolanaProgramError::IdlParseError {
            error: e.to_string(),
        })
    }

    /// Find an instruction by name
    pub fn find_instruction(&self, name: &str) -> Option<&IdlInstruction> {
        self.idl.instructions.iter().find(|ix| ix.name == name)
    }

    /// Get the serialization format, detecting Anchor if discriminators are present
    pub fn serialization_format(&self) -> SerializationFormat {
        self.serialization.unwrap_or_default()
    }
}

/// Resolved instruction with account constraints
#[derive(Debug, Clone)]
pub struct ResolvedInstruction {
    pub name: String,
    pub instruction: IdlInstruction,
    pub discriminator: Option<Vec<u8>>,
    pub serialization: SerializationFormat,
}

/// Account information from IDL
#[derive(Debug, Clone)]
pub struct IdlAccountInfo {
    pub name: String,
    pub is_mut: bool,
    pub is_signer: bool,
    pub pda: Option<PdaInfo>,
}

/// PDA derivation information
#[derive(Debug, Clone)]
pub struct PdaInfo {
    pub seeds: Vec<SeedComponent>,
    pub program_id: Option<Pubkey>,
}

/// Component of a PDA seed
#[derive(Debug, Clone)]
pub enum SeedComponent {
    /// Constant bytes
    Constant(Vec<u8>),
    /// Reference to another account's pubkey
    AccountKey { account: String },
    /// Reference to a field within an account
    AccountData { account: String, field: String },
    /// Reference to an instruction argument
    Arg { name: String },
}

// Re-export commonly used Anchor IDL types
pub use anchor_lang::idl::types::{IdlField as Field, IdlType};

