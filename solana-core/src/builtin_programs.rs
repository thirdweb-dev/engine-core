/// Built-in program registry for common Solana programs
///
/// Provides well-known program IDs and instruction builders for:
/// - System Program
/// - SPL Token & Token-2022
/// - Associated Token Account
/// - Memo
/// - Compute Budget
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;

/// Well-known Solana programs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WellKnownProgram {
    System,
    Token,
    Token2022,
    AssociatedToken,
    Memo,
    ComputeBudget,
}

impl WellKnownProgram {
    /// Get the program ID
    pub fn program_id(&self) -> Pubkey {
        match self {
            Self::System => solana_system_interface::program::ID,
            Self::Token => spl_token_interface::ID,
            Self::Token2022 => spl_token_2022_interface::ID,
            // Note: SPL interface crates may not export ID directly, we'll use known addresses
            Self::AssociatedToken => {
                // ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL
                Pubkey::from_str("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL")
                    .expect("valid pubkey")
            }
            Self::Memo => {
                // MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr
                Pubkey::from_str("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr")
                    .expect("valid pubkey")
            }
            Self::ComputeBudget => solana_compute_budget_interface::ID,
        }
    }

    /// Get the program name
    pub fn name(&self) -> &'static str {
        match self {
            Self::System => "system",
            Self::Token => "spl-token",
            Self::Token2022 => "spl-token-2022",
            Self::AssociatedToken => "spl-associated-token",
            Self::Memo => "spl-memo",
            Self::ComputeBudget => "compute-budget",
        }
    }

    /// Try to get well-known program from name or address
    pub fn from_identifier(identifier: &str) -> Option<Self> {
        // Try by name first
        match identifier.to_lowercase().as_str() {
            "system" | "system-program" => Some(Self::System),
            "spl-token" | "token" => Some(Self::Token),
            "spl-token-2022" | "token-2022" | "token2022" => Some(Self::Token2022),
            "spl-associated-token" | "associated-token" | "ata" => Some(Self::AssociatedToken),
            "spl-memo" | "memo" => Some(Self::Memo),
            "compute-budget" => Some(Self::ComputeBudget),
            _ => {
                // Try by pubkey
                if let Ok(pubkey) = Pubkey::from_str(identifier) {
                    Self::from_pubkey(&pubkey)
                } else {
                    None
                }
            }
        }
    }

    /// Get well-known program from pubkey
    pub fn from_pubkey(pubkey: &Pubkey) -> Option<Self> {
        if pubkey == &solana_system_interface::program::ID {
            Some(Self::System)
        } else if pubkey == &spl_token_interface::ID {
            Some(Self::Token)
        } else if pubkey == &spl_token_2022_interface::ID {
            Some(Self::Token2022)
        } else if pubkey
            == &Pubkey::from_str("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL").ok()?
        {
            Some(Self::AssociatedToken)
        } else if pubkey == &Pubkey::from_str("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr").ok()? {
            Some(Self::Memo)
        } else if pubkey == &solana_compute_budget_interface::ID {
            Some(Self::ComputeBudget)
        } else {
            None
        }
    }

    /// Check if this program has a built-in IDL
    pub fn has_builtin_idl(&self) -> bool {
        // System and SPL programs have well-defined instructions
        // We'll generate IDLs for them at runtime
        matches!(
            self,
            Self::System | Self::Token | Self::Token2022 | Self::AssociatedToken
        )
    }
}

/// Program information
#[derive(Debug, Clone)]
pub struct ProgramInfo {
    pub name: String,
    pub program_id: Pubkey,
    pub well_known: Option<WellKnownProgram>,
}

impl ProgramInfo {
    /// Create from identifier (name or address)
    pub fn from_identifier(identifier: &str) -> crate::error::Result<Self> {
        // Check if it's a well-known program
        if let Some(well_known) = WellKnownProgram::from_identifier(identifier) {
            return Ok(Self {
                name: well_known.name().to_string(),
                program_id: well_known.program_id(),
                well_known: Some(well_known),
            });
        }

        // Try to parse as pubkey
        let program_id = Pubkey::from_str(identifier).map_err(|e| {
            crate::error::SolanaProgramError::InvalidPubkey {
                value: identifier.to_string(),
                error: e.to_string(),
            }
        })?;

        Ok(Self {
            name: program_id.to_string(),
            program_id,
            well_known: None,
        })
    }
}

/// Program identifier that can be deserialized from either alias or pubkey
///
/// Accepts JSON in two formats:
/// 1. `{ "programName": "spl-token" }` - Well-known program alias
/// 2. `{ "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" }` - Direct pubkey
///
/// Examples:
/// ```json
/// { "programName": "spl-token" }
/// { "programName": "system" }
/// { "programId": "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4" }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase", untagged)]
pub enum ProgramIdentifier {
    /// Well-known program by name/alias
    #[serde(rename_all = "camelCase")]
    Named {
        /// Alias for well-known programs (e.g., "spl-token", "system", "spl-token-2022")
        program_name: WellKnownProgramName,
    },
    /// Custom program by pubkey string
    #[serde(rename_all = "camelCase")]
    Address {
        /// Program ID as base58 string
        program_id: String,
    },
}

/// Well-known program names/aliases
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum WellKnownProgramName {
    /// System Program (11111111111111111111111111111111)
    System,
    /// SPL Token Program
    #[serde(alias = "token")]
    SplToken,
    /// SPL Token-2022 Program
    #[serde(alias = "token-2022", alias = "token2022")]
    SplToken2022,
    /// Associated Token Account Program
    #[serde(alias = "ata", alias = "associated-token")]
    SplAssociatedToken,
    /// Memo Program
    SplMemo,
    /// Compute Budget Program
    ComputeBudget,
}

impl WellKnownProgramName {
    /// Convert to WellKnownProgram enum
    pub fn to_program(&self) -> WellKnownProgram {
        match self {
            Self::System => WellKnownProgram::System,
            Self::SplToken => WellKnownProgram::Token,
            Self::SplToken2022 => WellKnownProgram::Token2022,
            Self::SplAssociatedToken => WellKnownProgram::AssociatedToken,
            Self::SplMemo => WellKnownProgram::Memo,
            Self::ComputeBudget => WellKnownProgram::ComputeBudget,
        }
    }
}

impl ProgramIdentifier {
    /// Resolve to ProgramInfo
    pub fn resolve(&self) -> crate::error::Result<ProgramInfo> {
        match self {
            Self::Named { program_name } => {
                let program = program_name.to_program();
                Ok(ProgramInfo {
                    name: program.name().to_string(),
                    program_id: program.program_id(),
                    well_known: Some(program),
                })
            }
            Self::Address { program_id } => ProgramInfo::from_identifier(program_id),
        }
    }
    
    /// Get the raw string value for display
    pub fn as_str(&self) -> String {
        match self {
            Self::Named { program_name } => program_name.to_program().name().to_string(),
            Self::Address { program_id } => program_id.clone(),
        }
    }
}

impl From<String> for ProgramIdentifier {
    fn from(s: String) -> Self {
        Self::Address { program_id: s }
    }
}

impl From<&str> for ProgramIdentifier {
    fn from(s: &str) -> Self {
        Self::Address {
            program_id: s.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_well_known_program_from_name() {
        assert_eq!(
            WellKnownProgram::from_identifier("system"),
            Some(WellKnownProgram::System)
        );
        assert_eq!(
            WellKnownProgram::from_identifier("spl-token"),
            Some(WellKnownProgram::Token)
        );
        assert_eq!(
            WellKnownProgram::from_identifier("SPL-TOKEN"),
            Some(WellKnownProgram::Token)
        );
    }

    #[test]
    fn test_well_known_program_from_pubkey() {
        let system_id = solana_system_interface::program::ID;
        assert_eq!(
            WellKnownProgram::from_identifier(&system_id.to_string()),
            Some(WellKnownProgram::System)
        );
    }

    #[test]
    fn test_program_info_from_unknown_pubkey() {
        let random_pubkey = "11111111111111111111111111111112";
        let info = ProgramInfo::from_identifier(random_pubkey).unwrap();
        assert!(info.well_known.is_none());
        assert_eq!(info.program_id.to_string(), random_pubkey);
    }
    
    #[test]
    fn test_program_identifier_from_name() {
        // Test programName variant
        let json = r#"{"programName": "spl-token"}"#;
        let identifier: ProgramIdentifier = serde_json::from_str(json).unwrap();
        let info = identifier.resolve().unwrap();
        assert_eq!(info.well_known, Some(WellKnownProgram::Token));
        assert_eq!(info.program_id, spl_token_interface::ID);
    }
    
    #[test]
    fn test_program_identifier_from_id() {
        // Test programId variant
        let json = r#"{"programId": "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"}"#;
        let identifier: ProgramIdentifier = serde_json::from_str(json).unwrap();
        let info = identifier.resolve().unwrap();
        assert_eq!(
            info.program_id.to_string(),
            "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"
        );
    }
    
    #[test]
    fn test_program_identifier_aliases() {
        // Test various aliases
        let test_cases = vec![
            (r#"{"programName": "system"}"#, WellKnownProgram::System),
            (r#"{"programName": "token"}"#, WellKnownProgram::Token),
            (r#"{"programName": "spl-token"}"#, WellKnownProgram::Token),
            (r#"{"programName": "token-2022"}"#, WellKnownProgram::Token2022),
            (r#"{"programName": "ata"}"#, WellKnownProgram::AssociatedToken),
        ];
        
        for (json, expected) in test_cases {
            let identifier: ProgramIdentifier = serde_json::from_str(json).unwrap();
            let info = identifier.resolve().unwrap();
            assert_eq!(
                info.well_known,
                Some(expected),
                "Failed for JSON: {}",
                json
            );
        }
    }
}

