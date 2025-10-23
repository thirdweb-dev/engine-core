/// Account resolution and PDA derivation for Solana programs
///
/// Handles:
/// - PDA (Program Derived Address) derivation from seeds
/// - Account metadata resolution (signer, writable flags)
/// - System account injection (clock, rent, etc.)
use crate::error::{Result, SolanaProgramError};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;

/// Well-known system accounts
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum SystemAccount {
    /// System Program
    #[serde(alias = "system", alias = "system-program", alias = "system_program")]
    SystemProgram,
    
    /// Token Program (original SPL Token)
    #[serde(alias = "token", alias = "token-program", alias = "token_program")]
    TokenProgram,
    
    /// Token-2022 Program
    #[serde(alias = "token2022", alias = "token-2022", alias = "token_2022")]
    Token2022Program,
    
    /// Rent Sysvar
    #[serde(alias = "rent", alias = "sysvar-rent", alias = "sysvar_rent")]
    SysvarRent,
    
    /// Clock Sysvar
    #[serde(alias = "clock", alias = "sysvar-clock", alias = "sysvar_clock")]
    SysvarClock,
    
    /// Slot Hashes Sysvar
    #[serde(alias = "slotHashes", alias = "slot-hashes", alias = "slot_hashes")]
    SysvarSlotHashes,
    
    /// Epoch Schedule Sysvar
    #[serde(alias = "epochSchedule", alias = "epoch-schedule", alias = "epoch_schedule")]
    SysvarEpochSchedule,
    
    /// Recent Blockhashes Sysvar
    #[serde(alias = "recentBlockhashes", alias = "recent-blockhashes", alias = "recent_blockhashes")]
    SysvarRecentBlockhashes,
    
    /// Instructions Sysvar
    #[serde(alias = "instructions", alias = "sysvar-instructions", alias = "sysvar_instructions")]
    SysvarInstructions,
}

impl SystemAccount {
    /// Get the pubkey for this system account
    pub fn pubkey(&self) -> Pubkey {
        match self {
            Self::SystemProgram => solana_system_interface::program::ID,
            Self::TokenProgram => spl_token_interface::ID,
            Self::Token2022Program => spl_token_2022_interface::ID,
            Self::SysvarRent => solana_sdk::sysvar::rent::ID,
            Self::SysvarClock => solana_sdk::sysvar::clock::ID,
            Self::SysvarSlotHashes => solana_sdk::sysvar::slot_hashes::ID,
            Self::SysvarEpochSchedule => solana_sdk::sysvar::epoch_schedule::ID,
            Self::SysvarRecentBlockhashes => solana_sdk::sysvar::recent_blockhashes::ID,
            Self::SysvarInstructions => solana_sdk::sysvar::instructions::ID,
        }
    }
}

/// Account resolver for deriving and validating accounts
pub struct AccountResolver;

impl AccountResolver {
    /// Derive a PDA from seeds and program ID
    /// 
    /// The bump seed is automatically found - users don't need to specify it.
    /// This function finds the first valid bump (starting from 255 going down to 0).
    pub fn derive_pda(seeds: &[SeedValue], program_id: &Pubkey) -> Result<(Pubkey, u8)> {
        // Convert SeedValues to byte slices
        let seed_bytes: Result<Vec<Vec<u8>>> = seeds.iter().map(|s| s.to_bytes()).collect();
        let seed_bytes = seed_bytes?;
        let seed_refs: Vec<&[u8]> = seed_bytes.iter().map(|v| v.as_slice()).collect();

        Pubkey::try_find_program_address(&seed_refs, program_id).ok_or_else(|| {
            SolanaProgramError::PdaDerivationError {
                account: "pda".to_string(),
                error: "Failed to find valid program address".to_string(),
            }
        })
    }
    
    /// Derive an Associated Token Account (ATA) address
    /// 
    /// This is a convenience function for the common case of deriving SPL token accounts.
    /// Users only need to provide owner and mint - the bump is found automatically.
    /// 
    /// # Arguments
    /// * `owner` - The wallet that owns the token account
    /// * `mint` - The token mint address
    /// * `token_program` - The token program (TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA for SPL Token)
    /// 
    /// # Returns
    /// Returns `(ata_address, bump_seed)` where bump_seed is automatically found
    pub fn derive_ata(owner: &Pubkey, mint: &Pubkey, token_program: &Pubkey) -> Result<(Pubkey, u8)> {
        const ASSOCIATED_TOKEN_PROGRAM: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
        let ata_program = ASSOCIATED_TOKEN_PROGRAM.parse::<Pubkey>().unwrap();
        
        let seeds = vec![
            SeedValue::Pubkey { value: owner.to_string() },
            SeedValue::Pubkey { value: token_program.to_string() },
            SeedValue::Pubkey { value: mint.to_string() },
        ];
        
        Self::derive_pda(&seeds, &ata_program)
    }

    /// Resolve seed component to actual bytes
    pub fn resolve_seed(
        seed: &crate::idl_types::SeedComponent,
        provided_accounts: &HashMap<String, Pubkey>,
        args: &HashMap<String, serde_json::Value>,
    ) -> Result<SeedValue> {
        match seed {
            crate::idl_types::SeedComponent::Constant(bytes) => Ok(SeedValue::Bytes { value: bytes.clone() }),

            crate::idl_types::SeedComponent::AccountKey { account } => {
                let pubkey = provided_accounts.get(account).ok_or_else(|| {
                    SolanaProgramError::MissingAccount {
                        name: account.clone(),
                        hint: "Required as seed for PDA derivation".to_string(),
                    }
                })?;
                Ok(SeedValue::Pubkey { value: pubkey.to_string() })
            }

            crate::idl_types::SeedComponent::Arg { name } => {
                let value = args.get(name).ok_or_else(|| {
                    SolanaProgramError::InvalidArgument {
                        arg: name.clone(),
                        error: "Required as seed for PDA derivation".to_string(),
                    }
                })?;

                // Try to parse as string (which might be a pubkey or other data)
                if let Some(s) = value.as_str() {
                    // Try parsing as pubkey first
                    if let Ok(pubkey) = s.parse::<Pubkey>() {
                        return Ok(SeedValue::Pubkey { value: pubkey.to_string() });
                    }
                    // Otherwise treat as string bytes
                    return Ok(SeedValue::String { value: s.to_string() });
                }

                // Try to parse as number
                if let Some(num) = value.as_u64() {
                    return Ok(SeedValue::U64 { value: num });
                }

                Err(SolanaProgramError::InvalidSeed {
                    error: format!("Unsupported seed value type for argument '{}'", name),
                })
            }

            crate::idl_types::SeedComponent::AccountData { account, field } => {
                // For now, we can't read account data without RPC calls
                // This would require fetching the account and deserializing it
                Err(SolanaProgramError::UnsupportedType {
                    type_name: format!("AccountData seed for {}.{} requires RPC access", account, field),
                })
            }
        }
    }

    /// Try to get system account pubkey from enum or string
    pub fn get_system_account(identifier: &str) -> Option<Pubkey> {
        // Try to deserialize as SystemAccount enum
        serde_json::from_value::<SystemAccount>(serde_json::Value::String(identifier.to_string()))
            .ok()
            .map(|acc| acc.pubkey())
    }
}

/// Seed value for PDA derivation
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum SeedValue {
    /// Raw bytes
    Bytes { value: Vec<u8> },
    /// Public key (32 bytes)
    Pubkey { value: String },
    /// Unsigned 64-bit integer (little-endian)
    U64 { value: u64 },
    /// String (UTF-8 encoded)
    String { value: String },
}

impl SeedValue {
    /// Convert to bytes for PDA derivation
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        match self {
            SeedValue::Bytes { value } => Ok(value.clone()),
            SeedValue::Pubkey { value } => {
                let pubkey: Pubkey = value.parse().map_err(|e| {
                    SolanaProgramError::InvalidPubkey {
                        value: value.clone(),
                        error: format!("{}", e),
                    }
                })?;
                Ok(pubkey.to_bytes().to_vec())
            }
            SeedValue::U64 { value } => Ok(value.to_le_bytes().to_vec()),
            SeedValue::String { value } => Ok(value.as_bytes().to_vec()),
        }
    }

    /// Helper to get bytes reference (where possible)
    pub fn as_bytes(&self) -> Vec<u8> {
        self.to_bytes().unwrap_or_default()
    }
}

/// Resolved account with all metadata
#[derive(Debug, Clone)]
pub struct ResolvedAccount {
    pub name: String,
    pub pubkey: Pubkey,
    pub is_signer: bool,
    pub is_writable: bool,
    pub source: AccountSource,
}

/// Source of an account in the resolution process
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountSource {
    /// User provided in the request
    Provided,
    /// Derived from PDA seeds
    Derived,
    /// System account (injected automatically)
    System,
    /// The transaction signer
    Signer,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_account_resolution() {
        let system = AccountResolver::get_system_account("system_program");
        assert!(system.is_some());
        assert_eq!(system.unwrap(), solana_system_interface::program::ID);
    }

    #[test]
    fn test_constant_seed() {
        let seed = crate::idl_types::SeedComponent::Constant(b"metadata".to_vec());
        let resolved = AccountResolver::resolve_seed(
            &seed,
            &HashMap::new(),
            &HashMap::new(),
        );
        assert!(resolved.is_ok());
    }

    #[test]
    fn test_pda_derivation() {
        let program_id = spl_token_interface::ID;
        let seeds = vec![SeedValue::Bytes { value: b"metadata".to_vec() }];
        
        let result = AccountResolver::derive_pda(&seeds, &program_id);
        assert!(result.is_ok());
    }

    #[test]
    fn test_system_account_serde() {
        // Test that serde aliases work
        let json = r#""system""#;
        let acc: SystemAccount = serde_json::from_str(json).unwrap();
        assert_eq!(acc, SystemAccount::SystemProgram);

        let json = r#""systemProgram""#;
        let acc: SystemAccount = serde_json::from_str(json).unwrap();
        assert_eq!(acc, SystemAccount::SystemProgram);
    }
}

