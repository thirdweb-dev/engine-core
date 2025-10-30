/// High-level program call API
///
/// This module provides the main API for preparing Solana program instructions
/// from user-friendly JSON parameters.
use crate::{
    account_resolver::AccountSource,
    builtin_programs::{ProgramIdentifier, ProgramInfo},
    error::{Result, SolanaProgramError},
    idl_cache::IdlCache,
    idl_types::ProgramIdl,
    instruction_encoder::InstructionEncoder,
    transaction::{InstructionDataEncoding, SolanaAccountMeta, SolanaInstructionData},
};
use base64::Engine;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use std::{collections::HashMap, sync::Arc};

/// Resolved account with all metadata
#[derive(Debug, Clone)]
pub struct ResolvedAccount {
    pub name: String,
    pub pubkey: Pubkey,
    pub is_signer: bool,
    pub is_writable: bool,
    pub source: AccountSource,
}

/// High-level program call request
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProgramCall {
    /// Program address or well-known name (e.g., "spl-token", "system", or pubkey string)
    pub program: ProgramIdentifier,

    /// Instruction name (e.g., "transfer", "swapBaseIn")
    pub instruction: String,

    /// Named account mappings (account_name -> pubkey_string)
    #[serde(default)]
    pub accounts: HashMap<String, String>,

    /// Instruction arguments as JSON
    #[serde(default)]
    pub args: HashMap<String, serde_json::Value>,

    /// Optional IDL for custom programs (if not available in cache)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idl: Option<serde_json::Value>,
}

/// Prepared program call ready for execution
#[derive(Debug, Clone)]
pub struct PreparedProgramCall {
    /// The resolved instruction data
    pub instruction: SolanaInstructionData,

    /// Metadata about resolved accounts
    pub resolved_accounts: Vec<ResolvedAccount>,

    /// The program that will be invoked
    pub program_id: Pubkey,

    /// The instruction name
    pub instruction_name: String,
}

impl ProgramCall {
    /// Prepare this program call for execution
    ///
    /// This resolves the program, fetches/parses the IDL, resolves accounts,
    /// and encodes the instruction data.
    pub async fn prepare(
        &self,
        signer: Pubkey,
        idl_cache: &IdlCache,
    ) -> Result<PreparedProgramCall> {
        // 1. Resolve program ID
        let program_info = self.program.resolve()?;
        let program_id = program_info.program_id;

        // 2. Get or build IDL
        let idl = self.get_or_fetch_idl(&program_info, idl_cache).await?;

        // 3. Resolve accounts
        let resolved_accounts =
            self.resolve_accounts(&idl, &signer, &program_id).await?;

        // 4. Encode instruction data
        let instruction_data = InstructionEncoder::encode_instruction(
            &idl,
            &self.instruction,
            &self.args,
        )?;

        // 5. Build final instruction
        let accounts: Vec<SolanaAccountMeta> = resolved_accounts
            .iter()
            .map(|acc| SolanaAccountMeta {
                pubkey: acc.pubkey,
                is_signer: acc.is_signer,
                is_writable: acc.is_writable,
            })
            .collect();

        let instruction = SolanaInstructionData {
            program_id,
            accounts,
            data: base64::engine::general_purpose::STANDARD.encode(&instruction_data),
            encoding: InstructionDataEncoding::Base64,
        };

        Ok(PreparedProgramCall {
            instruction,
            resolved_accounts,
            program_id,
            instruction_name: self.instruction.clone(),
        })
    }

    /// Get IDL from cache, user-provided, or built-in
    async fn get_or_fetch_idl(
        &self,
        program_info: &ProgramInfo,
        idl_cache: &IdlCache,
    ) -> Result<Arc<ProgramIdl>> {
        // If user provided IDL, use that
        if let Some(ref idl_json) = self.idl {
            let idl_str = serde_json::to_string(idl_json).map_err(|e| {
                SolanaProgramError::IdlParseError {
                    error: e.to_string(),
                }
            })?;
            let idl = ProgramIdl::from_json(&idl_str)?;
            return Ok(Arc::new(idl));
        }

        // Check if it's a well-known program with built-in IDL
        if let Some(well_known) = program_info.well_known && well_known.has_builtin_idl() {
            // TODO: Return built-in IDL for system/SPL programs
            // For now, fall through to fetch
        }

        // Fetch from cache/API
        idl_cache.get_idl(&program_info.program_id).await
    }

    /// Resolve all accounts for the instruction based on IDL
    /// 
    /// Resolution strategy:
    /// 1. If account has `address` in IDL → Use that fixed address
    /// 2. If account has `pda` in IDL → Derive from seeds
    /// 3. If account is well-known program → Use system constant
    /// 4. Otherwise → User must provide it
    async fn resolve_accounts(
        &self,
        idl: &ProgramIdl,
        signer: &Pubkey,
        program_id: &Pubkey,
    ) -> Result<Vec<ResolvedAccount>> {
        use anchor_lang::idl::types::IdlInstructionAccountItem;
        
        // Find the instruction in IDL
        let idl_instruction = idl
            .find_instruction(&self.instruction)
            .ok_or_else(|| SolanaProgramError::InstructionNotFound {
                program: idl.idl.metadata.name.clone(),
                instruction: self.instruction.clone(),
            })?;

        // Parse user-provided accounts
        let mut provided_accounts: HashMap<String, Pubkey> = HashMap::new();
        for (name, pubkey_str) in &self.accounts {
            let pubkey = pubkey_str.parse().map_err(|e| {
                SolanaProgramError::InvalidPubkey {
                    value: pubkey_str.clone(),
                    error: format!("{}", e),
                }
            })?;
            provided_accounts.insert(name.clone(), pubkey);
        }
        
        // Add common implicit accounts (signer is always available)
        provided_accounts.entry("authority".to_string()).or_insert(*signer);
        provided_accounts.entry("signer".to_string()).or_insert(*signer);
        provided_accounts.entry("payer".to_string()).or_insert(*signer);

        let mut resolved = Vec::new();
        let mut resolved_map: HashMap<String, Pubkey> = provided_accounts.clone();

        // Resolve each account from IDL
        for account_item in &idl_instruction.accounts {
            match account_item {
                IdlInstructionAccountItem::Single(account) => {
                    let (pubkey, source) = if let Some(address_str) = &account.address {
                        // Fixed address in IDL
                        let pubkey = address_str.parse().map_err(|e| {
                            SolanaProgramError::InvalidPubkey {
                                value: address_str.clone(),
                                error: format!("{}", e),
                            }
                        })?;
                        (pubkey, AccountSource::System)
                        
                    } else if let Some(pda_info) = &account.pda {
                        // PDA - derive from seeds
                        let seeds = self.resolve_pda_seeds(
                            &pda_info.seeds,
                            &resolved_map,
                            &self.args,
                        )?;
                        
                        let program_for_derivation = if let Some(program_seed) = &pda_info.program {
                            // Custom program for derivation
                            self.resolve_seed_value(program_seed, &resolved_map, &self.args)?
                                .try_into()
                                .map_err(|_| SolanaProgramError::InvalidSeed {
                                    error: "Program seed must resolve to 32 bytes".to_string(),
                                })?
                        } else {
                            *program_id
                        };
                        
                        let (pda, _bump) = Pubkey::find_program_address(
                            &seeds.iter().map(|s| s.as_slice()).collect::<Vec<_>>(),
                            &program_for_derivation,
                        );
                        
                        // Store for later references
                        resolved_map.insert(account.name.clone(), pda);
                        
                        (pda, AccountSource::Derived)
                        
                    } else if Self::is_well_known_program(&account.name) {
                        // System programs
                        let pubkey = Self::get_well_known_program(&account.name)?;
                        (pubkey, AccountSource::System)
                        
                    } else {
                        // User must provide this account
                        let pubkey = provided_accounts
                            .get(&account.name)
                            .ok_or_else(|| SolanaProgramError::MissingAccount {
                                name: account.name.clone(),
                                hint: format!(
                                    "This account is not a PDA and must be provided. \
                                     Available accounts: {:?}",
                                    provided_accounts.keys().collect::<Vec<_>>()
                                ),
                            })?;
                        (*pubkey, AccountSource::Provided)
                    };
                    
                    // Store in map for later PDA derivations
                    resolved_map.insert(account.name.clone(), pubkey);
                    
                    resolved.push(ResolvedAccount {
                        name: account.name.clone(),
                        pubkey,
                        is_signer: account.signer || pubkey == *signer,
                        is_writable: account.writable,
                        source,
                    });
                }
                IdlInstructionAccountItem::Composite(_composite) => {
                    // TODO: Handle composite accounts (nested account structures)
                    return Err(SolanaProgramError::UnsupportedType {
                        type_name: "Composite accounts not yet supported".to_string(),
                    });
                }
            }
        }

        Ok(resolved)
    }
    
    /// Resolve PDA seeds from IDL seed definitions
    fn resolve_pda_seeds(
        &self,
        seed_defs: &[anchor_lang::idl::types::IdlSeed],
        resolved_accounts: &HashMap<String, Pubkey>,
        args: &HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Vec<u8>>> {
        let mut seeds = Vec::new();
        
        for seed_def in seed_defs {
            let seed_bytes = self.resolve_seed_value(seed_def, resolved_accounts, args)?;
            seeds.push(seed_bytes);
        }
        
        Ok(seeds)
    }
    
    /// Resolve a single seed value
    fn resolve_seed_value(
        &self,
        seed_def: &anchor_lang::idl::types::IdlSeed,
        resolved_accounts: &HashMap<String, Pubkey>,
        args: &HashMap<String, serde_json::Value>,
    ) -> Result<Vec<u8>> {
        
        match seed_def {
            anchor_lang::idl::types::IdlSeed::Const(const_seed) => {
                // Literal bytes
                Ok(const_seed.value.clone())
            }
            anchor_lang::idl::types::IdlSeed::Arg(arg_seed) => {
                // Reference to an argument
                let arg_value = args.get(&arg_seed.path).ok_or_else(|| {
                    SolanaProgramError::InvalidArgument {
                        arg: arg_seed.path.clone(),
                        error: format!("Argument '{}' required for PDA seed but not provided", arg_seed.path),
                    }
                })?;
                
                // Serialize argument value to bytes
                self.serialize_arg_for_seed(arg_value)
            }
            anchor_lang::idl::types::IdlSeed::Account(account_seed) => {
                // Reference to another account's pubkey
                let account_name = &account_seed.path;
                let pubkey = resolved_accounts.get(account_name).ok_or_else(|| {
                    SolanaProgramError::PdaDerivationError {
                        account: account_name.clone(),
                        error: format!(
                            "Account '{}' needed for PDA seed but not resolved yet. \
                             Available: {:?}",
                            account_name,
                            resolved_accounts.keys().collect::<Vec<_>>()
                        ),
                    }
                })?;
                
                Ok(pubkey.to_bytes().to_vec())
            }
        }
    }
    
    /// Serialize an argument value for use in PDA seeds
    fn serialize_arg_for_seed(&self, value: &serde_json::Value) -> Result<Vec<u8>> {
        use serde_json::Value;
        
        match value {
            Value::String(s) => {
                // Try parsing as pubkey first
                if let Ok(pubkey) = s.parse::<Pubkey>() {
                    Ok(pubkey.to_bytes().to_vec())
                } else {
                    // Otherwise use as string bytes
                    Ok(s.as_bytes().to_vec())
                }
            }
            Value::Number(n) => {
                if let Some(u) = n.as_u64() {
                    Ok(u.to_le_bytes().to_vec())
                } else if let Some(i) = n.as_i64() {
                    Ok(i.to_le_bytes().to_vec())
                } else {
                    Err(SolanaProgramError::InvalidArgument {
                        arg: "seed".to_string(),
                        error: "Unsupported number type for seed".to_string(),
                    })
                }
            }
            Value::Bool(b) => Ok(vec![if *b { 1 } else { 0 }]),
            Value::Array(arr) => {
                // Flatten array of bytes
                arr.iter()
                    .map(|v| {
                        v.as_u64()
                            .ok_or_else(|| SolanaProgramError::InvalidArgument {
                                arg: "seed".to_string(),
                                error: "Array elements must be numbers".to_string(),
                            })
                            .map(|n| n as u8)
                    })
                    .collect()
            }
            _ => Err(SolanaProgramError::InvalidArgument {
                arg: "seed".to_string(),
                error: format!("Unsupported value type for seed: {:?}", value),
            }),
        }
    }
    
    /// Check if an account name refers to a well-known program or sysvar
    fn is_well_known_program(name: &str) -> bool {
        use crate::builtin_programs::WellKnownProgram;
        
        // Check if it's a well-known program
        if WellKnownProgram::from_identifier(name).is_some() {
            return true;
        }
        
        // Check if it's a sysvar
        matches!(name, "rent" | "clock" | "rent_sysvar" | "clock_sysvar")
    }
    
    /// Get the pubkey for a well-known program or sysvar
    fn get_well_known_program(name: &str) -> Result<Pubkey> {
        use crate::builtin_programs::WellKnownProgram;
        
        // Try well-known programs first
        if let Some(program) = WellKnownProgram::from_identifier(name) {
            return Ok(program.program_id());
        }
        
        // Try sysvars
        let pubkey = match name {
            "rent" | "rent_sysvar" => solana_sdk::sysvar::rent::ID,
            "clock" | "clock_sysvar" => solana_sdk::sysvar::clock::ID,
            _ => {
                return Err(SolanaProgramError::MissingAccount {
                    name: name.to_string(),
                    hint: "Unknown well-known program or sysvar".to_string(),
                })
            }
        };
        
        Ok(pubkey)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_program_call_creation() {
        let call = ProgramCall {
            program: ProgramIdentifier::Address {
                program_id: "spl-token".to_string(),
            },
            instruction: "transfer".to_string(),
            accounts: HashMap::new(),
            args: HashMap::new(),
            idl: None,
        };

        assert_eq!(call.instruction, "transfer");
    }
}

