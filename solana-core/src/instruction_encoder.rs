/// Instruction encoding for Solana programs
///
/// Supports both bincode (native Solana) and Borsh (Anchor) serialization formats.
use crate::{
    error::{Result, SolanaProgramError},
    idl_types::{SerializationFormat, ProgramIdl},
};
use anchor_lang::idl::types::{IdlField, IdlType};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use sha2::{Sha256, Digest};

/// Encode instruction data from JSON arguments
pub struct InstructionEncoder;

impl InstructionEncoder {
    pub fn encode_instruction(
        idl: &ProgramIdl,
        instruction_name: &str,
        args: &HashMap<String, JsonValue>,
    ) -> Result<Vec<u8>> {
        let instruction = idl
            .find_instruction(instruction_name)
            .ok_or_else(|| SolanaProgramError::InstructionNotFound {
                program: idl.idl.metadata.name.clone(),
                instruction: instruction_name.to_string(),
            })?;

        let format = idl.serialization_format();
        
        let mut data = Vec::new();
        
        match format {
            SerializationFormat::Borsh => {
                // Anchor programs use 8-byte discriminator (first 8 bytes of sha256("global:<instruction_name>"))
                let discriminator = Self::calculate_anchor_discriminator(instruction_name);
                data.extend_from_slice(&discriminator);
                
                // Encode arguments using Borsh
                let encoded_args = Self::encode_borsh_args(&instruction.args, args)?;
                data.extend_from_slice(&encoded_args);
            }
            SerializationFormat::Bincode => {
                // Native Solana programs use bincode (simpler, just encode the args)
                return Err(SolanaProgramError::UnsupportedType {
                    type_name: "Bincode serialization not yet implemented".to_string(),
                });
            }
        }

        Ok(data)
    }
    
    /// Calculate Anchor instruction discriminator
    /// 
    /// Anchor uses first 8 bytes of sha256("global:<instruction_name>")
    /// Reference: https://www.anchor-lang.com/docs/basics/idl
    fn calculate_anchor_discriminator(instruction_name: &str) -> [u8; 8] {
        let preimage = format!("global:{}", instruction_name);
        let mut hasher = Sha256::new();
        hasher.update(preimage.as_bytes());
        let hash = hasher.finalize();
        
        let mut discriminator = [0u8; 8];
        discriminator.copy_from_slice(&hash[..8]);
        discriminator
    }

    /// Encode arguments using Borsh serialization
    fn encode_borsh_args(
        arg_definitions: &[IdlField],
        arg_values: &HashMap<String, JsonValue>,
    ) -> Result<Vec<u8>> {
        let mut encoded = Vec::new();
        
        for arg_def in arg_definitions {
            let value = arg_values.get(&arg_def.name).ok_or_else(|| {
                SolanaProgramError::InvalidArgument {
                    arg: arg_def.name.clone(),
                    error: "Missing required argument".to_string(),
                }
            })?;
            
            let arg_bytes = Self::encode_borsh_value(&arg_def.ty, value)?;
            encoded.extend_from_slice(&arg_bytes);
        }
        
        Ok(encoded)
    }
    
    /// Encode a single value using Borsh
    fn encode_borsh_value(idl_type: &IdlType, value: &JsonValue) -> Result<Vec<u8>> {
        match idl_type {
            // Basic types
            IdlType::Bool => {
                let v = value.as_bool().ok_or_else(|| SolanaProgramError::InvalidArgument {
                    arg: "value".to_string(),
                    error: "Expected boolean".to_string(),
                })?;
                Ok(vec![if v { 1 } else { 0 }])
            }
            
            IdlType::U8 => {
                let v = Self::parse_u64(value)? as u8;
                Ok(vec![v])
            }
            
            IdlType::I8 => {
                let v = value.as_i64().ok_or_else(|| SolanaProgramError::InvalidArgument {
                    arg: "value".to_string(),
                    error: "Expected i8".to_string(),
                })? as i8;
                Ok(vec![v as u8])
            }
            
            IdlType::U16 => {
                let v = Self::parse_u64(value)? as u16;
                Ok(v.to_le_bytes().to_vec())
            }
            
            IdlType::I16 => {
                let v = value.as_i64().ok_or_else(|| SolanaProgramError::InvalidArgument {
                    arg: "value".to_string(),
                    error: "Expected i16".to_string(),
                })? as i16;
                Ok(v.to_le_bytes().to_vec())
            }
            
            IdlType::U32 => {
                let v = Self::parse_u64(value)? as u32;
                Ok(v.to_le_bytes().to_vec())
            }
            
            IdlType::I32 => {
                let v = value.as_i64().ok_or_else(|| SolanaProgramError::InvalidArgument {
                    arg: "value".to_string(),
                    error: "Expected i32".to_string(),
                })? as i32;
                Ok(v.to_le_bytes().to_vec())
            }
            
            IdlType::U64 => {
                let v = Self::parse_u64(value)?;
                Ok(v.to_le_bytes().to_vec())
            }
            
            IdlType::I64 => {
                let v = value.as_i64().ok_or_else(|| SolanaProgramError::InvalidArgument {
                    arg: "value".to_string(),
                    error: "Expected i64".to_string(),
                })?;
                Ok(v.to_le_bytes().to_vec())
            }
            
            IdlType::U128 => {
                let v = Self::parse_u128(value)?;
                Ok(v.to_le_bytes().to_vec())
            }
            
            IdlType::I128 => {
                let v = Self::parse_i128(value)?;
                Ok(v.to_le_bytes().to_vec())
            }
            
            IdlType::Bytes => {
                let bytes = if let Some(s) = value.as_str() {
                    hex::decode(s).map_err(|e| SolanaProgramError::InvalidArgument {
                        arg: "bytes".to_string(),
                        error: format!("Invalid hex string: {}", e),
                    })?
                } else if let Some(arr) = value.as_array() {
                    arr.iter()
                        .map(|v| v.as_u64().ok_or_else(|| SolanaProgramError::InvalidArgument {
                            arg: "bytes".to_string(),
                            error: "Expected array of numbers".to_string(),
                        }).map(|n| n as u8))
                        .collect::<Result<Vec<u8>>>()?
                } else {
                    return Err(SolanaProgramError::InvalidArgument {
                        arg: "bytes".to_string(),
                        error: "Expected hex string or array".to_string(),
                    });
                };
                
                // Borsh encodes Vec as length prefix + data
                let mut encoded = Vec::new();
                let len = bytes.len() as u32;
                encoded.extend_from_slice(&len.to_le_bytes());
                encoded.extend_from_slice(&bytes);
                Ok(encoded)
            }
            
            IdlType::String => {
                let s = value.as_str().ok_or_else(|| SolanaProgramError::InvalidArgument {
                    arg: "value".to_string(),
                    error: "Expected string".to_string(),
                })?;
                
                // Borsh encodes String as length prefix + UTF-8 bytes
                let bytes = s.as_bytes();
                let mut encoded = Vec::new();
                let len = bytes.len() as u32;
                encoded.extend_from_slice(&len.to_le_bytes());
                encoded.extend_from_slice(bytes);
                Ok(encoded)
            }
            
            IdlType::Pubkey => {
                let pubkey_str = value.as_str().ok_or_else(|| SolanaProgramError::InvalidArgument {
                    arg: "value".to_string(),
                    error: "Expected public key string".to_string(),
                })?;
                
                let pubkey: solana_sdk::pubkey::Pubkey = pubkey_str.parse().map_err(|e| {
                    SolanaProgramError::InvalidPubkey {
                        value: pubkey_str.to_string(),
                        error: format!("{}", e),
                    }
                })?;
                
                Ok(pubkey.to_bytes().to_vec())
            }
            
            IdlType::Vec(inner_type) => {
                let arr = value.as_array().ok_or_else(|| SolanaProgramError::InvalidArgument {
                    arg: "value".to_string(),
                    error: "Expected array".to_string(),
                })?;
                
                let mut encoded = Vec::new();
                
                // Borsh Vec encoding: u32 length prefix + elements
                let len = arr.len() as u32;
                encoded.extend_from_slice(&len.to_le_bytes());
                
                for item in arr {
                    let item_bytes = Self::encode_borsh_value(inner_type, item)?;
                    encoded.extend_from_slice(&item_bytes);
                }
                
                Ok(encoded)
            }
            
            IdlType::Option(inner_type) => {
                if value.is_null() {
                    // None: encoded as 0u8
                    Ok(vec![0])
                } else {
                    // Some: encoded as 1u8 + value
                    let mut encoded = vec![1];
                    let inner_bytes = Self::encode_borsh_value(inner_type, value)?;
                    encoded.extend_from_slice(&inner_bytes);
                    Ok(encoded)
                }
            }
            
            IdlType::Defined { name, generics: _ } => {
                // For defined types (structs/enums), we need the type definition
                // For now, return error - this needs the full IDL context
                Err(SolanaProgramError::UnsupportedType {
                    type_name: format!("Defined type '{}' encoding not yet fully implemented. Please provide flattened values.", name),
                })
            }
            
            _ => {
                Err(SolanaProgramError::UnsupportedType {
                    type_name: format!("IDL type {:?} not yet supported", idl_type),
                })
            }
        }
    }
    
    /// Parse u64 from JSON (handles both number and string)
    fn parse_u64(value: &JsonValue) -> Result<u64> {
        if let Some(n) = value.as_u64() {
            Ok(n)
        } else if let Some(s) = value.as_str() {
            s.parse::<u64>().map_err(|e| SolanaProgramError::InvalidArgument {
                arg: "value".to_string(),
                error: format!("Invalid u64 string: {}", e),
            })
        } else {
            Err(SolanaProgramError::InvalidArgument {
                arg: "value".to_string(),
                error: "Expected u64 number or string".to_string(),
            })
        }
    }
    
    /// Parse u128 from JSON string
    fn parse_u128(value: &JsonValue) -> Result<u128> {
        let s = value.as_str().ok_or_else(|| SolanaProgramError::InvalidArgument {
            arg: "value".to_string(),
            error: "Expected u128 as string".to_string(),
        })?;
        s.parse::<u128>().map_err(|e| SolanaProgramError::InvalidArgument {
            arg: "value".to_string(),
            error: format!("Invalid u128 string: {}", e),
        })
    }
    
    /// Parse i128 from JSON string
    fn parse_i128(value: &JsonValue) -> Result<i128> {
        let s = value.as_str().ok_or_else(|| SolanaProgramError::InvalidArgument {
            arg: "value".to_string(),
            error: "Expected i128 as string".to_string(),
        })?;
        s.parse::<i128>().map_err(|e| SolanaProgramError::InvalidArgument {
            arg: "value".to_string(),
            error: format!("Invalid i128 string: {}", e),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_anchor_discriminator() {
        // Test discriminator calculation
        let disc = InstructionEncoder::calculate_anchor_discriminator("initialize");
        assert_eq!(disc.len(), 8);
        
        // Known discriminator for "initialize" instruction
        // This is deterministic based on sha256("global:initialize")
        println!("Discriminator for 'initialize': {:?}", disc);
    }
    
    #[test]
    fn test_discriminator_claim() {
        // Test Jupiter's "claim" instruction discriminator
        let disc = InstructionEncoder::calculate_anchor_discriminator("claim");
        assert_eq!(disc.len(), 8);
        println!("Discriminator for 'claim': {:?}", hex::encode(disc));
    }
}
