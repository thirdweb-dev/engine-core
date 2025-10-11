use base64::{Engine, engine::general_purpose::STANDARD as Base64Engine};
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_sdk::{
    instruction::Instruction,
    message::{AccountMeta, VersionedMessage, v0},
    pubkey::Pubkey,
    transaction::VersionedTransaction,
};

/// Solana instruction data provided by the user
/// This is a simplified representation that will be converted to a proper Instruction
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SolanaInstructionData {
    /// Program ID to invoke
    #[serde_as(as = "DisplayFromStr")]
    #[schema(value_type = PubkeyDef)]
    pub program_id: Pubkey,

    /// Account keys that will be passed to the program
    pub accounts: Vec<SolanaAccountMeta>,

    /// Instruction data (hex-encoded or base64)
    pub data: String,
    pub encoding: InstructionDataEncoding,
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub enum InstructionDataEncoding {
    #[serde(rename = "hex")]
    Hex,
    #[serde(rename = "base64")]
    Base64,
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct PubkeyDef(pub String);

/// Account metadata for Solana instructions
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SolanaAccountMeta {
    /// Public key of the account
    #[serde_as(as = "DisplayFromStr")]
    #[schema(value_type = PubkeyDef)]
    pub pubkey: Pubkey,

    /// Whether the account should sign the transaction
    pub is_signer: bool,

    /// Whether the account is writable
    pub is_writable: bool,
}

impl SolanaAccountMeta {
    pub fn to_account_meta(&self) -> AccountMeta {
        match self.is_writable {
            true => AccountMeta::new(self.pubkey, self.is_signer),
            false => AccountMeta::new_readonly(self.pubkey, self.is_signer),
        }
    }
}

/// Complete resolved Solana transaction request
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SolanaTransaction {
    /// List of instructions to execute in this transaction
    pub instructions: Vec<SolanaInstructionData>,

    /// Optional recent blockhash (if not provided, will be fetched)
    #[serde_as(as = "DisplayFromStr")]
    pub recent_blockhash: solana_sdk::hash::Hash,

    /// Compute budget limit (compute units)
    /// If not provided, the transaction will use default compute budget
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compute_unit_limit: Option<u32>,

    /// Compute budget price (micro-lamports per compute unit)
    /// This is the priority fee - higher values increase transaction priority
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compute_unit_price: Option<u64>,
}

impl SolanaInstructionData {
    pub fn get_data_bytes(&self) -> Result<Vec<u8>, SolanaTransactionError> {
        match self.encoding {
            // try to decode both prefixed and non-prefixed hex
            InstructionDataEncoding::Hex => {
                let s = self
                    .data
                    .strip_prefix("0x")
                    .or_else(|| self.data.strip_prefix("0X"))
                    .unwrap_or(self.data.as_str());
                hex::decode(s).map_err(|e| SolanaTransactionError::InvalidData {
                    error: e.to_string(),
                })
            }
            InstructionDataEncoding::Base64 => {
                Ok(Base64Engine.decode(&self.data).map_err(|e| {
                    SolanaTransactionError::InvalidData {
                        error: e.to_string(),
                    }
                })?)
            }
        }
    }

    pub fn to_instruction(&self) -> Result<Instruction, SolanaTransactionError> {
        let data_bytes = self.get_data_bytes()?;
        Ok(Instruction {
            program_id: self.program_id,
            accounts: self
                .accounts
                .iter()
                .map(|acc| acc.to_account_meta())
                .collect(),
            data: data_bytes,
        })
    }
}

impl SolanaTransaction {
    /// Build a VersionedTransaction from this transaction data
    pub fn to_versioned_transaction(
        &self,
        payer: Pubkey,
        recent_blockhash: solana_sdk::hash::Hash,
    ) -> Result<VersionedTransaction, SolanaTransactionError> {
        let mut instructions: Vec<Instruction> = Vec::new();

        // Add compute budget instructions if specified
        if let Some(limit) = self.compute_unit_limit {
            let compute_limit_ix = ComputeBudgetInstruction::set_compute_unit_limit(limit);
            instructions.push(compute_limit_ix);
        }

        if let Some(price) = self.compute_unit_price {
            let compute_price_ix = ComputeBudgetInstruction::set_compute_unit_price(price);
            instructions.push(compute_price_ix);
        }

        for inst in self.instructions.iter() {
            instructions.push(inst.to_instruction()?);
        }

        // Build message based on whether we have lookup tables
        // Use legacy v0 message without lookup tables
        let message = v0::Message::try_compile(
            &payer,
            &instructions,
            &[], // No address lookup tables
            recent_blockhash,
        )
        .map_err(|e| SolanaTransactionError::MessageCompilationFailed {
            error: e.to_string(),
        })?;

        let message = VersionedMessage::V0(message);

        let num_signatures = message.header().num_required_signatures as usize;
        let signatures = vec![solana_sdk::signature::Signature::default(); num_signatures];

        // Create unsigned transaction
        Ok(VersionedTransaction {
            signatures,
            message,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SolanaTransactionError {
    #[error("Invalid pubkey for {field}: {value} - {error}")]
    InvalidPubkey {
        field: String,
        value: String,
        error: String,
    },

    #[error("Invalid instruction data: {error}")]
    InvalidData { error: String },

    #[error("Failed to compile message: {error}")]
    MessageCompilationFailed { error: String },

    #[error("Invalid blockhash: {error}")]
    InvalidBlockhash { error: String },
}
