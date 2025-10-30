use engine_solana_core::SolanaInstructionData;
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use solana_commitment_config::CommitmentLevel as SolanaCommitmentLevel;
use solana_sdk::pubkey::Pubkey;

/// Solana chain identifier
#[derive(Deserialize, Serialize, Debug, Clone, utoipa::ToSchema, Hash, Eq, PartialEq, Copy)]
pub enum SolanaChainId {
    /// Solana mainnet-beta
    #[serde(rename = "solana:mainnet")]
    SolanaMainnet,
    /// Solana devnet
    #[serde(rename = "solana:devnet")]
    SolanaDevnet,
    /// Local Solana cluster (usually localhost)
    #[serde(rename = "solana:local")]
    SolanaLocal,
}

impl SolanaChainId {
    /// Get the RPC URL for this chain
    pub fn default_rpc_url(&self) -> &'static str {
        match self {
            SolanaChainId::SolanaMainnet => "https://api.mainnet-beta.solana.com",
            SolanaChainId::SolanaDevnet => "https://api.devnet.solana.com",
            SolanaChainId::SolanaLocal => "http://127.0.0.1:8899",
        }
    }

    /// Get a string representation for logging
    pub fn as_str(&self) -> &'static str {
        match self {
            SolanaChainId::SolanaMainnet => "solana:mainnet",
            SolanaChainId::SolanaDevnet => "solana:devnet",
            SolanaChainId::SolanaLocal => "solana:local",
        }
    }
}

/// ### Solana Execution Options
/// This struct configures Solana transaction execution.
///
/// Solana execution sends transactions directly to Solana RPC nodes with
/// support for priority fees, compute budgets, and commitment levels.
///
/// ### Use Cases
/// - SOL transfers
/// - SPL token operations
/// - Program interactions (NFTs, DeFi, etc.)
/// - Arbitrary instruction composition
///
/// ### Features
/// - Versioned transactions with address lookup tables
/// - Priority fee configuration for faster inclusion
/// - Compute budget optimization
/// - Configurable commitment levels
/// - Blockhash retry mechanism
#[serde_as]
#[derive(Deserialize, Serialize, Debug, Clone, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SolanaExecutionOptions {
    /// The Solana address (public key) to send transactions from
    /// This account must have sufficient SOL to pay for transaction fees
    #[serde_as(as = "DisplayFromStr")]
    #[schema(value_type = PubkeyDef)]
    pub signer_address: Pubkey,

    /// Solana chain to use
    pub chain_id: SolanaChainId,

    /// Maximum number of times to retry fetching a new blockhash if transaction fails
    /// Default: 0 (fail fast)
    /// Max: 1000
    #[serde(default)]
    pub max_blockhash_retries: u32,

    /// Commitment level for transaction confirmation
    /// Options: "processed", "confirmed", "finalized"
    /// Default: "finalized"
    #[serde(default)]
    pub commitment: CommitmentLevel,

    /// Priority fee for the transaction. If omitted, no priority fee will be used. If provided, engine will add the compute budget instruction to the transaction.
    #[serde(default)]
    pub priority_fee: Option<SolanaPriorityFee>,

    /// Compute unit limit for the transaction. If omitted, the transaction will use the default compute unit limit. If provided, engine will add the compute budget instruction to the transaction.
    #[serde(default)]
    pub compute_unit_limit: Option<u32>,
}

#[derive(Serialize, Deserialize, Clone, Debug, utoipa::ToSchema, Copy)]
#[schema(title = "Solana Priority Fee")]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum SolanaPriorityFee {
    /// Auto-select the priority fee based on the current network conditions
    Auto,
    /// Manually set the priority fee
    #[serde(rename_all = "camelCase")]
    Manual { micro_lamports_per_unit: u64 },
    /// Select the priority fee based on the current network conditions
    Percentile { percentile: u8 },
}

#[derive(Serialize, Deserialize, Clone, utoipa::ToSchema, Default, Debug)]
#[schema(title = "Commitment Level")]
pub enum CommitmentLevel {
    #[serde(rename = "confirmed")]
    Confirmed,
    #[serde(rename = "finalized")]
    #[default]
    Finalized,
}

#[derive(Serialize, Deserialize, Clone, utoipa::ToSchema)]
#[schema(title = "Pubkey")]
pub struct PubkeyDef(pub String);

impl CommitmentLevel {
    pub fn to_commitment_level(&self) -> SolanaCommitmentLevel {
        match self {
            CommitmentLevel::Confirmed => SolanaCommitmentLevel::Confirmed,
            CommitmentLevel::Finalized => SolanaCommitmentLevel::Finalized,
        }
    }
}


#[derive(Serialize, Deserialize, Clone, Debug, utoipa::ToSchema)]
#[schema(title = "Solana Transaction Options")]
#[serde(rename_all = "camelCase")]
pub struct SolanaTransactionOptions {
    /// Transaction input
    #[serde(flatten)]
    pub input: engine_solana_core::transaction::SolanaTransactionInput,

    /// Solana execution options
    pub execution_options: SolanaExecutionOptions,
}

/// Request to send a Solana transaction
#[derive(Serialize, Deserialize, Clone, Debug, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SendSolanaTransactionRequest {
    /// Idempotency key for this transaction (defaults to random UUID)
    #[serde(default = "super::default_idempotency_key")]
    pub idempotency_key: String,

    /// Transaction input (either instructions or serialized transaction)
    #[serde(flatten)]
    pub input: engine_solana_core::transaction::SolanaTransactionInput,

    /// Solana execution options
    pub execution_options: SolanaExecutionOptions,

    /// Webhook options for transaction status notifications
    #[serde(default)]
    pub webhook_options: Vec<super::WebhookOptions>,
}

/// Response for a queued Solana transaction
#[derive(Serialize, Deserialize, Clone, Debug, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueuedSolanaTransactionResponse {
    /// The idempotency key for this transaction
    pub transaction_id: String,
    /// The Solana chain
    pub chain_id: SolanaChainId,
    /// The signer address
    pub signer_address: String,
}
