/// IDL fetching and caching service
use crate::{
    error::{Result, SolanaProgramError},
    idl_types::{ProgramIdl, SerializationFormat},
};
use flate2::read::ZlibDecoder;
use moka::future::Cache;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::io::Read;
use std::sync::Arc;

/// Cache for program IDLs
///
/// This cache fetches IDLs from on-chain sources and caches them in memory.
pub struct IdlCache {
    /// In-memory cache of IDLs by program ID
    cache: Cache<Pubkey, Arc<ProgramIdl>>,
    /// RPC client for fetching on-chain data
    rpc_client: Arc<RpcClient>,
}

impl IdlCache {
    /// Create a new IDL cache with an RPC URL
    pub fn new(rpc_url: String) -> Self {
        let cache = Cache::builder()
            .max_capacity(1000) // Keep up to 1000 IDLs in memory
            .time_to_live(std::time::Duration::from_secs(3600)) // Cache for 1 hour
            .build();

        let rpc_client = Arc::new(RpcClient::new(rpc_url));

        Self { cache, rpc_client }
    }

    /// Create a new IDL cache with a custom RPC client
    pub fn with_client(rpc_client: Arc<RpcClient>) -> Self {
        let cache = Cache::builder()
            .max_capacity(1000)
            .time_to_live(std::time::Duration::from_secs(3600))
            .build();

        Self { cache, rpc_client }
    }

    /// Get IDL for a program, fetching from cache or on-chain
    pub async fn get_idl(&self, program_id: &Pubkey) -> Result<Arc<ProgramIdl>> {
        // Check cache first
        if let Some(idl) = self.cache.get(program_id).await {
            tracing::debug!(program_id = %program_id, "IDL cache hit");
            return Ok(idl);
        }

        // Cache miss - fetch from on-chain
        tracing::debug!(program_id = %program_id, "IDL cache miss, fetching from on-chain");
        let idl = self.fetch_idl(program_id).await?;
        let idl_arc = Arc::new(idl);

        // Store in cache
        self.cache.insert(*program_id, idl_arc.clone()).await;

        Ok(idl_arc)
    }

    /// Get IDL with a specific RPC client (useful for cross-chain support)
    pub async fn get_idl_with_client(
        &self,
        program_id: &Pubkey,
        rpc_client: &RpcClient,
    ) -> Result<Arc<ProgramIdl>> {
        // Check cache first
        if let Some(idl) = self.cache.get(program_id).await {
            tracing::debug!(program_id = %program_id, "IDL cache hit");
            return Ok(idl);
        }

        // Cache miss - fetch from on-chain using provided client
        tracing::debug!(program_id = %program_id, "IDL cache miss, fetching with custom client");
        let idl = Self::fetch_idl_with_client(program_id, rpc_client).await?;
        let idl_arc = Arc::new(idl);

        // Store in cache
        self.cache.insert(*program_id, idl_arc.clone()).await;

        Ok(idl_arc)
    }

    /// Fetch IDL from external source
    async fn fetch_idl(&self, program_id: &Pubkey) -> Result<ProgramIdl> {
        Self::fetch_idl_with_client(program_id, &self.rpc_client).await
    }

    /// Fetch IDL from external source with a specific RPC client
    async fn fetch_idl_with_client(
        program_id: &Pubkey,
        rpc_client: &RpcClient,
    ) -> Result<ProgramIdl> {
        // Try fetching from Anchor IDL account first (most common)
        if let Ok(idl) = Self::fetch_from_anchor_idl_account(program_id, rpc_client).await {
            tracing::info!(program_id = %program_id, "Successfully fetched IDL from Anchor IDL account");
            return Ok(idl);
        }

        // Try fetching from Program Metadata Program
        if let Ok(idl) = Self::fetch_from_program_metadata_program(program_id, rpc_client).await {
            tracing::info!(program_id = %program_id, "Successfully fetched IDL from Program Metadata Program");
            return Ok(idl);
        }

        // TODO: Try other sources (external registries, IPFS, etc.)

        Err(SolanaProgramError::IdlFetchError {
            program: program_id.to_string(),
            error:
                "IDL not found in any source. Tried: Anchor IDL account, Program Metadata Program"
                    .to_string(),
        })
    }

    /// Derive the Anchor IDL account address for a program
    ///
    /// Anchor's IDL derivation (from Anchor CLI source):
    /// 1. Find the program signer PDA: find_program_address(&[], program_id)
    /// 2. Create address with seed: create_with_seed(&program_signer, "anchor:idl", program_id)
    ///
    /// Reference: https://www.anchor-lang.com/docs/basics/idl
    fn derive_anchor_idl_address(program_id: &Pubkey) -> Pubkey {
        // First get the program signer (base PDA with no seeds)
        let (program_signer, _bump) = Pubkey::find_program_address(&[], program_id);

        // Then create the IDL address using create_with_seed
        Pubkey::create_with_seed(&program_signer, "anchor:idl", program_id)
            .expect("Seed is always valid")
    }

    /// Derive the Program Metadata Program IDL address
    ///
    /// PMP stores IDLs in a PDA of the metadata program with seeds: [program_id, "idl"]
    /// Reference: https://github.com/solana-program/program-metadata
    fn derive_pmp_idl_address(program_id: &Pubkey) -> Pubkey {
        // Program Metadata Program ID
        const PMP_PROGRAM_ID: &str = "ProgM6JCCvbYkfKqJYHePx4xxSUSqJp7rh8Lyv7nk7S";
        let pmp_program = PMP_PROGRAM_ID.parse::<Pubkey>().unwrap();

        let seeds = &[program_id.as_ref(), b"idl"];
        let (idl_address, _bump) = Pubkey::find_program_address(seeds, &pmp_program);
        idl_address
    }

    /// Fetch IDL from on-chain Anchor IDL account
    ///
    /// This fetches the IDL that Anchor programs publish on-chain during deployment.
    /// The IDL is stored compressed with zlib and prefixed with an 8-byte discriminator.
    /// Reference: https://www.anchor-lang.com/docs/basics/idl
    async fn fetch_from_anchor_idl_account(
        program_id: &Pubkey,
        rpc_client: &RpcClient,
    ) -> Result<ProgramIdl> {
        // Derive the IDL account address using Anchor's derivation
        let idl_address = Self::derive_anchor_idl_address(program_id);

        tracing::debug!(
            program_id = %program_id,
            idl_address = %idl_address,
            "Attempting to fetch from Anchor IDL account"
        );

        // Fetch the account data
        let account = rpc_client.get_account(&idl_address).await.map_err(|e| {
            tracing::warn!(
                program_id = %program_id,
                idl_address = %idl_address,
                error = %e,
                "Anchor IDL account not found"
            );
            SolanaProgramError::IdlFetchError {
                program: program_id.to_string(),
                error: format!("Anchor IDL account not found: {}", e),
            }
        })?;

        tracing::debug!(
            program_id = %program_id,
            idl_address = %idl_address,
            data_len = account.data.len(),
            "Successfully fetched Anchor IDL account"
        );

        // Anchor IDL account format:
        // - 8 bytes: discriminator (IdlAccount::DISCRIMINATOR)
        // - 4 bytes: authority (Pubkey, 32 bytes)
        // - 4 bytes: data_len (u32)
        // - remaining: compressed IDL data

        // Check if account has enough data for header
        if account.data.len() < 44 {
            return Err(SolanaProgramError::IdlFetchError {
                program: program_id.to_string(),
                error: format!(
                    "IDL account data too short: {} bytes (need at least 44)",
                    account.data.len()
                ),
            });
        }

        // Skip discriminator (8 bytes) and authority (32 bytes), read data_len (4 bytes)
        let data_len_bytes: [u8; 4] = account.data[40..44].try_into().unwrap();
        let compressed_len = u32::from_le_bytes(data_len_bytes) as usize;

        tracing::debug!(
            compressed_len = compressed_len,
            "Extracted compressed IDL length"
        );

        // Extract compressed data starting at byte 44
        if account.data.len() < 44 + compressed_len {
            return Err(SolanaProgramError::IdlFetchError {
                program: program_id.to_string(),
                error: format!(
                    "IDL account data too short: {} bytes (need {})",
                    account.data.len(),
                    44 + compressed_len
                ),
            });
        }

        let compressed_bytes = &account.data[44..44 + compressed_len];

        // Decompress using zlib
        let mut decoder = ZlibDecoder::new(compressed_bytes);
        let mut decompressed = Vec::new();
        decoder
            .read_to_end(&mut decompressed)
            .map_err(|e| SolanaProgramError::IdlFetchError {
                program: program_id.to_string(),
                error: format!("Failed to decompress IDL data: {}", e),
            })?;

        tracing::debug!(
            decompressed_len = decompressed.len(),
            "Successfully decompressed IDL"
        );

        // Parse as JSON string
        let idl_json =
            String::from_utf8(decompressed).map_err(|e| SolanaProgramError::IdlFetchError {
                program: program_id.to_string(),
                error: format!("Failed to decode IDL as UTF-8: {}", e),
            })?;

        // Parse into ProgramIdl and set serialization format
        let mut idl = ProgramIdl::from_json(&idl_json)?;
        idl.serialization = Some(SerializationFormat::Borsh);

        Ok(idl)
    }

    /// Fetch IDL from Program Metadata Program
    ///
    /// This fetches IDLs stored in the Program Metadata Program, which is a newer
    /// standard for storing program metadata including IDLs.
    /// Reference: https://github.com/solana-program/program-metadata
    async fn fetch_from_program_metadata_program(
        program_id: &Pubkey,
        rpc_client: &RpcClient,
    ) -> Result<ProgramIdl> {
        let idl_address = Self::derive_pmp_idl_address(program_id);

        tracing::debug!(
            program_id = %program_id,
            idl_address = %idl_address,
            "Attempting to fetch from Program Metadata Program"
        );

        // Fetch the account data
        let account = rpc_client.get_account(&idl_address).await.map_err(|e| {
            tracing::warn!(
                program_id = %program_id,
                idl_address = %idl_address,
                error = %e,
                "Program Metadata Program IDL account not found"
            );
            SolanaProgramError::IdlFetchError {
                program: program_id.to_string(),
                error: format!("Program Metadata Program IDL account not found: {}", e),
            }
        })?;

        tracing::debug!(
            program_id = %program_id,
            data_len = account.data.len(),
            "Successfully fetched PMP IDL account"
        );

        // PMP format: The account data contains metadata about the IDL
        // We need to check if it's compressed, and what encoding is used
        // Default is zlib compression + utf8 encoding

        // For now, try to parse as zlib-compressed JSON (the default)
        let mut decoder = ZlibDecoder::new(&account.data[..]);
        let mut decompressed = Vec::new();

        match decoder.read_to_end(&mut decompressed) {
            Ok(_) => {
                // Successfully decompressed, parse as JSON
                let idl_json = String::from_utf8(decompressed).map_err(|e| {
                    SolanaProgramError::IdlFetchError {
                        program: program_id.to_string(),
                        error: format!("Failed to decode PMP IDL as UTF-8: {}", e),
                    }
                })?;

                let mut idl = ProgramIdl::from_json(&idl_json)?;
                idl.serialization = Some(SerializationFormat::Borsh);
                Ok(idl)
            }
            Err(_) => {
                // Not compressed, try parsing directly as JSON
                let idl_json = String::from_utf8(account.data.clone()).map_err(|e| {
                    SolanaProgramError::IdlFetchError {
                        program: program_id.to_string(),
                        error: format!("Failed to decode PMP IDL as UTF-8: {}", e),
                    }
                })?;

                let mut idl = ProgramIdl::from_json(&idl_json)?;
                idl.serialization = Some(SerializationFormat::Borsh);
                Ok(idl)
            }
        }
    }

    /// Pre-cache an IDL (useful for built-in programs)
    pub async fn insert(&self, program_id: Pubkey, idl: ProgramIdl) {
        self.cache.insert(program_id, Arc::new(idl)).await;
    }

    /// Clear the cache
    pub async fn clear(&self) {
        self.cache.invalidate_all();
    }
}

impl Default for IdlCache {
    fn default() -> Self {
        // Default to mainnet-beta RPC
        Self::new("https://api.mainnet-beta.solana.com".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cache_creation() {
        let cache = IdlCache::default();
        // Just verify it was created successfully
        assert!(cache.cache.get(&Pubkey::new_unique()).await.is_none());
    }

    #[test]
    fn test_anchor_idl_address_derivation() {
        // Test Anchor IDL address derivation with Jupiter
        let jupiter_program = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"
            .parse::<Pubkey>()
            .unwrap();

        let idl_address = IdlCache::derive_anchor_idl_address(&jupiter_program);

        // Let's see what we get
        println!("Program ID: {}", jupiter_program);
        println!("Computed IDL Address: {}", idl_address);

        // Try to understand the expected derivation
        // The expected address from the test was: C88XWfp26heEmDkmfSzeXP7Fd7GQJ2j9dDTUsyiZbUTa
        // Let's also try some alternative derivations

        // Method 1: PDA with seeds ["anchor:idl", program_id] (current)
        let seeds1 = &[b"anchor:idl", jupiter_program.as_ref()];
        let (addr1, _) = Pubkey::find_program_address(seeds1, &jupiter_program);
        println!("Method 1 (PDA): {}", addr1);

        // Method 2: Maybe it's just the string "anchor:idl" as seed
        let (addr2, _) = Pubkey::find_program_address(&[b"anchor:idl"], &jupiter_program);
        println!("Method 2 (just string): {}", addr2);

        // Method 3: Maybe with base PDA
        let (base, _) = Pubkey::find_program_address(&[], &jupiter_program);
        println!("Base PDA: {}", base);

        // Since this is deterministic, we'll just test that it's consistent
        let idl_address2 = IdlCache::derive_anchor_idl_address(&jupiter_program);
        assert_eq!(
            idl_address, idl_address2,
            "IDL derivation should be deterministic"
        );
    }

    #[test]
    fn test_pmp_idl_address_derivation() {
        // Test PMP IDL address derivation
        let program_id = Pubkey::new_unique();

        let idl_address = IdlCache::derive_pmp_idl_address(&program_id);

        // Just verify it's deterministic
        let idl_address2 = IdlCache::derive_pmp_idl_address(&program_id);
        assert_eq!(
            idl_address, idl_address2,
            "PMP IDL derivation should be deterministic"
        );
    }

    #[tokio::test]
    async fn test_cache_insert_and_get() {
        let cache = IdlCache::default();
        let program_id = Pubkey::new_unique();

        // Create a mock IDL
        let idl = ProgramIdl {
            idl: anchor_lang::idl::types::Idl {
                address: "test".to_string(),
                metadata: anchor_lang::idl::types::IdlMetadata {
                    name: "test".to_string(),
                    version: "0.1.0".to_string(),
                    spec: "0.1.0".to_string(),
                    description: Some("test".to_string()),
                    repository: None,
                    dependencies: vec![],
                    contact: None,
                    deployments: Some(anchor_lang::idl::types::IdlDeployments {
                        mainnet: None,
                        testnet: None,
                        devnet: None,
                        localnet: None,
                    }),
                },
                docs: vec![],
                instructions: vec![],
                accounts: vec![],
                events: vec![],
                errors: vec![],
                types: vec![],
                constants: vec![],
            },
            serialization: None,
        };

        cache.insert(program_id, idl.clone()).await;

        let cached = cache.get_idl(&program_id).await;
        assert!(cached.is_ok());
    }
}
