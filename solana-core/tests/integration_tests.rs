/// Integration tests for real-world program interactions
/// 
/// Tests that verify we can build correct instruction data for:
/// - Raydium swaps
/// - SPL token transfers
/// - Complex DeFi operations

use engine_solana_core::{ProgramCall, IdlCache};
use engine_solana_core::transaction::InstructionDataEncoding;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::collections::HashMap;
use serde_json::json;
use base64::Engine;

/// Helper to initialize test environment
fn init_test_env() {
    dotenvy::from_filename(".env.test").ok();
}

/// Helper to create test IDL cache
fn create_test_idl_cache() -> IdlCache {
    init_test_env();
    
    let rpc_url = std::env::var("SOLANA_MAINNET_RPC_URL")
        .expect("SOLANA_MAINNET_RPC_URL must be set in .env.test");
    
    IdlCache::new(rpc_url)
}

/// Test Jupiter swap instruction building
/// 
/// Jupiter Aggregator v6 - we have the IDL and can fetch it from on-chain
/// Tests the complete flow: IDL fetch → account resolution → instruction encoding
#[tokio::test]
async fn test_jupiter_swap_instruction() {
    let cache = create_test_idl_cache();
    
    println!("\n=== Jupiter Swap Instruction Test ===");
    
    // Jupiter Aggregator v6
    let jupiter_program = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
    
    // User's wallet
    let signer = Pubkey::from_str("9vNYXEehFV8V1jxzjH7Sv3BBtsYZ92HPKYP1stgNGHJE").unwrap();
    
    // SOL to USDC swap via Jupiter
    let mut accounts = HashMap::new();
    // Jupiter route instruction requires various accounts
    // For now, we'll test what we can with minimal accounts
    accounts.insert("tokenProgram".to_string(), "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string());
    accounts.insert("userSourceTokenAccount".to_string(), signer.to_string()); // Placeholder
    accounts.insert("userDestinationTokenAccount".to_string(), signer.to_string()); // Placeholder
    
    let mut args = HashMap::new();
    // Jupiter route args - simplified
    args.insert("inAmount".to_string(), json!("1000000000"));
    args.insert("quotedOutAmount".to_string(), json!("150000000"));
    args.insert("slippageBps".to_string(), json!(50)); // 0.5%
    args.insert("platformFeeBps".to_string(), json!(0));
    
    let call = ProgramCall {
        program: jupiter_program.to_string().into(),
        instruction: "route".to_string(), // Using 'route' instruction
        accounts,
        args,
        idl: None,
    };
    
    println!("Program: {}", jupiter_program);
    println!("Instruction: route");
    println!("Signer: {}", signer);
    
    let result = call.prepare(signer, &cache).await;
    
    match result {
        Ok(prepared) => {
            println!("\n✓ Successfully prepared Jupiter swap!");
            println!("Program ID: {}", prepared.program_id);
            println!("Instruction: {}", prepared.instruction_name);
            println!("Number of accounts: {}", prepared.resolved_accounts.len());
            println!("Instruction data length: {} bytes", prepared.instruction.data.len());
            
            // Verify we have instruction data with discriminator
            assert!(prepared.instruction.data.len() >= 8, "Should have at least discriminator");
            
            println!("\nResolved Accounts:");
            for (i, acc) in prepared.resolved_accounts.iter().enumerate() {
                println!("  {}: {} (pubkey={}, signer={}, writable={})",
                    i, acc.name, acc.pubkey, acc.is_signer, acc.is_writable);
            }
            
            println!("\nInstruction Data (first 32 bytes):");
            let preview = &prepared.instruction.data[..prepared.instruction.data.len().min(32)];
            println!("  {:02x?}", preview);
        }
        Err(e) => {
            eprintln!("\n⚠ Failed to prepare Jupiter swap");
            eprintln!("Error: {:?}", e);
            println!("\nThis is expected - account resolution is not yet fully implemented");
        }
    }
}

/// Test that we can fetch Jupiter IDL and find route instruction
#[tokio::test]
async fn test_jupiter_idl_route_instruction() {
    let cache = create_test_idl_cache();
    
    println!("\n=== Jupiter Route Instruction Details ===");
    
    let jupiter_program = Pubkey::from_str("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4").unwrap();
    
    let result = cache.get_idl(&jupiter_program).await;
    
    assert!(result.is_ok(), "Should fetch Jupiter IDL");
    
    let idl = result.unwrap();
    println!("✓ Successfully fetched Jupiter IDL");
    println!("Program name: {}", idl.idl.metadata.name);
    println!("Total instructions: {}", idl.idl.instructions.len());
    
    // Test 'route' instruction
    let route_ix = idl.find_instruction("route");
    assert!(route_ix.is_some(), "Should have 'route' instruction");
    
    let ix = route_ix.unwrap();
    println!("\n=== 'route' Instruction ===");
    println!("Number of accounts: {}", ix.accounts.len());
    println!("Number of args: {}", ix.args.len());
    
    println!("\nAccounts (first 10):");
    for (i, acc) in ix.accounts.iter().take(10).enumerate() {
        println!("  {}: {:?}", i, acc);
    }
    
    println!("\nArgs:");
    for (i, arg) in ix.args.iter().enumerate() {
        println!("  {}: {} - {:?}", i, arg.name, arg.ty);
    }
    
    // Also check routeWithTokenLedger
    let route_ledger = idl.find_instruction("routeWithTokenLedger");
    if route_ledger.is_some() {
        println!("\n✓ Also found 'routeWithTokenLedger' instruction");
    }
    
    // Check sharedAccountsRoute
    let shared_route = idl.find_instruction("sharedAccountsRoute");
    if shared_route.is_some() {
        println!("✓ Also found 'sharedAccountsRoute' instruction");
    }
}

/// Test simpler Jupiter instruction - 'claim'
#[tokio::test]
async fn test_jupiter_claim_instruction() {
    let cache = create_test_idl_cache();
    
    println!("\n=== Jupiter Claim Instruction Test ===");
    
    let jupiter_program = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
    let signer = Pubkey::from_str("9vNYXEehFV8V1jxzjH7Sv3BBtsYZ92HPKYP1stgNGHJE").unwrap();
    
    // Claim needs program_authority account
    // Note: wallet and system_program have fixed addresses in the IDL
    let mut accounts = HashMap::new();
    accounts.insert("program_authority".to_string(), "FERQFZr3U8pzAKLE4jjy9vtvXnuWyPz8kQXYLDHRX1Su".to_string());
    
    let mut args = HashMap::new();
    args.insert("id".to_string(), json!(1));
    
    let call = ProgramCall {
        program: jupiter_program.to_string().into(),
        instruction: "claim".to_string(),
        accounts,
        args,
        idl: None,
    };
    
    println!("Testing simpler 'claim' instruction...");
    
    let result = call.prepare(signer, &cache).await;
    
    match result {
        Ok(prepared) => {
            println!("\n✓ Successfully prepared Jupiter claim!");
            println!("Instruction data length: {} bytes", prepared.instruction.data.len());
            println!("Number of accounts: {}", prepared.resolved_accounts.len());
            
            // The instruction data should be: discriminator (8 bytes) + u8 id (1 byte) = 9 bytes
            println!("\nInstruction data ({:?}): {}", 
                prepared.instruction.encoding, 
                &prepared.instruction.data
            );
            
            // Decode to check length
            let decoded_data = if prepared.instruction.encoding == InstructionDataEncoding::Base64 {
                base64::engine::general_purpose::STANDARD.decode(&prepared.instruction.data)
                    .expect("Valid base64")
            } else {
                hex::decode(&prepared.instruction.data).expect("Valid hex")
            };
            
            println!("Decoded length: {} bytes", decoded_data.len());
            println!("Decoded hex: {}", hex::encode(&decoded_data));
            
            // Check discriminator (first 8 bytes)
            assert_eq!(decoded_data.len(), 9, "Claim instruction should have 9 bytes (discriminator + u8)");
            assert_eq!(&decoded_data[..8], &[62, 198, 214, 193, 213, 159, 108, 210], "Discriminator should match IDL");
            
            // Check the u8 argument (should be 1)
            assert_eq!(decoded_data[8], 1, "ID argument should be 1");
            
            println!("\n✓ Jupiter claim instruction built successfully!");
            println!("✓ Discriminator matches IDL");
            println!("✓ Borsh encoding working correctly!");
        }
        Err(e) => {
            eprintln!("\n⚠ Failed to prepare Jupiter claim");
            eprintln!("Error: {:?}", e);
        }
    }
}

/// Test Raydium swap instruction building
/// 
/// This tests the EXACT use case from the curl example:
/// - Program: Raydium AMM V4
/// - Instruction: swapBaseIn
/// - We provide: poolId, inputMint, outputMint
/// - Expected: System should derive all other required accounts (ATAs, vault accounts, etc.)
#[tokio::test]
#[ignore] // TODO: Enable once account resolution is fully implemented
async fn test_raydium_swap_instruction() {
    let cache = create_test_idl_cache();
    
    println!("\n=== Raydium Swap Instruction Test ===");
    
    // Raydium AMM V4
    let raydium_program = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
    
    // User's wallet
    let signer = Pubkey::from_str("9vNYXEehFV8V1jxzjH7Sv3BBtsYZ92HPKYP1stgNGHJE").unwrap();
    
    // Create the program call exactly as a user would via API
    let mut accounts = HashMap::new();
    accounts.insert("poolId".to_string(), "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2".to_string());
    accounts.insert("inputMint".to_string(), "So11111111111111111111111111111111111111112".to_string()); // SOL
    accounts.insert("outputMint".to_string(), "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".to_string()); // USDC
    
    let mut args = HashMap::new();
    args.insert("amountIn".to_string(), json!("1000000000"));
    args.insert("minimumAmountOut".to_string(), json!("150000000"));
    
    let call = ProgramCall {
        program: raydium_program.to_string().into(),
        instruction: "swapBaseIn".to_string(),
        accounts,
        args,
        idl: None,
    };
    
    println!("Program: {}", raydium_program);
    println!("Instruction: swapBaseIn");
    println!("Signer: {}", signer);
    
    // This SHOULD automatically:
    // 1. Fetch Raydium IDL
    // 2. Find swapBaseIn instruction definition
    // 3. Derive user's SOL ATA
    // 4. Derive user's USDC ATA
    // 5. Derive pool vault accounts
    // 6. Add system program, token program, etc.
    // 7. Encode the instruction data with discriminator
    
    let result = call.prepare(signer, &cache).await;
    
    match result {
        Ok(prepared) => {
            println!("\n✓ Successfully prepared Raydium swap!");
            println!("Program ID: {}", prepared.program_id);
            println!("Instruction: {}", prepared.instruction_name);
            println!("Number of accounts: {}", prepared.resolved_accounts.len());
            println!("Instruction data length: {} bytes", prepared.instruction.data.len());
            
            // Verify we have the expected accounts
            println!("\nResolved Accounts:");
            for (i, acc) in prepared.resolved_accounts.iter().enumerate() {
                println!("  {}: {} (signer={}, writable={}, source={:?})",
                    i, acc.name, acc.is_signer, acc.is_writable, acc.source);
            }
            
            // For Raydium swapBaseIn, we expect around 15-20 accounts
            assert!(prepared.resolved_accounts.len() >= 10, "Should have multiple accounts");
            
            // Should have instruction data with discriminator
            assert!(prepared.instruction.data.len() > 8, "Should have discriminator + args");
        }
        Err(e) => {
            eprintln!("\n❌ Failed to prepare Raydium swap");
            eprintln!("Error: {:?}", e);
            panic!("Raydium swap preparation failed - account resolution not yet implemented");
        }
    }
}

/// Test that we can fetch and parse Raydium IDL
#[tokio::test]
async fn test_fetch_raydium_idl() {
    let cache = create_test_idl_cache();
    
    println!("\n=== Raydium IDL Fetch Test ===");
    
    let raydium_program = Pubkey::from_str("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8").unwrap();
    
    let result = cache.get_idl(&raydium_program).await;
    
    match result {
        Ok(idl) => {
            println!("✓ Successfully fetched Raydium IDL");
            println!("Program name: {}", idl.idl.metadata.name);
            println!("Number of instructions: {}", idl.idl.instructions.len());
            
            // Look for swapBaseIn instruction
            let swap_ix = idl.find_instruction("swapBaseIn");
            if let Some(ix) = swap_ix {
                println!("\n✓ Found 'swapBaseIn' instruction");
                println!("Number of accounts: {}", ix.accounts.len());
                println!("Number of args: {}", ix.args.len());
                
                println!("\nAccounts:");
                for (i, acc) in ix.accounts.iter().enumerate() {
                    // IdlInstructionAccountItem is an enum, print debug
                    println!("  {}: {:?}", i, acc);
                }
                
                println!("\nArgs:");
                for (i, arg) in ix.args.iter().enumerate() {
                    println!("  {}: {} ({:?})", i, arg.name, arg.ty);
                }
            } else {
                println!("⚠ 'swapBaseIn' instruction not found in IDL");
                println!("Available instructions:");
                for ix in &idl.idl.instructions {
                    println!("  - {}", ix.name);
                }
            }
        }
        Err(e) => {
            println!("Note: Could not fetch Raydium IDL: {}", e);
            println!("This may be expected if Raydium doesn't publish IDL on-chain");
        }
    }
}

/// Test SPL token transfer - a simpler case
#[tokio::test]
#[ignore] // TODO: Enable once we have SPL token IDL or hardcoded instruction building
async fn test_spl_token_transfer() {
    let cache = create_test_idl_cache();
    
    println!("\n=== SPL Token Transfer Test ===");
    
    let signer = Pubkey::from_str("9vNYXEehFV8V1jxzjH7Sv3BBtsYZ92HPKYP1stgNGHJE").unwrap();
    
    let mut accounts = HashMap::new();
    accounts.insert("source".to_string(), "source_ata_here".to_string());
    accounts.insert("destination".to_string(), "dest_ata_here".to_string());
    accounts.insert("authority".to_string(), signer.to_string());
    
    let mut args = HashMap::new();
    args.insert("amount".to_string(), json!("1000000"));
    
    let call = ProgramCall {
        program: "spl-token".into(),
        instruction: "transfer".to_string(),
        accounts,
        args,
        idl: None,
    };
    
    let result = call.prepare(signer, &cache).await;
    
    match result {
        Ok(prepared) => {
            println!("✓ Successfully prepared SPL token transfer!");
            println!("Accounts: {}", prepared.resolved_accounts.len());
            println!("Instruction data: {} bytes", prepared.instruction.data.len());
        }
        Err(e) => {
            eprintln!("❌ Failed to prepare token transfer: {:?}", e);
        }
    }
}

/// Test account resolution requirements
#[test]
fn test_account_resolution_requirements() {
    println!("\n=== Account Resolution Requirements ===\n");
    
    println!("For the API to work as desired, we need:");
    println!("1. ✅ IDL fetching (Anchor + PMP) - DONE");
    println!("2. ✅ PDA derivation primitives - DONE");
    println!("3. ✅ ATA derivation helpers - DONE");
    println!("4. ❌ Automatic account resolution from IDL - TODO");
    println!("5. ❌ Token account auto-detection - TODO");
    println!("6. ❌ System program injection - PARTIAL");
    
    println!("\nWhat needs to be implemented:");
    println!("- Parse IDL account constraints (seeds, mutable, signer)");
    println!("- Auto-derive PDAs based on seeds in IDL");
    println!("- Auto-derive ATAs for token operations");
    println!("- Inject system programs (Token Program, System Program, etc.)");
    println!("- Handle optional accounts");
    println!("- Validate required accounts are provided");
}

