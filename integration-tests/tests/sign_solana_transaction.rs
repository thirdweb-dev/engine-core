mod setup;

use anyhow::Result;
use base64::{Engine, engine::general_purpose::STANDARD as Base64Engine};
use engine_integration_tests::{create_system_transfer, verify_signature};
use serde::{Deserialize, Serialize};
use setup::TestEnvironment;
use solana_sdk::{hash::Hash, pubkey::Pubkey, transaction::VersionedTransaction};
use std::str::FromStr;
use tracing::info;

/// Request body for signing Solana transactions
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SignSolanaTransactionRequest {
    #[serde(flatten)]
    input: TransactionInput,
    execution_options: SolanaExecutionOptions,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[serde(untagged)]
enum TransactionInput {
    Serialized { transaction: String },
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SolanaExecutionOptions {
    chain_id: String,
    signer_address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    compute_unit_limit: Option<u32>,
}

/// Response from the sign endpoint
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SignSolanaTransactionResponse {
    result: SignResult,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SignResult {
    signature: String,
    signed_transaction: String,
}

/// Test partial signature support for Solana transactions
///
/// This test demonstrates the following flow:
/// 1. Create a Solana wallet in Vault (engine wallet)
/// 2. Build a system transfer transaction where engine wallet is fee payer
/// 3. Send to engine to sign with fee payer wallet (via HTTP)
/// 4. Verify the signature is present and valid
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_partial_signature_spl_transfer() -> Result<()> {
    info!("Starting partial signature test for Solana SPL transfer");

    // Create test environment (starts server programmatically)
    let env = TestEnvironment::new("partial_signature").await?;

    // Get admin key and wallet address from configuration
    let admin_key = env.vault_admin_key();
    let engine_pubkey = Pubkey::from_str(env.test_solana_wallet())?;

    info!(
        "Using configured Solana wallet as fee payer: {}",
        engine_pubkey
    );

    // Step 1: Create a simple transfer transaction (engine wallet pays fees)
    let recipient = Pubkey::new_unique();
    let recent_blockhash = Hash::default(); // In real test, fetch from RPC

    info!("Creating transaction with fee payer: {}", engine_pubkey);
    let transaction = create_system_transfer(
        engine_pubkey, // Fee payer (engine wallet)
        recipient,
        1_000_000, // 0.001 SOL
        recent_blockhash,
    )?;

    info!(
        "Transaction created with {} signatures needed",
        transaction.signatures.len()
    );

    // Step 2: Serialize the unsigned transaction to send to engine
    let tx_bytes = bincode::serde::encode_to_vec(&transaction, bincode::config::standard())?;
    let tx_base64 = Base64Engine.encode(&tx_bytes);

    info!(
        "Serialized transaction to base64 (length: {} bytes)",
        tx_bytes.len()
    );

    // Step 3: Call the engine API to sign the transaction
    let client = reqwest::Client::new();
    let sign_url = format!("{}/v1/solana/sign/transaction", env.server_url());

    let request_body = SignSolanaTransactionRequest {
        input: TransactionInput::Serialized {
            transaction: tx_base64,
        },
        execution_options: SolanaExecutionOptions {
            chain_id: "solana:devnet".to_string(),
            signer_address: engine_pubkey.to_string(),
            compute_unit_limit: None,
        },
    };

    info!("Sending transaction to engine for signing...");

    dbg!(admin_key);

    let response = client
        .post(&sign_url)
        .header("x-vault-access-token", admin_key)
        .header("Content-Type", "application/json")
        .json(&request_body)
        .send()
        .await?;

    let status = response.status();
    info!("Received response with status: {}", status);

    if !status.is_success() {
        let error_body = response.text().await?;
        tracing::error!("Error response body: {}", error_body);
        anyhow::bail!("Request failed with status {}: {}", status, error_body);
    }

    let sign_response: SignSolanaTransactionResponse = response.json().await?;
    info!(
        "Transaction signed! Signature: {}",
        sign_response.result.signature
    );

    // Step 4: Deserialize the signed transaction
    let signed_tx_bytes = Base64Engine.decode(&sign_response.result.signed_transaction)?;
    let (signed_transaction, _): (VersionedTransaction, _) =
        bincode::serde::decode_from_slice(&signed_tx_bytes, bincode::config::standard())?;

    info!("Deserialized signed transaction");
    info!(
        "Number of signatures: {}",
        signed_transaction.signatures.len()
    );

    // Step 5: Verify the signature
    let engine_sig_valid = verify_signature(&signed_transaction, &engine_pubkey);
    info!("Engine wallet signature valid: {}", engine_sig_valid);

    assert!(
        engine_sig_valid,
        "Engine wallet signature should be present and valid"
    );

    // Step 6: Verify transaction structure
    let account_keys = signed_transaction.message.static_account_keys();
    info!("Transaction account keys: {:?}", account_keys);

    assert_eq!(
        account_keys[0], engine_pubkey,
        "First account should be the fee payer (engine wallet)"
    );

    info!("✅ Partial signature test passed!");
    info!("Transaction is properly signed and ready for broadcast");

    Ok(())
}

/// Test transaction signature verification
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_transaction_signature_verification() -> Result<()> {
    info!("Starting signature verification test");

    let env = TestEnvironment::new("signature_verification").await?;
    let admin_key = env.vault_admin_key();
    let engine_pubkey = Pubkey::from_str(env.test_solana_wallet())?;

    let recipient = Pubkey::new_unique();
    let recent_blockhash = Hash::default();

    // Create and sign transaction
    let transaction = create_system_transfer(engine_pubkey, recipient, 500_000, recent_blockhash)?;

    let tx_bytes = bincode::serde::encode_to_vec(&transaction, bincode::config::standard())?;
    let tx_base64 = Base64Engine.encode(&tx_bytes);

    // Sign via engine
    let client = reqwest::Client::new();
    let sign_url = format!("{}/v1/solana/sign/transaction", env.server_url());

    let request_body = SignSolanaTransactionRequest {
        input: TransactionInput::Serialized {
            transaction: tx_base64,
        },
        execution_options: SolanaExecutionOptions {
            chain_id: "solana:devnet".to_string(),
            signer_address: engine_pubkey.to_string(),
            compute_unit_limit: None,
        },
    };

    let response = client
        .post(&sign_url)
        .header("x-vault-access-token", admin_key)
        .header("Content-Type", "application/json")
        .json(&request_body)
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        let error_body = response.text().await?;
        anyhow::bail!(
            "Request failed with status {}: {}",
            status,
            error_body
        );
    }

    let sign_response: SignSolanaTransactionResponse = response.json().await?;

    // Deserialize and verify
    let signed_tx_bytes = Base64Engine.decode(&sign_response.result.signed_transaction)?;
    let (signed_transaction, _): (VersionedTransaction, _) =
        bincode::serde::decode_from_slice(&signed_tx_bytes, bincode::config::standard())?;

    // Verify signature is not default (has been signed)
    assert!(
        verify_signature(&signed_transaction, &engine_pubkey),
        "Signature should be valid"
    );

    // Verify we can parse the signature string
    let signature = solana_sdk::signature::Signature::from_str(&sign_response.result.signature)?;
    assert_ne!(
        signature,
        solana_sdk::signature::Signature::default(),
        "Signature should not be default"
    );

    info!("✅ Signature verification test passed!");

    Ok(())
}

/// Test error handling for invalid transactions
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_invalid_transaction_handling() -> Result<()> {
    info!("Starting invalid transaction test");

    let env = TestEnvironment::new("invalid_transaction").await?;
    let admin_key = env.vault_admin_key();
    let engine_pubkey = Pubkey::from_str(env.test_solana_wallet())?;

    // Send invalid base64
    let client = reqwest::Client::new();
    let sign_url = format!("{}/v1/solana/sign/transaction", env.server_url());

    let request_body = SignSolanaTransactionRequest {
        input: TransactionInput::Serialized {
            transaction: "invalid_base64!!!".to_string(),
        },
        execution_options: SolanaExecutionOptions {
            chain_id: "solana:devnet".to_string(),
            signer_address: engine_pubkey.to_string(),
            compute_unit_limit: None,
        },
    };

    let response = client
        .post(&sign_url)
        .header("x-vault-access-token", admin_key)
        .header("Content-Type", "application/json")
        .json(&request_body)
        .send()
        .await?;

    assert!(
        !response.status().is_success(),
        "Invalid transaction should return error"
    );

    info!("✅ Invalid transaction handling test passed!");

    Ok(())
}
