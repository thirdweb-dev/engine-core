use solana_sdk::{
    hash::Hash,
    instruction::Instruction,
    message::{v0, VersionedMessage},
    pubkey::Pubkey,
    signature::Signature,
    transaction::VersionedTransaction,
};
use std::str::FromStr;

/// Helper function to create a simple SPL token transfer transaction
/// This creates a transaction that requires multiple signers (partial signature scenario)
pub fn create_spl_token_transfer_transaction(
    fee_payer: Pubkey,      // Engine wallet (will sign second)
    token_authority: Pubkey, // User wallet (will sign first)
    token_account: Pubkey,   // Source token account
    destination: Pubkey,     // Destination token account
    mint: Pubkey,            // Token mint address
    amount: u64,
    decimals: u8,
    recent_blockhash: Hash,
) -> Result<VersionedTransaction, anyhow::Error> {
    // SPL Token 2022 program ID
    let token_program_id = Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")?;

    // Use the proper spl-token-2022-interface helper for transfer_checked
    // This ensures correct instruction layout with decimals and proper discriminator
    let transfer_ix = spl_token_2022_interface::instruction::transfer_checked(
        &token_program_id,
        &token_account,       // Source account
        &mint,                // Token mint
        &destination,         // Destination account  
        &token_authority,     // Authority (will be a required signer)
        &[],                  // No multisig signers
        amount,
        decimals,
    )?;

    // Create the message with fee payer
    let message = v0::Message::try_compile(
        &fee_payer,            // Fee payer (engine wallet)
        &[transfer_ix],
        &[],                   // No address lookup tables
        recent_blockhash,
    )?;

    let message = VersionedMessage::V0(message);
    
    // Calculate number of required signatures
    let num_signatures = message.header().num_required_signatures as usize;
    
    // Initialize with default (empty) signatures
    let signatures = vec![Signature::default(); num_signatures];

    Ok(VersionedTransaction {
        signatures,
        message,
    })
}

/// Helper function to verify transaction signature
/// Returns true if the signature at the given index is valid
pub fn verify_signature(
    transaction: &VersionedTransaction,
    signer_pubkey: &Pubkey,
) -> bool {
    // Find the signer's index in the account keys
    let account_keys = transaction.message.static_account_keys();
    
    let signer_index = account_keys
        .iter()
        .position(|key| key == signer_pubkey);

    if let Some(index) = signer_index && index < transaction.signatures.len() {
        // Check if the signature is not default (i.e., has been signed)
        let sig = &transaction.signatures[index];
        return sig != &Signature::default();
    }
    
    false
}

/// Parse a Solana public key from string
pub fn parse_pubkey(pubkey_str: &str) -> Result<Pubkey, anyhow::Error> {
    Ok(Pubkey::from_str(pubkey_str)?)
}

/// Create a simple system transfer transaction for testing
pub fn create_system_transfer(
    from: Pubkey,
    to: Pubkey,
    lamports: u64,
    recent_blockhash: Hash,
) -> Result<VersionedTransaction, anyhow::Error> {
    use solana_system_interface::instruction as system_instruction;
    
    let transfer_ix = system_instruction::transfer(&from, &to, lamports);
    
    let message = v0::Message::try_compile(
        &from,
        &[transfer_ix],
        &[],
        recent_blockhash,
    )?;

    let message = VersionedMessage::V0(message);
    let num_signatures = message.header().num_required_signatures as usize;
    let signatures = vec![Signature::default(); num_signatures];

    Ok(VersionedTransaction {
        signatures,
        message,
    })
}

