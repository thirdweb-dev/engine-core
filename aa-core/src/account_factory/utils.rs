use alloy::{
    primitives::{Address, B256, Bytes, bytes, keccak256},
    sol_types::SolValue,
};

pub fn generate_salt(admin: &Address, data: &Bytes) -> B256 {
    keccak256((admin, data).abi_encode_params())
}

/// Predicts the deterministic address of a contract deployed using CREATE2
/// Equivalent to the Solidity inline assembly implementation
///
/// # Arguments
///
/// * `implementation` - The address of the implementation contract
/// * `salt` - The salt value used in CREATE2
/// * `deployer` - The address of the deployer contract/EOA
///
/// # Returns
///
/// * The predicted contract address
pub fn predict_deterministic_address(
    implementation: Address,
    salt: B256,
    deployer: Address,
) -> Address {
    let code_prefix = bytes!("0x3d602d80600a3d3981f3363d3d373d3d3d363d73");
    let code_suffix = bytes!("0x5af43d82803e903d91602b57fd5bf3");

    // Create the exact 55-byte init code that OpenZeppelin uses
    let mut init_code = Vec::with_capacity(55); // 0x37 bytes

    // Start with prefix, but skip the first 9 bytes
    init_code.extend_from_slice(&code_prefix);

    // Add the implementation address
    init_code.extend_from_slice(&implementation.as_slice());

    // Add the suffix, but without the final 'ff' byte
    // The suffix in your constant is 16 bytes, but OpenZeppelin uses 15 bytes
    init_code.extend_from_slice(&code_suffix);

    // Calculate init code hash and CREATE2 address
    let init_code_hash = keccak256(&init_code);
    deployer.create2(salt, init_code_hash)
}
