use alloy::primitives::{Address, address};

/// The minimal account implementation address used for EIP-7702 delegation
pub const MINIMAL_ACCOUNT_IMPLEMENTATION_ADDRESS: Address =
    address!("0xD6999651Fc0964B9c6B444307a0ab20534a66560");

/// EIP-7702 delegation prefix bytes
pub const EIP_7702_DELEGATION_PREFIX: [u8; 3] = [0xef, 0x01, 0x00];

/// EIP-7702 delegation code length (prefix + address)
pub const EIP_7702_DELEGATION_CODE_LENGTH: usize = 23; 