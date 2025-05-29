use serde::{Deserialize, Serialize};
use vault_types::enclave::auth::Auth;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SigningCredential {
    Vault(Auth),
}
