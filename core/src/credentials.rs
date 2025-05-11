use vault_types::enclave::auth::Auth;

#[derive(Debug, Clone)]
pub enum SigningCredential {
    Vault(Auth),
}
