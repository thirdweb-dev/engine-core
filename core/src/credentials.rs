use serde::{Deserialize, Serialize};
use thirdweb_core::auth::ThirdwebAuth;
use thirdweb_core::iaw::AuthToken;
use vault_types::enclave::auth::Auth;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SigningCredential {
    Vault(Auth),
    Iaw { 
        auth_token: AuthToken, 
        thirdweb_auth: ThirdwebAuth 
    },
}
