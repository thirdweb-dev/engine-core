use alloy::primitives::ChainId;
use alloy::signers::local::PrivateKeySigner;
use alloy_signer_aws::AwsSigner;
use aws_config::{BehaviorVersion, Region};
use aws_credential_types::provider::future::ProvideCredentials as ProvideCredentialsFuture;
use aws_sdk_kms::config::{Credentials, ProvideCredentials};
use serde::{Deserialize, Serialize};
use thirdweb_core::auth::ThirdwebAuth;
use thirdweb_core::iaw::AuthToken;
use vault_types::enclave::auth::Auth as VaultAuth;

use crate::error::EngineError;

impl SigningCredential {
    /// Create a random private key credential for testing
    pub fn random_local() -> Self {
        SigningCredential::PrivateKey(PrivateKeySigner::random())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SigningCredential {
    Vault(VaultAuth),
    Iaw {
        auth_token: AuthToken,
        thirdweb_auth: ThirdwebAuth,
    },
    AwsKms(AwsKmsCredential),
    /// Private key signer for testing and development
    /// Note: This should only be used in test environments
    #[serde(skip)]
    PrivateKey(PrivateKeySigner),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AwsKmsCredential {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub key_id: String,
    pub region: String,
}

impl ProvideCredentials for AwsKmsCredential {
    fn provide_credentials<'a>(&'a self) -> ProvideCredentialsFuture<'a>
    where
        Self: 'a,
    {
        let credentials = Credentials::new(
            self.access_key_id.clone(),
            self.secret_access_key.clone(),
            None,
            None,
            "engine-core",
        );
        ProvideCredentialsFuture::ready(Ok(credentials))
    }
}

impl AwsKmsCredential {
    pub async fn get_signer(&self, chain_id: Option<ChainId>) -> Result<AwsSigner, EngineError> {
        let config = aws_config::defaults(BehaviorVersion::latest())
            .credentials_provider(self.clone())
            .region(Region::new(self.region.clone()))
            .load()
            .await;
        let client = aws_sdk_kms::Client::new(&config);

        let signer = AwsSigner::new(client, self.key_id.clone(), chain_id).await?;
        Ok(signer)
    }
}
