use alloy::primitives::ChainId;
use alloy_signer_aws::AwsSigner;
use aws_config::BehaviorVersion;
use aws_credential_types::provider::future::ProvideCredentials as ProvideCredentialsFuture;
use aws_sdk_kms::config::{Credentials, ProvideCredentials};
use serde::{Deserialize, Serialize};
use thirdweb_core::auth::ThirdwebAuth;
use thirdweb_core::iaw::AuthToken;
use vault_types::enclave::auth::Auth as VaultAuth;

use crate::error::EngineError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SigningCredential {
    Vault(VaultAuth),
    Iaw {
        auth_token: AuthToken,
        thirdweb_auth: ThirdwebAuth,
    },
    AwsKms(AwsKmsCredential),
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
            .load()
            .await;
        let client = aws_sdk_kms::Client::new(&config);

        let signer = AwsSigner::new(client, self.key_id.clone(), chain_id).await?;
        Ok(signer)
    }
}
