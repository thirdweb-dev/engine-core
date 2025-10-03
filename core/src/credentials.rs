use alloy::primitives::ChainId;
use alloy::signers::local::PrivateKeySigner;
use alloy_signer_aws::AwsSigner;
use aws_config::{BehaviorVersion, Region};
use aws_credential_types::provider::future::ProvideCredentials as ProvideCredentialsFuture;
use aws_sdk_kms::config::{Credentials, ProvideCredentials};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use thirdweb_core::auth::ThirdwebAuth;
use thirdweb_core::iaw::AuthToken;
use vault_types::enclave::auth::Auth as VaultAuth;

use crate::error::EngineError;

/// Cache for AWS KMS clients to avoid recreating connections
pub type KmsClientCache = moka::future::Cache<u64, aws_sdk_kms::Client>;

impl SigningCredential {
    /// Create a random private key credential for testing
    pub fn random_local() -> Self {
        SigningCredential::PrivateKey(PrivateKeySigner::random())
    }

    /// Inject KMS cache into AWS KMS credentials (useful after deserialization)
    pub fn with_aws_kms_cache(self, kms_client_cache: &KmsClientCache) -> Self {
        match self {
            SigningCredential::AwsKms(creds) => {
                SigningCredential::AwsKms(creds.with_cache(kms_client_cache.clone()))
            }
            other => other,
        }
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
    #[serde(skip)]
    pub kms_client_cache: Option<KmsClientCache>,
}

impl Hash for AwsKmsCredential {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.access_key_id.hash(state);
        self.secret_access_key.hash(state);
        self.key_id.hash(state);
        self.region.hash(state);
        // Don't hash the cache - it's not part of the credential identity
    }
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
    /// Create a new AwsKmsCredential with cache
    pub fn new(
        access_key_id: String,
        secret_access_key: String,
        key_id: String,
        region: String,
        kms_client_cache: KmsClientCache,
    ) -> Self {
        Self {
            access_key_id,
            secret_access_key,
            key_id,
            region,
            kms_client_cache: Some(kms_client_cache),
        }
    }

    /// Inject cache into this credential (useful after deserialization)
    pub fn with_cache(mut self, kms_client_cache: KmsClientCache) -> Self {
        self.kms_client_cache = Some(kms_client_cache);
        self
    }

    /// Create a cache key from the credential
    fn cache_key(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }

    /// Create a new AWS KMS client (without caching)
    async fn create_kms_client(&self) -> Result<aws_sdk_kms::Client, EngineError> {
        let config = aws_config::defaults(BehaviorVersion::latest())
            .credentials_provider(self.clone())
            .region(Region::new(self.region.clone()))
            .load()
            .await;
        Ok(aws_sdk_kms::Client::new(&config))
    }

    /// Get a cached AWS KMS client, creating one if it doesn't exist
    async fn get_cached_kms_client(&self) -> Result<aws_sdk_kms::Client, EngineError> {
        match &self.kms_client_cache {
            Some(cache) => {
                let cache_key = self.cache_key();

                match cache.get(&cache_key).await {
                    Some(client) => {
                        tracing::debug!("Using cached KMS client for key: {}", cache_key);
                        Ok(client)
                    }
                    None => {
                        tracing::debug!("Creating new KMS client for key: {}", cache_key);
                        let client = self.create_kms_client().await?;
                        cache.insert(cache_key, client.clone()).await;
                        Ok(client)
                    }
                }
            }
            None => {
                // Fallback to creating a new client without caching
                tracing::debug!("No cache available, creating new KMS client");
                self.create_kms_client().await
            }
        }
    }

    /// Get signer (uses cache if available)
    pub async fn get_signer(&self, chain_id: Option<ChainId>) -> Result<AwsSigner, EngineError> {
        let client = self.get_cached_kms_client().await?;
        let signer = AwsSigner::new(client, self.key_id.clone(), chain_id).await?;
        Ok(signer)
    }
}
