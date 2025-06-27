use alloy::{
    consensus::{EthereumTypedTransaction, TxEip4844Variant},
    dyn_abi::TypedData,
    eips::eip7702::{Authorization, SignedAuthorization},
    hex,
    primitives::{Address, ChainId},
};
use serde::{Deserialize, Serialize};
use serde_json;
use std::time::Duration;
use thiserror::Error;


use crate::{auth::ThirdwebAuth, error::SerializableReqwestError};
use engine_aa_types::{VersionedUserOp, UserOpError, compute_user_op_v06_hash, compute_user_op_v07_hash};

/// Authentication token for IAW operations
pub type AuthToken = String;

/// Error types for IAW operations
#[derive(Error, Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IAWError {
    #[error("API error: {0}")]
    ApiError(String),
    #[error("Serialization error: {message}")]
    SerializationError { message: String },
    #[error("Network error: {error}")]
    NetworkError {
        #[from]
        error: SerializableReqwestError,
    },
    #[error("Authentication error: {0}")]
    AuthError(String),
    #[error("Thirdweb error: {0}")]
    ThirdwebError(#[from] crate::error::ThirdwebError),
    #[error("Unexpected error: {0}")]
    UnexpectedError(String),
    #[error("UserOp error: {0}")]
    UserOpError(#[from] UserOpError),
}

impl From<serde_json::Error> for IAWError {
    fn from(err: serde_json::Error) -> Self {
        IAWError::SerializationError {
            message: err.to_string(),
        }
    }
}

impl From<reqwest::Error> for IAWError {
    fn from(err: reqwest::Error) -> Self {
        IAWError::NetworkError {
            error: SerializableReqwestError::from(err),
        }
    }
}

/// Message format for signing operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageFormat {
    Text,
    Hex,
}

impl Default for MessageFormat {
    fn default() -> Self {
        MessageFormat::Text
    }
}

/// Response data for message signing operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignMessageData {
    pub signature: String,
}

/// Response data for typed data signing operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignTypedDataData {
    pub signature: String,
}

/// Response data for transaction signing operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignTransactionData {
    pub signature: String,
}

/// Response data for authorization signing operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignAuthorizationData {
    pub signed_authorization: SignedAuthorization,
}

/// Response data for userop signing operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignUserOpData {
    pub signature: String,
}



/// Client for interacting with the IAW (In-App Wallet) service
#[derive(Clone)]
pub struct IAWClient {
    _base_url: String,
    _http_client: reqwest::Client,
}

impl IAWClient {
    /// Create a new IAWClient with the given base URL
    pub fn new(base_url: impl Into<String>) -> Result<Self, IAWError> {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(10)
            .http2_keep_alive_interval(Duration::from_secs(20))
            .http2_keep_alive_timeout(Duration::from_secs(5))
            .http2_keep_alive_while_idle(true)
            .build()
            .map_err(IAWError::from)?;

        Ok(Self {
            _base_url: base_url.into(),
            _http_client: http_client,
        })
    }

    /// Create a new IAWClient with a custom HTTP client
    pub fn with_http_client(
        base_url: impl Into<String>,
        http_client: reqwest::Client,
    ) -> Self {
        Self {
            _base_url: base_url.into(),
            _http_client: http_client,
        }
    }

    /// Sign a message with an EOA
    pub async fn sign_message(
        &self,
        auth_token: AuthToken,
        thirdweb_auth: ThirdwebAuth,
        message: String,
        _from: Address,
        _chain_id: Option<ChainId>,
        format: Option<MessageFormat>,
    ) -> Result<SignMessageData, IAWError> {
        // Get ThirdwebAuth headers for billing/authentication
        let mut headers = thirdweb_auth.to_header_map()?;
        
        // Add IAW service authentication
        headers.insert(
            "Authorization",
            reqwest::header::HeaderValue::from_str(&format!("Bearer embedded-wallet-token:{}", auth_token))
                .map_err(|_| IAWError::AuthError("Invalid auth token format".to_string()))?,
        );
        
        // Add content type
        headers.insert(
            "Content-Type",
            reqwest::header::HeaderValue::from_static("application/json"),
        );

        tracing::info!("Headers: {:?}", headers);

        // Convert MessageFormat to isRaw boolean (Hex = true, Text = false)
        let is_raw = match format.unwrap_or_default() {
            MessageFormat::Hex => true,
            MessageFormat::Text => false,
        };

        // Build the request payload
        let payload = serde_json::json!({
            "messagePayload": {
                "message": message,
                "isRaw": is_raw,
            }
        });

        // Make the request to IAW service
        let url = format!("{}/api/v1/enclave-wallet/sign-message", self._base_url);
        let response = self._http_client
            .post(&url)
            .headers(headers)
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(IAWError::ApiError(format!(
                "Failed to sign message - {} {}",
                response.status(),
                response.status().canonical_reason().unwrap_or("Unknown error")
            )));
        }

        // Parse the response
        let signed_response: serde_json::Value = response.json().await?;
        
        // Extract just the signature as requested
        let signature = signed_response
            .get("signature")
            .and_then(|s| s.as_str())
            .ok_or_else(|| IAWError::ApiError("No signature in response".to_string()))?;

        Ok(SignMessageData {
            signature: signature.to_string(),
        })
    }

    /// Sign a typed data structure with an EOA
    pub async fn sign_typed_data(
        &self,
        auth_token: AuthToken,
        thirdweb_auth: ThirdwebAuth,
        typed_data: TypedData,
        _from: Address,
    ) -> Result<SignTypedDataData, IAWError> {
        // Get ThirdwebAuth headers for billing/authentication
        let mut headers = thirdweb_auth.to_header_map()?;
        
        // Add IAW service authentication
        headers.insert(
            "Authorization",
            reqwest::header::HeaderValue::from_str(&format!("Bearer embedded-wallet-token:{}", auth_token))
                .map_err(|_| IAWError::AuthError("Invalid auth token format".to_string()))?,
        );
        
        // Add content type
        headers.insert(
            "Content-Type",
            reqwest::header::HeaderValue::from_static("application/json"),
        );

        // Build the request payload
        let payload = serde_json::json!(typed_data);

        // Make the request to IAW service
        let url = format!("{}/api/v1/enclave-wallet/sign-typed-data", self._base_url);
        let response = self._http_client
            .post(&url)
            .headers(headers)
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(IAWError::ApiError(format!(
                "Failed to sign typed data - {} {}",
                response.status(),
                response.status().canonical_reason().unwrap_or("Unknown error")
            )));
        }

        // Parse the response
        let signed_response: serde_json::Value = response.json().await?;
        
        // Extract just the signature as requested
        let signature = signed_response
            .get("signature")
            .and_then(|s| s.as_str())
            .ok_or_else(|| IAWError::ApiError("No signature in response".to_string()))?;

        Ok(SignTypedDataData {
            signature: signature.to_string(),
        })
    }

    /// Sign a transaction with an EOA
    pub async fn sign_transaction(
        &self,
        auth_token: AuthToken,
        thirdweb_auth: ThirdwebAuth,
        transaction: EthereumTypedTransaction<TxEip4844Variant>,
    ) -> Result<SignTransactionData, IAWError> {
        // Get ThirdwebAuth headers for billing/authentication
        let mut headers = thirdweb_auth.to_header_map()?;
        
        // Add IAW service authentication
        headers.insert(
            "Authorization",
            reqwest::header::HeaderValue::from_str(&format!("Bearer embedded-wallet-token:{}", auth_token))
                .map_err(|_| IAWError::AuthError("Invalid auth token format".to_string()))?,
        );
        
        // Add content type
        headers.insert(
            "Content-Type",
            reqwest::header::HeaderValue::from_static("application/json"),
        );

        // Build the request payload
        let payload = serde_json::json!({
            "transactionPayload": transaction,
        });

        // Make the request to IAW service
        let url = format!("{}/api/v1/enclave-wallet/sign-transaction", self._base_url);
        let response = self._http_client
            .post(&url)
            .headers(headers)
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(IAWError::ApiError(format!(
                "Failed to sign transaction - {} {}",
                response.status(),
                response.status().canonical_reason().unwrap_or("Unknown error")
            )));
        }

        // Parse the response
        let signed_response: serde_json::Value = response.json().await?;
        
        // Extract just the signature as requested
        let signature = signed_response
            .get("signature")
            .and_then(|s| s.as_str())
            .ok_or_else(|| IAWError::ApiError("No signature in response".to_string()))?;

        Ok(SignTransactionData {
            signature: signature.to_string(),
        })
    }

    /// Sign an authorization with an EOA
    pub async fn sign_authorization(
        &self,
        auth_token: AuthToken,
        thirdweb_auth: ThirdwebAuth,
        _from: Address,
        authorization: Authorization,
    ) -> Result<SignAuthorizationData, IAWError> {
        // Get ThirdwebAuth headers for billing/authentication
        let mut headers = thirdweb_auth.to_header_map()?;
        
        // Add IAW service authentication
        headers.insert(
            "Authorization",
            reqwest::header::HeaderValue::from_str(&format!("Bearer embedded-wallet-token:{}", auth_token))
                .map_err(|_| IAWError::AuthError("Invalid auth token format".to_string()))?,
        );
        
        // Add content type
        headers.insert(
            "Content-Type",
            reqwest::header::HeaderValue::from_static("application/json"),
        );

        // Build the request payload
        let payload = serde_json::json!({
            "address": authorization.address,
            "chainId": authorization.chain_id,
            "nonce": authorization.nonce.to_string(),
        });

        // Make the request to IAW service
        let url = format!("{}/api/v1/enclave-wallet/sign-authorization", self._base_url);
        let response = self._http_client
            .post(&url)
            .headers(headers)
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(IAWError::ApiError(format!(
                "Failed to sign authorization - {} {}",
                response.status(),
                response.status().canonical_reason().unwrap_or("Unknown error")
            )));
        }

        // Parse the response
        let signed_response: serde_json::Value = response.json().await?;
        
        // Extract the signed authorization from the response
        let signed_authorization: SignedAuthorization = serde_json::from_value(
            signed_response.get("signedAuthorization")
                .ok_or_else(|| IAWError::ApiError("No signedAuthorization in response".to_string()))?
                .clone()
        )?;

        Ok(SignAuthorizationData {
            signed_authorization,
        })
    }

    /// Sign a user operation with an EOA
    pub async fn sign_userop(
        &self,
        auth_token: AuthToken,
        thirdweb_auth: ThirdwebAuth,
        userop: VersionedUserOp,
        entrypoint: Address,
        _from: Address,
        chain_id: ChainId,
    ) -> Result<SignUserOpData, IAWError> {
        // Compute the userop hash based on version
        let hash = match &userop {
            VersionedUserOp::V0_6(op) => {
                compute_user_op_v06_hash(op, entrypoint, chain_id)?
            }
            VersionedUserOp::V0_7(op) => {
                compute_user_op_v07_hash(op, entrypoint, chain_id)?
            }
        };
        
        let userop_hash = format!("0x{}", hex::encode(hash.as_slice()));
        tracing::info!("Computed userop hash: {}", userop_hash);
        // Get ThirdwebAuth headers for billing/authentication
        let mut headers = thirdweb_auth.to_header_map()?;
        
        // Add IAW service authentication
        headers.insert(
            "Authorization",
            reqwest::header::HeaderValue::from_str(&format!("Bearer embedded-wallet-token:{}", auth_token))
                .map_err(|_| IAWError::AuthError("Invalid auth token format".to_string()))?,
        );
        
        // Add content type
        headers.insert(
            "Content-Type",
            reqwest::header::HeaderValue::from_static("application/json"),
        );

        // Build the request payload - sign as hex message
        let payload = serde_json::json!({
            "messagePayload": {
                "message": userop_hash,
                "isRaw": true,
                "chainId": chain_id,
                "originalMessage": serde_json::to_string(&userop).unwrap(),
            }
        });

        // Make the request to IAW service with explicit timeout
        let url = format!("{}/api/v1/enclave-wallet/sign-message", self._base_url);
        let response = self._http_client
            .post(&url)
            .headers(headers)
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(IAWError::ApiError(format!(
                "Failed to sign userop - {} {}",
                response.status(),
                response.status().canonical_reason().unwrap_or("Unknown error")
            )));
        }

        // Parse the response
        let signed_response: serde_json::Value = response.json().await?;
        
        // Extract just the signature as requested
        let signature = signed_response
            .get("signature")
            .and_then(|s| s.as_str())
            .ok_or_else(|| IAWError::ApiError("No signature in response".to_string()))?;

        Ok(SignUserOpData {
            signature: signature.to_string(),
        })
    }
}
