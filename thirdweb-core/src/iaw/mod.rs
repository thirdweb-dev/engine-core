use alloy::{
    consensus::{EthereumTypedTransaction, TxEip4844Variant},
    dyn_abi::TypedData,
    eips::eip7702::{Authorization, SignedAuthorization},
    primitives::{Address, ChainId},
};
use serde::{Deserialize, Serialize};
use serde_json;
use thiserror::Error;

use crate::auth::ThirdwebAuth;

/// Authentication token for IAW operations
pub type AuthToken = String;

/// Error types for IAW operations
#[derive(Error, Debug)]
pub enum IAWError {
    #[error("API error: {0}")]
    ApiError(String),
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
    #[error("Network error: {0}")]
    NetworkError(#[from] reqwest::Error),
    #[error("Authentication error: {0}")]
    AuthError(String),
    #[error("Thirdweb error: {0}")]
    ThirdwebError(#[from] crate::error::ThirdwebError),
    #[error("Unexpected error: {0}")]
    UnexpectedError(String),
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
            .build()
            .map_err(|e| IAWError::NetworkError(e))?;

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
        _auth_token: AuthToken,
        _thirdweb_auth: ThirdwebAuth,
        _typed_data: TypedData,
        _from: Address,
    ) -> Result<SignTypedDataData, IAWError> {
        // TODO: Implement actual signing logic
        // The ThirdwebAuth headers would be applied like this:
        // let headers = thirdweb_auth.to_header_map()?;
        // Then add these headers to the HTTP request
        
        // For now, this is unimplemented as requested
        Err(IAWError::UnexpectedError(
            "sign_typed_data not yet implemented".to_string(),
        ))
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
        _auth_token: AuthToken,
        _thirdweb_auth: ThirdwebAuth,
        _from: Address,
        _authorization: Authorization,
    ) -> Result<SignAuthorizationData, IAWError> {
        // TODO: Implement actual signing logic
        // The ThirdwebAuth headers would be applied like this:
        // let headers = thirdweb_auth.to_header_map()?;
        // Then add these headers to the HTTP request
        
        // For now, this is unimplemented as requested
        Err(IAWError::UnexpectedError(
            "sign_authorization not yet implemented".to_string(),
        ))
    }
}
