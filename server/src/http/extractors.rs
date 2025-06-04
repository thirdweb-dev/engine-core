use aide::OperationIo;
use axum::{
    Json,
    extract::{FromRequestParts, rejection::JsonRejection},
    http::request::Parts,
};
use engine_core::{chain::RpcCredentials, credentials::SigningCredential, error::EngineError};
use thirdweb_core::auth::ThirdwebAuth;
use vault_types::enclave::auth::Auth;

use crate::http::error::ApiEngineError;

/// Extractor for RPC credentials from headers
#[derive(OperationIo)]
pub struct RpcCredentialsExtractor(pub RpcCredentials);

impl<S> FromRequestParts<S> for RpcCredentialsExtractor
where
    S: Send + Sync,
{
    type Rejection = ApiEngineError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // try secret key first
        let secret_key = parts
            .headers
            .get("x-thirdweb-secret-key")
            .and_then(|v| v.to_str().ok());

        if let Some(secret_key) = secret_key {
            return Ok(RpcCredentialsExtractor(RpcCredentials::Thirdweb(
                ThirdwebAuth::SecretKey(secret_key.to_string()),
            )));
        }

        // if not, try client id and service key
        let client_id = parts
            .headers
            .get("x-thirdweb-client-id")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| {
                ApiEngineError(EngineError::ValidationError {
                    message: "Missing x-thirdweb-client-id header".to_string(),
                })
            })?;

        let service_key = parts
            .headers
            .get("x-thirdweb-service-key")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| {
                ApiEngineError(EngineError::ValidationError {
                    message: "Missing x-thirdweb-service-key header".to_string(),
                })
            })?;

        Ok(RpcCredentialsExtractor(RpcCredentials::Thirdweb(
            ThirdwebAuth::ClientIdServiceKey(thirdweb_core::auth::ThirdwebClientIdAndServiceKey {
                client_id: client_id.to_string(),
                service_key: service_key.to_string(),
            }),
        )))
    }
}

/// Extractor for optional RPC credentials from headers
#[derive(OperationIo)]
pub struct OptionalRpcCredentialsExtractor(pub Option<RpcCredentials>);

impl<S> FromRequestParts<S> for OptionalRpcCredentialsExtractor
where
    S: Send + Sync,
{
    type Rejection = ApiEngineError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        match RpcCredentialsExtractor::from_request_parts(parts, state).await {
            Ok(RpcCredentialsExtractor(creds)) => Ok(OptionalRpcCredentialsExtractor(Some(creds))),
            Err(_) => Ok(OptionalRpcCredentialsExtractor(None)),
        }
    }
}

/// Extractor for signing credentials from headers
#[derive(OperationIo)]
pub struct SigningCredentialsExtractor(pub SigningCredential);

impl<S> FromRequestParts<S> for SigningCredentialsExtractor
where
    S: Send + Sync,
{
    type Rejection = ApiEngineError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let vault_access_token = parts
            .headers
            .get("x-vault-access-token")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| {
                ApiEngineError(EngineError::ValidationError {
                    message: "Missing x-vault-access-token header".to_string(),
                })
            })?;

        Ok(SigningCredentialsExtractor(SigningCredential::Vault(
            Auth::AccessToken {
                access_token: vault_access_token.to_string(),
            },
        )))
    }
}

/// Helper to extract ThirdwebAuth from RpcCredentials
impl RpcCredentialsExtractor {
    pub fn into_thirdweb_auth(self) -> Option<ThirdwebAuth> {
        match self.0 {
            RpcCredentials::Thirdweb(auth) => Some(auth),
            // _ => None,
        }
    }
}

impl OptionalRpcCredentialsExtractor {
    pub fn into_thirdweb_auth(self) -> Option<ThirdwebAuth> {
        self.0.and_then(|creds| match creds {
            RpcCredentials::Thirdweb(auth) => Some(auth),
            // _ => None,
        })
    }
}

/// Custom JSON extractor that converts serde errors to ApiEngineError
#[derive(OperationIo)]
#[aide(input_with = "Json<T>", json_schema)]
pub struct EngineJson<T>(pub T);

impl<T, S> axum::extract::FromRequest<S> for EngineJson<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = ApiEngineError;

    async fn from_request(req: axum::extract::Request, state: &S) -> Result<Self, Self::Rejection> {
        match Json::<T>::from_request(req, state).await {
            Ok(Json(data)) => Ok(EngineJson(data)),
            Err(rejection) => {
                let message = match rejection {
                    JsonRejection::JsonDataError(err) => format!("Invalid JSON data: {}", err),
                    JsonRejection::JsonSyntaxError(err) => format!("JSON syntax error: {}", err),
                    JsonRejection::MissingJsonContentType(_) => {
                        "Missing or invalid Content-Type header. Expected application/json"
                            .to_string()
                    }
                    JsonRejection::BytesRejection(err) => {
                        format!("Failed to read request body: {}", err)
                    }
                    _ => "Invalid JSON request".to_string(),
                };

                Err(ApiEngineError(EngineError::ValidationError { message }))
            }
        }
    }
}
