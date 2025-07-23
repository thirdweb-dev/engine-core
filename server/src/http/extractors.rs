use aide::OperationIo;
use aws_arn::known;
use axum::{
    Json,
    extract::{FromRequestParts, rejection::JsonRejection},
    http::request::Parts,
};
use engine_core::{
    chain::RpcCredentials,
    credentials::{AwsKmsCredential, SigningCredential},
    error::EngineError,
};
use thirdweb_core::auth::ThirdwebAuth;
use vault_types::enclave::auth::Auth;

use crate::http::error::ApiEngineError;

// Header name constants
const HEADER_THIRDWEB_SECRET_KEY: &str = "x-thirdweb-secret-key";
const HEADER_THIRDWEB_CLIENT_ID: &str = "x-thirdweb-client-id";
const HEADER_THIRDWEB_SERVICE_KEY: &str = "x-thirdweb-service-key";
const HEADER_WALLET_ACCESS_TOKEN: &str = "x-wallet-access-token";
const HEADER_VAULT_ACCESS_TOKEN: &str = "x-vault-access-token";
const HEADER_AWS_KMS_ARN: &str = "x-aws-kms-arn";
const HEADER_AWS_ACCESS_KEY_ID: &str = "x-aws-access-key-id";
const HEADER_AWS_SECRET_ACCESS_KEY: &str = "x-aws-secret-access-key";

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
            .get(HEADER_THIRDWEB_SECRET_KEY)
            .and_then(|v| v.to_str().ok());

        if let Some(secret_key) = secret_key {
            return Ok(RpcCredentialsExtractor(RpcCredentials::Thirdweb(
                ThirdwebAuth::SecretKey(secret_key.to_string()),
            )));
        }

        // if not, try client id and service key
        let client_id = parts
            .headers
            .get(HEADER_THIRDWEB_CLIENT_ID)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| {
                ApiEngineError(EngineError::ValidationError {
                    message: "Missing x-thirdweb-client-id header".to_string(),
                })
            })?;

        let service_key = parts
            .headers
            .get(HEADER_THIRDWEB_SERVICE_KEY)
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
        // Try AWS KMS credentials first
        if let Some(aws_kms) = Self::try_extract_aws_kms(parts)? {
            return Ok(SigningCredentialsExtractor(SigningCredential::AwsKms(
                aws_kms,
            )));
        }

        // Try IAW credentials second
        if let Some(iaw) = Self::try_extract_iaw(parts)? {
            return Ok(SigningCredentialsExtractor(SigningCredential::Iaw {
                auth_token: iaw.0,
                thirdweb_auth: iaw.1,
            }));
        }

        // Try Vault credentials last
        if let Some(vault_token) = Self::get_header_value(parts, HEADER_VAULT_ACCESS_TOKEN) {
            return Ok(SigningCredentialsExtractor(SigningCredential::Vault(
                Auth::AccessToken {
                    access_token: vault_token.to_string(),
                },
            )));
        }

        // No valid credentials found
        Err(ApiEngineError(EngineError::ValidationError {
            message: "Missing valid authentication credentials. Provide either AWS KMS headers (x-aws-kms-arn, x-aws-kms-access-key-id, x-aws-secret-access-key), IAW credentials (x-wallet-access-token + x-thirdweb-client-id + x-thirdweb-service-key), or Vault credentials (x-vault-access-token)".to_string(),
        }))
    }
}

impl SigningCredentialsExtractor {
    /// Extract header value as string
    fn get_header_value<'a>(parts: &'a Parts, header_name: &str) -> Option<&'a str> {
        parts.headers.get(header_name).and_then(|v| v.to_str().ok())
    }

    /// Try to extract AWS KMS credentials from headers
    fn try_extract_aws_kms(parts: &Parts) -> Result<Option<AwsKmsCredential>, ApiEngineError> {
        let arn = Self::get_header_value(parts, HEADER_AWS_KMS_ARN);
        let access_key_id = Self::get_header_value(parts, HEADER_AWS_ACCESS_KEY_ID);
        let secret_access_key = Self::get_header_value(parts, HEADER_AWS_SECRET_ACCESS_KEY);

        match (arn, access_key_id, secret_access_key) {
            (Some(arn), Some(access_key_id), Some(secret_access_key)) => {
                let (key_id, region) = Self::parse_kms_arn(arn)?;
                Ok(Some(AwsKmsCredential {
                    access_key_id: access_key_id.to_string(),
                    secret_access_key: secret_access_key.to_string(),
                    key_id,
                    region,
                }))
            }
            _ => Ok(None),
        }
    }

    /// Parse and validate KMS ARN, returning (key_id, region)
    fn parse_kms_arn(arn: &str) -> Result<(String, String), ApiEngineError> {
        let parsed_arn: aws_arn::ResourceName = arn.parse().map_err(|e| {
            ApiEngineError(EngineError::ValidationError {
                message: format!("Invalid AWS ARN format: {}", e),
            })
        })?;

        // Validate it's a KMS service
        if parsed_arn.service != known::Service::KeyManagement.into() {
            return Err(ApiEngineError(EngineError::ValidationError {
                message: format!("ARN must be for KMS service, got: {}", parsed_arn.service),
            }));
        }

        // Extract and validate key ID
        let key_id = parsed_arn
            .resource
            .path_split()
            .last()
            .map(|id| id.to_string())
            .ok_or_else(|| {
                ApiEngineError(EngineError::ValidationError {
                    message: "KMS ARN must contain a valid key ID in the resource part".to_string(),
                })
            })?;

        // Extract and validate region
        let region = parsed_arn.region.ok_or_else(|| {
            ApiEngineError(EngineError::ValidationError {
                message: "KMS ARN must contain a valid region".to_string(),
            })
        })?;

        Ok((key_id, region.to_string()))
    }

    /// Try to extract IAW credentials from headers, returning (auth_token, thirdweb_auth)
    fn try_extract_iaw(parts: &Parts) -> Result<Option<(String, ThirdwebAuth)>, ApiEngineError> {
        let wallet_token = Self::get_header_value(parts, HEADER_WALLET_ACCESS_TOKEN);

        if let Some(wallet_token) = wallet_token {
            let client_id =
                Self::get_header_value(parts, HEADER_THIRDWEB_CLIENT_ID).ok_or_else(|| {
                    ApiEngineError(EngineError::ValidationError {
                        message:
                            "Missing x-thirdweb-client-id header when using x-wallet-access-token"
                                .to_string(),
                    })
                })?;

            let service_key = Self::get_header_value(parts, HEADER_THIRDWEB_SERVICE_KEY)
                .ok_or_else(|| {
                    ApiEngineError(EngineError::ValidationError {
                        message:
                            "Missing x-thirdweb-service-key header when using x-wallet-access-token"
                                .to_string(),
                    })
                })?;

            let thirdweb_auth = ThirdwebAuth::ClientIdServiceKey(
                thirdweb_core::auth::ThirdwebClientIdAndServiceKey {
                    client_id: client_id.to_string(),
                    service_key: service_key.to_string(),
                },
            );

            Ok(Some((wallet_token.to_string(), thirdweb_auth)))
        } else {
            Ok(None)
        }
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
        self.0.map(|creds| match creds {
            RpcCredentials::Thirdweb(auth) => auth,
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
