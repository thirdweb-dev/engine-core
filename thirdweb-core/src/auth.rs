use reqwest::header::{HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};

use crate::error::ThirdwebError;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ThirdwebClientIdAndServiceKey {
    pub client_id: String,
    pub service_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ecosystem_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ecosystem_partner_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ThirdwebAuth {
    ClientIdServiceKey(ThirdwebClientIdAndServiceKey),
    SecretKey(String),
}

impl ThirdwebAuth {
    pub fn to_header_map(&self) -> Result<HeaderMap, ThirdwebError> {
        match self {
            ThirdwebAuth::ClientIdServiceKey(creds) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    "x-client-id",
                    HeaderValue::from_str(&creds.client_id)
                        .map_err(|_| ThirdwebError::header_value(creds.client_id.clone()))?,
                );
                headers.insert(
                    "x-service-api-key",
                    HeaderValue::from_str(&creds.service_key)
                        .map_err(|_| ThirdwebError::header_value(creds.client_id.clone()))?,
                );
                if let Some(ecosystem_id) = &creds.ecosystem_id {
                    headers.insert(
                        "x-ecosystem-id",
                        HeaderValue::from_str(ecosystem_id)
                            .map_err(|_| ThirdwebError::header_value(ecosystem_id.clone()))?,
                    );
                }
                if let Some(ecosystem_partner_id) = &creds.ecosystem_partner_id {
                    headers.insert(
                        "x-ecosystem-partner-id",
                        HeaderValue::from_str(ecosystem_partner_id).map_err(|_| {
                            ThirdwebError::header_value(ecosystem_partner_id.clone())
                        })?,
                    );
                }
                Ok(headers)
            }
            ThirdwebAuth::SecretKey(secret_key) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    "x-secret-key",
                    HeaderValue::from_str(secret_key)
                        .map_err(|_| ThirdwebError::header_value(secret_key.clone()))?,
                );
                Ok(headers)
            }
        }
    }
}
