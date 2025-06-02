use reqwest::header::{HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};

use crate::error::ThirdwebError;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ThirdwebClientIdAndServiceKey {
    pub client_id: String,
    pub service_key: String,
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
                Ok(headers)
            }
            ThirdwebAuth::SecretKey(secret_key) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    "x-secret-key",
                    HeaderValue::from_str(&secret_key)
                        .map_err(|_| ThirdwebError::header_value(secret_key.clone()))?,
                );
                Ok(headers)
            }
        }
    }
}
