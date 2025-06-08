use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Serialize, Deserialize, Debug, Clone, JsonSchema, utoipa::ToSchema)]
pub enum ThirdwebError {
    #[error("SerializationError: {0}")]
    SerializationError(ThirdwebSerializationError),

    #[error("UrlParseError: {message}")]
    UrlParseError { value: String, message: String },

    #[error("HttpClientBackendError: {0}")]
    HttpClientBackendError(String),

    #[error("HttpError: {0}")]
    HttpError(#[from] SerializableReqwestError),
}

#[derive(Error, Serialize, Deserialize, Debug, Clone, JsonSchema, utoipa::ToSchema)]
pub enum ThirdwebSerializationError {
    #[error("InvalidHeaderValue: {value:?}")]
    HeaderValue { value: String },
}

impl ThirdwebError {
    pub fn header_value(value: String) -> Self {
        Self::SerializationError(ThirdwebSerializationError::HeaderValue { value })
    }

    pub fn url(value: String, error: url::ParseError) -> Self {
        Self::UrlParseError {
            value,
            message: error.to_string(),
        }
    }

    pub fn http_client_backend(error: reqwest::Error) -> Self {
        Self::HttpClientBackendError(error.to_string())
    }
}

impl From<ThirdwebSerializationError> for ThirdwebError {
    fn from(err: ThirdwebSerializationError) -> Self {
        Self::SerializationError(err)
    }
}

#[derive(Error, Serialize, Deserialize, Debug, Clone, JsonSchema, utoipa::ToSchema)]
pub enum SerializableReqwestError {
    #[error("builder error")]
    Builder {
        message: String,
        url: Option<String>,
    },

    #[error("error sending request")]
    Request {
        message: String,
        url: Option<String>,
    },

    #[error("operation timed out")]
    Timeout {
        message: String,
        url: Option<String>,
    },

    #[error("connection failed")]
    Connect {
        message: String,
        url: Option<String>,
    },

    #[error("error following redirect")]
    Redirect {
        message: String,
        url: Option<String>,
    },

    #[error("HTTP status client error ({status})")]
    ClientError {
        status: u16,
        message: String,
        url: Option<String>,
    },

    #[error("HTTP status server error ({status})")]
    ServerError {
        status: u16,
        message: String,
        url: Option<String>,
    },

    #[error("request or response body error")]
    Body {
        message: String,
        url: Option<String>,
    },

    #[error("error decoding response body")]
    Decode {
        message: String,
        url: Option<String>,
    },

    #[error("error upgrading connection")]
    Upgrade {
        message: String,
        url: Option<String>,
    },

    #[error("unknown error: {message}")]
    Unknown {
        message: String,
        url: Option<String>,
    },
}

impl From<&reqwest::Error> for SerializableReqwestError {
    fn from(error: &reqwest::Error) -> Self {
        let message = error.to_string();
        let url = error.url().map(|u| u.to_string());

        if error.is_timeout() {
            Self::Timeout { message, url }
        } else if error.is_connect() {
            Self::Connect { message, url }
        } else if error.is_builder() {
            Self::Builder { message, url }
        } else if error.is_request() {
            Self::Request { message, url }
        } else if error.is_redirect() {
            Self::Redirect { message, url }
        } else if error.is_status() {
            if let Some(status) = error.status() {
                let status_code = status.as_u16();
                if status.is_client_error() {
                    Self::ClientError {
                        status: status_code,
                        message,
                        url,
                    }
                } else {
                    Self::ServerError {
                        status: status_code,
                        message,
                        url,
                    }
                }
            } else {
                Self::Unknown { message, url }
            }
        } else if error.is_body() {
            Self::Body { message, url }
        } else if error.is_decode() {
            Self::Decode { message, url }
        } else {
            Self::Unknown { message, url }
        }
    }
}

impl From<reqwest::Error> for SerializableReqwestError {
    fn from(error: reqwest::Error) -> Self {
        Self::from(&error)
    }
}
