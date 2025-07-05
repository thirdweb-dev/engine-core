mod fixtures;
use fixtures::*;

use std::sync::Arc;
use std::time::Duration;
use serde_json::json;
use twmq::job::{Job, JobError, JobResult, BorrowedJob};
use wiremock::{Mock, MockServer, ResponseTemplate};
use wiremock::matchers::{method, path, header};
use engine_executors::webhook::{
    WebhookJobHandler, WebhookJobPayload, WebhookJobOutput, WebhookError, WebhookRetryConfig,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_webhook_job_handler_new() {
    setup_tracing();
    let retry_config = WebhookRetryConfig::default();
    let handler = WebhookJobHandler {
        http_client: reqwest::Client::new(),
        retry_config: Arc::new(retry_config),
    };
    
    assert_eq!(handler.retry_config.max_attempts, 5);
    assert_eq!(handler.retry_config.initial_delay_ms, 1000);
    assert_eq!(handler.retry_config.max_delay_ms, 30000);
    assert_eq!(handler.retry_config.backoff_factor, 2.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_webhook_job_handler_custom_config() {
    setup_tracing();
    let retry_config = WebhookRetryConfig {
        max_attempts: 3,
        initial_delay_ms: 500,
        max_delay_ms: 15000,
        backoff_factor: 1.5,
    };
    
    let handler = WebhookJobHandler {
        http_client: reqwest::Client::new(),
        retry_config: Arc::new(retry_config),
    };
    
    assert_eq!(handler.retry_config.max_attempts, 3);
    assert_eq!(handler.retry_config.initial_delay_ms, 500);
    assert_eq!(handler.retry_config.max_delay_ms, 15000);
    assert_eq!(handler.retry_config.backoff_factor, 1.5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_successful_webhook_post() {
    setup_tracing();
    let mock_server = MockServer::start().await;
    
    Mock::given(method("POST"))
        .and(path("/webhook"))
        .and(header("content-type", "application/json; charset=utf-8"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"success": true})))
        .mount(&mock_server)
        .await;
    
    let handler = WebhookJobHandler {
        http_client: reqwest::Client::new(),
        retry_config: Arc::new(WebhookRetryConfig::default()),
    };
    
    let payload = WebhookJobPayload {
        url: format!("{}/webhook", mock_server.uri()),
        body: json!({"message": "test"}).to_string(),
        headers: None,
        hmac_secret: None,
        http_method: Some("POST".to_string()),
    };
    
    let job = Job::new(payload);
    let borrowed_job = BorrowedJob::new(&job);
    
    let result = handler.process(&borrowed_job).await;
    
    assert!(result.is_ok());
    let output = result.unwrap();
    assert_eq!(output.status_code, 200);
    assert!(output.response_body.is_some());
    assert!(output.response_body.unwrap().contains("success"));
}

#[tokio::test]
async fn test_webhook_with_custom_headers() {
    let mock_server = MockServer::start().await;
    
    Mock::given(method("POST"))
        .and(path("/webhook"))
        .and(header("X-Custom-Header", "custom-value"))
        .and(header("Authorization", "Bearer token123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"received": true})))
        .mount(&mock_server)
        .await;
    
    let handler = WebhookJobHandler {
        http_client: reqwest::Client::new(),
        retry_config: Arc::new(WebhookRetryConfig::default()),
    };
    
    let mut headers = std::collections::HashMap::new();
    headers.insert("X-Custom-Header".to_string(), "custom-value".to_string());
    headers.insert("Authorization".to_string(), "Bearer token123".to_string());
    
    let payload = WebhookJobPayload {
        url: format!("{}/webhook", mock_server.uri()),
        body: json!({"data": "test"}).to_string(),
        headers: Some(headers),
        hmac_secret: None,
        http_method: Some("POST".to_string()),
    };
    
    let job = Job::new(payload);
    let borrowed_job = BorrowedJob::new(&job);
    
    let result = handler.process(&borrowed_job).await;
    
    assert!(result.is_ok());
    let output = result.unwrap();
    assert_eq!(output.status_code, 200);
}

#[tokio::test]
async fn test_webhook_with_hmac_authentication() {
    let mock_server = MockServer::start().await;
    
    Mock::given(method("POST"))
        .and(path("/webhook"))
        .and(header("X-Signature-SHA256", wiremock::matchers::any()))
        .and(header("X-Request-Timestamp", wiremock::matchers::any()))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"authenticated": true})))
        .mount(&mock_server)
        .await;
    
    let handler = WebhookJobHandler {
        http_client: reqwest::Client::new(),
        retry_config: Arc::new(WebhookRetryConfig::default()),
    };
    
    let payload = WebhookJobPayload {
        url: format!("{}/webhook", mock_server.uri()),
        body: json!({"secure": "data"}).to_string(),
        headers: None,
        hmac_secret: Some("secret_key".to_string()),
        http_method: Some("POST".to_string()),
    };
    
    let job = Job::new(payload);
    let borrowed_job = BorrowedJob::new(&job);
    
    let result = handler.process(&borrowed_job).await;
    
    assert!(result.is_ok());
    let output = result.unwrap();
    assert_eq!(output.status_code, 200);
}

#[tokio::test]
async fn test_webhook_put_method() {
    let mock_server = MockServer::start().await;
    
    Mock::given(method("PUT"))
        .and(path("/webhook"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"method": "PUT"})))
        .mount(&mock_server)
        .await;
    
    let handler = WebhookJobHandler {
        http_client: reqwest::Client::new(),
        retry_config: Arc::new(WebhookRetryConfig::default()),
    };
    
    let payload = WebhookJobPayload {
        url: format!("{}/webhook", mock_server.uri()),
        body: json!({"update": "data"}).to_string(),
        headers: None,
        hmac_secret: None,
        http_method: Some("PUT".to_string()),
    };
    
    let job = Job::new(payload);
    let borrowed_job = BorrowedJob::new(&job);
    
    let result = handler.process(&borrowed_job).await;
    
    assert!(result.is_ok());
    let output = result.unwrap();
    assert_eq!(output.status_code, 200);
}

#[tokio::test]
async fn test_webhook_client_error_no_retry() {
    let mock_server = MockServer::start().await;
    
    Mock::given(method("POST"))
        .and(path("/webhook"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({"error": "Bad Request"})))
        .mount(&mock_server)
        .await;
    
    let handler = WebhookJobHandler {
        http_client: reqwest::Client::new(),
        retry_config: Arc::new(WebhookRetryConfig::default()),
    };
    
    let payload = WebhookJobPayload {
        url: format!("{}/webhook", mock_server.uri()),
        body: json!({"bad": "data"}).to_string(),
        headers: None,
        hmac_secret: None,
        http_method: Some("POST".to_string()),
    };
    
    let job = Job::new(payload);
    let borrowed_job = BorrowedJob::new(&job);
    
    let result = handler.process(&borrowed_job).await;
    
    assert!(result.is_err());
    match result.unwrap_err() {
        JobError::Fail(WebhookError::Http { status, .. }) => {
            assert_eq!(status, 400);
        }
        _ => panic!("Expected Http error"),
    }
}

#[tokio::test]
async fn test_webhook_server_error_retry() {
    let mock_server = MockServer::start().await;
    
    Mock::given(method("POST"))
        .and(path("/webhook"))
        .respond_with(ResponseTemplate::new(500).set_body_json(json!({"error": "Server Error"})))
        .mount(&mock_server)
        .await;
    
    let handler = WebhookJobHandler {
        http_client: reqwest::Client::new(),
        retry_config: Arc::new(WebhookRetryConfig {
            max_attempts: 3,
            initial_delay_ms: 100,
            max_delay_ms: 1000,
            backoff_factor: 2.0,
        }),
    };
    
    let payload = WebhookJobPayload {
        url: format!("{}/webhook", mock_server.uri()),
        body: json!({"data": "test"}).to_string(),
        headers: None,
        hmac_secret: None,
        http_method: Some("POST".to_string()),
    };
    
    let job = Job::new(payload);
    let borrowed_job = BorrowedJob::new(&job);
    
    let result = handler.process(&borrowed_job).await;
    
    assert!(result.is_err());
    match result.unwrap_err() {
        JobError::Nack { error, delay, .. } => {
            assert!(matches!(error, WebhookError::Http { status: 500, .. }));
            assert!(delay.is_some());
        }
        _ => panic!("Expected Nack error"),
    }
}

#[tokio::test]
async fn test_webhook_max_retry_attempts() {
    let mock_server = MockServer::start().await;
    
    Mock::given(method("POST"))
        .and(path("/webhook"))
        .respond_with(ResponseTemplate::new(500).set_body_json(json!({"error": "Server Error"})))
        .mount(&mock_server)
        .await;
    
    let handler = WebhookJobHandler {
        http_client: reqwest::Client::new(),
        retry_config: Arc::new(WebhookRetryConfig {
            max_attempts: 2,
            initial_delay_ms: 100,
            max_delay_ms: 1000,
            backoff_factor: 2.0,
        }),
    };
    
    let payload = WebhookJobPayload {
        url: format!("{}/webhook", mock_server.uri()),
        body: json!({"data": "test"}).to_string(),
        headers: None,
        hmac_secret: None,
        http_method: Some("POST".to_string()),
    };
    
    let mut job = Job::new(payload);
    job.attempts = 2; // Set to max attempts
    let borrowed_job = BorrowedJob::new(&job);
    
    let result = handler.process(&borrowed_job).await;
    
    assert!(result.is_err());
    match result.unwrap_err() {
        JobError::Fail(WebhookError::Http { status: 500, .. }) => {
            // Should fail permanently after max attempts
        }
        _ => panic!("Expected permanent fail after max attempts"),
    }
}

#[tokio::test]
async fn test_webhook_network_error() {
    let handler = WebhookJobHandler {
        http_client: reqwest::Client::new(),
        retry_config: Arc::new(WebhookRetryConfig::default()),
    };
    
    let payload = WebhookJobPayload {
        url: "http://invalid.domain.that.does.not.exist/webhook".to_string(),
        body: json!({"data": "test"}).to_string(),
        headers: None,
        hmac_secret: None,
        http_method: Some("POST".to_string()),
    };
    
    let job = Job::new(payload);
    let borrowed_job = BorrowedJob::new(&job);
    
    let result = handler.process(&borrowed_job).await;
    
    assert!(result.is_err());
    match result.unwrap_err() {
        JobError::Nack { error, delay, .. } => {
            assert!(matches!(error, WebhookError::Network(_)));
            assert!(delay.is_some());
        }
        _ => panic!("Expected Network error with retry"),
    }
}

#[tokio::test]
async fn test_webhook_empty_hmac_secret() {
    let handler = WebhookJobHandler {
        http_client: reqwest::Client::new(),
        retry_config: Arc::new(WebhookRetryConfig::default()),
    };
    
    let payload = WebhookJobPayload {
        url: "http://example.com/webhook".to_string(),
        body: json!({"data": "test"}).to_string(),
        headers: None,
        hmac_secret: Some("".to_string()), // Empty secret
        http_method: Some("POST".to_string()),
    };
    
    let job = Job::new(payload);
    let borrowed_job = BorrowedJob::new(&job);
    
    let result = handler.process(&borrowed_job).await;
    
    assert!(result.is_err());
    match result.unwrap_err() {
        JobError::Fail(WebhookError::HmacGeneration(msg)) => {
            assert!(msg.contains("HMAC secret cannot be empty"));
        }
        _ => panic!("Expected HMAC generation error"),
    }
}

#[tokio::test]
async fn test_webhook_unsupported_http_method() {
    let handler = WebhookJobHandler {
        http_client: reqwest::Client::new(),
        retry_config: Arc::new(WebhookRetryConfig::default()),
    };
    
    let payload = WebhookJobPayload {
        url: "http://example.com/webhook".to_string(),
        body: json!({"data": "test"}).to_string(),
        headers: None,
        hmac_secret: None,
        http_method: Some("INVALID".to_string()),
    };
    
    let job = Job::new(payload);
    let borrowed_job = BorrowedJob::new(&job);
    
    let result = handler.process(&borrowed_job).await;
    
    assert!(result.is_err());
    match result.unwrap_err() {
        JobError::Fail(WebhookError::UnsupportedHttpMethod(method)) => {
            assert_eq!(method, "INVALID");
        }
        _ => panic!("Expected UnsupportedHttpMethod error"),
    }
}

#[tokio::test]
async fn test_webhook_invalid_header() {
    let handler = WebhookJobHandler {
        http_client: reqwest::Client::new(),
        retry_config: Arc::new(WebhookRetryConfig::default()),
    };
    
    let mut headers = std::collections::HashMap::new();
    headers.insert("Invalid\x00Header".to_string(), "value".to_string());
    
    let payload = WebhookJobPayload {
        url: "http://example.com/webhook".to_string(),
        body: json!({"data": "test"}).to_string(),
        headers: Some(headers),
        hmac_secret: None,
        http_method: Some("POST".to_string()),
    };
    
    let job = Job::new(payload);
    let borrowed_job = BorrowedJob::new(&job);
    
    let result = handler.process(&borrowed_job).await;
    
    assert!(result.is_err());
    match result.unwrap_err() {
        JobError::Fail(WebhookError::RequestConstruction(msg)) => {
            assert!(msg.contains("Invalid header name"));
        }
        _ => panic!("Expected RequestConstruction error"),
    }
}

#[tokio::test]
async fn test_webhook_default_post_method() {
    let mock_server = MockServer::start().await;
    
    Mock::given(method("POST"))
        .and(path("/webhook"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"success": true})))
        .mount(&mock_server)
        .await;
    
    let handler = WebhookJobHandler {
        http_client: reqwest::Client::new(),
        retry_config: Arc::new(WebhookRetryConfig::default()),
    };
    
    let payload = WebhookJobPayload {
        url: format!("{}/webhook", mock_server.uri()),
        body: json!({"message": "test"}).to_string(),
        headers: None,
        hmac_secret: None,
        http_method: None, // Should default to POST
    };
    
    let job = Job::new(payload);
    let borrowed_job = BorrowedJob::new(&job);
    
    let result = handler.process(&borrowed_job).await;
    
    assert!(result.is_ok());
    let output = result.unwrap();
    assert_eq!(output.status_code, 200);
}

#[tokio::test]
async fn test_webhook_rate_limit_retry() {
    let mock_server = MockServer::start().await;
    
    Mock::given(method("POST"))
        .and(path("/webhook"))
        .respond_with(ResponseTemplate::new(429).set_body_json(json!({"error": "Rate Limited"})))
        .mount(&mock_server)
        .await;
    
    let handler = WebhookJobHandler {
        http_client: reqwest::Client::new(),
        retry_config: Arc::new(WebhookRetryConfig {
            max_attempts: 3,
            initial_delay_ms: 100,
            max_delay_ms: 1000,
            backoff_factor: 2.0,
        }),
    };
    
    let payload = WebhookJobPayload {
        url: format!("{}/webhook", mock_server.uri()),
        body: json!({"data": "test"}).to_string(),
        headers: None,
        hmac_secret: None,
        http_method: Some("POST".to_string()),
    };
    
    let job = Job::new(payload);
    let borrowed_job = BorrowedJob::new(&job);
    
    let result = handler.process(&borrowed_job).await;
    
    assert!(result.is_err());
    match result.unwrap_err() {
        JobError::Nack { error, delay, .. } => {
            assert!(matches!(error, WebhookError::Http { status: 429, .. }));
            assert!(delay.is_some());
        }
        _ => panic!("Expected Nack error for rate limiting"),
    }
}

#[tokio::test]
async fn test_webhook_output_serialization() {
    let output = WebhookJobOutput {
        status_code: 200,
        response_body: Some("test response".to_string()),
    };
    
    let serialized = serde_json::to_string(&output).unwrap();
    let deserialized: WebhookJobOutput = serde_json::from_str(&serialized).unwrap();
    
    assert_eq!(deserialized.status_code, 200);
    assert_eq!(deserialized.response_body, Some("test response".to_string()));
}

#[tokio::test]
async fn test_webhook_error_serialization() {
    let error = WebhookError::Http {
        status: 500,
        body_preview: "Internal Server Error".to_string(),
    };
    
    let serialized = serde_json::to_string(&error).unwrap();
    let deserialized: WebhookError = serde_json::from_str(&serialized).unwrap();
    
    match deserialized {
        WebhookError::Http { status, body_preview } => {
            assert_eq!(status, 500);
            assert_eq!(body_preview, "Internal Server Error");
        }
        _ => panic!("Expected Http error"),
    }
}