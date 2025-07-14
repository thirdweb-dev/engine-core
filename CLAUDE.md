# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Build and Test
- `cargo build --release` - Build production binary
- `cargo test` - Run all tests including Redis integration tests
- `cargo nextest run -p twmq --profile ci` - Run specific component tests
- `RUST_LOG=debug cargo run` - Run server with debug logging

### Development Setup
Redis is required for development:
```bash
docker run -d --name redis -p 6379:6379 redis:7-alpine
```

Required environment variables:
```bash
export APP__THIRDWEB__SECRET="your_secret_key"
export APP__THIRDWEB__CLIENT_ID="your_client_id"
```

## Architecture Overview

This is a **Rust workspace** with 7 crates providing blockchain transaction infrastructure:

- **`server/`** - Main HTTP API server (Axum-based REST API with OpenAPI docs)
- **`core/`** - Core blockchain functionality (chain management, transactions, UserOps)
- **`aa-core/`** - Account Abstraction engine (ERC-4337 v0.6/v0.7 support)
- **`aa-types/`** - Account Abstraction type definitions
- **`executors/`** - Background job handlers (webhooks, transaction confirmation)
- **`twmq/`** - Thirdweb Message Queue (Redis-backed job queue with lease-based concurrency)
- **`thirdweb-core/`** - Thirdweb service integrations (Vault SDK, IAW)

### Key Technologies
- **Axum** for HTTP server
- **Alloy** for Ethereum interactions
- **Redis** for job queue and state
- **Tokio** for async runtime
- **Vault SDK** for secure key management

## Configuration System

Hierarchical configuration priority:
1. Environment variables (`APP__` prefix)
2. Environment-specific YAML (`server_development.yaml`, `server_production.yaml`)
3. Base YAML (`server_base.yaml`)

Configuration files located in `server/configuration/`

## Transaction Types Supported

- **EOA transactions** - Traditional wallet transactions
- **Account Abstraction** - ERC-4337 smart accounts with gas sponsorship
- **EIP-7702** - Delegated transaction execution

## Key Development Areas

### API Routes
Located in `server/src/http/routes/` - follows RESTful patterns with OpenAPI documentation

### Background Jobs
Implemented in `executors/src/` - uses TWMQ for reliable job processing with Redis persistence

### Blockchain Core
`core/src/` contains chain management, transaction building, and UserOperation support

### Account Abstraction
`aa-core/src/` implements complete ERC-4337 flow including bundler integration

## Error Handling

Uses comprehensive error types with context. All errors are structured and logged with tracing spans.

## Testing

Integration tests require Redis. Tests cover job queue operations, transaction building, and API endpoints.

## Production Features

- Horizontal scaling via shared Redis backend
- Graceful shutdown with job completion guarantees
- Vault-backed private key management
- Comprehensive metrics and health checks
- Docker support with multi-stage builds