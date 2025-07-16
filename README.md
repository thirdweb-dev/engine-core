# Thirdweb Engine Core

Production-grade blockchain transaction infrastructure built in Rust. Engine Core is a high-performance, horizontally-scalable system designed for developers building serious Web3 applications that require reliable smart contract interactions, Account Abstraction support, and enterprise-level transaction processing.

## Why Engine Core?

### Performance & Scalability
Built with Rust's zero-cost abstractions and memory safety guarantees. The architecture supports horizontal scaling through Redis-backed job queues with configurable worker pools and lease-based concurrency control.

### Production-Ready Infrastructure
- Redis-backed message queues with atomic operations and retry logic
- Graceful shutdown handling with job completion guarantees  
- Comprehensive error handling and transaction rollback mechanisms
- Built-in monitoring and observability through structured logging

### Developer Experience
- Complete OpenAPI specification with interactive documentation
- Type-safe configuration system with environment variable overrides
- Modular architecture allowing selective component deployment
- Extensive test coverage including integration tests with Redis

## 🏗️ Architecture

Engine Core implements a microservices-like architecture within a single binary, using Rust's workspace system for clean module separation:

### Core Infrastructure (`core/`)
**Purpose**: Fundamental blockchain operations and abstractions
- **Chain Management** (`chain.rs`): Multi-chain RPC client management with automatic failover
- **Transaction Primitives** (`transaction.rs`): Raw transaction building, signing, and broadcasting
- **UserOperation Support** (`userop.rs`): Complete ERC-4337 implementation with v0.6/v0.7 compatibility
- **RPC Clients** (`rpc_clients/`): Specialized clients for bundlers, paymasters, and JSON-RPC endpoints
- **Error Handling** (`error.rs`): Comprehensive error types with context preservation

### Account Abstraction Engine (`aa-core/`)
**Purpose**: Complete ERC-4337 Account Abstraction implementation
- **Smart Account Management** (`smart_account/`): Account factory integrations and deployment
- **UserOperation Builder** (`userop/`): Gas estimation, signature aggregation, and bundler submission
- **Account Factory Support** (`account_factory/`): Pluggable factory implementations (default, chained)
- **Signature Handling** (`signer.rs`): Multi-signature support with Vault integration

### HTTP API Server (`server/`)
**Purpose**: REST API layer with comprehensive endpoint coverage
- **Contract Operations**: Read, write, and encode smart contract functions
- **Transaction Management**: Raw transaction sending with AA support
- **Message Signing**: EIP-712 typed data and personal message signing
- **Dynamic ABI**: Runtime contract introspection and interaction
- **OpenAPI Documentation**: Auto-generated specs with Scalar UI

### Background Job System (`executors/` + `twmq/`)
**Purpose**: Reliable asynchronous processing with Redis persistence

#### TWMQ (Thirdweb Message Queue)
Advanced Redis-backed job queue with enterprise features:
- **Lease-Based Concurrency**: Prevents job duplication across worker instances
- **Atomic Operations**: All queue operations use Lua scripts for consistency
- **Retry Logic**: Configurable backoff strategies with failure categorization
- **Job Lifecycle Management**: Pending → Active → Success/Failed with full audit trail
- **Delayed Jobs**: Schedule jobs for future execution with precise timing
- **Cancellation Support**: Cancel jobs in any state with immediate or pending cancellation

#### Executor Types
- **Webhook Delivery**: Reliable HTTP webhook notifications with configurable retries
- **Transaction Confirmation**: Block confirmation tracking with reorganization handling  
- **External Bundler Integration**: UserOperation submission and status monitoring
- **EOA Transaction Processing**: Production-grade EOA (Externally Owned Account) transaction management with advanced nonce handling, crash recovery, and optimal throughput

##### EOA Executor Deep Dive

The EOA executor implements a sophisticated single-worker-per-EOA architecture that ensures transaction consistency while maximizing throughput:

**Key Features:**
- **Crash-Resilient Recovery**: Borrowed transaction pattern prevents loss during worker restarts
- **Intelligent Nonce Management**: Optimistic nonce allocation with recycling for failed transactions
- **Three-Phase Processing**: Recovery → Confirmation → Send phases ensure complete transaction lifecycle management
- **Adaptive Capacity Control**: Dynamic in-flight transaction limits based on network conditions
- **Health Monitoring**: Automatic EOA balance checking with funding state awareness

**Transaction Flow:**
1. **Recovery Phase**: Rebroadcasts any prepared transactions from crashes
2. **Confirmation Phase**: Efficiently tracks transaction confirmations using nonce progression
3. **Send Phase**: Processes new transactions with recycled nonce prioritization and capacity management

**Error Classification:**
- **Deterministic Failures**: Immediate requeue with nonce recycling (invalid signatures, malformed transactions)
- **Success Cases**: Transaction tracking for known/duplicate transactions
- **Indeterminate Cases**: Optimistic handling for network timeouts and unknown errors

This architecture provides strong consistency guarantees while handling high-volume transaction processing with graceful degradation under network stress.
For more details, see [README_EOA.md](README_EOA.md).

### Thirdweb Service Integration (`thirdweb-core/`)
**Purpose**: First-party service integrations
- **Vault SDK**: Hardware-backed private key management
- **IAW (In-App Wallets)**: Embedded wallet creation and management
- **ABI Service**: Dynamic contract ABI resolution and caching

## 🚀 Getting Started

### System Requirements

- **Rust 1.70+** (2021 edition with async support)
- **Redis 6.0+** (required for job queue persistence and atomic operations)
- **Thirdweb API Credentials** (secret key and client ID from dashboard)

### Quick Setup

```bash
# Clone and build
git clone <repo-url> && cd engine-core
cargo build --release

# Start Redis (Docker recommended for development)
docker run -d --name redis -p 6379:6379 redis:7-alpine

# Configure credentials
export APP__THIRDWEB__SECRET="your_secret_key"
export APP__THIRDWEB__CLIENT_ID="your_client_id"

# Launch engine
RUST_LOG=info ./target/release/thirdweb-engine
```

### Configuration System

Engine Core uses a hierarchical configuration system: YAML files + environment variables with full type safety and validation.

#### Configuration Layers
1. **Base Configuration** (`server_base.yaml`) - Default values
2. **Environment-Specific** (`server_development.yaml`, `server_production.yaml`)
3. **Environment Variables** - Highest priority, prefix with `APP__`

#### Essential Configuration

```yaml
# server/configuration/server_local.yaml
server:
  host: "0.0.0.0"
  port: 3069

thirdweb:
  secret: "your_thirdweb_secret_key"
  client_id: "your_thirdweb_client_id"
  urls:
    vault: "https://vault.thirdweb.com"
    bundler: "bundler.thirdweb.com" 
    paymaster: "bundler.thirdweb.com"

redis:
  url: "redis://localhost:6379"

queue:
  webhook_workers: 50
  external_bundler_send_workers: 20
  userop_confirm_workers: 10
  local_concurrency: 100
  polling_interval_ms: 100
  lease_duration_seconds: 600
```

#### Environment Variable Override Examples

```bash
# Scale worker pools for high throughput
export APP__QUEUE__WEBHOOK_WORKERS=200
export APP__QUEUE__LOCAL_CONCURRENCY=500

# Custom Redis configuration
export APP__REDIS__URL="redis://redis-cluster:6379"

# Debug logging for development
export RUST_LOG="thirdweb_engine=debug,twmq=debug"
```

### Docker Deployment

```dockerfile
FROM rust:1.70 as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates
COPY --from=builder /app/target/release/thirdweb-engine /usr/local/bin/
EXPOSE 3069
CMD ["thirdweb-engine"]
```

### Health Checks & Monitoring

```bash
# API health check
curl http://localhost:3069/v1/api.json

# Queue statistics (if monitoring endpoint enabled)
curl http://localhost:3069/v1/queue/stats

# Structured logging output
RUST_LOG="thirdweb_engine=info,twmq=warn" ./thirdweb-engine
```

## 🌩️ Thirdweb Engine Cloud

**Want Engine without the ops overhead?** [**Thirdweb Engine Cloud**](https://thirdweb.com/engine) is our fully-managed, production-ready service built on Engine Core with enterprise enhancements:

### ⚡ Enhanced Features Beyond Core
- **Auto Execution Resolution**: Smart execution strategy selection with cached Account Abstraction details
- **Streamlined Wallet Management**: Convenient wallet creation and management through dashboard
- **Smart Account Cache**: Pre-resolved AA configurations (signer addresses, factory details, gas policies)
- **Global Edge Network**: Optimized RPC routing and intelligent caching for sub-100ms response times
- **Advanced Analytics**: Real-time transaction monitoring, gas usage insights, and performance metrics
- **Zero-Config Account Abstraction**: Automatic paymaster selection and gas sponsorship

### 🛡️ Production-Ready Operations
- **High Availability** with automated failover and disaster recovery
- **Horizontal Auto-Scaling** based on transaction volume and queue depth
- **Enterprise Security**: Encryption at rest/transit, comprehensive audit logging
- **Expert Support** with dedicated technical assistance
- **Custom Rate Limits** and priority processing for high-volume applications

### 🚀 Get Started Instantly
```bash
# No infrastructure setup required
curl -X POST "https://api.engine.thirdweb.com/contract/write" \
  -H "Authorization: Bearer YOUR_ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "executionOptions": {
      "chainId": 137,
      "type": "auto",
      "from": "0x..."
    },
    "params": [{
      "contractAddress": "0x...",
      "functionName": "mint",
      "args": ["0x...", "1"]
    }]
  }'
```

**[Start Building →](https://thirdweb.com/engine)** | **[View Cloud API Reference →](https://engine.thirdweb.com/reference)**

---

## 📚 Self-Hosted API Documentation

For self-hosted Engine Core instances:
- **Interactive Documentation**: `http://localhost:3069/reference`
- **OpenAPI Specification**: `http://localhost:3069/api.json`

## 🔧 Development

### Running Tests

```bash
# Run all tests
cargo test

# Run tests for a specific component
cargo test -p twmq
cargo test -p engine-core

# Run with Redis integration tests
cargo nextest run -p twmq --profile ci
```

### Code Structure

```
├── server/           # Main HTTP server
│   ├── src/
│   │   ├── http/     # REST API routes and handlers
│   │   ├── queue/    # Queue management
│   │   └── config.rs # Configuration management
│   └── configuration/ # YAML config files
├── core/             # Core blockchain functionality
│   └── src/
│       ├── userop.rs # Account Abstraction support
│       ├── chain.rs  # Chain management
│       └── error.rs  # Error types
├── twmq/             # Message queue system
│   └── src/
│       ├── queue.rs  # Queue implementation
│       └── job.rs    # Job processing
├── executors/        # Background job handlers
│   └── src/
│       ├── webhook/  # Webhook delivery
│       └── external_bundler/ # AA bundler integration
│       └── eoa/      # EOA transaction processing
│           ├── worker/       # Main worker logic
│           ├── store/        # Redis-backed state management
│           ├── events.rs     # Transaction lifecycle events
│           └── error_classifier.rs # Error categorization
└── thirdweb-core/    # Thirdweb integrations
```

### Adding New Features

1. **New API Endpoints**: Add routes in `server/src/http/routes/`
2. **Background Jobs**: Implement `DurableExecution` trait in `executors/`
3. **Chain Support**: Extend `ThirdwebChainService` in `server/src/chains.rs`

## 🔍 Monitoring & Operations

### Queue Statistics

The system provides comprehensive queue monitoring for webhook delivery, transaction sending, and confirmation processing:

```rust
// Queue statistics are available through the QueueManager
let stats = queue_manager.get_stats().await?;
println!("Pending jobs: {}", stats.webhook.pending);
```

### Logging

Configure logging levels using the `RUST_LOG` environment variable:

```bash
# Detailed debugging
RUST_LOG="thirdweb_engine=debug,twmq=debug,engine_executors=debug"

# Production logging
RUST_LOG="thirdweb_engine=info,twmq=warn"
```

### Health Checks

The server provides graceful shutdown handling and can be monitored for:

- HTTP server health
- Redis connectivity
- Background worker status

## 🔒 Security Considerations

- **Vault Integration**: All private keys are managed through Thirdweb Vault
- **API Authentication**: Requests are authenticated using Thirdweb secret keys
- **Network Security**: Configure appropriate CORS policies for production
- **Redis Security**: Secure your Redis instance with authentication and network restrictions

## 🌐 Production Deployment

### Environment Configuration

Set `APP_ENVIRONMENT=production` and create `server_production.yaml`:

```yaml
server:
  host: "0.0.0.0"
  port: 3069

redis:
  url: "redis://your-redis-cluster"

queue:
  webhook_workers: 50
  external_bundler_send_workers: 20
  userop_confirm_workers: 10
```

### Resource Requirements

- **CPU**: 2+ cores recommended
- **Memory**: 1GB+ RAM (more for high-throughput scenarios)
- **Storage**: Minimal (logs and temporary data only)
- **Network**: Stable internet connection for blockchain RPC calls

### Scaling Considerations

- **Horizontal Scaling**: Multiple instances can share the same Redis backend
- **Queue Workers**: Adjust worker counts based on throughput requirements
- **Redis**: Consider Redis clustering for high availability

## 📦 Dependencies

### Key External Dependencies

- **Alloy** - Ethereum library for Rust
- **Axum** - Web framework
- **Redis** - Message queue backend
- **Tokio** - Async runtime
- **Thirdweb Vault SDK** - Secure wallet management

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests where appropriate
5. Run the test suite: `cargo test`
6. Submit a pull request

## 📄 License

MIT

## 🆘 Support

For issues and questions:

1. Check the API documentation at `/reference`
2. Review server logs for error details
3. Ensure Redis is running and accessible
4. Verify Thirdweb credentials are valid

---

**Built with ❤️ by the Thirdweb team**

