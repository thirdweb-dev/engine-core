# Thirdweb Engine Core

A high-performance, Rust-based blockchain transaction engine that provides robust infrastructure for Web3 applications. Engine Core handles smart contract interactions, Account Abstraction (ERC-4337), and transaction management with enterprise-grade reliability.

## 🏗️ Architecture Overview

Thirdweb Engine Core is built as a modular Rust workspace with the following key components:

### Core Components

- **`server/`** - Main HTTP API server (`thirdweb-engine`)
- **`core/`** - Core blockchain functionality (`engine-core`)
- **`aa-core/`** - Account Abstraction support (`engine-aa-core`)
- **`executors/`** - Background job processors (`engine-executors`)
- **`thirdweb-core/`** - Thirdweb-specific integrations
- **`twmq/`** - Thirdweb Message Queue - Redis-backed job queue system

### Key Features

- ✅ **Account Abstraction (ERC-4337)** - Full support for UserOperations v0.6 and v0.7
- ✅ **Multi-chain Support** - Works with any EVM-compatible blockchain
- ✅ **Smart Contract Interactions** - Read, write, and encode contract calls
- ✅ **Background Job Processing** - Reliable webhook delivery and transaction confirmation
- ✅ **Secure Wallet Management** - Integration with Thirdweb Vault for key management
- ✅ **REST API** - OpenAPI-documented endpoints with Scalar documentation
- ✅ **Enterprise Reliability** - Redis-backed job queues with retry logic and error handling

## 🚀 Quick Start

### Prerequisites

- **Rust** (latest stable version)
- **Redis** server running locally or accessible remotely
- **Thirdweb API credentials** (secret key and client ID)

### Environment Setup

1. **Clone the repository:**

   ```bash
   git clone <repo-url>
   cd engine-core
   ```

2. **Install dependencies:**

   ```bash
   cargo build
   ```

3. **Set up Redis:**

   ```bash
   # Using Docker
   docker run -d --name redis -p 6379:6379 redis:7-alpine

   # Or install locally (macOS)
   brew install redis
   brew services start redis
   ```

### Configuration

The server uses a layered configuration system with YAML files and environment variables.

#### Configuration Files

Create configuration files in the `server/configuration/` directory:

**`server/configuration/server_local.yaml`:**

```yaml
thirdweb:
  secret: "your_thirdweb_secret_key"
  client_id: "your_thirdweb_client_id"
```

#### Or with Environment Variables

You can also override any configuration using environment variables with the prefix `APP__`:

```bash
export APP__THIRDWEB__SECRET="your_secret_key"
export APP__THIRDWEB__CLIENT_ID="your_client_id"
export APP__REDIS__URL="redis://localhost:6379"
export APP__SERVER__PORT=3069
```

#### Required Configuration

- **`thirdweb.secret`** - Your Thirdweb secret key
- **`thirdweb.client_id`** - Your Thirdweb client ID
- **`redis.url`** - Redis connection URL (default: `redis://localhost:6379`)

### Running the Server

1. **Start Redis** (if not already running):

   ```bash
   redis-server
   ```

2. **Run the server:**

   ```bash
   # Development mode with debug logging
   RUST_LOG=debug cargo run --bin thirdweb-engine

   # Or build and run the optimized binary
   cargo build --release
   ./target/release/thirdweb-engine
   ```

3. **Verify the server is running:**
   ```bash
   curl http://localhost:3069/v1/api.json
   ```

The server will start on `http://localhost:3069` by default.

## 📚 API Documentation

Once the server is running, you can access:

- **API Documentation**: `http://localhost:3069/v1/scalar`
- **OpenAPI Spec**: `http://localhost:3069/v1/api.json`

### Available Endpoints

- `POST /v1/read/contract` - Read from smart contracts
- `POST /v1/write/contract` - Write to smart contracts (with AA support)
- `POST /v1/encode/contract` - Encode contract function calls
- `POST /v1/write/transaction` - Send raw transactions

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

1. Check the API documentation at `/v1/scalar`
2. Review server logs for error details
3. Ensure Redis is running and accessible
4. Verify Thirdweb credentials are valid

---

**Built with ❤️ by the Thirdweb team**
