# Engine Integration Tests

This crate contains integration tests for the Engine server with **full code coverage**. Tests programmatically start the Engine server within the test process, following the same pattern as the Vault integration tests.

## Quick Start

### 1. Copy and configure test settings

```bash
cd integration-tests/configuration
cp test_local.yaml.example test_local.yaml
# Edit test_local.yaml and fill in your credentials
```

### 2. Start required services

```bash
# Start Redis
redis-server

# Start Vault (or use remote Vault)
# Update vault.url in test_local.yaml
```

### 3. Run tests

```bash
cargo test -p engine-integration-tests
```

## Configuration

Tests use YAML configuration files (like the server does):

- **`test_base.yaml`** - Base configuration with defaults and structure
- **`test_local.yaml`** - Local overrides (create from `.example` file)

### Configuration Structure

```yaml
vault:
  url: http://127.0.0.1:3001

redis:
  url: redis://127.0.0.1:6379

thirdweb:
  secret_key: YOUR_SECRET_KEY_HERE  # Required!
  client_id: YOUR_CLIENT_ID_HERE     # Required!
  urls:
    rpc: https://rpc.thirdweb-dev.com
    bundler: https://bundler.thirdweb-dev.com
    # ... other URLs

solana:
  devnet:
    http_url: https://api.devnet.solana.com
    ws_url: wss://api.devnet.solana.com
  # ... mainnet, local

queue:
  # Test-optimized queue configuration
  webhook_workers: 1
  solana_executor_workers: 1
  # ... other workers
```

### Environment Variable Overrides

You can override any config value with environment variables using `TEST_` prefix:

```bash
# Override vault URL
export TEST__VAULT__URL=http://localhost:3001

# Override thirdweb secret
export TEST__THIRDWEB__SECRET_KEY=your_key

# Run tests
cargo test -p engine-integration-tests
```

## Architecture

The tests import and start Engine server components directly:
- **No external server required** - server is started programmatically
- **Full code coverage** - all server code runs in the test process
- **Isolated environments** - each test gets its own server instance
- **Proper cleanup** - resources are automatically cleaned up after tests

## Prerequisites

### Required Services

1. **Redis**: Tests require a running Redis instance
   ```bash
   docker run -d -p 6379:6379 redis:latest
   # or
   redis-server
   ```

2. **Vault**: Tests require access to a Vault instance
   ```bash
   # Configure vault.url in test_local.yaml
   ```

### Required Configuration

Fill in these values in `configuration/test_local.yaml`:

- ✅ **`thirdweb.secret_key`** - Get from https://thirdweb.com/dashboard
- ✅ **`thirdweb.client_id`** - Get from https://thirdweb.com/dashboard
- ✅ **`vault.url`** - Your Vault instance URL
- ✅ **`redis.url`** - Your Redis instance URL

## Running Tests

### Run all integration tests

```bash
cd /Users/d4mr/work/thirdweb/engine-core
cargo test -p engine-integration-tests
```

### Run specific test

```bash
cargo test -p engine-integration-tests test_partial_signature_spl_transfer
```

### Run with full logging

```bash
RUST_LOG=debug cargo test -p engine-integration-tests -- --nocapture
```

### Use different environment

```bash
# Use test_staging.yaml instead of test_local.yaml
TEST_ENVIRONMENT=staging cargo test -p engine-integration-tests
```

## Test Scenarios

### 1. Partial Signature Support (`test_partial_signature_spl_transfer`)

This test demonstrates the core partial signature flow:

1. **Test Environment Setup**: Programmatically starts Engine server using config files
2. **Vault Wallet Creation**: Creates a service account and Solana wallet in Vault
3. **Transaction Construction**: Builds a system transfer transaction where the Vault wallet is fee payer
4. **HTTP API Call**: Sends unsigned transaction to Engine's `/v1/solana/sign/transaction` endpoint
5. **Vault Signing**: Engine uses Vault to sign the transaction
6. **Verification**: Confirms the signature is valid and properly positioned
7. **Broadcast Ready**: Transaction is fully signed and ready for broadcast

**What it tests:**
- Server initialization and routing
- Configuration loading from YAML files
- HTTP API endpoint for transaction signing
- Vault integration for key management
- Partial signature scenarios
- Transaction serialization/deserialization (bincode + base64)
- Signature verification using Solana SDK v3

### 2. Signature Verification (`test_transaction_signature_verification`)

Tests that:
- Signatures are properly applied to transactions
- Signature format is correct (base58)
- Signatures can be parsed and validated
- Signed transactions match expected structure
- Non-default signatures indicate proper signing

### 3. Error Handling (`test_invalid_transaction_handling`)

Tests error cases:
- Invalid base64 transaction data
- Malformed requests
- Proper error response formatting
- HTTP status codes

## Configuration Files

```
integration-tests/
├── configuration/
│   ├── test_base.yaml              # Base config with defaults
│   ├── test_local.yaml.example     # Example local config
│   └── test_local.yaml             # Your local config (git-ignored)
├── src/
│   └── lib.rs                      # Helper functions
└── tests/
    ├── setup.rs                    # TestEnvironment + config loading
    └── sign_solana_transaction.rs  # Test cases
```

## How It Works

### Configuration Loading

The `TestEnvironment` loads configuration from YAML files:

```rust
let env = TestEnvironment::new("test_name").await?;
```

This:
1. Loads `test_base.yaml` for defaults
2. Merges `test_local.yaml` for overrides
3. Applies environment variable overrides (`TEST__*`)
4. Initializes all Engine components from config
5. Starts HTTP server on random available port
6. Returns environment ready for testing

### Test Flow

```rust
#[tokio::test]
async fn my_test() -> Result<()> {
    // Start server programmatically (uses YAML config)
    let env = TestEnvironment::new("my_test").await?;
    
    // Create test wallet in Vault
    let (admin_key, wallet) = create_test_solana_wallet(env.vault_client()).await?;
    
    // Make HTTP requests to env.server_url()
    let response = client
        .post(&format!("{}/v1/solana/sign/transaction", env.server_url()))
        .header("x-vault-access-token", format!("Bearer {}", admin_key))
        .json(&request)
        .send()
        .await?;
    
    // Test assertions...
    Ok(())
}
```

## Solana SDK v3 Features Used

This test suite uses **Solana SDK v3** features according to the [migration guide](https://github.com/anza-xyz/solana-sdk):

- `Address` type (though `Pubkey` remains as type alias)
- `VersionedTransaction` for modern transaction format
- `v0::Message` for versioned message construction
- `solana-system-interface` v2 (compatible with SDK v3)
- Proper signature verification with SDK v3 APIs
- `Hash::as_bytes()` for hash access (private inner bytes in v3)

## Troubleshooting

### "Failed to build test configuration" error

- Ensure `configuration/test_local.yaml` exists
- Copy from `test_local.yaml.example` if needed
- Check YAML syntax is valid

### "Failed to connect to Redis" error

- Ensure Redis is running: `redis-cli ping` should return `PONG`
- Check `redis.url` in configuration
- For tests: `docker run -d -p 6379:6379 redis:latest`

### "Failed to create Vault client" error

- Ensure Vault server is running
- Check `vault.url` in configuration
- Verify network connectivity to Vault

### "Missing thirdweb credentials" errors

- Fill in `thirdweb.secret_key` in `test_local.yaml`
- Fill in `thirdweb.client_id` in `test_local.yaml`
- Get credentials from https://thirdweb.com/dashboard

### Tests hang or timeout

- Check Redis is responsive
- Verify Vault is accessible
- Look for port conflicts (server uses random ports)
- Check logs with `RUST_LOG=debug`

## Code Coverage

Since the server runs in-process, all code executed during tests is included in code coverage reports:

```bash
# Generate coverage report
cargo tarpaulin --out Html --output-dir coverage -p engine-integration-tests

# Or use llvm-cov
cargo llvm-cov --html -p engine-integration-tests
```

This captures:
- Server initialization code
- Configuration loading
- HTTP routing and handlers
- Vault integration code
- Transaction signing logic
- Error handling paths
- Serialization/deserialization

## Future Enhancements

- [ ] Add tests for multi-signature scenarios with multiple wallets
- [ ] Test with actual SPL token transfers (using spl-token-interface-2022)
- [ ] Add tests for transaction broadcast to local Solana validator
- [ ] Test different chain IDs (mainnet, devnet, testnet, local)
- [ ] Add performance/benchmarking tests
- [ ] Test error scenarios (rate limits, network failures)
- [ ] Add tests for compute budget instructions
- [ ] Test with address lookup tables (v0 transactions)
