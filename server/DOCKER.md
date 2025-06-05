# Docker Setup for Thirdweb Engine Server

This document describes how to build and run the Thirdweb Engine server using Docker.

## Building the Image

### Local Build
```bash
# From the engine-core directory
docker build -f server/Dockerfile -t thirdweb-engine .
```

### Multi-platform Build
```bash
# Build for multiple platforms using buildx
docker buildx build -f server/Dockerfile --platform linux/amd64,linux/arm64 -t thirdweb-engine .
```

## Running the Container

### Required Environment Variables

The following environment variables must be set when running the container:

```bash
# Redis Configuration
APP__REDIS__URL=redis://localhost:6379

# Thirdweb Configuration
APP__THIRDWEB__SECRET=your_secret_key_here
APP__THIRDWEB__CLIENT_ID=your_client_id_here
APP__THIRDWEB__URLS__RPC=https://your-rpc-url.com
APP__THIRDWEB__URLS__BUNDLER=https://your-bundler-url.com
APP__THIRDWEB__URLS__VAULT=https://your-vault-url.com
APP__THIRDWEB__URLS__PAYMASTER=https://your-paymaster-url.com
APP__THIRDWEB__URLS__ABI_SERVICE=https://your-abi-service-url.com

# Queue Configuration (optional)
APP__QUEUE__EXECUTION_NAMESPACE=your_namespace
APP__QUEUE__WEBHOOK_WORKERS=10
APP__QUEUE__EXTERNAL_BUNDLER_SEND_WORKERS=5
APP__QUEUE__USEROP_CONFIRM_WORKERS=5

# Server Configuration (optional)
APP__SERVER__HOST=0.0.0.0
APP__SERVER__PORT=8080
```

### Basic Run Command

```bash
docker run -p 8080:8080 \
  -e APP__REDIS__URL=redis://host.docker.internal:6379 \
  -e APP__THIRDWEB__SECRET=your_secret_key \
  -e APP__THIRDWEB__CLIENT_ID=your_client_id \
  -e APP__THIRDWEB__URLS__RPC=https://your-rpc-url.com \
  -e APP__THIRDWEB__URLS__BUNDLER=https://your-bundler-url.com \
  -e APP__THIRDWEB__URLS__VAULT=https://your-vault-url.com \
  -e APP__THIRDWEB__URLS__PAYMASTER=https://your-paymaster-url.com \
  -e APP__THIRDWEB__URLS__ABI_SERVICE=https://your-abi-service-url.com \
  thirdweb-engine
```

### Using Environment File

Create a `.env` file with your configuration:

```bash
# .env file
APP__REDIS__URL=redis://localhost:6379
APP__THIRDWEB__SECRET=your_secret_key_here
APP__THIRDWEB__CLIENT_ID=your_client_id_here
APP__THIRDWEB__URLS__RPC=https://your-rpc-url.com
APP__THIRDWEB__URLS__BUNDLER=https://your-bundler-url.com
APP__THIRDWEB__URLS__VAULT=https://your-vault-url.com
APP__THIRDWEB__URLS__PAYMASTER=https://your-paymaster-url.com
APP__THIRDWEB__URLS__ABI_SERVICE=https://your-abi-service-url.com
APP__QUEUE__EXECUTION_NAMESPACE=production
```

Then run:
```bash
docker run -p 8080:8080 --env-file .env thirdweb-engine
```

## GitHub Container Registry

The Docker image is automatically built and published to GitHub Container Registry when tags are pushed.

### Pulling from GHCR

```bash
# Pull latest version
docker pull ghcr.io/[your-org]/[your-repo]/server:latest

# Pull specific version
docker pull ghcr.io/[your-org]/[your-repo]/server:v1.0.0
```

### Authentication for Private Registry

```bash
# Login to GitHub Container Registry
echo $GITHUB_TOKEN | docker login ghcr.io -u USERNAME --password-stdin
```

## Docker Compose Example

Create a `docker-compose.yml` file for local development:

```yaml
version: '3.8'

services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5

  thirdweb-engine:
    image: thirdweb-engine:latest
    ports:
      - "8080:8080"
    environment:
      - APP__REDIS__URL=redis://redis:6379
      - APP__THIRDWEB__SECRET=${APP__THIRDWEB__SECRET}
      - APP__THIRDWEB__CLIENT_ID=${APP__THIRDWEB__CLIENT_ID}
      - APP__THIRDWEB__URLS__RPC=${APP__THIRDWEB__URLS__RPC}
      - APP__THIRDWEB__URLS__BUNDLER=${APP__THIRDWEB__URLS__BUNDLER}
      - APP__THIRDWEB__URLS__VAULT=${APP__THIRDWEB__URLS__VAULT}
      - APP__THIRDWEB__URLS__PAYMASTER=${APP__THIRDWEB__URLS__PAYMASTER}
      - APP__THIRDWEB__URLS__ABI_SERVICE=${APP__THIRDWEB__URLS__ABI_SERVICE}
    depends_on:
      redis:
        condition: service_healthy
```

Run with Docker Compose:
```bash
docker-compose up -d
```

## Configuration

The container uses the production configuration (`server_production.yaml`) by default. The configuration supports environment variable substitution for sensitive values.

### Custom Configuration

To use a custom configuration, you can mount your own configuration files:

```bash
docker run -p 8080:8080 \
  -v /path/to/your/config:/app/configuration \
  -e APP_ENVIRONMENT=production \
  thirdweb-engine
```

## Build Optimization

The Dockerfile uses `cargo-chef` for dependency caching, which significantly speeds up subsequent builds by caching the dependency compilation step. This is especially beneficial in CI/CD environments.

## Security Notes

- The container runs as a non-root user (`appuser`) for security
- Only the necessary runtime dependencies are included in the final image
- Sensitive configuration is provided via environment variables, not baked into the image
- The image is based on `debian:bookworm-slim` for a minimal attack surface

## Troubleshooting

### Common Issues

1. **Connection to Redis fails**: Ensure Redis is running and accessible from the container
2. **Invalid configuration**: Check that all required environment variables are set
3. **Port conflicts**: Ensure port 8080 is not already in use on the host

### Viewing Logs

```bash
# View container logs
docker logs <container_id>

# Follow logs in real-time
docker logs -f <container_id>
```

### Debug Mode

To run with debug logging:
```bash
docker run -p 8080:8080 \
  -e RUST_LOG=debug \
  --env-file .env \
  thirdweb-engine
```
