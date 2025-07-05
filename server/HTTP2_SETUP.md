# HTTP/2 Support Setup

The thirdweb-engine server now supports HTTP/2 with TLS encryption. This provides improved performance, multiplexing, and server push capabilities.

## Features

- **HTTP/2 Protocol Support**: Full HTTP/2 implementation with multiplexing and header compression
- **TLS 1.3 Support**: Secure connections with modern TLS encryption
- **Backward Compatibility**: Falls back to HTTP/1.1 when TLS is not configured
- **Graceful Shutdown**: Proper connection handling during server shutdown

## Configuration

### Enabling HTTP/2 with TLS

To enable HTTP/2 support, add TLS configuration to your server configuration file:

```yaml
server:
  host: 0.0.0.0
  port: 3069
  tls:
    cert_path: "/path/to/your/certificate.pem"
    key_path: "/path/to/your/private_key.pem"
```

### Environment Variables

You can also configure TLS using environment variables:

```bash
export APP__SERVER__TLS__CERT_PATH="/path/to/your/certificate.pem"
export APP__SERVER__TLS__KEY_PATH="/path/to/your/private_key.pem"
```

### Generating Self-Signed Certificates (Development Only)

For development purposes, you can generate self-signed certificates:

```bash
# Generate private key
openssl genrsa -out private_key.pem 2048

# Generate certificate
openssl req -new -x509 -key private_key.pem -out certificate.pem -days 365 -subj "/CN=localhost"
```

**Note**: Self-signed certificates should only be used for development. Use proper certificates from a Certificate Authority for production.

## Usage

### With TLS/HTTP/2 Enabled

When TLS is configured, the server will:
1. Accept incoming connections on the specified port
2. Perform TLS handshake with HTTP/2 ALPN negotiation
3. Serve requests over HTTP/2 protocol
4. Log "HTTPS/2 server starting on [address]"

### Without TLS (HTTP/1.1 fallback)

When TLS is not configured, the server will:
1. Run in HTTP/1.1 mode (existing behavior)
2. Log "HTTP server starting on [address] (HTTP/1.1 only)"

## Benefits of HTTP/2

- **Multiplexing**: Multiple requests can be sent simultaneously over a single connection
- **Header Compression**: Reduces bandwidth usage with HPACK compression
- **Server Push**: Server can proactively send resources to clients
- **Binary Protocol**: More efficient than text-based HTTP/1.1
- **Stream Prioritization**: Important requests can be prioritized

## Testing HTTP/2

You can test HTTP/2 support using curl:

```bash
# Test HTTP/2 with self-signed certificate (development)
curl -k --http2 -v https://localhost:3069/v1/api.json

# Test HTTP/2 with proper certificate
curl --http2 -v https://yourdomain.com:3069/v1/api.json
```

## Dependencies

The following dependencies have been added to support HTTP/2:

- `hyper` - HTTP/2 server implementation
- `hyper-util` - Utilities for hyper
- `tower` - Service abstraction layer
- `tokio-rustls` - Async TLS implementation
- `rustls` - Modern TLS library
- `rustls-pemfile` - PEM file parsing

## Performance Considerations

- HTTP/2 performs best with TLS enabled
- Multiple concurrent requests benefit from HTTP/2 multiplexing
- Header compression reduces bandwidth usage for repeated requests
- Binary protocol reduces parsing overhead

## Troubleshooting

### Common Issues

1. **Certificate Issues**: Ensure your certificate and private key are valid and readable
2. **Port Conflicts**: Make sure the configured port is available
3. **File Permissions**: Ensure the server process can read the certificate files
4. **Client Support**: Verify that your client supports HTTP/2

### Logs

Monitor server logs for HTTP/2 related messages:
- "HTTPS/2 server starting on [address]" - HTTP/2 enabled
- "HTTP server starting on [address] (HTTP/1.1 only)" - HTTP/1.1 fallback
- "Error handling connection from [address]" - Connection errors

## Security Notes

- Always use proper certificates from a trusted Certificate Authority in production
- Keep your private keys secure and never commit them to version control
- Consider using certificate rotation for long-running services
- Monitor TLS certificate expiration dates