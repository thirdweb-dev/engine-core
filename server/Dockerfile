# Use cargo chef for dependency caching
FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR /app

# Install ssh and git for GitHub Actions environment
RUN apt-get update && apt-get install -y ssh git && rm -rf /var/lib/apt/lists/*

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json

# Setup SSH for GitHub in clean CI environment
RUN mkdir -p ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts

# Force cargo to use git CLI for SSH
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true

# Build dependencies - this is the caching Docker layer
RUN --mount=type=ssh cargo chef cook --release --recipe-path recipe.json

# Build application
COPY . .
RUN --mount=type=ssh cargo build --release --bin thirdweb-engine

# Runtime stage
FROM debian:bookworm-slim AS runtime

# Install required system dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the binary
COPY --from=builder /app/target/release/thirdweb-engine /app/thirdweb-engine

# Copy configuration files
COPY --from=builder /app/server/configuration /app/configuration

# Create non-root user for security
RUN useradd -r -s /bin/false appuser
RUN chown -R appuser:appuser /app
USER appuser

# Set environment to production
ENV APP_ENVIRONMENT=production

# Expose port (assuming default port 8080, adjust if needed)
EXPOSE 8080

# Set entrypoint
ENTRYPOINT ["./thirdweb-engine"]