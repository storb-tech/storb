# Build Stage: Using Rust nightly with Bookworm-slim
FROM rust:latest AS builder

# Install required build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    clang libclang-dev pkg-config libudev-dev libssl-dev librocksdb-dev ca-certificates  && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .

RUN cargo build --release 

# Runtime Stage: Minimal Debian image
FROM debian:bookworm-slim AS runtime

# Install necessary runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libudev-dev libssl-dev librocksdb-dev ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user and set up home directory
RUN useradd -m -u 1000 appuser
WORKDIR /home/appuser

# Copy the built binary from the builder stage
COPY --from=builder /app/target/release/storb ./storb
RUN chown -R appuser:appuser /home/appuser

USER appuser

# Set the command to run the application
CMD ["/home/appuser/storb"]
