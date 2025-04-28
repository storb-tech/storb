FROM ubuntu:24.04 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates clang curl libclang-dev libssl-dev libudev-dev librocksdb-dev  pkg-config && \
    rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && \
    . $HOME/.cargo/env && \
    rustup install nightly && \
    rustup default nightly

WORKDIR /app

COPY . .

RUN . $HOME/.cargo/env && \
    ROCKSDB_LIB_DIR=/usr/lib/x86_64-linux-gnu cargo build --release

FROM ubuntu:24.04 AS runtime

# Install necessary runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates librocksdb-dev libssl-dev libudev-dev pkg-config

COPY --from=builder /app/target/release/storb /usr/local/bin/storb

ENV RUST_LOG="info,libp2p=info,opentelemetry-http=info,opentelemetry-otlp=info,hyper_util=info"

ENV NODE_TYPE=""

VOLUME ["/app", "/root/.bittensor/wallets"]

WORKDIR /app

CMD ["sh", "-c", "/usr/local/bin/storb ${NODE_TYPE}"]
