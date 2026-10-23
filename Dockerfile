# Build: RocksDB is compiled from source (C++ toolchain + libclang for bindgen).
FROM rust:1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends clang libclang-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY clients/rust ./clients/rust
RUN mkdir -p src/bin && echo 'fn main() {}' > src/main.rs && echo '' > src/lib.rs \
    && for b in fsgen fsq fakechain; do echo 'fn main() {}' > src/bin/$b.rs; done \
    && cargo build --release --locked -p faster-sync && rm -rf src
COPY src ./src
RUN touch src/main.rs src/lib.rs && cargo build --release --locked -p faster-sync --bin faster-sync --bin fakechain

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home /data fsync && mkdir -p /data && chown fsync /data
COPY --from=build /src/target/release/faster-sync /src/target/release/fakechain /usr/local/bin/
USER fsync
ENV DATA_DIR=/data LISTEN=0.0.0.0:3100 ADMIN_LISTEN=0.0.0.0:9101
VOLUME /data
EXPOSE 3100 9101
HEALTHCHECK --interval=15s --timeout=5s --retries=3 CMD curl -fs http://localhost:3100/health || exit 1
CMD ["faster-sync"]
