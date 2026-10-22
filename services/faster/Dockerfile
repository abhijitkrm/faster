# Build: RocksDB is compiled from source, which needs a C++ toolchain and
# libclang (bindgen).
FROM rust:1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends clang libclang-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release --locked && rm -rf src
COPY src ./src
RUN touch src/main.rs && cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home /data faster && mkdir -p /data && chown faster /data
COPY --from=build /src/target/release/faster /usr/local/bin/faster
USER faster
ENV DATA_DIR=/data PORT=3000
VOLUME /data
EXPOSE 3000
HEALTHCHECK --interval=15s --timeout=5s --retries=3 CMD curl -fs http://localhost:3000/health || exit 1
# Exec form so SIGTERM reaches the process for a clean shutdown.
CMD ["faster"]
