FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && echo '' > src/lib.rs && cargo build --release --locked && rm -rf src
COPY src ./src
RUN touch src/main.rs src/lib.rs && cargo build --release --locked --bin faster-rpc

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 frpc
COPY --from=build /src/target/release/faster-rpc /usr/local/bin/
USER frpc
ENV LISTEN=0.0.0.0:8545 ADMIN_LISTEN=0.0.0.0:9102
EXPOSE 8545 9102
HEALTHCHECK --interval=15s --timeout=5s --retries=3 CMD curl -fs http://localhost:8545/health || exit 1
CMD ["faster-rpc"]
