# syntax=docker/dockerfile:1
# K20: the sandbox Container. Rust's `gonvex-sandbox-worker` built unchanged with
# the `duckdb` feature (as Dockerfile.runtime builds it with GONVEX_DUCKDB=true),
# Rust's `data_ingest.rs` as `k20-ingest` (ingest/), and the manager (manager.mjs).
# Build from the repository root:
#
#   docker build -f cloudflare/sandbox/container/Dockerfile -t gonvex-cf-dev-k20-sandbox .
FROM rust:1.94-bookworm AS rust-build
WORKDIR /src/rust
COPY rust/Cargo.toml rust/Cargo.lock ./
COPY rust/.cargo ./.cargo
COPY rust/crates ./crates
COPY rust/vendor ./vendor
COPY packages/module-sdk/runtime-contract /src/packages/module-sdk/runtime-contract
COPY cloudflare/sandbox/container/ingest /src/cloudflare/sandbox/container/ingest
RUN --mount=type=cache,id=gonvex-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=gonvex-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=gonvex-k20-sandbox-rust-1.94,target=/src/rust/target,sharing=locked \
    cargo build --locked --release -p gonvex-sandbox-worker --features gonvex-sandbox-worker/duckdb \
    && (cd /src/cloudflare/sandbox/container/ingest && CARGO_TARGET_DIR=/src/rust/target cargo build --locked --release) \
    && mkdir -p /out \
    && cp target/release/gonvex-sandbox-worker target/release/k20-ingest /out/

FROM node:22.18-bookworm-slim
COPY --from=rust-build /out/gonvex-sandbox-worker /usr/local/bin/gonvex-sandbox-worker
COPY --from=rust-build /out/k20-ingest /usr/local/bin/k20-ingest
COPY cloudflare/sandbox/container/manager.mjs /app/manager.mjs
# Root, as Dockerfile.runtime: the worker chroots into each workspace and drops to 65534.
USER 0:0
ENV PORT=8080 GONVEX_SANDBOX_ROOT=/var/lib/gonvex/sandboxes NODE_ENV=production
EXPOSE 8080
CMD ["node", "/app/manager.mjs"]
