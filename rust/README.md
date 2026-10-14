# Gonvex Rust runtime

The Rust workspace contains the Gonvex server and its isolated TypeScript
execution processes.

```text
TypeScript module
  -> deterministic JavaScript ESM bundle
  -> gonvex-module-host
  -> bounded V8 isolate
  -> capability-checked host calls
  -> Gonvex runtime transaction and services
```

Application modules expose exactly three executable kinds: Query, Reducer, and
Action. Query database access is read-only. A Reducer receives database calls
bound to the one transaction held by the runtime and may enqueue durable Action
work. An Action may use external capabilities and must call a Reducer to change
application tables.

The V8 runtime provides no Node.js process, filesystem, environment, raw socket,
or database credentials. Each invocation has explicit capabilities, a deadline,
bounded output, and an isolate heap limit. Module generations are prewarmed and
atomically activated; calls already running on an older generation may finish
before its isolates are destroyed.

## Crates

- `admin-cli`: explicit, resumable database migration commands.
- `module-runtime`: language-neutral invocation ABI and capability model.
- `module-runtime-v8`: JavaScript/V8 implementation.
- `module-host`: process protocol, artifact verification, generation lifecycle,
  and host-call forwarding.
- `protocol`: server representation of the published WebSocket contract.
- `postgres`: Control Plane, tenant routing, transactions, change feed, and
  provisioning.
- `runtime`: HTTP/WebSocket server, authentication, visibility, Live Queries,
  Replica delivery, scheduling, storage, telemetry, and worker supervision.
- `server`: generation-registry primitives shared by the Rust components.
- `sandbox-worker`: disposable TypeScript code-execution process with a
  workspace-only file API and an optional, locked-down DuckDB binding.

The runtime owns every database transaction. The V8 process receives scoped
host operations over a local socket and never receives a database URL or
credential.

## Development

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo build -p gonvex-admin -p gonvex-runtime -p gonvex-module-host -p gonvex-sandbox-worker
```

The production runtime image installs all four binaries. The sandbox remains off
until the operator enables `GONVEX_SANDBOX_ENABLED`; the module host remains
selected through `GONVEX_MODULE_HOST_BINARY`.

Run the identity upgrade with the shipped admin binary:

```bash
gonvex-admin migrate identity-v2 --plan --source PROJECT --run-id RUN --input identities.json
gonvex-admin migrate identity-v2 --apply --plan-file identity-v2-plan.json
gonvex-admin migrate identity-v2 --verify --plan-file identity-v2-plan.json
```

[Public tenant function admission and abuse limits](../docs/public-functions.md).

## WebSocket compression

The runtime's `/ws` endpoint negotiates RFC 7692 permessage-deflate when the client offers it. `GONVEX_WS_COMPRESSION` defaults to `true`; set it to `false` to disable negotiation and retain the axum transport. Clients that do not offer compression always retain that transport.

Both directions negotiate no context takeover. The server uses at most a 12-bit compressor window and level 1. `GONVEX_WS_COMPRESSION_THRESHOLD` defaults to 1024 bytes and must be a positive integer. Smaller messages and messages whose compressed representation would grow are sent uncompressed. Existing frame/message limits and public payload limits still apply, including the expanded payload limit inside the inflater.

JSON messages, auth capabilities, replay, visibility checks, and SDK APIs do not change. Browsers decompress natively before JavaScript receives text. No Whagons app change or client-contract version bump is needed. See [the measurements and reproduction steps](../bench/ws-deflate/README.md).
