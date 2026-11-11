# 0008: Replica epochs hash the tenant's `database_url`

Date: 2026-11-10. Status: decided (lead). Affects: P03, K10, M03.

The replica `epoch` sent to clients is a hash of `{protocolVersion, project, database, functions, schema,
moduleHash}` where `database` is the tenant's `database_url` string, `sqlite://{absolute path}`
(`rust/crates/runtime/src/modules.rs:520-535`). If the kernel computes the epoch from anything else, every
client's replica resets at cutover.

Decision: tenant import (P03) stores the source `database_url` string in the tenant object's runtime
metadata, and K10 uses that stored string, not a Cloudflare identifier, when computing the epoch. A new
tenant created on Cloudflare gets a stable synthetic `database_url` assigned once at creation.
