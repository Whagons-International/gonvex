# @gonvex/protocol

Shared TypeScript protocol types for Gonvex browser clients, React bindings, and
tooling.

Most applications do not need to import this package directly. It is primarily
used by `@gonvex/client`, `@gonvex/react`, and generated bindings.

## Install

```bash
npm install @gonvex/protocol
```

## Types

```ts
import type {
  ClientMessage,
  FunctionKind,
  GonvexManifest,
  JsonValue,
  ServerMessage,
} from "@gonvex/protocol";
```

The package includes:

- JSON-safe value types
- function manifest types
- WebSocket client and server message unions
- live-query result, progress, patch, and revision frames
- Replica Collection open/batch-open, snapshot, delta, reset, ready, and cursor types
- browser telemetry and message trace types

## Related Packages

- `@gonvex/client` - uses these messages over WebSocket
- `@gonvex/react` - React hooks for generated functions
- `@gonvex/cli` - generates bindings and syncs manifests

## Documentation

Full docs live at https://desarso.github.io/gonvex/

[Public tenant authentication adds `public: true` to an `auth` frame](../../docs/public-functions.md).
