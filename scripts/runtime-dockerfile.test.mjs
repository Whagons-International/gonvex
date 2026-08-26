import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import test from "node:test";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const dockerfile = readFileSync(path.join(root, "Dockerfile.runtime"), "utf8");
const packageJSON = JSON.parse(readFileSync(path.join(root, "package.json"), "utf8"));
const localCompose = readFileSync(path.join(root, "docker-compose.yml"), "utf8");
const productionCompose = readFileSync(path.join(root, "docker-compose.production.yml"), "utf8");
const persistentRoot = "/var/lib/gonvex";
test("runtime image contains no application compiler or Go plugin state", () => {
  const stages = dockerfile.split(/(?=^FROM )/m).filter((stage) => stage.startsWith("FROM "));
  const build = stages.find((stage) => / AS rust-build\s*$/m.test(stage));
  const runtime = stages.at(-1);
  assert.ok(build, "Rust build stage must exist");
  assert.ok(runtime, "runtime stage must exist");

  assert.match(build, /^FROM rust:1\.94-bookworm AS rust-build$/m);
  assert.match(build, /cargo build --locked --release/);
  assert.match(build, /-p gonvex-runtime/);
  assert.match(runtime, /^FROM debian:bookworm-slim/m);
  assert.match(runtime, /GONVEX_DATA_DIR=\/var\/lib\/gonvex\/data/);
  assert.doesNotMatch(runtime, /GOCACHE|GOMODCACHE|GOPATH|GONVEX_PLUGIN_CACHE_DIR|GONVEX_MODULE_ROOT/);
  assert.doesNotMatch(dockerfile, /^FROM golang:/m);
  assert.doesNotMatch(dockerfile, /buildmode=plugin|plugin\.Open|project bundle|synced project bundles/i);
  assert.match(runtime, /WORKDIR \/var\/lib\/gonvex\nUSER 0:0/);
  assert.doesNotMatch(runtime, /useradd|groupadd|chown/);
});

test("runtime has no Go sandbox runner or compiler dependency", () => {
  assert.equal(
    existsSync(path.join(root, "server/internal/sandbox/go_runner.go")),
    false,
    "the removed Go sandbox runner must not return",
  );
  const runtime = dockerfile.split(/(?=^FROM )/m).filter((stage) => stage.startsWith("FROM ")).at(-1);
  assert.ok(runtime);
  assert.doesNotMatch(runtime, /golang|\bgo\s+run\b|GOCACHE|GOMODCACHE|GOPATH/i);
});

test("runtime image always contains and supervises the Rust TypeScript module host", () => {
  const stages = dockerfile.split(/(?=^FROM )/m).filter((stage) => stage.startsWith("FROM "));
  const rustBuild = stages.find((stage) => / AS rust-build\s*$/m.test(stage));
  const runtime = stages.at(-1);
  assert.ok(rustBuild, "Rust module-host build stage must exist");
  assert.ok(runtime, "runtime stage must exist");

  assert.match(rustBuild, /COPY rust\/Cargo\.toml rust\/Cargo\.lock \.\//);
  assert.match(rustBuild, /COPY rust\/crates \.\/crates/);
  assert.match(rustBuild, /-p gonvex-module-host/);
  assert.match(
    runtime,
    /COPY --from=rust-build \/src\/rust\/target\/release\/gonvex-module-host \/usr\/local\/bin\/gonvex-module-host/,
  );
  assert.match(runtime, /GONVEX_MODULE_HOST_ENABLED=true/);
  assert.match(runtime, /GONVEX_MODULE_HOST_BINARY=\/usr\/local\/bin\/gonvex-module-host/);
  assert.match(runtime, /COPY --from=rust-build \/src\/rust\/target\/release\/gonvex-sandbox-worker \/usr\/local\/bin\/gonvex-sandbox-worker/);
  assert.match(runtime, /GONVEX_SANDBOX_WORKER_BINARY=\/usr\/local\/bin\/gonvex-sandbox-worker/);
});

test("local runtime development starts the Rust server and selects its workers", () => {
  const command = packageJSON.scripts?.["dev:runtime"] ?? "";
  assert.match(command, /cargo build --locked --manifest-path rust\/Cargo\.toml -p gonvex-module-host -p gonvex-sandbox-worker/);
  assert.match(command, /GONVEX_MODULE_HOST_BINARY=.*rust\/target\/debug\/gonvex-module-host/);
  assert.match(command, /GONVEX_SANDBOX_WORKER_BINARY=.*rust\/target\/debug\/gonvex-sandbox-worker/);
  assert.match(command, /cargo run --locked --manifest-path rust\/Cargo\.toml -p gonvex-runtime/);
  assert.doesNotMatch(command, /\bair\b|\bgo run\b/);
});

test("compose deployments use the canonical Control Plane configuration", () => {
  for (const [name, compose] of [["local", localCompose], ["production", productionCompose]]) {
    assert.match(compose, /GONVEX_CONTROL_PLANE_DATABASE_URL:/, `${name} compose must configure the Control Plane`);
    assert.doesNotMatch(compose, /GONVEX_LANDLORD_DATABASE_URL:/, `${name} compose must not configure the legacy landlord alias`);
    assert.doesNotMatch(compose, /GOCACHE|GOMODCACHE|GONVEX_PLUGIN_CACHE_DIR/, `${name} compose must not configure removed Go application caches`);
    assert.match(compose, /GONVEX_SANDBOX_ENABLED: \$\{GONVEX_SANDBOX_ENABLED:-false\}/, `${name} sandbox must be opt-in`);
    assert.match(compose, /cap_add:\s*\n\s*- CHOWN\s*\n\s*- SYS_CHROOT\s*\n\s*- SETUID\s*\n\s*- SETGID/, `${name} must let the runtime prepare the workspace and let the worker chroot and drop privileges`);
  }
});
