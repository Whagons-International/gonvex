import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import test from "node:test";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const dockerfile = readFileSync(path.join(root, "Dockerfile.runtime"), "utf8");
const persistentRoot = "/var/lib/gonvex";
const moduleRoot = "/opt/gonvex/go";
const moduleCache = `${moduleRoot}/pkg/mod`;
const buildCache = "/var/cache/gonvex/go-build";
const temporaryBuildRoot = "/var/cache/gonvex/tmp";

test("dashboard build and runtime use the same immutable fixed Node image", () => {
  const dashboard = readFileSync(path.join(root, "Dockerfile.dashboard"), "utf8");
  const images = [...dashboard.matchAll(/^FROM (\S+)(?: AS \S+)?$/gm)].map((match) => match[1]);
  assert.equal(images.length, 2, "check both dashboard stages");
  assert.match(images[0], /^node:22\.23\.4-alpine3\.24@sha256:[a-f0-9]{64}$/);
  assert.equal(images[1], images[0], "build and runtime must share Node and Alpine versions");
});

test("runtime compiler state stays visible when persistent and tmpfs roots are mounted", () => {
  const firstFrom = dockerfile.indexOf("FROM ");
  const secondFrom = dockerfile.indexOf("\nFROM ", firstFrom + 1);
  const build = dockerfile.slice(firstFrom, secondFrom);
  const runtime = dockerfile.slice(secondFrom + 1);

  assert.match(build, new RegExp(`GOCACHE=${buildCache}`));
  assert.match(build, new RegExp(`GOMODCACHE=${moduleCache}`));
  assert.match(build, new RegExp(`GOPATH=${moduleRoot}`));
  assert.match(runtime, new RegExp(`GOCACHE=${buildCache}`));
  assert.match(runtime, new RegExp(`GOMODCACHE=${moduleCache}`));
  assert.match(runtime, new RegExp(`GOPATH=${moduleRoot}`));
  assert.match(runtime, new RegExp(`TMPDIR=${temporaryBuildRoot}(?:\\s|$)`));
  assert.match(
    runtime,
    new RegExp(`COPY --from=build[^\\n]* ${moduleCache} ${moduleCache}`),
  );
  for (const compilerPath of [moduleRoot, moduleCache, buildCache, temporaryBuildRoot]) {
    assert.equal(
      compilerPath.startsWith(`${persistentRoot}/`),
      false,
      `${compilerPath} must not be hidden by the production volume`,
    );
  }
  assert.equal(
    temporaryBuildRoot === "/tmp" || temporaryBuildRoot.startsWith("/tmp/"),
    false,
    `${temporaryBuildRoot} must not live on the production noexec tmpfs`,
  );
  assert.match(runtime, /GONVEX_DATA_DIR=\/var\/lib\/gonvex\/data/);
  assert.doesNotMatch(dockerfile, /COPY --from=build[^\n]* \/go\/pkg\/mod/);
  assert.match(runtime, /WORKDIR \/var\/lib\/gonvex\nUSER 0:0/);
  assert.doesNotMatch(runtime, /useradd|groupadd|chown/);
});
