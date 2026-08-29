import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const cli = fileURLToPath(new URL("../dist/index.js", import.meta.url));

test("codegen preserves a TypeScript Replica Collection contract", () => {
  const project = mkdtempSync(join(tmpdir(), "gonvex-cli-replica-collection-"));
  try {
    mkdirSync(join(project, "gonvex"));
    writeFileSync(join(project, "gonvex.json"), JSON.stringify({ project: "replica-test", module: { entrypoint: "gonvex/index.ts" } }));
    writeFileSync(join(project, "gonvex", "index.ts"), `
const schema = {
  object: (fields) => ({ kind: "object", fields }),
  array: (items) => ({ kind: "array", items }),
  id: (entity) => ({ kind: "id", entity }),
  string: () => ({ kind: "string" }),
};
const replicaCollection = (options) => options;
export const tasks = replicaCollection({
  args: schema.object({}),
  result: schema.array(schema.object({ id: schema.id("tasks"), title: schema.string() })),
  replica: {
    table: "tasks",
    key: "id",
    columns: ["id", "title"],
    mode: "progressive",
    maxRows: 100,
    maxBytes: 4194304,
  },
  run: async () => [],
});
`);
    const environment = Object.fromEntries(
      Object.entries(process.env).filter(([, value]) => !value?.trimStart().startsWith("()")),
    );
    const generated = spawnSync(process.execPath, [cli, "codegen", "--project", project], { env: environment, encoding: "utf8" });
    assert.equal(generated.status, 0, generated.stderr);
    const manifest = JSON.parse(readFileSync(join(project, "gonvex", "_generated", "manifest.json"), "utf8"));
    assert.deepEqual(manifest.functions.tasks.replica, {
      table: "tasks",
      key: "id",
      columns: ["id", "title"],
      mode: "progressive",
      maxRows: 100,
      maxBytes: 4194304,
    });
    const apiSource = readFileSync(join(project, "gonvex", "_generated", "api.ts"), "utf8");
    assert.match(apiSource, /replica:\s*\{[\s\S]*?key:\s*"id"[\s\S]*?table:\s*"tasks"/);
  } finally {
    rmSync(project, { recursive: true, force: true });
  }
});
