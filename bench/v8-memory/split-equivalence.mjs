#!/usr/bin/env node
// For a split artifact ({module} JSON from build-artifact.mjs), checks that
// every function listed in reducerBundle resolves, by exact path, to a
// definition equivalent to the one the complete bundle resolves through its
// entrypoint namespace (same kind, same handler source text), and that
// unlisted Queries/Reducers are reported.
import { readFileSync, writeFileSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

const artifact = JSON.parse(readFileSync(process.argv[2], "utf8")).module;
const dir = mkdtempSync(join(process.env.TMPDIR ?? tmpdir(), "split-eq-"));
const load = async (name, bundle) => {
  const file = join(dir, name);
  writeFileSync(file, Buffer.from(bundle.code, "base64"));
  return import(pathToFileURL(file).href);
};
const complete = await load("complete.mjs", artifact.javascript);
const reducers = await load("reducers.mjs", artifact.reducerBundle);
const walk = (root, path) => path.split(".").reduce((node, key) => node?.[key], root);
const handlerOf = (binding) => binding?.handler ?? binding?.run ?? binding?.options?.run ?? binding;
// The two bundles deconflict top-level names independently (`text$12` vs
// `text$5`), so suffixes are normalized before comparing source text.
const normalized = (fn) => String(fn).replace(/([A-Za-z_][\w]*)\$\d+\b/g, "$1");
let same = 0;
const problems = [];
// The module host tries the `export`/`handler` name on the entry namespace
// before the path. Where that name is an unrelated top-level export, the
// complete bundle resolves a different binding than the path names.
const collisions = [];
for (const path of artifact.reducerBundle.functions) {
  const entry = artifact.functions[path];
  const byName = [entry.export, entry.handler].map((name) => name && complete[name]).find((value) => value !== undefined);
  const left = complete[path] ?? walk(complete, path);
  if (byName !== undefined && byName !== left) collisions.push(`${path} (${entry.kind}${entry.delivery ? "/" + entry.delivery : ""}) -> top-level ${entry.export}`);
  const right = reducers[path];
  if (!right) { problems.push(`${path}: missing from reducer bundle`); continue; }
  if (!left) { problems.push(`${path}: complete bundle does not resolve it`); continue; }
  const l = handlerOf(left), r = handlerOf(right);
  if (left.kind !== right.kind || normalized(l) !== normalized(r)) problems.push(`${path}: definitions differ`);
  else same += 1;
}
const listed = new Set(artifact.reducerBundle.functions);
const unlisted = Object.entries(artifact.functions)
  .filter(([path, f]) => f.kind !== "action" && !listed.has(path)).map(([path]) => path);
console.log(JSON.stringify({ listed: listed.size, equivalent: same, problems: problems.slice(0, 20), problemCount: problems.length,
  hostOrderCollisions: collisions.length, collisionSample: collisions.slice(0, 5),
  unlistedQueriesReducers: unlisted.length, unlistedSample: unlisted.slice(0, 5) }, null, 1));
process.exit(problems.length ? 1 : 0);
