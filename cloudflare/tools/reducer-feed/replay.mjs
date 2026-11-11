#!/usr/bin/env node
// Replays a tenant's Reducer log feed from one Gonvex Rust runtime into another
// (P04). The source runs with GONVEX_REDUCER_FEED=1, the target is a shadow
// started with GONVEX_REDUCER_FEED_REPLAY=1 from the same base state and the
// same module artifact. Afterwards the two tenants' per-table checksums are
// compared. No dependencies; Node 18+.
//
//   GONVEX_SOURCE_ADMIN_KEY=… GONVEX_TARGET_ADMIN_KEY=… node replay.mjs \
//     --source http://127.0.0.1:8080 --target http://127.0.0.1:8081 \
//     --project whagons --tenant acme [--after 0] [--batch 200] [--no-checksums]
//
// Admin keys come from the environment only, never from arguments, so they
// do not show up in process listings. Exit codes: 0 replayed and identical,
// 1 a replayed call diverged or a request failed, 2 checksums differ,
// 3 usage.

import { parseArgs } from "node:util";

const usage = (message) => {
  if (message) console.error(message);
  console.error("usage: node replay.mjs --source URL --target URL --project ID [--tenant ID] [--after SEQ] [--batch N] [--no-checksums]");
  process.exit(3);
};

let options;
try {
  ({ values: options } = parseArgs({
    options: {
      source: { type: "string" },
      target: { type: "string" },
      project: { type: "string" },
      tenant: { type: "string", default: "" },
      after: { type: "string", default: "0" },
      batch: { type: "string", default: "200" },
      "no-checksums": { type: "boolean", default: false },
    },
  }));
} catch (error) {
  usage(error.message);
}
if (!options.source || !options.target || !options.project) usage("--source, --target and --project are required");
const sourceKey = process.env.GONVEX_SOURCE_ADMIN_KEY;
const targetKey = process.env.GONVEX_TARGET_ADMIN_KEY ?? sourceKey;
if (!sourceKey || !targetKey) usage("set GONVEX_SOURCE_ADMIN_KEY (and GONVEX_TARGET_ADMIN_KEY if it differs)");
const batch = Math.min(Math.max(Number.parseInt(options.batch, 10) || 200, 1), 5000);
const tenantQuery = new URLSearchParams({ project: options.project, tenant: options.tenant });

async function request(base, key, path, init = {}) {
  const response = await fetch(new URL(path, base), {
    ...init,
    headers: { authorization: `Bearer ${key}`, "content-type": "application/json", ...init.headers },
  });
  const text = await response.text();
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    body = { error: text };
  }
  return { status: response.status, body };
}

function fail(message, detail) {
  console.error(message);
  if (detail !== undefined) console.error(JSON.stringify(detail, null, 2));
  process.exit(1);
}

let after = Number.parseInt(options.after, 10) || 0;
const totals = { applied: 0, pages: 0, gaps: [], revisionMismatches: [], lastRevision: null };
const started = performance.now();
for (;;) {
  const query = new URLSearchParams(tenantQuery);
  query.set("after", String(after));
  query.set("limit", String(batch));
  const page = await request(options.source, sourceKey, `/dev/reducer-feed?${query}`);
  if (page.status !== 200) fail(`source feed: HTTP ${page.status}`, page.body);
  totals.gaps.push(...page.body.gaps);
  const entries = page.body.entries;
  if (entries.length === 0) break;
  totals.pages += 1;
  const replay = await request(options.target, targetKey, "/dev/reducer-feed/replay", {
    method: "POST",
    body: JSON.stringify({ project: options.project, tenant: options.tenant, entries }),
  });
  if (replay.body?.applied !== undefined) {
    totals.applied += replay.body.applied;
    totals.revisionMismatches.push(...replay.body.revisionMismatches);
    totals.lastRevision = replay.body.lastRevision ?? totals.lastRevision;
  }
  if (replay.status !== 200) fail(`target replay stopped: HTTP ${replay.status}`, replay.body);
  after = page.body.next;
  if (!page.body.more) break;
}

const report = {
  applied: totals.applied,
  pages: totals.pages,
  cursor: after,
  lastRevision: totals.lastRevision,
  gaps: totals.gaps,
  revisionMismatches: totals.revisionMismatches,
  seconds: Number(((performance.now() - started) / 1000).toFixed(3)),
};

if (!options["no-checksums"]) {
  const [source, target] = await Promise.all([
    request(options.source, sourceKey, `/dev/reducer-feed/checksums?${tenantQuery}`),
    request(options.target, targetKey, `/dev/reducer-feed/checksums?${tenantQuery}`),
  ]);
  if (source.status !== 200) fail(`source checksums: HTTP ${source.status}`, source.body);
  if (target.status !== 200) fail(`target checksums: HTTP ${target.status}`, target.body);
  const names = [...new Set([...Object.keys(source.body.tables), ...Object.keys(target.body.tables)])].sort();
  const differing = names.filter(
    (name) => JSON.stringify(source.body.tables[name]) !== JSON.stringify(target.body.tables[name]),
  );
  report.tables = names.length;
  report.identical = differing.length === 0;
  report.differing = differing.map((name) => ({ table: name, source: source.body.tables[name] ?? null, target: target.body.tables[name] ?? null }));
  report.excludedTables = source.body.excludedTables;
  report.excludedColumns = source.body.excludedColumns;
}

console.log(JSON.stringify(report, null, 2));
process.exit(report.identical === false ? 2 : 0);
