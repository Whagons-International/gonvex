// P03 export from a kernel-provisioned object back to a Rust file (0022 §5).
//
//   node verify/export-roundtrip.mjs <tenant.db> --work <dir> --migrations <dir> --stage-bin <gonvex-tenant-stage>
//        [--url http://localhost:8799] [--token-file <.dev.vars>] [--json out.json]
//
// 1. stage in: the tenant file through Rust provisioning (P03 `stage`), the
//    file Rust would serve;
// 2. the kernel's schema over those rows (the transform's image, as K02
//    provisions it), pushed into an empty object with P03's transport;
// 3. export: P03 `pull` (the object verbatim, added objects included), then
//    `stage` out through Rust provisioning;
// 4. the restored file must have exactly Rust's schema (the staged file's
//    sqlite_schema, no `_gonvex_st_` object, no hidden column), no stage
//    findings, and the same checksum in every copied table as the staged file.
import { execFileSync } from "node:child_process";
import { mkdirSync, rmSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { join, resolve } from "node:path";
import { ADDED_PREFIX } from "../index.ts";
import { buildTransformed } from "./build-transformed.mjs";
import { ObjectClient, compareChecksums, fileChecksums, pullFile, pushFile } from "../../../tools/tenant-transfer/src/transfer.mjs";

const args = process.argv.slice(2);
const option = (name, d = null) => (args.includes(name) ? args[args.indexOf(name) + 1] : d);
const tenant = args[0];
const work = resolve(option("--work"));
const stageBin = option("--stage-bin");
const migrations = resolve(option("--migrations"));
const url = option("--url", "http://localhost:8799");
const tokenFile = option("--token-file", new URL("../../../tools/tenant-transfer/.dev.vars", import.meta.url).pathname);
const token = process.env.TRANSFER_TOKEN ?? (existsSync(tokenFile) ? readFileSync(tokenFile, "utf8").match(/^TRANSFER_TOKEN=(.+)$/m)?.[1].trim() : undefined);
rmSync(work, { recursive: true, force: true });
mkdirSync(work, { recursive: true });
const file = (n) => join(work, n);
const name = tenant.split("/").pop().replace(/\.db$/, "");
const stage = (source, out) => {
  let stdout;
  try {
    stdout = execFileSync(stageBin, ["stage", "--source", source, "--migrations", migrations, "--out", out, "--template", file("template.db")], { maxBuffer: 64 << 20 }).toString();
  } catch (error) {
    if (error.status === 2 && error.stdout) stdout = error.stdout.toString(); // findings
    else throw error;
  }
  return JSON.parse(stdout);
};
const schemaRows = (path) => {
  const db = new DatabaseSync(path, { readOnly: true });
  const rows = db.prepare("SELECT type, name, sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_stat%' ORDER BY type, name").all();
  const hidden = db.prepare("SELECT count(*) n FROM sqlite_schema s, pragma_table_xinfo(s.name) x WHERE s.type = 'table' AND x.hidden IN (2, 3)").get().n;
  db.close();
  return { rows, hidden };
};

const report = { tenant: name };
const t0 = Date.now();
const stageIn = stage(resolve(tenant), file("staged.db"));
report.stageIn = { findings: stageIn.findings, copiedTables: Object.keys(stageIn.copiedTables).length };

execFileSync("cp", ["--reflink=auto", file("staged.db"), file("kernel.db")]);
const built = buildTransformed(file("kernel.db"), { name });
report.kernelFile = { added: built.transform.addedObjects().length, schema: built.schemaStats };

const client = new ObjectClient({ url, token, tenant: `a01-export-${name}` });
await client.call("wipe");
const push = await pushFile(client, file("kernel.db"));
report.push = { schemaObjects: push.schemaObjects, refused: push.schemaFailures.length, rows: push.rows };

const pull = await pullFile(client, file("raw.db"));
const raw = schemaRows(file("raw.db"));
report.pull = { rows: pull.rows ?? null, addedObjectsInRaw: raw.rows.filter((o) => o.name.startsWith(ADDED_PREFIX)).length, hiddenColumnsInRaw: raw.hidden };

const stageOut = stage(file("raw.db"), file("out.db"));
const out = schemaRows(file("out.db"));
const staged = schemaRows(file("staged.db"));
const key = (o) => `${o.type}\u0000${o.name}\u0000${o.sql ?? ""}`;
const outKeys = new Set(out.rows.map(key));
const stagedKeys = new Set(staged.rows.map(key));
const copied = Object.keys(stageIn.copiedTables);
const sums = compareChecksums(fileChecksums(file("staged.db"), copied), fileChecksums(file("out.db"), copied), copied);
report.export = {
  findings: stageOut.findings,
  skippedAsKernelObjects: Object.entries(stageOut.skippedTables ?? {}).filter(([t]) => t.startsWith(ADDED_PREFIX)).length,
  schemaEqualsRust: outKeys.size === stagedKeys.size && [...outKeys].every((k) => stagedKeys.has(k)),
  schemaObjects: out.rows.length,
  addedObjectsInOut: out.rows.filter((o) => o.name.startsWith(ADDED_PREFIX)).length,
  hiddenColumnsInOut: out.hidden,
  checksums: { tables: sums.tables, rows: sums.rows, mismatches: sums.mismatches.length },
  installerAccepts: stageOut.accept ?? null,
};
await client.call("wipe");
report.ms = Date.now() - t0;
report.pass =
  report.push.refused === 0 && report.pull.addedObjectsInRaw > 0 && report.export.findings.length === 0 && report.export.schemaEqualsRust &&
  report.export.addedObjectsInOut === 0 && report.export.hiddenColumnsInOut === 0 && report.export.checksums.mismatches === 0;
console.log(JSON.stringify(report));
if (option("--json")) writeFileSync(option("--json"), JSON.stringify(report, null, 2) + "\n");
if (!args.includes("--keep")) rmSync(work, { recursive: true, force: true });
process.exit(report.pass ? 0 : 1);
