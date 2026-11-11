// Prints the per-step Markdown table of .artifact/spike-results.json for the report.
import { readFileSync } from "node:fs";
import { join, resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
const spike = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const r = JSON.parse(readFileSync(join(spike, ".artifact/spike-results.json"), "utf8"));
const rows = ["| Step | Function | Purpose | In the Durable Object | Same outcome as like-for-like reference | Full-schema reference | Statements | Rows read (host catalog) | Rows written | Savepoints | One JS turn |", "|---|---|---|---|---|---|---|---|---|---|---|"];
for (const s of r.steps) {
  const verdict = s.blockedBy ? `blocked: \`${s.blockedBy}\` refused` : s.blockedByFixture ? "blocked: a prerequisite step was blocked" : s.vacuous ? "vacuous: failed earlier, on a blocked object" : s.pass ? (s.ok ? "pass" : "pass (expected error, rolled back)") : "FAIL";
  rows.push(`| ${s.name.split(" ")[0]} | \`${s.path}\` | ${(s.why ?? s.name.slice(3)).replaceAll("|", "/")} | ${verdict} | ${s.sameOutcomeAsReference ? "yes" : "no"} | ${s.fullSchemaReference.pass ? "pass" : "fail"} | ${s.statements} | ${s.rowsRead} (${s.catalogRowsRead}) | ${s.rowsWritten} | ${s.savepoints} | ${s.withinOneTurn ? "yes" : "no"} |`);
}
console.log(rows.join("\n"));
