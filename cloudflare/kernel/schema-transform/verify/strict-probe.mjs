// Is each loaded object on a strict host? Reads the 22 reference views S1 saw
// fail on strict hosts ("VIEWs and/or subqueries nested too deep"); each is
// the original aggregate over its union (T1 only), which a strict host refuses.
//   S1_TOKEN=... node verify/strict-probe.mjs <base> <object>... [--out f]
import { writeFileSync } from "node:fs";
const args = process.argv.slice(2);
const out = args.includes("--out") ? args[args.indexOf("--out") + 1] : null;
const [base, ...objects] = args.filter((a, i) => a !== "--out" && args[i - 1] !== "--out");
const views = ["approvals", "audiencePresets", "gamificationPointTransactions", "goals", "kpiCards", "marketRecords", "nfcTags", "notifications", "reportDeliveries", "reportDocuments", "reports", "spotCustomFieldValues", "taskCustomFieldValues", "taskFindings", "taskRecurrences", "tasks", "templates", "workflowVersions", "workplanItems", "workspacePreferences", "workspaceViews", "workspaces"].map((v) => `referenceExpected_${v}`);
const results = [];
for (const object of objects) {
  const failing = {};
  for (const view of views) {
    const r = await (await fetch(`${base}/query?o=${encodeURIComponent(object)}`, { method: "POST", headers: { "content-type": "application/json", "x-s1-token": process.env.S1_TOKEN || "" }, body: JSON.stringify({ sql: `SELECT count(*) AS n FROM "${view}"` }) })).json().catch(() => ({ error: "bad json" }));
    if (r.error) (failing[String(r.error).replace(/: \S+$/, "").slice(0, 60)] ||= []).push(view);
  }
  const strict = Object.keys(failing).some((k) => /nested too deep/i.test(k));
  results.push({ object, strict, failing });
  console.log(JSON.stringify({ object, strict, failing: Object.fromEntries(Object.entries(failing).map(([k, v]) => [k, v.length])) }));
}
if (out) writeFileSync(out, JSON.stringify(results, null, 2) + "\n");
