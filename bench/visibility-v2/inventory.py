#!/usr/bin/env python3
"""Visibility inventory of a Whagons manifest: which plans are legacy (v1) and
which data tables each legacy plan depends on.

    python3 inventory.py LEGACY_MANIFEST.json [V2_MANIFEST.json] > inventory.json
    python3 inventory.py --markdown LEGACY_MANIFEST.json V2_MANIFEST.json

A legacy plan is any plan without `version: 2` (the runtime logs
"DEPRECATED legacy visibility manifest" for every module with one). Its
"data-table dependencies" are the tables its sets read (base or join) that
are not permission tables. The permission-table list is the union of the
`permissionTables` declared by the version 2 manifest (the app's canonical
list); without one, every set table counts as a dependency. In the legacy
runtime every insert or delete of a dependency table counts as a principal
change for every member subscribed to the plan (SQL re-resolution and a
collection rebuild).
"""
import json
import sys
from collections import defaultdict


def tables_of(node, acc):
    if isinstance(node, dict):
        if isinstance(node.get("table"), str):
            acc.add(node["table"])
        for value in node.values():
            tables_of(value, acc)
    elif isinstance(node, list):
        for value in node:
            tables_of(value, acc)
    return acc


def set_tables(plan):
    acc = set()
    for definition in plan.get("sets", {}).values():
        acc.add(definition["table"])
        for join in definition.get("joins", []):
            acc.add(join["table"])
    return acc


def inherited(node, acc):
    if isinstance(node, dict):
        if node.get("operator") in ("inherits", "inheritsAny"):
            acc.add(node["table"])
        for value in node.values():
            inherited(value, acc)
    elif isinstance(node, list):
        for value in node:
            inherited(value, acc)
    return acc


def main(argv):
    markdown = "--markdown" in argv
    argv = [a for a in argv if a != "--markdown"]
    legacy = json.load(open(argv[0]))
    v2 = json.load(open(argv[1])) if len(argv) > 1 else None
    plans = legacy["visibility"]
    functions = legacy["module"]["functions"] if "module" in legacy else legacy["functions"]
    permission = set()
    if v2:
        for plan in v2["visibility"].values():
            permission |= set(plan.get("permissionTables", []))
    collections = defaultdict(list)
    live = defaultdict(list)
    for path, definition in functions.items():
        if "replica" in definition:
            collections[definition["replica"]["table"]].append(path)
        plan = (definition.get("dependencies") or {}).get("liveQueryPlan")
        if plan:
            live[plan["table"]].append(path)
    rows = []
    for table, plan in sorted(plans.items()):
        reads = set_tables(plan)
        deps = sorted(reads - permission) if v2 else sorted(reads)
        new = v2["visibility"].get(table) if v2 else None
        rows.append({
            "table": table,
            "version": plan.get("version") or 1,
            "sets": len(plan.get("sets", {})),
            "setTables": sorted(reads),
            "dataTableDependencies": deps,
            "selfDependency": table in reads,
            "collections": sorted(collections.get(table, [])),
            "liveQueries": sorted(live.get(table, [])),
            "v2Version": new.get("version") if new else None,
            "v2SetKinds": sorted({s.get("kind", "") for s in new.get("sets", {}).values()}) if new else [],
            "v2Inherits": sorted(inherited(new["where"], set())) if new else [],
        })
    if not markdown:
        json.dump({"permissionTables": sorted(permission), "plans": rows}, sys.stdout, indent=1)
        print()
        return
    flagged = [r for r in rows if r["dataTableDependencies"]]
    print(f"Plans: {len(rows)}; legacy: {sum(r['version'] != 2 for r in rows)}; "
          f"with data-table dependencies: {len(flagged)}; "
          f"collections over flagged tables: {sum(len(r['collections']) for r in flagged)}; "
          f"live queries: {sum(len(r['liveQueries']) for r in flagged)}\n")
    counts = defaultdict(int)
    for r in flagged:
        for dep in r["dataTableDependencies"]:
            counts[dep] += 1
    print("Dependency table | legacy plans reading it")
    print("--- | ---")
    for dep, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"`{dep}` | {n}")
    print()
    print("Table | v1 data-table dependencies | collections | live queries | v2 set kinds / inherits")
    print("--- | --- | --- | --- | ---")
    for r in flagged:
        deps = ", ".join(f"`{d}`" for d in r["dataTableDependencies"])
        kinds = ",".join(k for k in r["v2SetKinds"] if k) or "-"
        if r["v2Inherits"]:
            kinds += " / inherits " + ",".join(r["v2Inherits"])
        print(f"`{r['table']}` | {deps} | {len(r['collections'])} | {len(r['liveQueries'])} | {kinds}")


if __name__ == "__main__":
    main(sys.argv[1:])
