#!/usr/bin/env python3
"""Prove the production capability policy and reject a bypass of every gate."""
import argparse
import os
from pathlib import Path
import re
import shutil

from proof_runner import item, run

ROOT = Path(__file__).resolve().parents[2]
TYPES = ROOT / "rust/crates/module-runtime/src/lib.rs"
POLICY = ROOT / "rust/crates/module-runtime/src/capabilities.rs"
FIELDS = ["db_read", "db_write", "action_outbox", "action_tools", "scheduler",
          "network", "storage", "sandbox", "secrets", "functions"]
# This specification states the existing Query/Reducer/Action contract.
ALLOWED = {
    "Query": {"db_read"},
    "Reducer": {"db_read", "db_write", "storage", "action_outbox", "scheduler"},
    "Action": {"action_tools", "functions", "scheduler", "network", "storage", "sandbox", "secrets"},
}


def annotate(function, granted):
    marker = "-> Capabilities {"
    if function.count(marker) != 1:
        raise RuntimeError("production signature changed; review the adapter")
    branches = []
    for kind, allowed in ALLOWED.items():
        terms = []
        for field in FIELDS:
            value = f"granted.{field}" if granted and field in allowed else str(field in allowed).lower()
            terms.append(f"result.{field} == {value}")
        branches.append(f"        FunctionKind::{kind} => " + " && ".join(terms) + ",")
    contract = "-> (result: Capabilities)\n    ensures match kind {\n" + "\n".join(branches) + "\n    },\n{"
    return function.replace(marker, contract, 1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--verus", default=os.environ.get("VERUS_BIN") or shutil.which("verus"))
    args = parser.parse_args()
    if not args.verus:
        parser.error("pass --verus with the pinned verifier")
    kind = item(TYPES, "pub enum FunctionKind {")
    capabilities = item(TYPES, "pub struct Capabilities {")
    if re.findall(r"pub (\w+): bool", capabilities) != FIELDS:
        raise RuntimeError("capability fields changed; review the specification")
    if re.findall(r"^    (\w+),$", kind, re.M) != list(ALLOWED):
        raise RuntimeError("function kinds changed; review the specification")
    structural = annotate(item(POLICY, "pub fn structural_capabilities("), False)
    effective = annotate(item(POLICY, "pub fn effective_capabilities("), True)
    laws = """pub fn capability_laws(kind: &FunctionKind, granted: &Capabilities) {
    let ceiling = structural_capabilities(kind);
    let once = effective_capabilities(kind, granted);
    let twice = effective_capabilities(kind, &once);
"""
    for field in FIELDS:
        laws += f"    assert(once.{field} == (ceiling.{field} && granted.{field}));\n"
        laws += f"    assert(once.{field} ==> granted.{field});\n"
        laws += f"    assert(once.{field} ==> ceiling.{field});\n"
        laws += f"    assert(once.{field} == twice.{field});\n"
    laws += "}\n"
    code = "\n".join([kind, capabilities, structural, effective, laws])
    variants = [("production", code, True)]
    for field in FIELDS:
        before = f"{field}: structural.{field} && granted.{field},"
        if code.count(before) != 1:
            raise RuntimeError("negative control no longer matches " + field)
        for gate, after in [("grant", f"{field}: structural.{field},"),
                            ("kind", f"{field}: granted.{field},")]:
            variants.append((f"bypass_{gate}_{field}", code.replace(before, after, 1), False))
    receipt = run(args.verus, args.output, ROOT, [TYPES, POLICY], variants,
                  "All function kinds and Boolean host grants. Exact structural ceilings, intersection, no grant escalation, no kind escalation, and idempotence. Actual datatype declarations and executable function bodies are copied unchanged; only return binders/contracts and a law harness are added. Serde/derive attributes, V8 dispatch, host operation checks, compiler and ABI behavior are outside the proof.",
                  Path(__file__))
    print(f"Capability proof and {len(receipt['checks']) - 1} deliberate failing controls passed.")


if __name__ == "__main__":
    main()
