#!/usr/bin/env python3
"""Prove the production cursor-resume policy, including actual String equality."""
import argparse
import os
from pathlib import Path
import shutil

from proof_runner import item, run

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "rust/crates/runtime/src/replica_cursor.rs"
TYPES = ROOT / "rust/crates/protocol/src/lib.rs"
CONTRACT = """-> (result: bool)
    ensures result == match client {
        None => false,
        Some(cursor) => cursor.epoch@ == current.epoch@
            && retained <= cursor.revision && cursor.revision <= current.revision,
    },
{"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--verus", default=os.environ.get("VERUS_BIN") or shutil.which("verus"))
    args = parser.parse_args()
    if not args.verus:
        parser.error("pass --verus with the pinned verifier")
    declaration = item(TYPES, "pub struct ReplicaCursor {")
    if declaration != "pub struct ReplicaCursor {\n    pub epoch: String,\n    pub revision: u64,\n}":
        raise RuntimeError("cursor fields changed; review the specification")
    function = item(SOURCE, "pub(crate) fn cursor_resumable(")
    marker = "-> bool {"
    if function.count(marker) != 1:
        raise RuntimeError("production signature changed; review the adapter")
    def harness(body):
        return declaration + "\n" + body.replace(marker, CONTRACT, 1)
    variants = [("production", harness(function), True)]
    for name, before, after in [
        ("accept_missing", "None => false", "None => true"),
        ("ignore_epoch", "client.epoch == current.epoch", "true"),
        ("accept_future", "client.revision <= current.revision", "true"),
        ("accept_expired", "client.revision >= retained", "true"),
        ("exclude_current", "client.revision <= current.revision", "client.revision < current.revision"),
        ("exclude_retained", "client.revision >= retained", "client.revision > retained"),
    ]:
        if function.count(before) != 1:
            raise RuntimeError("negative control no longer matches: " + name)
        variants.append((name, harness(function.replace(before, after, 1)), False))
    receipt = run(args.verus, args.output, ROOT, [SOURCE, TYPES], variants,
                  "Actual production cursor decision and ReplicaCursor fields. For all strings and u64 revisions: a cursor exists, epochs match, and retained <= client <= current. No retained/current ordering precondition. Serialization, history retention, hash validation and caller behavior are outside this proof. String equality uses Verus standard-library specifications.",
                  Path(__file__))
    print(f"Cursor policy and {len(receipt['checks']) - 1} failing controls passed.")


if __name__ == "__main__":
    main()
