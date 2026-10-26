#!/usr/bin/env python3
"""Verify the frame-length policy used by the real reader and writer."""
import argparse
import os
from pathlib import Path
import shutil

from proof_runner import item, run

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "rust/crates/module-host/src/framing.rs"
CONTRACT = """-> (result: Option<u32>)
    ensures match result {
        Some(length) => length > 0 && length as int == size as int
            && size <= limit && size as int <= u32::MAX as int,
        None => size == 0 || size > limit || size as int > u32::MAX as int,
    },
{"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--verus", default=os.environ.get("VERUS_BIN") or shutil.which("verus"))
    args = parser.parse_args()
    if not args.verus:
        parser.error("pass --verus with the pinned verifier")
    function = item(SOURCE, "fn frame_length(")
    marker = "-> Option<u32> {"
    if function.count(marker) != 1:
        raise RuntimeError("production signature changed; review the adapter")
    code = function.replace(marker, CONTRACT, 1)
    variants = [("production", code, True)]
    for name, before, after in [
        ("allow_empty", "size == 0 || ", ""),
        ("ignore_configured_limit", "limit.min(u32::MAX as usize)", "u32::MAX as usize"),
        ("ignore_wire_limit", "limit.min(u32::MAX as usize)", "limit"),
        ("shorten_frame", "Some(size as u32)", "Some((size - 1) as u32)"),
    ]:
        if function.count(before) != 1:
            raise RuntimeError("negative control no longer matches: " + name)
        mutated = function.replace(before, after, 1).replace(marker, CONTRACT, 1)
        variants.append((name, mutated, False))
    receipt = run(args.verus, args.output, ROOT, [SOURCE], variants,
                  "Pure production length admission for every usize size/limit: nonempty, exact u32 length, within configured and wire limits, rejection iff invalid. Rust error adapters, payload allocation, transport and async scheduling are outside this proof.",
                  Path(__file__))
    print(f"Frame length policy and {len(receipt['checks']) - 1} failing controls passed.")


if __name__ == "__main__":
    main()
