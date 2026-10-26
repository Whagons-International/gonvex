#!/usr/bin/env python3
"""Prove actual mutable snapshot ACK progress, excluding delivery credit queues."""
import argparse
import os
from pathlib import Path
import shutil

from proof_runner import item, run

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "rust/crates/runtime/src/snapshot_ack.rs"
SPECS = """
spec fn valid_ack(sent: Option<u64>, sequence: u64) -> bool {
    match sent { None => false, Some(sent) => sequence <= sent }
}
spec fn advances(acked: Option<u64>, sequence: u64) -> bool {
    match acked { None => true, Some(acked) => sequence > acked }
}
spec fn ordered(progress: &AckProgress) -> bool {
    match progress.acked {
        None => true,
        Some(acked) => match progress.sent { None => false, Some(sent) => acked <= sent },
    }
}
"""
CONTRACT = """-> (result: Result<bool, ()>)
    ensures
        final(progress).sent == old(progress).sent,
        result.is_err() == !valid_ack(old(progress).sent, sequence),
        (result == Ok(true)) == (valid_ack(old(progress).sent, sequence)
            && advances(old(progress).acked, sequence)),
        final(progress).acked == if result == Ok(true) { Some(sequence) } else { old(progress).acked },
        ordered(old(progress)) ==> ordered(final(progress)),
{"""
LAWS = """
fn repeated_ack_does_not_advance(progress: &mut AckProgress, sequence: u64)
    ensures final(progress).sent == old(progress).sent,
{
    let _first = record_ack(progress, sequence);
    let after_first = progress.acked;
    let second = record_ack(progress, sequence);
    assert(second != Ok(true));
    assert(progress.acked == after_first);
}
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--verus", default=os.environ.get("VERUS_BIN") or shutil.which("verus"))
    args = parser.parse_args()
    if not args.verus:
        parser.error("pass --verus with the pinned verifier")
    declaration = item(SOURCE, "pub(crate) struct AckProgress {")
    if declaration != "pub(crate) struct AckProgress {\n    pub(crate) sent: Option<u64>,\n    pub(crate) acked: Option<u64>,\n}":
        raise RuntimeError("progress fields changed; review specification")
    function = item(SOURCE, "pub(crate) fn record_ack(")
    marker = "-> Result<bool, ()> {"
    if function.count(marker) != 1:
        raise RuntimeError("production signature changed; review adapter")
    def harness(body):
        return declaration + SPECS + body.replace(marker, CONTRACT, 1) + LAWS
    variants = [("production", harness(function), True)]
    for name, before, after in [
        ("accept_unsent", "None => return Err(())", "None => return Ok(false)"),
        ("accept_future", "Some(sent) if sequence > sent", "Some(sent) if sequence > sent && false"),
        ("reject_sent_boundary", "sequence > sent", "sequence >= sent"),
        ("advance_duplicate", "sequence <= acked", "sequence < acked"),
        ("regress_progress", "sequence <= acked", "sequence == acked"),
        ("forget_progress", "progress.acked = Some(sequence);", "progress.acked = None;"),
        ("mutate_sent", "progress.acked = Some(sequence);", "progress.sent = None; progress.acked = Some(sequence);"),
    ]:
        if function.count(before) != 1:
            raise RuntimeError("negative control no longer matches: " + name)
        variants.append((name, harness(function.replace(before, after, 1)), False))
    receipt = run(args.verus, args.output, ROOT, [SOURCE], variants,
                  "Actual mutable production ACK transition for all optional u64 sent/acked states. Invalid and duplicate ACKs do not mutate progress; accepted advances are bounded by sent; sent stays unchanged; valid ordering is preserved; replay cannot advance twice. Retired-snapshot routing, frame queues, credits, transport and durable client storage remain outside this proof.",
                  Path(__file__))
    print(f"ACK transition and {len(receipt['checks']) - 1} failing controls passed.")


if __name__ == "__main__":
    main()
