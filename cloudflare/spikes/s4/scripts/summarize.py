#!/usr/bin/env python3
"""Folds results/raw/*.json (gitignored) into results/summary.json (kept).

One entry per run: outcome, attempts, effects applied, lease renewal cadence,
claim latency, per-step measurements and a compact event timeline.
"""
import glob
import json
import os

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
KEEP_STEP = ("kind", "mode", "bytes", "contentLength", "chunks", "maxChunk", "sha256", "events",
             "turns", "contiguous", "allContiguous", "sawDone", "maxGapMs", "firstByteMs",
             "polls", "status", "ms")

runs = {}
for path in sorted(glob.glob(os.path.join(HERE, "results", "raw", "*.json"))):
    name = os.path.basename(path)[:-5]
    data = json.load(open(path))
    s = data["summary"]
    events = data["events"]
    t0 = events[0]["at"] if events else 0
    result = s.get("result") or {}
    steps = []
    for step in result.get("steps", []):
        kept = {k: v for k, v in step.items() if k in KEEP_STEP}
        if "sink" in step:
            kept["sinkBytes"] = step["sink"].get("bytes")
            kept["sinkSha256Matches"] = step["sink"].get("sha256") == step.get("sha256")
        if step.get("kind") == "download" and s.get("expectedSha256"):
            kept["sha256MatchesSource"] = step.get("sha256") == s["expectedSha256"]
            kept.pop("sha256", None)
        steps.append(kept)
    timeline = [
        [round((e["at"] - t0) / 1000, 1), e["kind"], e["attempt"]]
        + ([json.loads(e["detail"])] if e.get("detail") and e["kind"] not in ("enqueued",) else [])
        for e in events
        if e["kind"] != "renewed"
    ]
    for entry in timeline:
        if len(entry) == 4 and isinstance(entry[3], dict):
            entry[3].pop("handle", None)
            if "owner" in entry[3]:
                entry[3]["owner"] = entry[3]["owner"].split(":")[0] + ":" + entry[3]["owner"][-8:]
    runs[name] = {
        "runner": s["runner"],
        "preset": s["preset"],
        "status": s["status"],
        "attempts": s["attempts"],
        "effectsApplied": s["effects"],
        "totalS": s["totalS"],
        "enqueueToClaimMs": s["enqueueToClaimMs"],
        "renewGapMs": s["renewGapMs"],
        "renewals": result.get("renewals"),
        "maxRenewRpcMs": result.get("maxRenewMs"),
        "actionWallMs": result.get("wallMs"),
        "steps": steps,
        "replay": s.get("replay"),
        "lastError": (s.get("lastError") or None) and s["lastError"][:200],
        "timeline": timeline,
    }

out = os.path.join(HERE, "results", "summary.json")
with open(out, "w") as handle:
    json.dump(runs, handle, indent=1)
    handle.write("\n")
print(f"{len(runs)} runs -> {out}")
