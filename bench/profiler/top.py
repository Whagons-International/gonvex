#!/usr/bin/env python3
"""Top-style view of the gonvex runtime profiler (/dev/profile).

Live (rates and percentiles over each interval, from bucket deltas):
    bench/profiler/top.py --url http://127.0.0.1:3210 --token "$ADMIN_TOKEN"
One cumulative snapshot:
    bench/profiler/top.py --url ... --once
Save a dump, then diff two dumps (what happened between them):
    bench/profiler/top.py --url ... --save before.json
    bench/profiler/top.py --delta before.json after.json
Read a saved dump:
    bench/profiler/top.py --file after.json

Standard library only. The token is an operator token with projects:read
(the same `admin` bearer the Whagons bench uses); env GONVEX_ADMIN_TOKEN works too.
"""
import argparse
import json
import os
import sys
import time
import urllib.request

REDUCER_PATH = [
    # (stage, indent) in pipeline order; indented stages are inside the one above.
    ("fn.prepare", 0),
    ("invoke.connect", 0),
    ("invoke.first_frame", 0),
    ("invoke.isolate_wait", 1),
    ("invoke.js", 0),
    ("invoke.host_calls", 0),
    ("db.writer_wait", 1),
    ("db.sql", 1),
    ("host.other", 1),
    ("fn.finish", 0),
    ("commit.finalize", 1),
    ("commit.commit", 1),
    ("commit.fsync", 1),
    ("commit.publish", 1),
]
TOP_LEVEL = {"fn.prepare", "invoke.connect", "invoke.first_frame", "invoke.js", "invoke.host_calls", "fn.finish"}
COUNT_STAGES = {"invoke.host_call_count", "delivery.frames", "sqlite.checkpoint_pages"}
BYTE_STAGES = {"snapshot_cache.evict"}
GROUP_ORDER = ["reducer", "store", "delivery", "transport", "snapshot", "scheduler", "background"]
# Background work that costs CPU (or a thread) without a request attached:
# (stage, short name). Their time per second of wall clock is printed as a
# rough "where did idle CPU go" line.
BACKGROUND = [
    ("sqlite.checkpoint", "checkpoint"),
    ("sqlite.checkpoint_inline", "inline ckpt"),
    ("v8.gc_minor", "gc minor"),
    ("v8.gc_major", "gc major"),
    ("v8.isolate_start", "isolate start"),
    ("v8.isolate_stop", "isolate stop"),
    ("mem.malloc_trim", "malloc_trim"),
    ("snapshot_cache.build", "cache build"),
    ("offload.busy", "offload busy"),
]


def fetch(url, token):
    request = urllib.request.Request(url.rstrip("/") + "/dev/profile")
    if token:
        request.add_header("authorization", "Bearer " + token)
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def quantile(buckets, bounds, count, maximum, q):
    if count <= 0:
        return 0.0
    rank = max(1, int(-(-q * count // 1)))
    seen = 0
    for index, hits in buckets:
        if hits <= 0:
            continue
        if seen + hits >= rank:
            lower = bounds[index]
            upper = bounds[index + 1] if index + 1 < len(bounds) else lower * 2
            if maximum:
                upper = min(upper, max(maximum, lower))
            return lower + (upper - lower) * (rank - seen) / hits
        seen += hits
    return float(maximum)


def stage_rows(current, previous=None):
    """(stage, label) -> dict with count, sum, max, p50..p999 over the window."""
    bounds = current["bucketLowerBoundsNs"]
    before = {}
    if previous:
        for stage in previous["stages"]:
            before[(stage["stage"], stage["label"])] = stage
    rows = {}
    for stage in current["stages"]:
        key = (stage["stage"], stage["label"])
        buckets = {i: c for i, c in stage["buckets"]}
        count, total = stage["count"], stage["sumNs"]
        old = before.get(key)
        if old:
            for i, c in old["buckets"]:
                buckets[i] = buckets.get(i, 0) - c
            count -= old["count"]
            total -= old["sumNs"]
        if count <= 0:
            continue
        ordered = sorted((i, c) for i, c in buckets.items() if c > 0)
        maximum = stage["maxNs"]  # cumulative max (no per-window max)
        rows[key] = {
            "count": count,
            "sum": total,
            "mean": total / count,
            "max": maximum,
            **{name: quantile(ordered, bounds, count, maximum, q)
               for name, q in (("p50", .5), ("p90", .9), ("p99", .99), ("p999", .999))},
        }
    return rows


def fmt_bytes(value):
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            return f"{value:5.1f}{unit}" if unit != "B" else f"{value:5.0f}B "
        value /= 1024
    return f"{value:5.1f}GB"


def fmt_ns(ns, count_stage=False, byte_stage=False):
    if byte_stage:
        return fmt_bytes(ns)
    if count_stage:
        return f"{ns:7.1f}"
    if ns < 1e3:
        return f"{ns:5.0f}ns"
    if ns < 1e6:
        return f"{ns / 1e3:5.1f}us"
    if ns < 1e9:
        return f"{ns / 1e6:5.1f}ms"
    return f"{ns / 1e9:5.2f}s "


def render(current, previous=None, seconds=None, stream=sys.stdout):
    out = []
    groups = {info["stage"]: info["group"] for info in current["stageInfo"]}
    rows = stage_rows(current, previous)
    window = f"last {seconds:.1f}s" if seconds else "since start"
    tokio = current.get("tokio", {})
    watchdog = current.get("watchdog", {})
    polls = current.get("longPolls", {})
    out.append(
        f"gonvex profile ({window})  uptime {current['uptimeNs'] / 1e9:,.0f}s  "
        f"enabled={current['enabled']}  workers={tokio.get('workers')} busy={tokio.get('busyRatio', 0) * 100:.1f}% "
        f"globalQ={tokio.get('globalQueueDepth')} (peak {tokio.get('globalQueueDepthPeak')}) tasks={tokio.get('aliveTasks')}")
    out.append(
        f"watchdog: probe silent {watchdog.get('probeSilentMs', 0):.1f} ms, stalls {watchdog.get('stalls', 0)}   "
        f"long polls (>{current['config']['longPollMs']:.0f} ms): {polls.get('total', 0)}")
    gauges = current.get("gauges", {})
    out.append("gauges: " + "  ".join(
        f"{name}={value['current']}" + (f"/{value['peak']}" if value.get("peak") not in (None, 0) else "")
        for name, value in sorted(gauges.items())))
    queues = [q for q in current.get("writerQueues", []) if q["waiting"] or q["acquisitions"]][:5]
    if queues:
        out.append("writer queues (waiting, acquisitions, mean wait, max wait): " + "  ".join(
            f"{q['database']}:{q['waiting']},{q['acquisitions']},"
            f"{fmt_ns(q['waitNsTotal'] / max(q['acquisitions'], 1)).strip()},{fmt_ns(q['waitMaxNs']).strip()}"
            for q in queues))
    out.append("")
    out.append(f"{'stage':<24} {'label':<10} {'rate/s':>9} {'mean':>8} {'p50':>8} {'p90':>8} {'p99':>8} {'p999':>8} {'max':>8}")
    for group in GROUP_ORDER + sorted(set(groups.values()) - set(GROUP_ORDER)):
        keys = [key for key in rows if groups.get(key[0]) == group]
        if not keys:
            continue
        out.append(f"-- {group}")
        order = [info["stage"] for info in current["stageInfo"]]
        for key in sorted(keys, key=lambda k: (order.index(k[0]) if k[0] in order else 999, k[1])):
            row = rows[key]
            counted = key[0] in COUNT_STAGES
            sized = key[0] in BYTE_STAGES
            rate = row["count"] / seconds if seconds else row["count"] / (current["uptimeNs"] / 1e9)
            out.append(
                f"{key[0]:<24} {key[1]:<10} {rate:9.1f} " + " ".join(
                    fmt_ns(row[name], counted, sized).rjust(8) for name in ("mean", "p50", "p90", "p99", "p999", "max")))
    span = seconds or current["uptimeNs"] / 1e9
    render_isolate_wait(out, rows, gauges)
    render_background(out, current, rows, span)
    for label in ("heartbeat", "presence", "reducer"):
        total = rows.get(("fn.total", label))
        if not total:
            continue
        out.append("")
        out.append(f"where did a {label} reducer spend its time ({total['count']} calls; "
                   f"fn.total mean {fmt_ns(total['mean']).strip()}, p99 {fmt_ns(total['p99']).strip()})")
        out.append(f"  {'stage':<22} {'mean':>8} {'share':>6} {'p99':>8} {'p99/total.p99':>14}")
        accounted = 0.0
        for stage, indent in REDUCER_PATH:
            row = rows.get((stage, label))
            if not row:
                continue
            share = row["mean"] / total["mean"] if total["mean"] else 0
            if stage in TOP_LEVEL:
                accounted += row["mean"]
            out.append(f"  {'  ' * indent + stage:<22} {fmt_ns(row['mean']):>8} {share * 100:5.1f}% "
                       f"{fmt_ns(row['p99']):>8} {row['p99'] / total['p99'] * 100 if total['p99'] else 0:13.1f}%")
        rest = total["mean"] - accounted
        out.append(f"  {'(unaccounted: reply, outbox spawn)':<22} {fmt_ns(max(rest, 0)):>8} "
                   f"{max(rest, 0) / total['mean'] * 100 if total['mean'] else 0:5.1f}%")
        out.append("  share = stage mean / fn.total mean (top-level rows add up). p99s do not add: "
                   "the right column shows which stage alone reaches the total's tail.")
    slow = current.get("slowTraces") or {}
    traces = slow.get("recent", [])
    if previous and traces:
        last_seq = max((t["seq"] for t in (previous.get("slowTraces") or {}).get("recent", [])), default=0)
        traces = [t for t in traces if t["seq"] > last_seq]
    if traces:
        fields = slow["fields"]
        out.append("")
        out.append(f"slowest calls: {len(traces)} reducers over {current['config'].get('slowTraceMs', 100):.0f} ms "
                   f"in this window (ring keeps 128; {slow.get('total', 0)} since start)")
        top_fields = ["fn.prepare", "invoke.connect", "invoke.first_frame", "invoke.isolate_wait",
                      "invoke.js", "db.writer_wait", "db.sql", "fn.finish"]
        out.append("  mean share of each slow call's own total (per label):")
        out.append(f"  {'label':<10} {'n':>4} {'total':>8} " + " ".join(f"{f.split('.')[-1][:11]:>11}" for f in top_fields))
        by_label = {}
        by_function = {}
        for trace in traces:
            item = dict(zip(fields, trace["values"]))
            by_label.setdefault(trace["label"], []).append(item)
            by_function.setdefault(trace.get("function") or "?", []).append(item)

        def share_rows(groups, width):
            for name, items in sorted(groups.items(), key=lambda kv: -len(kv[1])):
                mean_total = sum(i["fn.total"] for i in items) / len(items)
                shares = []
                for field in top_fields:
                    shares.append(sum(i.get(field, 0) / i["fn.total"] for i in items if i["fn.total"]) / len(items) * 100)
                out.append(f"  {name[-width:]:<{width}} {len(items):>4} {fmt_ns(mean_total):>8} "
                           + " ".join(f"{v:10.1f}%" for v in shares))

        share_rows(by_label, 10)
        out.append("  (isolate_wait is inside first_frame; writer_wait and sql are inside host_calls)")
        out.append("  per function (top 8 by count):")
        out.append(f"  {'function':<34} {'n':>4} {'total':>8} " + " ".join(f"{f.split('.')[-1][:11]:>11}" for f in top_fields))
        share_rows(dict(sorted(by_function.items(), key=lambda kv: -len(kv[1]))[:8]), 34)
        out.append("  newest slow calls:")
        for trace in traces[:5]:
            item = dict(zip(fields, trace["values"]))
            parts = " ".join(f"{f.split('.')[-1]}={fmt_ns(item[f]).strip()}" for f in top_fields if item.get(f))
            out.append(f"    {trace['label']:<10} {(trace.get('function') or '?')[-40:]:<40} "
                       f"{fmt_ns(item['fn.total']).strip():>8}  {parts}  calls={item['invoke.host_call_count']}")
    render_functions(out, current, previous, seconds)
    recent = polls.get("recent", [])[:10]
    if recent:
        out.append("")
        out.append("recent long polls / stalls (newest first):")
        now_ms = current.get("generatedAtUnixMs", 0)
        for poll in recent:
            age = (now_ms - poll["atUnixMs"]) / 1000 if now_ms else 0
            out.append(f"  {age:7.1f}s ago  {poll['durationMs']:9.1f} ms  {poll['task']:<14} ({poll['source']})")
    stream.write("\n".join(out) + "\n")


def render_isolate_wait(out, rows, gauges):
    """The in-process V8 isolate queue: the 500-user failure mode."""
    waits = sorted((key[1], row) for key, row in rows.items() if key[0] == "invoke.isolate_wait")
    if not waits:
        return
    gauge = gauges.get("module_host.isolate_waiting", {})
    out.append("")
    out.append(f"isolate queue: waiting now {gauge.get('current', 0)} (10 s peak {gauge.get('peak') or 0}); "
               f"wait per call (request -> isolate leased; deadline-expired waits included):")
    for label, row in waits:
        total = rows.get(("invoke.total", label))
        share = row["mean"] / total["mean"] * 100 if total and total["mean"] else 0
        out.append(f"  {label:<10} n={row['count']:<7} mean {fmt_ns(row['mean']).strip():>8}  p99 {fmt_ns(row['p99']).strip():>8}  "
                   f"max {fmt_ns(row['max']).strip():>8}  ({share:4.1f}% of invoke.total)")


def render_background(out, current, rows, span):
    """Background work: CPU/thread time per second with no request attached."""
    parts = []
    for stage, name in BACKGROUND:
        row = rows.get((stage, "-"))
        if row:
            parts.append(f"{name} {row['sum'] / span / 1e6:.1f} ms/s ({row['count'] / span:.2f}/s)")
    pages = rows.get(("sqlite.checkpoint_pages", "-"))
    evicted = rows.get(("snapshot_cache.evict", "-"))
    if pages:
        parts.append(f"ckpt pages {pages['sum'] / span:.0f}/s")
    if evicted:
        parts.append(f"cache evictions {evicted['count']} ({fmt_bytes(evicted['sum']).strip()})")
    if parts:
        out.append("")
        out.append("background work (time per second of wall clock): " + ";  ".join(parts))
    isolates = current.get("v8Isolates") or {}
    fields = isolates.get("fields") or []
    table = [dict(zip(fields, row)) for row in isolates.get("rows", [])]
    if table:
        out.append(f"v8 isolates ({len(table)} live, cumulative per isolate as of its last call):")
        out.append(f"  {'used heap':>10} {'total heap':>10} {'calls':>8} {'gc minor':>9} {'minor ms':>9} {'gc major':>9} {'major ms':>9}")
        for row in sorted(table, key=lambda r: -r.get("usedHeapBytes", 0))[:8]:
            out.append(f"  {fmt_bytes(row.get('usedHeapBytes', 0)):>10} {fmt_bytes(row.get('totalHeapBytes', 0)):>10} "
                       f"{row.get('calls', 0):>8} {row.get('gcMinor', 0):>9} {row.get('gcMinorNs', 0) / 1e6:9.1f} "
                       f"{row.get('gcMajor', 0):>9} {row.get('gcMajorNs', 0) / 1e6:9.1f}")


def render_functions(out, current, previous, seconds, limit=15):
    """Per-function table (server-side counters), by total time."""
    section = current.get("functions") or {}
    fields = section.get("fields") or []
    if not fields:
        return
    before = {}
    if previous:
        for row in (previous.get("functions") or {}).get("rows", []):
            before[row["slot"]] = dict(zip(fields, row["values"]))
    rows = []
    for row in section.get("rows", []):
        values = dict(zip(fields, row["values"]))
        old = before.get(row["slot"])
        if old:
            for name in ("invocations", "errors", "cancelled", "totalNs"):
                values[name] -= old.get(name, 0)
        if values["invocations"] <= 0:
            continue
        rows.append((row, values))
    if not rows:
        return
    span = seconds or current["uptimeNs"] / 1e9
    grand = sum(values["totalNs"] for _, values in rows) or 1
    rows.sort(key=lambda item: -item[1]["totalNs"])
    out.append("")
    out.append(f"functions by total time ({len(rows)} called; max is cumulative since start):")
    out.append(f"  {'function':<40} {'calls/s':>8} {'calls':>8} {'errors':>6} {'cancel':>6} {'mean':>8} {'max':>8} {'time%':>6}")
    for row, values in rows[:limit]:
        name = row["path"] if not row.get("project") else f"{row['project']}/{row['path']}"
        out.append(f"  {name[-40:]:<40} {values['invocations'] / span:8.1f} {values['invocations']:>8} "
                   f"{values['errors']:>6} {values['cancelled']:>6} "
                   f"{fmt_ns(values['totalNs'] / values['invocations']):>8} {fmt_ns(values['maxNs']):>8} "
                   f"{values['totalNs'] / grand * 100:5.1f}%")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default=os.environ.get("GONVEX_URL", "http://127.0.0.1:3210"))
    parser.add_argument("--token", default=os.environ.get("GONVEX_ADMIN_TOKEN", "admin"))
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--once", action="store_true", help="print one cumulative snapshot")
    parser.add_argument("--save", help="write the fetched dump to this file and print it")
    parser.add_argument("--file", help="render a saved dump instead of fetching")
    parser.add_argument("--delta", nargs=2, metavar=("BEFORE", "AFTER"), help="render the difference of two dumps")
    args = parser.parse_args()
    if args.delta:
        before, after = (json.load(open(path)) for path in args.delta)
        seconds = (after["uptimeNs"] - before["uptimeNs"]) / 1e9
        render(after, before, seconds)
        return
    if args.file:
        render(json.load(open(args.file)))
        return
    current = fetch(args.url, args.token)
    if args.save:
        with open(args.save, "w") as handle:
            json.dump(current, handle)
    if args.once or args.save:
        render(current)
        return
    while True:
        time.sleep(args.interval)
        previous, current = current, fetch(args.url, args.token)
        seconds = (current["uptimeNs"] - previous["uptimeNs"]) / 1e9
        sys.stdout.write("\x1b[2J\x1b[H")
        render(current, previous, seconds)
        sys.stdout.flush()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
