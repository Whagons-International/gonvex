#!/usr/bin/env python3
"""Per-kind latency in time windows around a wall-clock instant (e.g. the :00 cron herd).

    window-report.py LAB LABEL EPOCH_SECONDS [before_s=60] [after_s=120]

Reads the run.ts shard results LAB/LABEL.json.shard-* (raw samples carry the
operation start `at` in ms) and prints a markdown table: samples, p50, p95, p99
for [T-before, T) and [T, T+after)."""
import glob, json, sys

lab, label, t = sys.argv[1], sys.argv[2], float(sys.argv[3]) * 1000
before = float(sys.argv[4]) * 1000 if len(sys.argv) > 4 else 60000
after = float(sys.argv[5]) * 1000 if len(sys.argv) > 5 else 120000
samples = {}
for f in sorted(glob.glob(f'{lab}/{label}.json.shard-[0-9]*')):
    if f.endswith(('.log', '.jsonl', '.json')) and not f.split('.shard-')[1].isdigit():
        continue
    try:
        r = json.load(open(f))
    except (OSError, ValueError):
        continue
    for kind, xs in (r.get('rawSamples') or {}).items():
        samples.setdefault(kind, []).extend(xs)


def q(xs, f):
    return xs[min(len(xs) - 1, int(f * len(xs)))] if xs else None


def fmt(v):
    return '–' if v is None else f'{v:,.0f}'


print(f'| kind | before n | before p50 | before p95 | before p99 | after n | after p50 | after p95 | after p99 |')
print('|---|---:|---:|---:|---:|---:|---:|---:|---:|')
for kind in sorted(samples):
    a = sorted(x['ms'] for x in samples[kind] if t - before <= x['at'] < t)
    b = sorted(x['ms'] for x in samples[kind] if t <= x['at'] < t + after)
    if a or b:
        print(f'| {kind} | {len(a)} | {fmt(q(a, .5))} | {fmt(q(a, .95))} | {fmt(q(a, .99))} | {len(b)} | {fmt(q(b, .5))} | {fmt(q(b, .95))} | {fmt(q(b, .99))} |')
