#!/usr/bin/env python3
"""CPU by thread and, inside each thread, by delivery work, from a frame-pointer
on-CPU capture (CASE/oncpu.folded), scaled to the runtime cgroup's measured cores.

    thread-attribution.py LAB LABEL

Cores come from LAB/LABEL/step-report.md (runtime cgroup CPU over the plateau);
transactions per second from the /dev/profile dumps (feed.read) when present.
Each stack goes to the first matching rule (inclusive). Needs a runtime built with
-C force-frame-pointers=yes and PERF_CALLGRAPH=fp (see cpu-attribution.py)."""
import collections, json, pathlib, re, sys

lab, label = pathlib.Path(sys.argv[1]), sys.argv[2]
case = lab / label
report = (case / 'step-report.md').read_text()
cores = float(re.search(r'runtime cgroup: CPU ([0-9.]+) cores', report).group(1))
RULES = [
    ('wake dispatch (tenant feed)', r'wake::WakeRegistry>::dispatch|wake::Interest>::affected'),
    ('snapshot open', r'open_replica|enqueue_replica_open|collection_rows|snapshot_cache|scan_window|ReplicaOpenQueue|cached_view|prepare_replica'),
    ('delivery: shared fan-out', r'fanout::shape|Runtime>::fan_out|fan_out_inner|fan_out_waiting|wake_interest'),
    ('delivery: full path', r'apply_feed_event|merge_waiting_feed|collapse_changes|mask_step'),
    ('delivery: feed receive/skip', r'FeedReceiver|HubReceiver|next_feed_event|wake::Slot'),
    ('socket encode/compress/write', r'write_loop|write_text|poll_write_buffer|compress|gonvex_websocket::|send_json|ReplicaBatcher|batcher'),
    ('presence', r'presence'),
    ('reducers/actions (V8)', r'gonvex_module_runtime_v8|execute_tenant_reducer|module_host|in_process'),
    ('scheduler/crons', r'gonvex_runtime::schedul|gonvex_runtime::cron|scheduled_lane'),
    ('commit/storage', r'commit_with|checkpoint|sync_barrier|TenantTransaction'),
]
by = collections.defaultdict(collections.Counter)
total = 0
for line in (case / 'oncpu.folded').read_text().splitlines():
    stack, n = line.rsplit(' ', 1)
    n = int(n)
    total += n
    frames = stack.split(';')
    thread = re.sub(r'[-_ ]?\d+$', '', frames[0])
    name = 'other'
    for rule, rx in RULES:
        if any(re.search(rx, f) for f in frames):
            name = rule
            break
    by[thread][name] += n
reads = None
start = end = None
try:
    result = json.loads((lab / f'{label}.json').read_text())
    start, end = result.get('steadyStartMs'), result.get('steadyEndMs')
    dumps = sorted((case / 'profile').glob('*.json'), key=lambda p: int(p.stem))
    a = min(dumps, key=lambda p: abs(int(p.stem) - start))
    b = min(dumps, key=lambda p: abs(int(p.stem) - end))
    count = lambda d: sum(s['count'] for s in json.loads(d.read_text())['stages'] if s['stage'] == 'feed.read')
    reads = (count(b) - count(a)) / ((int(b.stem) - int(a.stem)) / 1e3)
except Exception:
    pass
print(f'# {label}: {cores:.3f} cores (cgroup), {total} samples' + (f', {reads:.2f} transactions/s' if reads else ''))
for thread, counter in sorted(by.items(), key=lambda kv: -sum(kv[1].values())):
    t = sum(counter.values())
    if t / total < 0.01:
        continue
    tc = cores * t / total
    print(f'{thread}: {tc:.3f} cores' + (f' ({1000 * tc / reads:.2f} ms CPU per transaction)' if reads else ''))
    for name, n in counter.most_common():
        c = cores * n / total
        if n / total >= 0.002:
            print(f'    {name:34s} {c:.3f} cores' + (f'  {1000 * c / reads:6.2f} ms/txn' if reads else ''))
