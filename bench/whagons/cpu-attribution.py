#!/usr/bin/env python3
"""Inclusive CPU attribution from a frame-pointer on-CPU capture (<case>/oncpu.folded).

    cpu-attribution.py CASEDIR

Each sampled stack goes to the first category whose rule matches any of its frames
(rules in priority order). Needs a runtime built with -C force-frame-pointers=yes and
PERF_CALLGRAPH=fp (DWARF unwinding stops after one frame on these binaries)."""
import collections, re, sys, pathlib

RULES = [
    ('snapshot open (build, cache, visibility)', r'open_replica|enqueue_replica_open|collection_rows|snapshot_cache|scan_window|ReplicaOpenQueue|cached_view|prepare_replica'),
    ('delivery: feed apply per connection', r'apply_feed_event|merge_waiting_feed|collapse_changes|deliver_'),
    ('delivery: socket encode/compress/write', r'write_loop|write_text|poll_write_buffer|compress|gonvex_websocket::.*Encode|send_json|batcher.*flush|ReplicaBatcher'),
    ('presence', r'presence'),
    ('reducers/actions (V8 + inline SQL)', r'gonvex_module_runtime_v8|execute_tenant_reducer|module_host|in_process'),
    ('scheduled functions (scheduler)', r'gonvex_runtime::schedul|gonvex_runtime::cron|run_scheduled|gonvex_runtime::scheduled_lane'),
    ('outbox drainer', r'outbox'),
    ('commit / checkpoint / storage', r'commit_with|checkpoint|sync_barrier|TenantTransaction'),
    ('websocket inbound / auth / other socket', r'gonvex_runtime::websocket|axum::serve|hyper'),
]
case = pathlib.Path(sys.argv[1])
cat = collections.Counter(); tot = 0; by_thread = collections.defaultdict(collections.Counter)
for line in (case / 'oncpu.folded').read_text().splitlines():
    stack, n = line.rsplit(' ', 1); n = int(n); tot += n
    frames = stack.split(';'); thread = frames[0]
    name = 'other'
    for label, rx in RULES:
        if any(re.search(rx, f) for f in frames):
            name = label; break
    if name == 'other' and any(re.search(r'gonvex_profiler', f) for f in frames[-3:]):
        name = 'profiler (own work: leaf frames)'
    if name == 'other' and thread.startswith('sqlx-sqlite'):
        name = 'SQLite worker threads (caller not on stack)'
    cat[name] += n; by_thread[name][thread] += n
for k, v in cat.most_common():
    threads = ', '.join(f'{t} {100*c/v:.0f}%' for t, c in by_thread[k].most_common(3))
    print(f'{100*v/tot:5.1f} %  {k}   [{threads}]')
