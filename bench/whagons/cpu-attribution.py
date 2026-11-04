#!/usr/bin/env python3
"""Inclusive CPU attribution from a frame-pointer on-CPU capture (<case>/oncpu.folded).

    cpu-attribution.py CASEDIR [--cores N] [--json OUT]

Each sampled stack goes to the first category whose rule matches any of its frames
(rules in priority order), then to the first matching sub-rule of that category.
Cross-cutting columns (kernel, allocator) are reported per category: kernel = frames
from the syscall/interrupt entry down, allocator = malloc/free/realloc leaf work.
With --cores (the runtime cgroup's measured cores over the same plateau) shares are
also printed as cores. Needs a runtime built with -C force-frame-pointers=yes and
PERF_CALLGRAPH=fp (DWARF unwinding stops after one frame on these binaries)."""
import collections, json, re, sys, pathlib

SQLITE = r'sqlite3[A-Z_a-z]|libsqlite3'
KERNEL_ENTRY = re.compile(r'^(entry_SYSCALL|asm_|ret_from_fork|irq_|__irqentry|do_syscall)')
ALLOC = re.compile(r'^(malloc|free|realloc|calloc|_int_malloc|_int_free|__libc_malloc|__libc_free|cfree|mi_|je_|tcache|alloc::alloc::|__rust_alloc|__rust_dealloc|__rdl_)')

CATEGORIES = [
    ('snapshot open', r'open_replica|enqueue_replica_open|collection_rows|snapshot_cache|scan_window|ReplicaOpenQueue|cached_view|prepare_replica|raw_log::|snapshot_raw', [
        ('SQLite reads', SQLITE),
        ('visibility / ACL', r'visibility|acl|principal'),
        ('row encode / JSON', r'serde_json|write_stable|project_row|encode'),
    ]),
    ('delivery: feed apply', r'apply_feed_event|apply_feed_events|merge_waiting_feed|take_waiting_feed|collapse_changes|deliver_committed|fan_out|replica_fanout|feed_share', [
        ('shared fan-out path (shape outcome, network thread)', r'::fan_out$|fan_out_inner|fan_out_waiting|replica::fanout::|FanOut'),
        ('copy merge', r'merge_waiting_feed|merge_transactions'),
        ('visibility decisions', r'principal_change_concerns|active_grant_sets|derived_change_concerns|visibility::'),
        ('row projection / encode / prewarm', r'project_row|write_stable|prewarm|serde_json|encode'),
        ('SQLite reads (delivery lookups)', SQLITE),
    ]),
    ('delivery: socket write', r'write_loop|write_text|poll_write_buffer|compress|deflate|gonvex_websocket::.*Encode|send_json|ReplicaBatcher|batcher|flush_batch|tungstenite.*write|send_connection_json', [
        ('kernel send (tcp/syscalls)', r'tcp_sendmsg|sock_sendmsg|__x64_sys_(sendto|write|writev)|sys_sendmsg'),
        ('compression', r'compress|deflate|miniz|flate2|zlib'),
        ('JSON encode', r'serde_json|encode|Encode'),
    ]),
    ('presence', r'presence', []),
    ('reducers / actions (V8)', r'gonvex_module_runtime_v8|execute_tenant_reducer|module_host|in_process|gonvex_module_runtime', [
        ('SQL host calls', SQLITE + r'|host_calls|db_read|db_write'),
        ('JS execution', r'v8::|Builtins_|JSFunction|Isolate'),
    ]),
    ('scheduled functions (scheduler)', r'gonvex_runtime::schedul|gonvex_runtime::cron|run_scheduled|scheduled_lane', []),
    ('outbox drainer', r'outbox', []),
    ('commit / checkpoint / storage', r'commit_with|checkpoint|sync_barrier|TenantTransaction|finalize_direct|wal_', [
        ('fsync / kernel IO', r'fsync|fdatasync|ext4|btrfs|blk_'),
        ('SQLite', SQLITE),
    ]),
    ('websocket inbound / session / auth', r'gonvex_runtime::websocket|axum::serve|hyper|tungstenite|revalidat|session', [
        ('kernel recv / epoll', r'tcp_recvmsg|sock_recvmsg|__x64_sys_(recvfrom|read)|ep_poll|epoll'),
        ('session revalidation / auth', r'revalidat|verify|jwt|jsonwebtoken|auth'),
        ('frame decode / JSON', r'serde_json|decode|Decode|parse'),
    ]),
]
FALLBACKS = [
    ('SQLite worker threads (caller not on stack)', lambda th, fr: th.startswith('sqlx-sqlite'), [
        ('statement prepare (cache misses / dynamic SQL)', r'sqlite3_prepare|sqlite3LockAndPrepare'),
        ('statement execution', r'sqlite3VdbeExec|sqlite3_step'),
        ('connection open / close', r'sqlite3Close|openDatabase|sqlite3_open'),
        ('page cache / IO', r'Pager|pcache|unixRead|pread|filemap|__x64_sys_pread'),
        ('other SQLite (prepare, finalize, expr)', SQLITE),
        ('worker channel wakeups (futex, flume)', r'futex|flume|crossbeam|park|schedule'),
    ]),
    ('V8 platform workers (GC, compile)', lambda th, fr: th.startswith('V8_'), []),
    ('runtime idle / wakeups (tokio park, epoll, futex)', lambda th, fr: th.startswith(('tokio-rt', 'gonvex-offload')) and not any('gonvex' in f for f in fr), [
        ('epoll / futex / scheduler (kernel)', r'epoll|futex|schedule|switch'),
    ]),
    ('profiler (own work: leaf frames)', lambda th, fr: any('gonvex_profiler' in f for f in fr[-3:]), []),
]

args = sys.argv[1:]
case = pathlib.Path(args[0])
cores = float(args[args.index('--cores') + 1]) if '--cores' in args else None
out_json = args[args.index('--json') + 1] if '--json' in args else None

tot = 0
cat = collections.Counter(); sub = collections.defaultdict(collections.Counter)
kern = collections.Counter(); alloc = collections.Counter(); threads = collections.defaultdict(collections.Counter)
compiled = [(n, re.compile(r), [(sn, re.compile(sr)) for sn, sr in subs]) for n, r, subs in CATEGORIES]
fallbacks = [(n, f, [(sn, re.compile(sr)) for sn, sr in subs]) for n, f, subs in FALLBACKS]
for line in (case / 'oncpu.folded').read_text().splitlines():
    stack, n = line.rsplit(' ', 1); n = int(n); tot += n
    frames = stack.split(';'); thread = frames[0]
    name, subs = 'other', []
    for cn, rx, cs in compiled:
        if any(rx.search(f) for f in frames):
            name, subs = cn, cs; break
    else:
        for cn, test, cs in fallbacks:
            if test(thread, frames):
                name, subs = cn, cs; break
    cat[name] += n; threads[name][re.sub(r'[-_]?\d+$', '', thread)] += n
    tail = frames
    sname = 'rest'
    for sn, sr in subs:
        if any(sr.search(f) for f in tail):
            sname = sn; break
    sub[name][sname] += n
    if any(KERNEL_ENTRY.search(f) for f in frames): kern[name] += n
    if ALLOC.search(frames[-1]) or (len(frames) > 1 and ALLOC.search(frames[-2])): alloc[name] += n

def fmt(v):
    s = f'{100*v/tot:5.1f} %'
    return s + (f'  {cores*v/tot:6.3f} cores' if cores else '')
result = {'case': str(case), 'samples': tot, 'cores': cores, 'categories': []}
for k, v in cat.most_common():
    th = ', '.join(f'{t} {100*c/v:.0f}%' for t, c in threads[k].most_common(3))
    print(f'{fmt(v)}  {k}   [kernel {100*kern[k]/v:.0f}%, alloc {100*alloc[k]/v:.0f}%; {th}]')
    entry = {'name': k, 'share': v / tot, 'kernel': kern[k] / v, 'alloc': alloc[k] / v, 'subs': {}}
    if len(sub[k]) > 1 or 'rest' not in sub[k]:
        for sk, sv in sub[k].most_common():
            print(f'      {fmt(sv)}  {sk}')
            entry['subs'][sk] = sv / tot
    result['categories'].append(entry)
print(f'kernel overall {100*sum(kern.values())/tot:.1f} %, allocator overall {100*sum(alloc.values())/tot:.1f} %')
if out_json:
    pathlib.Path(out_json).write_text(json.dumps(result, indent=1))
