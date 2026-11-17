#!/usr/bin/env python3
"""CPU cost model for one measured case (run-remote-case.sh output).

    cost-model.py LAB LABEL [--json OUT]

Splits the runtime cgroup's CPU over the plateau into work units and divides by
the unit rates measured in the same window:

* CPU per thread group: the on-CPU perf sample (LABEL/oncpu.perf.data, kept with
  PERF_KEEP_DATA=1, frames resolved through LABEL/symfs) gives each thread's share;
  shares are scaled to the cgroup's measured cores over the plateau
  (LABEL/host.jsonl). Thread groups map to work:
    gonvex-offload          snapshot builds (opens) + replica batch item encoding
    tokio-rt-worker         sockets: per-connection delivery, frames, compression
    gonvex-module-isolate   V8: reducers, actions, scheduled functions
    sqlx-sqlite-worker      SQLite reads (snapshot scans, queries) and writer statements
    other                   checkpoint thread, profiler, presence ticker, misc
* Rates from the /dev/profile dumps bracketing the plateau (stage counts and
  per-function invocation counters) and from the generator result (windows,
  sign-ins, reconnects).

Assumptions (stated in the report): offload work splits between snapshot opens and
batch-item encoding by the samples whose symbols name replica batch/encode code;
V8 time splits across functions in proportion to invocations x the per-call cost
measured by bridge_bench (BRIDGE_COST below, ms of CPU per invoke incl. SQL);
SQLite worker time goes to snapshot reads except the writer's share (commit stages).
"""
import collections, glob, json, pathlib, re, subprocess, sys

lab, label = pathlib.Path(sys.argv[1]), sys.argv[2]
out_json = sys.argv[sys.argv.index('--json') + 1] if '--json' in sys.argv else None
case = lab / label
result = json.loads((lab / f'{label}.json').read_text())
start, end = result['steadyStartMs'], result['steadyEndMs']
users = result['targetUsers']

# Per-invoke CPU (ms, runtime incl. inline SQL, no commit) from bridge_bench on the
# largest lab tenant (lab-5k-new/bridge/A1.json): see step-log "Slow reducers".
BRIDGE_COST = {'tasks.update': 1.47, 'tasks.updateDescription': 1.41, 'taskResources.createNote': 1.25,
               'tasks.create': 1.62, 'taskResources.recordTaskViewByTaskId': 0.91}
DEFAULT_REDUCER_MS = 1.4

# ---- profile window -------------------------------------------------------
dumps = sorted(case.glob('profile/*.json'), key=lambda p: int(p.stem))
def at(p): return int(p.stem)
a = min(dumps, key=lambda p: abs(at(p) - start))
b = min(dumps, key=lambda p: abs(at(p) - end))
A, B = json.loads(a.read_text()), json.loads(b.read_text())
seconds = (at(b) - at(a)) / 1000

def stages(d):
    return {(s['stage'], s.get('label') or '-'): (s['count'], s['sumNs']) for s in d['stages']}
SA, SB = stages(A), stages(B)
def delta(stage, lbl='-'):
    c1, s1 = SB.get((stage, lbl), (0, 0)); c0, s0 = SA.get((stage, lbl), (0, 0))
    return c1 - c0, (s1 - s0) / 1e9
def rate(stage, lbl='-'): return delta(stage, lbl)[0] / seconds

def functions(d):
    f = d.get('functions') or {}
    idx = {k: i for i, k in enumerate(f.get('fields', []))}
    return {r['path']: r['values'][idx['invocations']] for r in f.get('rows', [])}
FA, FB = functions(A), functions(B)
calls = {p: (FB.get(p, 0) - FA.get(p, 0)) / seconds for p in FB}

# ---- cgroup CPU over the same window -------------------------------------
host = [json.loads(l) for l in (case / 'host.jsonl').read_text().splitlines() if l.startswith('{')]
def usage(h): return int(re.search(r'usage_usec (\d+)', h['runtime']).group(1))
h0 = min(host, key=lambda h: abs(h['at'] * 1000 - at(a))); h1 = min(host, key=lambda h: abs(h['at'] * 1000 - at(b)))
cores = (usage(h1) - usage(h0)) / 1e6 / (h1['at'] - h0['at'])

# ---- thread split from perf ------------------------------------------------
perf = case / 'oncpu.perf.data'
groups = collections.Counter(); offload_kind = collections.Counter()
if perf.exists():
    report = subprocess.run(['perf', 'report', '-i', str(perf), f'--symfs={case / "symfs"}', '--kallsyms=/proc/kallsyms',
                             '--no-children', '--sort', 'comm,sym', '--stdio', '-g', 'none', '--percent-limit', '0'],
                            capture_output=True, text=True).stdout
    for line in report.splitlines():
        m = re.match(r'\s*([\d.]+)%\s+(\S+)\s+\[(.)\]\s+(.*)', line)
        if not m: continue
        p, comm, sym = float(m.group(1)), m.group(2), m.group(4)
        g = {'gonvex-offload': 'offload', 'tokio-rt-worker': 'tokio', 'gonvex-module-i': 'v8',
             'sqlx-sqlite-wor': 'sqlx'}.get(comm, 'other')
        groups[g] += p
        if g == 'offload':
            if re.search(r'apply_feed_event|CollapsedChange|collapse_changes|principal_change_concerns|delivery::|merge_waiting_feed|replica_batch|ReplicaBatcher|feed::|membership_affects', sym):
                offload_kind['delivery'] += p
            elif re.search(r'snapshot_cache|scan_window|snapshot|intern_row|raw_rows|json_rows|cached_view|collection_rows|open_replica|RawRow|raw_log', sym):
                offload_kind['snapshot'] += p
            else:
                offload_kind['shared'] += p
total_pct = sum(groups.values()) or 1
core = {g: cores * v / total_pct for g, v in groups.items()}
ident = offload_kind['delivery'] + offload_kind['snapshot']
delivery_frac = offload_kind['delivery'] / ident if ident else 0.0
# Unattributed offload samples (hashing, alloc, JSON, visibility) split like the attributed ones.
batch_cores = core.get('offload', 0) * delivery_frac

# ---- unit rates -----------------------------------------------------------
reducer_calls = {p: r for p, r in calls.items() if r > 0}
opens = rate('snapshot.acl')                     # opens that got a permit (did work)
opens_dropped = rate('snapshot.build') - opens   # dropped while waiting (no work)
deliveries = rate('delivery.total')              # per receiving connection
feed_events = rate('feed.read')                  # per shared change (one per tenant tx)
commit_cpu = sum(delta(s, l)[1] for (s, l) in SB if s in ('commit.finalize', 'commit.commit')) / seconds
bg = {s: delta(s)[1] / seconds for s in ('sqlite.checkpoint', 'sqlite.checkpoint_inline', 'v8.gc_minor', 'v8.gc_major', 'mem.malloc_trim')}

# Generator side: windows by kind over the plateau (query samples carry 'at').
shards = [json.loads(p.read_text()) for p in lab.glob(f'{label}.json.shard-[0-9]*') if p.suffix not in ('.log', '.jsonl')]
raw = collections.defaultdict(list)
for s in shards:
    for k, xs in (s.get('rawSamples') or {}).items():
        raw[k] += [x for x in xs if start <= x['at'] < end]
windows = len(raw['query']) / seconds            # detail + workspace/notification/chat windows
reconnects = len(raw['reconnect']) / seconds

# ---- costs ------------------------------------------------------------------
v8 = core.get('v8', 0)
weights = {p: r * BRIDGE_COST.get(p.split('/')[-1], DEFAULT_REDUCER_MS) for p, r in reducer_calls.items()}
wsum = sum(weights.values()) or 1
per_fn = {p: v8 * w / wsum / r * 1000 for p, w in ((p, weights[p]) for p in weights) for r in [reducer_calls[p]]}
snapshot_cores = core.get('offload', 0) - batch_cores + core.get('sqlx', 0) * 0.8
model = {
    'label': label, 'users': users, 'seconds': round(seconds, 1), 'cores': round(cores, 3),
    'threadCores': {g: round(v, 3) for g, v in core.items()},
    'offloadSamples': {k: round(v, 2) for k, v in offload_kind.items()},
    'rates': {'reducerCallsPerS': round(sum(reducer_calls.values()), 2), 'opensPerS': round(opens, 2),
              'opensDroppedPerS': round(max(0, opens_dropped), 2), 'windowsPerS': round(windows, 2),
              'reconnectsPerS': round(reconnects, 2), 'deliveriesPerS': round(deliveries, 1),
              'feedEventsPerS': round(feed_events, 2)},
    'costMs': {
        'perOpen': round(snapshot_cores / opens * 1000, 2) if opens else None,
        'perDeliveryPerConnection': round(core.get('tokio', 0) / deliveries * 1000, 3) if deliveries else None,
        'offloadDeliveryPerDelivery': round(batch_cores / deliveries * 1000, 3) if deliveries else None,
        'offloadDeliveryPerSharedChange': round(batch_cores / feed_events * 1000, 2) if feed_events else None,
        'perReducerByFunction': {p.split('/')[-1]: round(c, 2) for p, c in sorted(per_fn.items(), key=lambda x: -reducer_calls[x[0]])[:12]},
        'commitPerReducer': round(commit_cpu / max(1e-9, rate('fn.total', 'reducer')) * 1000, 2),
    },
    'backgroundMsPerS': {k: round(v * 1000, 1) for k, v in bg.items()},
    'callsPerS': {p.split('/')[-1]: round(r, 3) for p, r in sorted(reducer_calls.items(), key=lambda x: -x[1])[:15]},
}
print(json.dumps(model, indent=1))
if out_json:
    pathlib.Path(out_json).write_text(json.dumps(model, indent=1))
