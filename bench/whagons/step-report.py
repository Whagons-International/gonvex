#!/usr/bin/env python3
"""Markdown section for docs/performance/step-log.md from one run-remote-case.sh case.

    step-report.py LAB LABEL [--profile-out FILE]

Reads LAB/LABEL.json (run.ts merged result or paths.ts result), LAB/LABEL/host.jsonl
(runtime cgroup, processes, proxy/tunnel cgroups), LAB/LABEL.json.mac-host.jsonl and
.rtt.log (generator host), LAB/LABEL/profile/*.json (/dev/profile dumps). Prints
the section; with --profile-out writes `top.py --delta` over the dumps that
bracket the plateau."""
import json, pathlib, re, subprocess, sys

lab, label = pathlib.Path(sys.argv[1]), sys.argv[2]
profile_out = sys.argv[sys.argv.index('--profile-out') + 1] if '--profile-out' in sys.argv else None
case = lab / label
r = json.loads((lab / f'{label}.json').read_text())
paths = 'scenario' in r
start, end = r.get('steadyStartMs'), r.get('steadyEndMs')


def lines(p):
    return [json.loads(x) for x in p.read_text().splitlines() if x.startswith('{')] if p.exists() else []


def q(xs, f):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(f * len(xs)))] if xs else None


def ms(v):
    return '–' if v is None else f'{v:,.0f}' if v >= 100 else f'{v:.1f}'


out = []
w = out.append
commit = (case / 'commit').read_text().strip() if (case / 'commit').exists() else '?'
bins = (case / 'binaries.sha256').read_text().split('\n') if (case / 'binaries.sha256').exists() else []
runtime_sha = next((b.split()[0] for b in bins if b.endswith('gonvex-runtime')), '?')
w(f'- commit `{commit[:10]}`, runtime binary sha256 `{runtime_sha[:16]}…`')
limits = (case / 'runtime-limits').read_text().split() if (case / 'runtime-limits').exists() else ['0', '0']
env = dict(l.split('=', 1) for l in (case / 'runtime-env').read_text().splitlines() if '=' in l) if (case / 'runtime-env').exists() else {}
w(f'- runtime container: {int(limits[0]) / 1e9:g} vCPU / {int(limits[1]) / 2**30:g} GiB; '
  + ', '.join(f'{k}={v}' for k, v in sorted(env.items()) if k in ('GONVEX_SQLITE_SYNCHRONOUS', 'GONVEX_DB_MAX_OPEN_CONNS', 'GONVEX_SQLITE_CACHE_KIB', 'GONVEX_REPLICA_SNAPSHOT_CONCURRENCY') or 'PRESENCE' in k or 'CHECKPOINT' in k or 'IN_PROCESS' in k or 'CACHE_BUDGET' in k))
if paths:
    w(f"- scenario `{r['scenario']}` at {r['offered']['ratePerSecond']}/s offered, {r['offered']['planned']} planned, "
      f"{r['offered']['achievedPerSecond']:.2f}/s completed; users {r['users']['connected']}/{r['users']['requested']}; "
      f"overflow {r['overflow']}; error rate {r['errorRate']:.2%}; **{'PASS' if r['passed'] else 'FAIL'}**")
    lat = r['latencyMs']
else:
    c = r.get('client', {})
    w(f"- client {c.get('kind')} ({c.get('rows')} rows), SDK users {c.get('sdkUsers')}, oracle {c.get('oracle')}, schedule {r.get('schedule', {}).get('kind')}, "
      f"shards {r.get('shards', 1)}; users {r.get('connectedUsers')}/{r.get('targetUsers')}; error rate {r.get('errorRate', 0):.2%}; "
      f"visibility checks {r.get('visibilityChecks')}; thin unsupported {c.get('thin', {}).get('unsupported')}; "
      f"**{'PASS' if r.get('clientSloPassed') else 'FAIL'}** (failing kinds: {r.get('failingKinds')})")
    lat = r.get('steadyLatencyMs') or r.get('latencyMs')
w('')
w('| kind | samples | p50 ms | p95 ms | p99 ms |')
w('|---|---:|---:|---:|---:|')
for k, v in lat.items():
    if v.get('samples'):
        w(f"| {k} | {v['samples']} | {ms(v.get('p50'))} | {ms(v.get('p95'))} | {ms(v.get('p99'))} |")
errs = r.get('errors') or {}
if errs:
    w('')
    w('Errors: ' + '; '.join(f'{n}× {k[:140]}' for k, n in sorted(errs.items(), key=lambda x: -x[1])[:6]))
# Runtime cgroup over the plateau.
host = lines(case / 'host.jsonl')
plateau = [x for x in host if start and start <= x['at'] * 1000 < end] or host


def cpu(x, key='runtime'):
    t = x.get(key, '').splitlines()
    return ({a: int(b) for a, b in (l.split() for l in t[:-1])}, int(t[-1])) if len(t) > 1 else None


samples = [(x['at'], cpu(x)) for x in plateau if cpu(x)]
w('')
if len(samples) > 1:
    (t0, (a, _)), (t1, (b, _)) = samples[0], samples[-1]
    secs = t1 - t0
    mem = [m for _, (_, m) in samples]
    peak_all = max(c[1] for c in (cpu(x) for x in host) if c)
    anon = [x['memoryStat']['runtime']['anon'] for x in plateau if x.get('memoryStat', {}).get('runtime')]
    periods = b['nr_periods'] - a['nr_periods']
    w(f"- runtime cgroup: CPU {(b['usage_usec'] - a['usage_usec']) / 1e6 / secs:.2f} cores (user {(b['user_usec'] - a['user_usec']) / 1e6 / secs:.2f}, "
      f"system {(b['system_usec'] - a['system_usec']) / 1e6 / secs:.2f}), throttled {b['nr_throttled'] - a['nr_throttled']}/{periods} periods, "
      f"{(b['throttled_usec'] - a['throttled_usec']) / 1e6:.1f} s throttled over {secs:.0f} s; memory current {mem[-1] / 2**20:,.0f} MiB, "
      f"plateau peak {max(mem) / 2**20:,.0f} MiB, whole-case peak {peak_all / 2**20:,.0f} MiB, anon peak {max(anon) / 2**20:,.0f} MiB")
    procs = {}
    for x in plateau:
        for p in x.get('runtimeProcesses', {}).values():
            procs.setdefault(p['command'], []).append(p.get('RssBytes', 0))
    w('- process RSS (plateau max): ' + ', '.join(f'{k} {max(v) / 2**20:,.0f} MiB' for k, v in procs.items()))
    aux0, aux1 = plateau[0].get('serverAuxiliary', {}), plateau[-1].get('serverAuxiliary', {})
    parts = []
    for name, v in aux1.items():
        if name in aux0:
            label_ = 'proxy container' if name.startswith('docker-') else 'ssh tunnel'
            rss = sum(p.get('Rss:', 0) for p in v['processes'].values())
            parts.append(f"{label_} {(v['cpu']['usage_usec'] - aux0[name]['cpu']['usage_usec']) / 1e6 / secs:.3f} cores, RSS {rss / 2**20:.0f} MiB")
    if parts:
        w('- arch3 transport: ' + '; '.join(parts))
    w(f"- host memory PSI full max {max(x['memory_full_avg10'] for x in plateau):.2f}%, io PSI full mean {sum(x.get('io_full_avg10', 0) for x in plateau) / len(plateau):.2f}%")
# Generator host.
mac = lines(lab / f'{label}.json.mac-host.jsonl')
if mac:
    def cputime(s):
        p = [float(x) for x in re.split('[:]', s)]
        return p[0] * 60 + p[1] if len(p) == 2 else p[0] * 3600 + p[1] * 60 + p[2]
    rss = [sum(int(p['rssKiB']) for p in x.get('processes', [])) * 1024 for x in mac if x.get('processes')]
    c0 = [x for x in mac if x.get('processes')]
    tot = lambda x: sum(cputime(p['cpuTime']) for p in x['processes'])
    pressure = [re.search(r'free percentage: (\d+)%', x.get('memoryPressure', '')) for x in mac]
    free = [int(m.group(1)) for m in pressure if m]
    w(f"- generator (Mac, {len(c0[-1]['processes'])} processes): CPU {(tot(c0[-1]) - tot(c0[0])) / (c0[-1]['at'] - c0[0]['at']):.2f} cores avg over the case, "
      f"peak RSS {max(rss) / 2**20:,.0f} MiB, system memory free min {min(free) if free else '?'}%")
gen = r.get('generatorCost') or r.get('generator')
if gen:
    w(f"- generator self-report: {gen.get('cpuSeconds', 0):.0f} CPU s, peak RSS {(gen.get('peakRssBytes') or gen.get('peakRssBytesSum') or 0) / 2**20:,.0f} MiB"
      + (f", event-loop p99 {gen['eventLoopP99Ms']:.0f} ms" if 'eventLoopP99Ms' in gen else ''))
rtt = [float(x) for x in re.findall(r'time=([\d.]+)', (lab / f'{label}.json.rtt.log').read_text())] if (lab / f'{label}.json.rtt.log').exists() else []
if rtt:
    w(f'- RTT Mac→arch3 (ping, Tailscale): p50 {q(rtt, .5):.1f} ms, p95 {q(rtt, .95):.1f} ms, max {max(rtt):.1f} ms ({len(rtt)} samples)')
offsets = []
for name in ('before', 'after'):
    f = case / f'clock-offset-{name}.json'
    if f.exists():
        try:
            o = json.loads(f.read_text())
            offsets.append(f"{name} {o['offsetMs']:+.1f} ms (rtt {o['rttMs']:.1f} ms)")
        except (ValueError, KeyError):
            pass
if offsets:
    w('- clock offset Mac − arch3: ' + ', '.join(offsets))
# Sockets vs authenticated accounts at the peak (a transport retry storm shows as
# far more sockets than users).
dm = case / 'dev-metrics.jsonl'
if dm.exists():
    peak = None
    for line in dm.read_text().splitlines():
        try:
            ws = json.loads(line)['metrics'].get('websocket') or {}
        except ValueError:
            continue
        if peak is None or (ws.get('connections') or 0) > (peak.get('connections') or 0):
            peak = ws
    if peak:
        ratio = (peak.get('connections') or 0) / max(1, peak.get('accounts') or 0)
        w(f"- runtime sockets at peak: {peak.get('connections')} for {peak.get('accounts')} accounts, {peak.get('subscriptions')} subscriptions"
          + (f" (**{ratio:.2f} sockets per account: transport retries; results not comparable**)" if ratio > 1.1 else ''))
# Profile delta over the plateau.
dumps = sorted((case / 'profile').glob('*.json'), key=lambda p: int(p.stem)) if (case / 'profile').exists() else []
if dumps and start:
    a = min(dumps, key=lambda p: abs(int(p.stem) - start))
    b = min(dumps, key=lambda p: abs(int(p.stem) - end))
    if a != b:
        top = pathlib.Path(__file__).resolve().parent.parent / 'profiler' / 'top.py'
        text = subprocess.run([sys.executable, str(top), '--delta', str(a), str(b)], capture_output=True, text=True).stdout
        if profile_out:
            pathlib.Path(profile_out).write_text(text)
        w(f'- /dev/profile delta {a.name} → {b.name}' + (f' saved as `{pathlib.Path(profile_out).name}`' if profile_out else ''))
for name in ('oncpu.svg', 'offcpu.svg'):
    if (case / name).exists():
        w(f'- flamegraph: `{label}/{name}`')
print('\n'.join(out))
