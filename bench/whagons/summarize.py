#!/usr/bin/env python3
"""Summarize local phase telemetry; absent, partial or stopped phases never pass.

Latency and host samples are scoped to the steady plateau (every user
connected); whole-phase latency is reported alongside."""
import datetime, json, os, pathlib, re, subprocess, sys

PREFIX = os.environ.get('LOAD_CONTAINER_PREFIX', 'gvsq-')
root = pathlib.Path(sys.argv[1])
output = []


def loadlines(path):
    if not path.exists():
        return []
    out = []
    for line in path.read_text().splitlines():
        if line.startswith('{'):
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return out


def utc(seconds):
    return datetime.datetime.fromtimestamp(seconds, datetime.timezone.utc).isoformat()


for phase in sys.argv[2:]:
    d = root / phase
    result = root / (phase + '.json')
    r = {}
    for candidate in [result, pathlib.Path(str(result) + '.partial.json')]:
        try:
            value = json.loads(candidate.read_text())
            if isinstance(value, dict):
                r = value
                break
        except (OSError, json.JSONDecodeError):
            pass
    host = loadlines(d / 'host.jsonl')
    whole_host = host[:]
    stats = {
        'phase': phase, 'users': r.get('connectedUsers'), 'target': r.get('targetUsers'),
        'errors': sum(r.get('errors', {}).values()) if r else None,
        'errorRate': r.get('errorRate'), 'failingKinds': r.get('failingKinds'),
        'latencyMs': r.get('steadyLatencyMs', r.get('latencyMs', {})),
        'wholePhaseLatencyMs': r.get('latencyMs', {}),
        'latencyScope': r.get('qualificationLatencyScope', 'whole sign-in burst and plateau'),
        'capacityPassed': r.get('clientSloPassed', r.get('capacityPassed', False)),
        'memoryStopped': (d / 'STOPPED_MEMORY_PRESSURE').exists(),
    }
    if r.get('steadyStartMs'):
        host = [x for x in host if r['steadyStartMs'] <= x['at'] * 1000 < r['steadyEndMs']]
    for name in ['runtime', 'generator']:
        samples = []
        for x in host:
            lines = x.get(name, '').splitlines()
            if len(lines) > 1:
                fields = {a: int(b) for a, b in (line.split() for line in lines[:-1])}
                samples.append((x['at'], fields, int(lines[-1])))
        if len(samples) > 1:
            a, b = samples[0], samples[-1]
            seconds = b[0] - a[0]
            stats[name] = {
                'userCores': (b[1]['user_usec'] - a[1]['user_usec']) / 1e6 / seconds,
                'systemCores': (b[1]['system_usec'] - a[1]['system_usec']) / 1e6 / seconds,
                'peakMemoryBytes': max(x[2] for x in samples),
            }
    if whole_host:
        anon = [x['memoryStat']['runtime']['anon'] for x in whole_host if x.get('memoryStat', {}).get('runtime')]
        if anon:
            stats['runtimePeakAnonBytes'] = max(anon)
        # Peak memory over the whole phase (sign-in burst included).
        peaks = [int(x['runtime'].splitlines()[-1]) for x in whole_host if x.get('runtime', '').count('\n')]
        if peaks:
            stats['runtimePeakMemoryBytesWholePhase'] = max(peaks)
    if host:
        stats['hostMemoryFullPsiMax'] = max(x['memory_full_avg10'] for x in host)
        stats['hostIoFullPsiMean'] = sum(x.get('io_full_avg10', 0) for x in host) / len(host)
        stats['oomKills'] = {
            name: max((x.get('memoryEvents', {}).get(name, {}).get('oom_kill', 0) for x in whole_host), default=0)
            - whole_host[0].get('memoryEvents', {}).get(name, {}).get('oom_kill', 0)
            for name in ['runtime', 'generator']}
        if any(stats['oomKills'].values()):
            stats['capacityPassed'] = False
        # Unsharded runs write <phase>.json.metrics.jsonl; sharded runs write
        # it from shard 0 only.
        candidates = [root / (phase + '.json.metrics.jsonl'), root / (phase + '.json.shard-0.metrics.jsonl'),
                      root / 'fixture.json.metrics.jsonl']
        metrics = loadlines(next((c for c in candidates if c.exists()), candidates[-1]))
        metrics = [x for x in metrics if host[0]['at'] * 1000 <= x['at'] <= host[-1]['at'] * 1000
                   and isinstance(x.get('metrics'), dict) and 'websocket' in x['metrics'] and 'database' in x['metrics']]
        if r.get('steadyStartMs') and not metrics:
            stats['capacityPassed'] = False
        if metrics:
            steady = [x['metrics']['websocket']['accounts'] for x in metrics]
            stats['steadyAccountSamples'] = len(steady)
            stats['minSteadyAuthenticatedAccounts'] = min(steady)
            # The reconnect wave drops ~30% of sessions on purpose. Outside its
            # window (plus 30 s for those users to sign in and reload), at
            # least 99% of users must be authenticated in every sample (a
            # client re-authenticating its session shows as a momentary dip),
            # and every user at the end. Lost users also fail the client rule.
            target_users = r.get('targetUsers', 0)
            # Older results lack the window; the wave starts at the plateau midpoint.
            middle = (r.get('steadyStartMs', 0) + r.get('steadyEndMs', 0)) / 2
            window = r.get('reconnectWindowMs') or [middle, middle + 10000]
            if r.get('reloadsPerUserHour') is not None:
                window = [float('inf'), float('inf')]  # no stress-wave exemption for realistic reloads
            outside = [x['metrics']['websocket']['accounts'] for x in metrics
                       if not window[0] <= x['at'] <= window[1] + 30000]
            stats['fullyConnectedSampleShare'] = sum(x >= target_users for x in steady) / len(steady)
            stats['disconnectedSamplesOutsideReconnectWave'] = sum(x < target_users for x in outside)
            stats['minConnectedShareOutsideReconnectWave'] = min(outside) / target_users if outside and target_users else None
            if any(x < 0.99 * target_users for x in outside) or steady[-1] < target_users:
                stats['capacityPassed'] = False
            first = metrics[0]['metrics']['database']
            last = metrics[-1]['metrics']['database']
            count = last['waitCount'] - first['waitCount']
            stats['admissionMeanMsBetweenSamples'] = (last['waitDurationMs'] - first['waitDurationMs']) / count if count else None
            stats['admissionMaxMsSinceRuntimeStart'] = last.get('maxWaitDurationMs')
        logs = subprocess.run(['docker', 'logs', '--since', utc(whole_host[0]['at']), '--until', utc(whole_host[-1]['at']),
                               PREFIX + 'runtime'], capture_output=True, text=True)
        stats['runtimeErrorLogLines'] = sum(' ERROR ' in re.sub(r'\x1b\[[0-9;]*m', '', line)
                                            for line in (logs.stdout + logs.stderr).splitlines())
        if logs.returncode or stats['runtimeErrorLogLines']:
            stats['capacityPassed'] = False
    if (stats['memoryStopped'] or not r or r.get('aborted') or r.get('partial') or not host
            or not r.get('steadyStartMs')
            # Shards start their plateaus a few hundred ms apart; allow 2%.
            or r.get('steadyEndMs', 0) - r.get('steadyStartMs', 0) < r.get('steadySeconds', 180) * 980):
        stats['capacityPassed'] = False
    output.append(stats)
print(json.dumps(output, indent=2))
