#!/usr/bin/env python3
"""Delivery coverage over a case's plateau (docs/performance/delivery-coverage.md).

    coverage-report.py LAB LABEL

Reads LAB/LABEL.json (steadyStartMs / steadyEndMs) and the /dev/profile dumps
in LAB/LABEL/profile/, takes the two dumps nearest the plateau's ends, and
prints per connection-and-transaction how deliveries were decided
(`deliveryCoverage`: shared fan-out or the connection's own full path, and
why), with the per-connection time of both paths (`delivery.shared`,
`delivery.individual`), the delivery stages and the transactions read
(`feed.read`) over the same window. Builds without `deliveryCoverage` print
the stages only."""
import json, pathlib, sys

lab, label = pathlib.Path(sys.argv[1]), sys.argv[2]
result = json.loads((lab / f'{label}.json').read_text())
start, end = result.get('steadyStartMs'), result.get('steadyEndMs')
dumps = sorted((lab / label / 'profile').glob('*.json'), key=lambda p: int(p.stem))
if not dumps or not start:
    sys.exit('no profile dumps or plateau')
a = min(dumps, key=lambda p: abs(int(p.stem) - start))
b = min(dumps, key=lambda p: abs(int(p.stem) - end))
A, B = json.loads(a.read_text()), json.loads(b.read_text())
seconds = (int(b.stem) - int(a.stem)) / 1e3


def stages(d):
    out = {}
    for s in d['stages']:
        c, n = out.get(s['stage'], (0, 0))
        out[s['stage']] = (c + s['count'], n + s['sumNs'])
    return out


sa, sb = stages(A), stages(B)


def delta(name):
    c0, n0 = sa.get(name, (0, 0))
    c1, n1 = sb.get(name, (0, 0))
    return c1 - c0, n1 - n0


print(f'# {label}: plateau {a.stem} -> {b.stem} ({seconds:.0f} s)')
reads, _ = delta('feed.read')
print(f'- transactions read (feed.read): {reads} ({reads / seconds:.2f}/s)')
for name in ('delivery.visibility', 'delivery.send', 'delivery.shared', 'delivery.individual', 'delivery.noop', 'offload.busy', 'ws.compress', 'ws.write'):
    count, ns = delta(name)
    if count:
        print(f'- {name}: {count} ({count / seconds:.0f}/s), {ns / 1e9 / seconds:.3f} cores, {ns / count / 1e3:.1f} us each'
              + (f', {ns / reads / 1e6:.2f} ms per transaction' if reads else ''))
cov_a, cov_b = A.get('deliveryCoverage'), B.get('deliveryCoverage')
if cov_a and cov_b:
    before = {p['path']: p['count'] for p in cov_a['paths']}
    paths = [(p['path'], p['count'] - before.get(p['path'], 0)) for p in cov_b['paths']]
    total = sum(c for _, c in paths)
    shared = sum(c for p, c in paths if p.startswith('shared.'))
    print(f'- connection deliveries decided: {total} ({total / seconds:.0f}/s'
          + (f', {total / reads:.1f} per transaction' if reads else '') + f'); shared {shared} ({shared / max(total, 1):.1%})')
    if 'wakeups' in cov_b:
        wakeups = cov_b['wakeups'] - cov_a.get('wakeups', 0)
        noop = cov_b['noopWakeups'] - cov_a.get('noopWakeups', 0)
        print(f'- batch connection wakeups for transactions: {wakeups} ({wakeups / seconds:.0f}/s'
              + (f', {wakeups / reads:.1f} per transaction' if reads else '')
              + f'); with nothing but a watermark to send (no-op): {noop} ({noop / max(wakeups, 1):.1%})')
    print()
    print('| path | count | share |')
    print('|---|---:|---:|')
    for path, count in sorted(paths, key=lambda x: -x[1]):
        if count:
            print(f'| {path} | {count} | {count / max(total, 1):.1%} |')
