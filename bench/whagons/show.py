#!/usr/bin/env python3
"""Prints measured cases: show.py LAB LABEL..."""
import json,pathlib,sys
lab=pathlib.Path(sys.argv[1])
# Fixed-load latency targets (at a comfortable load, e.g. 100 users): local work
# is ~10-50 ms, so anything far above is queueing. Not part of the capacity rule.
TARGETS={'save':(50,100),'reducer':(50,100),'delivery':(100,None),'taskDelivery':(100,None),'heartbeat':(20,None)}
INITIAL_LOAD_TARGET_MS=1000
for label in sys.argv[2:]:
    d=json.load(open(lab/f'{label}-summary.json'))[0]
    r=json.load(open(lab/f'{label}.json')) if (lab/f'{label}.json').exists() else {}
    l=d.get('latencyMs',{})
    print(label, {k:d.get(k) for k in ['users','target','errors','capacityPassed','memoryStopped','runtimeErrorLogLines']},
          'errorRate=%s failing=%s' % (r.get('errorRate'), r.get('failingKinds')))
    for k in ['save','heartbeat','reducer','query','initialLoad','delivery','taskDelivery','login']:
        v=l.get(k) or {}
        print('  ',k, {q:(round(v[q]) if isinstance(v.get(q),(int,float)) else v.get(q)) for q in ['samples','p50','p95','p99']})
    misses=[]
    for k,(p95,p99) in TARGETS.items():
        v=l.get(k) or {}
        if v.get('p95') is None or v['p95']>=p95 or (p99 and (v.get('p99') or 0)>=p99):
            misses.append('%s p95=%s/%s p99=%s/%s'%(k,v.get('p95') and round(v['p95']),p95,v.get('p99') and round(v['p99']),p99))
    print('   fixed-load targets:', 'met' if not misses else 'missed: '+'; '.join(misses))
    whole=d.get('wholePhaseLatencyMs',{}).get('initialLoad') or {}
    if whole.get('p95') is not None:
        print('   initial load p50/p95/p99 %s/%s/%s ms (target p95 < %d ms)'%(round(whole['p50']),round(whole['p95']),round(whole['p99']),INITIAL_LOAD_TARGET_MS))
    for name in ['runtime','generator']:
        x=d.get(name) or {}
        print('   %s cores %.2f peak %dMB' % (name, x.get('userCores',0)+x.get('systemCores',0), x.get('peakMemoryBytes',0)/1e6))
    if d.get('runtimePeakAnonBytes'):
        print('   runtime peak anon %dMB (heap; the cgroup peak includes page cache)' % (d['runtimePeakAnonBytes']/1e6))
    if r.get('errors'):
        print('   top errors', sorted(r['errors'].items(),key=lambda e:-e[1])[:3])
