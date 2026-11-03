#!/usr/bin/env python3
"""Prints measured cases: show.py LAB LABEL..."""
import json,pathlib,sys
lab=pathlib.Path(sys.argv[1])
for label in sys.argv[2:]:
    d=json.load(open(lab/f'{label}-summary.json'))[0]
    r=json.load(open(lab/f'{label}.json')) if (lab/f'{label}.json').exists() else {}
    l=d.get('latencyMs',{})
    print(label, {k:d.get(k) for k in ['users','target','errors','capacityPassed','memoryStopped','runtimeErrorLogLines']},
          'errorRate=%s failing=%s' % (r.get('errorRate'), r.get('failingKinds')))
    for k in ['save','heartbeat','reducer','query','delivery','taskDelivery','login']:
        v=l.get(k) or {}
        print('  ',k, {q:(round(v[q]) if isinstance(v.get(q),(int,float)) else v.get(q)) for q in ['samples','p50','p95','p99']})
    for name in ['runtime','generator']:
        x=d.get(name) or {}
        print('   %s cores %.2f peak %dMB' % (name, x.get('userCores',0)+x.get('systemCores',0), x.get('peakMemoryBytes',0)/1e6))
    if r.get('errors'):
        print('   top errors', sorted(r['errors'].items(),key=lambda e:-e[1])[:3])
