#!/usr/bin/env python3
"""Read private traces locally. Emit aggregate counts only, never identifiers or rows."""
import argparse, collections, csv, datetime, json, statistics
p=argparse.ArgumentParser();p.add_argument('csv');a=p.parse_args()
rows=list(csv.DictReader(open(a.csv)))
for r in rows:r['time']=datetime.datetime.fromisoformat(r['created_at'].replace('Z','+00:00')).timestamp()
accounts=collections.defaultdict(list); tenants=collections.defaultdict(set)
for r in rows:
 accounts[r['account_id']].append(r);tenants[r['tenant_id']].add(r['account_id'])
gaps={}
selected={'taskResources.recordTaskViewByTaskId','workspacePreferences.save','taskResources.addTagByTaskId','tasks.start','tasks.create','tasks.requestTransition','taskResources.createNote'}
foreground=[];active_seconds=0
for rs in accounts.values():
 ts=sorted(r['time'] for r in rs if r['path'] in selected)
 foreground += [b-a for a,b in zip(ts,ts[1:]) if 0<b-a<=1800]
 end=0
 for a,b in sorted((r['time'],r['time']+30) for r in rs if r['path']=='presence.beat'):
  active_seconds+=max(0,b-max(a,end));end=max(end,b)
quantiles={str(p):sorted(foreground)[int((len(foreground)-1)*p/100)] for p in [25,50,75,95]}

for path in ['presence.beat','control.support.heartbeat']:
 ds=[]
 for rs in accounts.values():
  ts=sorted(r['time'] for r in rs if r['path']==path)
  ds += [b-a for a,b in zip(ts,ts[1:]) if 0<b-a<600]
 gaps[path]={'samples':len(ds),'median_seconds':statistics.median(ds) if ds else None}
bins=collections.Counter(int(r['time']//300) for r in rows)
print(json.dumps({'foreground_gap_quantiles_seconds':quantiles,'foreground_gap_samples':len(foreground),'observed_heartbeat_person_minutes':active_seconds/60,'foreground_calls_per_observed_person_minute':sum(r['path'] in selected for r in rows)/(active_seconds/60),'peak_5min_traces':max(bins.values()),'mean_5min_traces':len(rows)/((max(r['time'] for r in rows)-min(r['time'] for r in rows))/300),'traces':len(rows),'accounts':len(accounts),'tenants':len(tenants),'observed_accounts_per_tenant_desc':sorted(map(len,tenants.values()),reverse=True),'path_counts':dict(collections.Counter(r['path'] for r in rows)),'heartbeat_gaps':gaps,'warning':'Observed traces do not establish complete traffic rates or concurrent users.'},indent=2))
