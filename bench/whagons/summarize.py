#!/usr/bin/env python3
"""Summarize local phase telemetry; absent or stopped phases never pass."""
import os
PREFIX=os.environ.get("LOAD_CONTAINER_PREFIX","gvload-")
import datetime,json,pathlib,sys,subprocess,re
root=pathlib.Path(sys.argv[1]); output=[]
def loadlines(p):
 if not p.exists():return []
 out=[]
 for line in p.read_text().splitlines():
  if line.startswith('{'):
   try:out.append(json.loads(line))
   except json.JSONDecodeError:pass
 return out
for phase in sys.argv[2:]:
 d=root/phase;result=root/(phase+'.json');r={}
 for candidate in [result,pathlib.Path(str(result)+'.partial.json')]:
  try:
   value=json.loads(candidate.read_text())
   if isinstance(value,dict):r=value;break
  except (OSError,json.JSONDecodeError):pass
 pg=loadlines(d/'postgres.jsonl');host=loadlines(d/'host.jsonl');whole_host=host[:]
 stats={'phase':phase,'users':r.get('connectedUsers'),'target':r.get('targetUsers'),'errors':sum(r.get('errors',{}).values()) if r else None,'latencyMs':r.get('steadyLatencyMs',r.get('latencyMs',{})),'wholePhaseLatencyMs':r.get('latencyMs',{}),'latencyScope':r.get('qualificationLatencyScope','whole sign-in burst and plateau'),'capacityPassed':r.get('clientSloPassed',r.get('capacityPassed',False)),'memoryStopped':(d/'STOPPED_MEMORY_PRESSURE').exists()}
 if r.get('steadyStartMs'):
  pg=[x for x in pg if r['steadyStartMs'] <= datetime.datetime.fromisoformat(x['at']).timestamp()*1000 < r['steadyEndMs']]
  host=[x for x in host if r['steadyStartMs'] <= x['at']*1000 < r['steadyEndMs']]
 if pg:
  stats['maxConnections']=max(x['connections'] for x in pg)
  stats['meanConnections']=sum(x['connections'] for x in pg)/len(pg)
  seconds=(datetime.datetime.fromisoformat(pg[-1]['at'])-datetime.datetime.fromisoformat(pg[0]['at'])).total_seconds()
  stats['newConnectionsPerSecond']=(pg[-1]['sessions']-pg[0]['sessions'])/seconds if seconds else None
 for name in ['pg','runtime','generator']:
  samples=[]
  for x in host:
   lines=x.get(name,'').splitlines()
   if len(lines)>1:
    fields={a:int(b) for a,b in (line.split() for line in lines[:-1])}
    samples.append((x['at'],fields,int(lines[-1])))
  if len(samples)>1:
   a,b=samples[0],samples[-1];seconds=b[0]-a[0]
   stats[name]={'userCores':(b[1]['user_usec']-a[1]['user_usec'])/1e6/seconds,'systemCores':(b[1]['system_usec']-a[1]['system_usec'])/1e6/seconds,'peakMemoryBytes':max(x[2] for x in samples)}
 if host:
  stats['hostMemoryFullPsiMax']=max(x['memory_full_avg10'] for x in host)
  stats['hostIoFullPsiMean']=sum(x.get('io_full_avg10',0) for x in host)/len(host)
  stats['oomKills']={name:max((x.get('memoryEvents',{}).get(name,{}).get('oom_kill',0) for x in host),default=0)-host[0].get('memoryEvents',{}).get(name,{}).get('oom_kill',0) for name in ['pg','runtime','generator']}
  if any(stats['oomKills'].values()):stats['capacityPassed']=False
  metrics=loadlines(root/(phase+'.json.metrics.jsonl') if (root/(phase+'.json.metrics.jsonl')).exists() else root/'fixture.json.metrics.jsonl')
  metrics=[x for x in metrics if host[0]['at']*1000<=x['at']<=host[-1]['at']*1000 and isinstance(x.get('metrics'),dict) and 'websocket' in x['metrics'] and 'database' in x['metrics']]
  if r.get('steadyStartMs') and not metrics:stats['capacityPassed']=False
  if metrics:
   steady=[x['metrics']['websocket']['accounts'] for x in metrics if r.get('steadyStartMs',float('inf'))<=x['at']<r.get('steadyEndMs',0)]
   stats['steadyAccountSamples']=len(steady)
   stats['minSteadyAuthenticatedAccounts']=min(steady) if steady else None
   if r.get('steadyStartMs') and (not steady or min(steady)<r.get('targetUsers',0)):stats['capacityPassed']=False
   stats['maxAuthenticatedAccounts']=max(x['metrics']['websocket']['accounts'] for x in metrics)
   first=metrics[0]['metrics']['database'];last=metrics[-1]['metrics']['database'];count=last['waitCount']-first['waitCount']
   stats['admissionMeanMsBetweenSamples']=(last['waitDurationMs']-first['waitDurationMs'])/count if count else None
   stats['admissionMaxMsSinceRuntimeStart']=last.get('maxWaitDurationMs')
  since=datetime.datetime.fromtimestamp(whole_host[0]['at'],datetime.timezone.utc).isoformat()
  until=datetime.datetime.fromtimestamp(whole_host[-1]['at'],datetime.timezone.utc).isoformat()
  logs=subprocess.run(['docker','logs','--since',since,'--until',until,PREFIX+'pg'],capture_output=True,text=True)
  steady_since=datetime.datetime.fromtimestamp(host[0]['at'],datetime.timezone.utc).isoformat()
  steady_until=datetime.datetime.fromtimestamp(host[-1]['at'],datetime.timezone.utc).isoformat()
  steady_logs=subprocess.run(['docker','logs','--since',steady_since,'--until',steady_until,PREFIX+'pg'],capture_output=True,text=True)
  lines=(steady_logs.stdout+steady_logs.stderr).splitlines();duration=host[-1]['at']-host[0]['at']
  databases=[m.group(1) for line in lines if 'gvmac-monitor-' not in line and (m:=re.search(r'connection authorized:.* database=([^ ]+)',line))]
  stats['newApplicationConnectionsPerSecond']=sum(x.startswith('gonvex_') for x in databases)/duration if duration else None
  stats['newHealthConnectionsPerSecond']=sum(x=='postgres' for x in databases)/duration if duration else None
  stats['connectionRejections']=sum(bool(re.search(r'too many (clients|connections)|remaining connection slots',x,re.I)) for x in (logs.stdout+logs.stderr).splitlines())
  if logs.returncode or stats['connectionRejections']:stats['capacityPassed']=False
  runtime_logs=subprocess.run(['docker','logs','--since',since,'--until',until,PREFIX+'runtime'],capture_output=True,text=True)
  stats['runtimeErrorLogLines']=sum(' ERROR ' in re.sub(r'\x1b\[[0-9;]*m','',line) for line in (runtime_logs.stdout+runtime_logs.stderr).splitlines())
  if runtime_logs.returncode or stats['runtimeErrorLogLines']:stats['capacityPassed']=False
 stats['invalidBuildOverlap']=(d/'INVALID_BUILD_OVERLAP').exists()
 if stats['memoryStopped'] or stats['invalidBuildOverlap'] or not r:stats['capacityPassed']=False
 output.append(stats)
print(json.dumps(output,indent=2))
