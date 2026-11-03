#!/usr/bin/env python3
"""Summarize local phase telemetry; absent or stopped phases never pass."""
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
 d=root/phase; result=root/(phase+'.json');r=json.loads(result.read_text()) if result.exists() else json.loads(pathlib.Path(str(result)+'.partial.json').read_text()) if pathlib.Path(str(result)+'.partial.json').exists() else {}
 host=loadlines(d/'host.jsonl')
 stats={'phase':phase,'users':r.get('connectedUsers'),'target':r.get('targetUsers'),'errors':sum(r.get('errors',{}).values()),'latencyMs':r.get('latencyMs',{}),'capacityPassed':r.get('clientSloPassed',r.get('capacityPassed',False)),'memoryStopped':(d/'STOPPED_MEMORY_PRESSURE').exists()}
 for name in ['runtime','generator']:
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
  stats['oomKills']={name:max((x.get('memoryEvents',{}).get(name,{}).get('oom_kill',0) for x in host),default=0)-host[0].get('memoryEvents',{}).get(name,{}).get('oom_kill',0) for name in ['runtime','generator']}
  if any(stats['oomKills'].values()):stats['capacityPassed']=False
  metrics=loadlines(root/'fixture.json.metrics.jsonl')
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
  since=datetime.datetime.fromtimestamp(host[0]['at'],datetime.timezone.utc).isoformat()
  until=datetime.datetime.fromtimestamp(host[-1]['at'],datetime.timezone.utc).isoformat()
  runtime_logs=subprocess.run(['docker','logs','--since',since,'--until',until,'gvsq-runtime'],capture_output=True,text=True)
  stats['runtimeErrorLogLines']=sum(' ERROR ' in re.sub(r'\x1b\[[0-9;]*m','',line) for line in (runtime_logs.stdout+runtime_logs.stderr).splitlines())
  if runtime_logs.returncode or stats['runtimeErrorLogLines']:stats['capacityPassed']=False
 if stats['memoryStopped'] or not r:stats['capacityPassed']=False
 output.append(stats)
print(json.dumps(output,indent=2))
