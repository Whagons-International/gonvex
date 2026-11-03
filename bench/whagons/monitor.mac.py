#!/usr/bin/env python3
"""Local host-cgroup telemetry, one PG sampler, independent memory-pressure stop."""
import json,pathlib,subprocess,sys,time,os,signal,uuid,threading
out=pathlib.Path(sys.argv[1]);out.mkdir(parents=True,exist_ok=True)
identity='gvmac-monitor-'+uuid.uuid4().hex;pidfile='/tmp/'+identity+'.pid'
done=threading.Event();containers={}
def full_psi(kind):
 raw=subprocess.check_output(['docker','exec','gvmac-pod','cat','/proc/pressure/'+kind],text=True,timeout=5)
 return float(raw.split('full avg10=')[1].split()[0])
def stop_generator():
 subprocess.run(['docker','stop','-t','2','gvmac-generator'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=10)
def guard():
 while not done.is_set():
  full=full_psi('memory')
  if full>5:
   (out/'STOPPED_MEMORY_PRESSURE').write_text(str(full));stop_generator()
  done.wait(1)
def stopped(signum,frame):raise SystemExit(128+signum)
signal.signal(signal.SIGTERM,stopped);signal.signal(signal.SIGINT,stopped)
def cleanup(p):
 done.set()
 if guard_thread is not None:guard_thread.join(timeout=8)
 signal.signal(signal.SIGTERM,signal.SIG_IGN);signal.signal(signal.SIGINT,signal.SIG_IGN)
 # docker exec does not forward signals to its child. Kill the actual sampler,
 # checking its unique environment identity before using the in-container PID.
 try:
  pid=subprocess.check_output(['docker','exec','gvmac-pg','cat',pidfile],text=True,timeout=10).strip()
  if not pid.isdecimal() or int(pid)<2:raise ValueError('invalid sampler PID')
  env=subprocess.check_output(['docker','exec','gvmac-pg','cat',f'/proc/{pid}/environ'],timeout=10)
  if ('PGAPPNAME='+identity).encode() in env.split(b'\0'):
   subprocess.run(['docker','exec','gvmac-pg','kill','-TERM',pid],check=True,timeout=10)
 except (subprocess.SubprocessError,ValueError):pass
 try:subprocess.run(['docker','exec','gvmac-pg','rm','-f',pidfile],timeout=10)
 except subprocess.SubprocessError:pass
 try:p.wait(timeout=5)
 except subprocess.TimeoutExpired:
  p.terminate()
  try:p.wait(timeout=5)
  except subprocess.TimeoutExpired:p.kill();p.wait()
query="""SELECT json_build_object('at',clock_timestamp(),'connections',(select count(*) from pg_stat_activity where backend_type='client backend'),'sessions',(select sum(sessions) from pg_stat_database),'active',(select count(*) from pg_stat_activity where state='active'),'waiting',(select count(*) from pg_stat_activity where wait_event_type='Lock'),'ioWaiting',(select count(*) from pg_stat_activity where wait_event_type='IO'));\n\\watch 1\n"""
with (out/'postgres.jsonl').open('w') as pg,(out/'host.jsonl').open('w') as host:
 p=subprocess.Popen(['docker','exec','-i','-e','PGAPPNAME='+identity,'gvmac-pg','sh','-c',f'echo $$ > {pidfile}; exec psql -X -U postgres -At gonvex_runtime'],stdin=subprocess.PIPE,stdout=pg,stderr=subprocess.DEVNULL,text=True)
 guard_thread=None
 try:
  p.stdin.write(query);p.stdin.close()
  guard_thread=threading.Thread(target=guard,daemon=True);guard_thread.start()
  while not done.is_set():
   sample={'at':time.time(),'memory_full_avg10':full_psi('memory'),'io_full_avg10':full_psi('io'),'memoryEvents':{}}
   for name in ['pg','runtime','generator']:
    try:
     raw=subprocess.check_output(['docker','exec','gvmac-'+name,'sh','-c','cat /sys/fs/cgroup/cpu.stat; cat /sys/fs/cgroup/memory.current; echo EVENTS; cat /sys/fs/cgroup/memory.events'],text=True,stderr=subprocess.DEVNULL,timeout=5)
     cpu,events=raw.split('EVENTS\n');sample[name]=cpu.strip()
     sample['memoryEvents'][name]={k:int(n) for k,n in (line.split() for line in events.strip().splitlines())}
    except (subprocess.SubprocessError,ValueError):pass
   host.write(json.dumps(sample)+'\n');host.flush();done.wait(1)
 finally:cleanup(p)
