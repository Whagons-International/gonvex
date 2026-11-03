#!/usr/bin/env python3
"""Local host-cgroup telemetry and an independent memory-pressure stop (SQLite lab)."""
import json,pathlib,subprocess,sys,time,os,signal,uuid,threading
out=pathlib.Path(sys.argv[1]);out.mkdir(parents=True,exist_ok=True)
done=threading.Event();containers={}
def full_psi(kind):
 return float(pathlib.Path('/proc/pressure/'+kind).read_text().split('full avg10=')[1].split()[0])
def locate(name):
 if name not in containers:
  try:
   raw=subprocess.check_output(['docker','inspect','gvsq-'+name,'--format','{{.State.Pid}} {{.Id}}'],text=True,stderr=subprocess.DEVNULL,timeout=2).split()
   pid=int(raw[0]);identifier=raw[1]
   path=pathlib.Path(f'/proc/{pid}/cgroup').read_text().strip().split('0::')[-1]
   if pid>1 and identifier in path:containers[name]=(pid,identifier,pathlib.Path('/sys/fs/cgroup'+path))
  except (subprocess.SubprocessError,OSError,ValueError,IndexError):pass
 return containers.get(name)
def stop_generator():
 entry=containers.get('generator')
 if entry:
  pid,identifier,_=entry
  # A reused host PID must never be signalled.
  try:
   if identifier in pathlib.Path(f'/proc/{pid}/cgroup').read_text():os.kill(pid,signal.SIGKILL)
  except (OSError,ProcessLookupError):pass
 try:subprocess.run(['docker','stop','-t','2','gvsq-generator'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=5)
 except subprocess.SubprocessError:pass
def guard():
 tripped=False
 while not done.is_set():
  full=full_psi('memory')
  if full>5:
   (out/'STOPPED_MEMORY_PRESSURE').write_text(str(full));tripped=True
  if tripped:
   locate('generator');stop_generator()
  done.wait(1)
def stopped(signum,frame):raise SystemExit(128+signum)
signal.signal(signal.SIGTERM,stopped);signal.signal(signal.SIGINT,stopped)
def cleanup():
 done.set()
 if guard_thread is not None:guard_thread.join(timeout=8)
with (out/'host.jsonl').open('w') as host:
 guard_thread=None
 try:
  guard_thread=threading.Thread(target=guard,daemon=True);guard_thread.start()
  while not done.is_set():
   sample={'at':time.time(),'memory_full_avg10':full_psi('memory'),'io_full_avg10':full_psi('io'),'memoryEvents':{}}
   for name in ['runtime','generator']:
    entry=locate(name)
    if not entry:continue
    pid,_,cg=entry
    try:
     sample[name]=(cg/'cpu.stat').read_text().strip()+'\n'+(cg/'memory.current').read_text().strip()
     sample['memoryEvents'][name]={k:int(n) for k,n in (line.split() for line in (cg/'memory.events').read_text().splitlines())}
     if name=='runtime':
      processes={};children=pathlib.Path(f'/proc/{pid}/task/{pid}/children').read_text().split()
      for process in [str(pid),*children]:
       stat=pathlib.Path(f'/proc/{process}/stat').read_text().rsplit(')',1)[1].split()
       command=pathlib.Path(pathlib.Path(f'/proc/{process}/cmdline').read_bytes().split(b'\0')[0].decode()).name
       processes[command]={'userSeconds':int(stat[11])/os.sysconf('SC_CLK_TCK'),'systemSeconds':int(stat[12])/os.sysconf('SC_CLK_TCK')}
      sample['runtimeProcesses']=processes
    except (OSError,ValueError):pass
   host.write(json.dumps(sample)+'\n');host.flush();done.wait(1)
 finally:cleanup()
