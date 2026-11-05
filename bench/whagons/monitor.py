#!/usr/bin/env python3
"""Local host-cgroup telemetry and an independent memory-pressure stop (SQLite lab)."""
import json,pathlib,subprocess,sys,time,os,signal,uuid,threading
out=pathlib.Path(sys.argv[1]);out.mkdir(parents=True,exist_ok=True)
done=threading.Event();containers={}
PREFIX=os.environ.get('LOAD_PREFIX','gvsq')+'-'
def full_psi(kind):
 try:return float(pathlib.Path('/proc/pressure/'+kind).read_text().split('full avg10=')[1].split()[0])
 except (OSError,IndexError,ValueError):return 0.0
def locate(name):
 if name not in containers:
  try:
   raw=subprocess.check_output(['docker','inspect',PREFIX+name,'--format','{{.State.Pid}} {{.Id}}'],text=True,stderr=subprocess.DEVNULL,timeout=2).split()
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
 try:subprocess.run(['docker','stop','-t','2',PREFIX+'generator'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=5)
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
     # memory.current includes reclaimable page cache (SQLite reads); anon is
     # the heap and stacks that cannot be reclaimed.
     stat={k:int(n) for k,n in (line.split() for line in (cg/'memory.stat').read_text().splitlines()) if k in ('anon','file','kernel','sock')}
     sample.setdefault('memoryStat',{})[name]=stat
     sample['memoryEvents'][name]={k:int(n) for k,n in (line.split() for line in (cg/'memory.events').read_text().splitlines())}
     sample.setdefault('memorySwapBytes',{})[name]=int((cg/'memory.swap.current').read_text())
     sample.setdefault('limits',{})[name]={k:(cg/k).read_text().strip() for k in ['memory.max','memory.swap.max','cpu.max']}
     sample.setdefault('memoryBreakdown',{})[name]={k:int(n) for k,n in (line.split() for line in (cg/'memory.stat').read_text().splitlines()) if k in ['anon','file','kernel','sock','pagetables','file_mapped','file_dirty','file_writeback']}
     if name=='runtime':
      processes={}
      # Include every process in the runtime cgroup, including grandchildren
      # and multiple module hosts. Summed RSS double-counts shared mappings;
      # PSS apportions them. Cgroup anon/file/kernel remain separate measures.
      for process in sorted(set((cg/'cgroup.procs').read_text().split())):
       try:
        stat=pathlib.Path(f'/proc/{process}/stat').read_text().rsplit(')',1)[1].split()
        command=pathlib.Path(pathlib.Path(f'/proc/{process}/cmdline').read_bytes().split(b'\0')[0].decode()).name
        usage={'command':command,'userSeconds':int(stat[11])/os.sysconf('SC_CLK_TCK'),'systemSeconds':int(stat[12])/os.sysconf('SC_CLK_TCK'),'residentBytes':int(stat[21])*os.sysconf('SC_PAGE_SIZE')}
        rollup=pathlib.Path(f'/proc/{process}/smaps_rollup').read_text()
        for line in rollup.splitlines():
         fields=line.split()
         if fields[0] in ['Rss:','Pss:','Private_Clean:','Private_Dirty:','Swap:']:
          usage[fields[0][:-1]+'Bytes']=int(fields[1])*1024
        processes[process]=usage
       except (OSError,ValueError,IndexError):pass
      sample['runtimeProcesses']=processes
      sample['runtimeProcessMemory']={kind:sum(p.get(kind,0) for p in processes.values()) for kind in ['residentBytes','RssBytes','PssBytes','Private_CleanBytes','Private_DirtyBytes','SwapBytes']}
    except (OSError,ValueError):pass
   # Optional task-owned transport/proxy cgroups live outside the container.
   # Include their CPU counters and RSS/PSS rather than hiding remote transport
   # cost in the generator or OS allowance.
   for extra in sys.argv[2:]:
    try:
     cg=pathlib.Path(extra)
     auxiliary={'cpu':{k:int(v) for k,v in (line.split() for line in (cg/'cpu.stat').read_text().splitlines())},'chargedBytes':int((cg/'memory.current').read_text()),'processes':{}}
     for process in (cg/'cgroup.procs').read_text().split():
      try:
       fields={line.split()[0]:int(line.split()[1])*1024 for line in pathlib.Path(f'/proc/{process}/smaps_rollup').read_text().splitlines() if line.split()[0] in ['Rss:','Pss:','Swap:']}
       auxiliary['processes'][process]=fields
      except (OSError,ValueError,IndexError):pass
     sample.setdefault('serverAuxiliary',{})[cg.name]=auxiliary
    except (OSError,ValueError):pass
   host.write(json.dumps(sample)+'\n');host.flush();done.wait(1)
 finally:cleanup()
