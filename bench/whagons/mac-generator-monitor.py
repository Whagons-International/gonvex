#!/usr/bin/env python3
import json,sys,pathlib,subprocess,time,signal
out=pathlib.Path(sys.argv[1]);pidfile=pathlib.Path(str(out)+'.pids');running=True
def stop(*args):
 global running
 running=False
signal.signal(signal.SIGTERM,stop)
with pathlib.Path(str(out)+'.mac-host.jsonl').open('w') as f:
 while running:
  sample={'at':time.time()}
  try:
   pids=pidfile.read_text().split()
   ps=subprocess.check_output(['ps','-p',','.join(pids),'-o','pid=,rss=,%cpu=,time='],text=True)
   sample['processes']=[dict(zip(['pid','rssKiB','cpuPercent','cpuTime'],x.split())) for x in ps.splitlines()]
   sample['vmStat']=subprocess.check_output(['vm_stat'],text=True)
   sample['memoryPressure']=subprocess.check_output(['memory_pressure','-Q'],text=True)
   sample['swapUsage']=subprocess.check_output(['sysctl','vm.swapusage'],text=True).strip()
  except Exception as e:sample['error']=str(e)
  f.write(json.dumps(sample)+'\n');f.flush();time.sleep(2)
