import {openSync,writeFileSync,fsyncSync,closeSync,renameSync} from 'node:fs';
import {dirname} from 'node:path';
/** Shutdown receipts must survive a forced worker exit. Normal progress uses
 * atomic rename without sync; the stop handler syncs both progress and result. */
export function workerReceipt(path,value,{durable=false}={}){
 const temp=path+`.tmp-${process.pid}`,fd=openSync(temp,'w',0o600);
 try{writeFileSync(fd,JSON.stringify(value));if(durable)fsyncSync(fd);}finally{closeSync(fd);}
 renameSync(temp,path);
 if(durable){const dir=openSync(dirname(path),'r');try{fsyncSync(dir);}finally{closeSync(dir);}}
}
