import type { JsonValue, ReplicaCursor, ServerMessage } from '@gonvex/protocol';

export type TransactionEvidenceResult<T> = {
  result:T;
  confirmation:{source:'server-durable';basis:'matching-replica-transaction';cursor:ReplicaCursor};
};
/** Fixed diagnostics only. After send, rejection is UNKNOWN: never cancellation or permission to retry. */
export class TransactionEvidenceError extends Error {
  readonly code='transaction-evidence-unavailable';
  readonly outcome='unknown';
  constructor(readonly reason:'precondition'|'superseded'|'conflicting-evidence'|'server-refused'|'processing-failed'|'deadline', readonly possibleWriteApplied:boolean) {
    super(`Transaction evidence unavailable: ${reason}. Do not retry an ambiguous write.`);
  }
}
const revision=(v:unknown):v is number=>typeof v==='number' && Number.isSafeInteger(v) && v>=0;
export const evidenceIdentity=(v:unknown):v is string=>typeof v==='string' && v.length>0 && v.length<=1024;

/** One call, one transaction, one read. No history, frame retention, queue, transport or app-field inspection. */
export class TransactionEvidenceCall {
  readonly promise:Promise<TransactionEvidenceResult<JsonValue>>;
  private resolve!:(value:TransactionEvidenceResult<JsonValue>)=>void;
  private reject!:(error:TransactionEvidenceError)=>void;
  private timer:ReturnType<typeof setTimeout>;

  private finished=false;
  private sent=false;
  private resultSeen=false;
  private result:JsonValue=null;
  private resultRevision?:number;
  private transactionRevision?:number;
  private processed=false;
  private receiptSent=false;
  constructor(readonly id:string, readonly receiptId:string, readonly path:string,
    private readonly owner:{epoch:string;revision:number;accountId:string;memberId:string},
    private readonly timeoutMs:number, private readonly current:()=>boolean,
    private readonly barrier:()=>number, private readonly sendReceipt:()=>void,
    private readonly acknowledge:(revision:number)=>void, private readonly dispose:()=>void,private readonly start=performance.now()) {
    this.promise=new Promise((resolve,reject)=>{this.resolve=resolve;this.reject=reject;});
    this.timer=setTimeout(()=>this.fail('deadline'),Math.max(0,timeoutMs-(performance.now()-this.start)));
  }
  send(write:()=>void) {if(!this.owns())return;this.sent=true;try{write();}catch{this.fail();}}
  fail(reason:TransactionEvidenceError['reason']='superseded') {
    if(this.finished)return;
    this.finished=true;clearTimeout(this.timer);this.result=null;this.dispose();
    this.reject(new TransactionEvidenceError(reason,this.sent));
  }
  private owns() {
    if(this.finished)return false;
    if(performance.now()-this.start>=this.timeoutMs){this.fail('deadline');return false;}
    if(!this.current()){this.fail();return false;}return !this.finished;
  }
  observe(message:ServerMessage) {
    if(!this.owns())return;
    if('cursor' in message && message.cursor && message.cursor.epoch!==this.owner.epoch){this.fail();return;}
    if(message.type==='reducer.result' && (message.id===this.id || message.originCommandId===this.id)) {
      if(this.resultSeen || message.id!==this.id || message.path!==this.path || message.originCommandId!==this.id
        || !revision(message.committedRevision) || message.committedRevision<=this.owner.revision) {this.fail('conflicting-evidence');return;}
      this.resultSeen=true;this.result=message.result;this.resultRevision=message.committedRevision;
    } else if(message.type==='reducer.error' && message.id===this.id) {this.fail('server-refused');return;}
    else if(message.type==='replica.transaction' && (message.originCommandId===this.id || message.provenance?.rootCommandId===this.id)) {
      const p=message.provenance,c=message.cursor;
      // Duplicate matching transactions are refused, even if identical. No payload fingerprint/history needed.
      if(this.transactionRevision!==undefined || message.originCommandId!==this.id || !c || c.epoch!==this.owner.epoch
        || !revision(c.revision) || c.revision<=this.owner.revision || !Array.isArray(message.changes) || !message.changes.length
        || !p || p.rootCommandId!==this.id || p.actorAccountId!==this.owner.accountId || p.actorMemberId!==this.owner.memberId
        || !['ui','api'].includes(p.channel) || p.rootChannel!==p.channel || p.onBehalfOfMemberId!=null || p.agentExecutionId!=null) {
        this.fail('conflicting-evidence');return;
      }
      this.transactionRevision=c.revision;
    } else if('id' in message && message.id===this.receiptId) {
      if(!this.receiptSent || message.type!=='query.result' || message.path!=='__gonvex.reducerReceipt') {this.fail('server-refused');return;}
      const value=message.result as {found?:unknown;cursor?:{epoch?:unknown;revision?:unknown}}|null;
      const fence=value?.cursor;
      if(value?.found!==true || !fence || fence.epoch!==this.owner.epoch || !revision(fence.revision)
        || this.resultRevision===undefined || fence.revision<this.resultRevision || !this.processed
        || this.barrier()<this.resultRevision || !this.owns()) {this.fail('conflicting-evidence');return;}
      try {
        this.acknowledge(this.resultRevision);
        if(!this.owns())return;
        const result=this.result,cursor={epoch:this.owner.epoch,revision:this.transactionRevision!};
        this.finished=true;clearTimeout(this.timer);this.result=null;this.dispose();
        this.resolve({result,confirmation:{source:'server-durable',basis:'matching-replica-transaction',cursor}});
      } catch {this.fail('processing-failed');}
      return;
    }
    this.pump();
  }
  applied(id:string,epoch:string,r:number) {
    if(!this.owns() || id!==this.id)return;
    if(epoch!==this.owner.epoch || r!==this.transactionRevision){this.fail('conflicting-evidence');return;}
    this.processed=true;this.pump();
  }
  pump() {
    if(!this.owns())return;
    if(this.resultRevision!==undefined && this.transactionRevision!==undefined && this.resultRevision!==this.transactionRevision){this.fail('conflicting-evidence');return;}
    if(!this.receiptSent && this.resultSeen && this.processed && this.resultRevision!==undefined && this.barrier()>=this.resultRevision) {
      this.receiptSent=true;
      try {if(this.owns())this.sendReceipt();} catch {this.fail('superseded');}
    }
  }
}
