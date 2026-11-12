// The delivery interface between the tenant object (subscriptions, feed,
// snapshots) and the object that holds a socket (a gateway by default, or the
// tenant object itself). One payload per commit or open per holder; texts
// shared by several sockets appear once (`shared`) and are referenced by
// index, so a delta is encoded once per visibility class, not per socket.
//
// Versioned and additive (the deploy skew of S3 §7): unknown fields are
// ignored, and a holder that does not know `replica` drops it.

export const REPLICA_DELIVERY_VERSION = 1;

/** Server-owned initial operation identity. Never decoded from a client frame. */
export interface InitialRequestRef {
  readonly kind: "query" | "replica";
  readonly id: string;
  readonly path: string;
  readonly epoch: number;
  readonly generation: number;
}

export function validInitialRef(ref: unknown): ref is InitialRequestRef {
  if (!ref || typeof ref !== "object") return false;
  const r = ref as InitialRequestRef;
  return (r.kind === "query" || r.kind === "replica") && typeof r.id === "string" && typeof r.path === "string"
    && Number.isSafeInteger(r.epoch) && r.epoch >= 0 && Number.isSafeInteger(r.generation) && r.generation > 0;
}

/** Conservative own-key/ref/array/number/copy estimate, shared with outlet admission. */
export const initialRefBytes = (ref: InitialRequestRef): number => 1024 + (ref.kind.length + ref.id.length + ref.path.length) * 8;
export function validRecoveryInitialRefs(refs: unknown): refs is readonly InitialRequestRef[] {
  if (!Array.isArray(refs)) return false;
  const seen = new Set<number>();
  return refs.every(ref => {
    if (!validInitialRef(ref) || seen.has(ref.generation)) return false;
    seen.add(ref.generation); return true;
  });
}
/** Optional metadata is legal only on the two existing EMPTY recovery entries. */
export function validRecoveryInitialDelivery(payload: ReplicaDelivery): boolean {
  try {
    const whole = payload.recoveryInitials;
    const scoped = Object.values(payload.sockets).some(work=>work.recoveryInitials!==undefined);
    if (whole===undefined&&!scoped) return true;
    if (payload.v!==REPLICA_DELIVERY_VERSION) return false;
    if (payload.shared.length || payload.watermark!==undefined || payload.membership || payload.initialFailure) return false;
    if (whole!==undefined) return !scoped && !!payload.recovery && payload.recovery.reason==='change-feed-lagged'
      && !Object.keys(payload.sockets).length && !!whole && typeof whole==='object' && !Array.isArray(whole)
      && Object.values(whole).every(validRecoveryInitialRefs);
    return !payload.recovery && payload.scopeFence===1 && Object.values(payload.sockets).every(work=>
      !work.ops.length && work.watermark===undefined && work.opened===undefined && work.removed===undefined
      && work.initialScope===undefined && work.initialCompletions===undefined
      && (work.recoveryInitials===undefined||(validRecoveryInitialRefs(work.recoveryInitials)&&work.recoveryInitials.every(ref=>ref.epoch===work.subscriptionEpoch))));
  } catch { return false; }
}

export function initialFailureOnly(payload: ReplicaDelivery): boolean {
  try {
    const marker = payload.initialFailure;
    return payload.v === REPLICA_DELIVERY_VERSION && !!marker && marker.v === 1 && marker.reason === "delivery-recovery-failed"
      && Array.isArray(payload.shared) && !payload.shared.length && !!payload.sockets && typeof payload.sockets === "object" && !Array.isArray(payload.sockets) && !Object.keys(payload.sockets).length && payload.watermark === undefined
      && !payload.membership && !payload.recovery && payload.recoveryInitials === undefined && payload.scopeFence === undefined
      && !!marker.requests && typeof marker.requests === "object" && !Array.isArray(marker.requests)
      && Object.values(marker.requests).every(refs => {
        const generations = new Set<number>();
        return Array.isArray(refs) && refs.every(ref => {
          if (!validInitialRef(ref) || generations.has(ref.generation)) return false;
          generations.add(ref.generation); return true;
        });
      });
  } catch { return false; }
}

/** One operation on one socket, in order. */
export type SocketOp =
  /** A plain frame (sent after any open batch is flushed). */
  | readonly ["f", string]
  /** A batch data item (a transaction), `shared[index]`. Non-batch sockets never get it. */
  | readonly ["i", number]
  /** A delta item: subscription id, stream token, `shared[index]` (body without its closing brace), the per-socket tail. */
  | readonly ["d", string, string, number, string]
  /** A ready item: subscription id, stream token, the frame addressed to the token. */
  | readonly ["r", string, string, string]
  /** End the connection (Rust returns from the socket loop: subscriptions of two database epochs). */
  | readonly ["x"];

export interface InitialCompletion {
  readonly request: InitialRequestRef;
  readonly opIndex: number;
  readonly outcome: "accepted" | "refused";
}

/** Read only top-level string metadata, skipping row values without parsing
 * or copying them. Application objects/strings named id/path grant nothing. */
export function initialFrameFields(text:string):Record<string,string> {
  const fields:Record<string,string>=Object.create(null); let at=0;
  const space=()=>{while(/\s/.test(text[at]??'')&&at<text.length)at++;};
  const string=()=>{const start=at++;for(;at<text.length;at++){if(text[at]==='\\'){at++;continue;}if(text[at]==='"'){at++;return JSON.parse(text.slice(start,at)) as string;}}throw new Error('unterminated initial frame string');};
  const skip=()=>{let depth=0;for(;at<text.length;at++){
    const char=text[at];if(char==='"'){string();at--;continue;}
    if(char==='{'||char==='[')depth++;
    else if(char==='}'||char===']'){if(depth===0)return;depth--;}
    else if(char===','&&depth===0)return;
  }};
  space();if(text[at++]!=='{')return fields;
  while(at<text.length){space();if(text[at]!== '"')break;const key=string();space();if(text[at++]!==':')break;space();
    if(['type','id','path','reason'].includes(key)&&text[at]==='"')fields[key]=string();else skip();
    if(fields.type&&fields.id!==undefined&&fields.path!==undefined&&(fields.type!=='query.result'||fields.reason!==undefined))return fields;
    space();if(text[at++]!==',')break;
  }
  return fields;
}

/** Only fixed server-frame headers are inspected, never application row JSON. */
export function completionMatches(completion: InitialCompletion, op: SocketOp | undefined): boolean {
  if (!completion || !validInitialRef(completion.request) || !Number.isSafeInteger(completion.opIndex) || completion.opIndex < 0 || !op) return false;
  const text = op[0] === "f" ? op[1] : op[0] === "r" ? op[3] : "";
  let fields:Record<string,string>;try{fields=initialFrameFields(text);}catch{return false;}
  const type=fields.type,id=op[0]==="r"?op[1]:fields.id;
  const ref = completion.request;
  if (id !== ref.id || fields.path !== ref.path) return false;
  if (completion.outcome === "refused") return type === `${ref.kind}.error` || (ref.kind === "replica" && type === "replica.needHashes");
  if (completion.outcome !== "accepted") return false;
  return ref.kind === "replica" ? type === "replica.ready" : type === "query.progress"
    || (type === "query.result" && (fields.reason==="initial"||fields.reason==="reconnect"));
}

export function initialCompletion(request: InitialRequestRef, op: SocketOp, opIndex: number): InitialCompletion | undefined {
  for (const outcome of ["accepted", "refused"] as const) {
    const completion = { request, opIndex, outcome };
    if (completionMatches(completion, op)) return completion;
  }
  return undefined;
}

export function validInitialCompletions(work: SocketWork): boolean {
  const seen = new Set<number>(), generations = new Set<number>();
  return !work.initialCompletions || (Array.isArray(work.initialCompletions) && work.initialCompletions.every(c => {
    if (!c || seen.has(c.opIndex) || generations.has(c.request?.generation) || !completionMatches(c, work.ops[c.opIndex])) return false;
    seen.add(c.opIndex); generations.add(c.request.generation); return true;
  }));
}

export function sameInitialRef(a: InitialRequestRef,b: InitialRequestRef): boolean {
  return a.kind===b.kind&&a.id===b.id&&a.path===b.path&&a.epoch===b.epoch&&a.generation===b.generation;
}
/** Initial-only entry validation; initialScope fences data and grants no
 * terminal/progress authority. Normal commits never use this receiver. */
export function initialDeliveryOnly(payload:ReplicaDelivery):boolean {
 try {
  if(payload.v!==REPLICA_DELIVERY_VERSION||payload.membership||payload.recovery||payload.recoveryInitials!==undefined||payload.initialFailure||payload.watermark!==undefined||!Object.keys(payload.sockets).length)return false;
  return Object.values(payload.sockets).every(work=>{
    if(work.recoveryInitials!==undefined||!validInitialCompletions(work))return false;
    const scope=work.initialScope;
    if(!scope)return work.watermark===undefined&&!work.opened&&!work.removed&&work.ops.length>0&&work.ops.every((_,index)=>work.initialCompletions?.some(c=>c.opIndex===index&&c.outcome==='refused'));
    if(!validInitialRef(scope)||scope.epoch!==work.subscriptionEpoch||work.initialCompletions?.some(c=>!sameInitialRef(c.request,scope)))return false;
    if(Object.entries(work.opened??{}).some(([id,path])=>id!==scope.id||path!==scope.path)||(work.removed??[]).some(id=>id!==scope.id))return false;
    const progress=work.initialCompletions?.some(c=>c.outcome==='accepted')??false;
    if(work.watermark!==undefined&&!progress)return false;
    return work.ops.every((op,index)=>{
      if(op[0]==='x')return true;
      const text=op[0]==='f'?op[1]:op[0]==='r'?op[3]:op[0]==='i'?payload.shared[op[1]]:payload.shared[op[3]];
      if(typeof text!=='string')return false;
      const fields=initialFrameFields(text),type=fields.type;
      if(type==='replica.watermark')return scope.kind==='replica'&&progress&&work.initialCompletions!.every(c=>c.outcome==='accepted'&&c.opIndex<index);
      const allowed=scope.kind==='query'?['query.result','query.progress','query.error']:['replica.snapshot','replica.snapshotPage','replica.delta','replica.ready','replica.needHashes','replica.error'];
      if(!type||!allowed.includes(type))return false;
      const id=op[0]==='r'||op[0]==='d'?op[1]:fields.id;
      return id===scope.id&&fields.path===scope.path&&(type!=='query.result'||fields.reason==='initial'||fields.reason==='reconnect');
    });
  });
 }catch{return false;}
}

export interface SocketWork {
  /** 0070 original references, only on a genuine empty scoped bootstrap. */
  readonly recoveryInitials?: readonly InitialRequestRef[];
  /** 0067 origin fence only, retained on every initial page/split. */
  readonly initialScope?: InitialRequestRef;
  readonly initialCompletions?: readonly InitialCompletion[];
  /** K22: fence delayed work after reauth/member/scope changes, before touching holder state. */
  readonly subscriptionEpoch?: number;
  readonly ops: readonly SocketOp[];
  /** The commit's watermark for this socket. */
  readonly watermark?: number;
  /** Subscriptions installed by this delivery: id → path (the holder resets them if the tenant instance changes). */
  readonly opened?: Readonly<Record<string, string>>;
  /** Subscriptions the tenant removed (needHashes, error, reset). */
  readonly removed?: readonly string[];
}

export interface ReplicaDelivery {
  /** 0070 bounded original references, only on universal whole-holder recovery. */
  readonly recoveryInitials?: Readonly<Record<string, readonly InitialRequestRef[]>>;
  /** Processing only, sent exclusively to deliverInitialFailures on the same queue. */
  readonly initialFailure?: { readonly v: 1; readonly reason: "delivery-recovery-failed"; readonly requests: Readonly<Record<string, readonly InitialRequestRef[]>> };
  readonly v: number;
  /** X01 bounded transport telemetry; never part of the SDK/wire protocol. */
  /** Whole-holder ordered lag recovery (K22); old holders must not fake acknowledgment. */
  readonly recovery?: { readonly reason: "change-feed-lagged" };
  readonly pressure?: { readonly queueAgeMs: number; readonly rpcMs: number };
  /** The tenant object's instance: a new one means its in-memory subscriptions are gone. */
  readonly instance: string;
  /** 0061: sender understands the server-only empty-work restart fence. */
  readonly scopeFence?: 1;
  readonly shared: readonly string[];
  /** Work per connection id. */
  readonly sockets: Readonly<Record<string, SocketWork>>;
  /** A commit's watermark for every other socket of the tenant on the holder. */
  readonly watermark?: number;
  /** A commit changed these Members' rows (lib.rs `membership_affects`): their sessions are discarded. */
  readonly membership?: { readonly revision: number; readonly members: readonly string[]; readonly accounts: readonly string[] };
}

/** The holder's answer: connections it no longer has (the tenant drops their state). */
export interface InitialCompletionStatus { readonly generation: number; readonly status: "enqueued" | "obsolete" }
export interface ReplicaReceipt {
  readonly initialCompletions?: Readonly<Record<string, readonly InitialCompletionStatus[]>>;
  readonly initialFailureAck?: 1;
  readonly recoveryAck?: 1;
  readonly v: number;
  readonly sockets: number;
  readonly missing?: readonly string[];
  /** No data/instance/watermark was applied for these current holder scopes. */
  readonly scopeRequired?: Readonly<Record<string, number>>;
  /** Only after the empty fence's real authorized reset/rebuild completed. */
  readonly scopeAck?: Readonly<Record<string, number>>;
}

/** `replica.batch` timing (delivery.rs `BatchConfig` defaults). */
export const BATCH_TICK_MS = 50;
export const BATCH_IDLE_MS = 1000;
export const BATCH_MAX_BYTES = 512 << 10;
