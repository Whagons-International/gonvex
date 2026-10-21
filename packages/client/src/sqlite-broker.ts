import {validSQLitePrecommitDisplay,type SQLitePrecommitDisplay} from './sqlite-precommit-display.js';
import {sqliteRequestBytes,sqliteTransferRequest} from './sqlite-request-frame.js';
import { sqliteWorkerOperations as publicOperations,sqliteWorkerProtocol,sqliteOwnerRequestCapacity } from './sqlite-worker-protocol.js';
import { sqlitePayloadBytes } from './sqlite-admission.js';
import type { SQLiteTypedSchema } from './sqlite-typed-schema.js';

const protocol = sqliteWorkerProtocol;
const ownershipLock = 'gonvex-sqlite-owner-v1';
const controlName = 'gonvex-sqlite-control-v1';
export type SQLiteBrokerState = 'opening' | 'ready' | 'pending' | 'unavailable' | 'closed';
export type SQLiteBrokerOptions = {
  schema?: SQLiteTypedSchema;
  schemaVersion?:string;
  workerURL?: URL;
  hubURL?: URL;
  /** Static new Worker(new URL(..., import.meta.url)) factories let bundlers
   * fingerprint worker graphs. Only the elected tab invokes createWorker. */
  createWorker?():Worker;
  createHub?():SharedWorker;
  onState?(state: SQLiteBrokerState): void;
  onChange?(scope: string, sequence: number,reset?:boolean): void;
};
type Owner = { id: string; token: string; epoch: number; seen: number; opening?: boolean };
type Generation = { token: string; worker: Worker; adapter:'sqlite';ready: boolean; epoch: number; release(): void; channel: BroadcastChannel; lastProgress: number;announced?:boolean };
type Call = { promise?:Promise<any>;id: string; op: string; args: unknown; bytes: number; lastSent: number; busy?:boolean; replay:boolean;sentToken?:string; timer: ReturnType<typeof setTimeout>; resolve(value: any): void; reject(error: unknown): void };
type WorkerCall = { generation: Generation; resolve(value: any): void; reject(error: unknown): void };
export class SQLiteBrokerError extends Error {
  constructor(readonly code: string, message: string, readonly outcome: 'rejected' | 'unknown' = 'rejected') { super(message); this.name = 'SQLiteBrokerError'; }
}
const error = (code: string, message: string, outcome?: 'rejected' | 'unknown') => new SQLiteBrokerError(code, message, outcome);

const permanent = new Set(['SCHEMA_MISMATCH', 'STORAGE_MODE', 'UNSUPPORTED', 'PROTOCOL']);

/** One elected dedicated worker per origin. Never opens SQLite on the main
 * thread, in a follower, or in the optional SharedWorker router. A timeout is
 * an unknown write outcome; the caller must retain the same durable intent ID.
 * All graphs retained by either endpoint are admitted under byte and count caps.
 * This broker is a storage building block, not the application transport. */
export class SQLiteReplicaBroker {
  readonly tabID = crypto.randomUUID();
  readonly protocolVersion=protocol;
  private readonly control = new BroadcastChannel(controlName);
  private readonly replies = new BroadcastChannel(controlName + ':reply:' + this.tabID);
  private readonly changeListeners=new Set<(scope:string,sequence:number,reset?:boolean,preview?:SQLitePrecommitDisplay)=>void>();
  subscribeChanges(listener:(scope:string,sequence:number,reset?:boolean,preview?:SQLitePrecommitDisplay)=>void):()=>void{if(this.changeListeners.size>=64)throw error('BUDGET','Storage change listener budget exceeded');this.changeListeners.add(listener);return ()=>{this.changeListeners.delete(listener);};}
  private readonly lastChanges=new Map<string,{epoch:number;sequence:number;bytes:number;notification?:number}>();
  private lastChangeBytes=0;
  private heartbeatToken?:string;
  private readonly pending = new Map<string, Call>();
  private readonly workerCalls = new Map<string, WorkerCall>();
  private readonly inflight = new Map<string, { generation: Generation; bytes: number }>();
  private pendingBytes = 0;
  private inflightBytes = 0;
  private serial = 0;
  private owner?: Owner;
  private generation?: Generation;
  private election?: AbortController;
  private recovering = false;
  private closed = false;
  private failed = false;
  private stateValue: SQLiteBrokerState = 'opening';
  private cooldown = 0;
  private adapter:'sqlite'='sqlite';
  private hub?: SharedWorker;
  private port?: MessagePort;
  private lastHubReply = 0;
  private direct?: { port: MessagePort; token: string; ready: boolean; started: number; seen: number };
  private readonly timer: ReturnType<typeof setInterval>;
  private readonly onPageHide = () => this.handoff();
  private readonly onPageShow = () => { if (!this.closed) { this.cooldown = 0; this.elect(); } };
  private readonly onFreeze = () => this.handoff();
  private readonly onResume = () => this.onPageShow();
  constructor(private readonly options: SQLiteBrokerOptions) {
    if(!options.workerURL&&!options.createWorker)throw new TypeError('SQLite broker requires its dedicated worker');
    if(options.schema!==undefined)sqlitePayloadBytes(options.schema,1_048_576);if(options.schemaVersion!==undefined&&!/^[0-9a-f]{64}$/.test(options.schemaVersion)||options.schema===undefined&&options.schemaVersion===undefined)throw new TypeError('SQLite broker requires generated schema metadata');
    this.control.onmessage = event => this.receiveControl(event.data);
    this.replies.onmessage = event => this.receiveReply(event.data);
    this.connectHub();
    globalThis.addEventListener('pagehide', this.onPageHide);
    globalThis.addEventListener('pageshow', this.onPageShow);
    document.addEventListener('freeze', this.onFreeze);
    document.addEventListener('resume', this.onResume);
    this.timer = setInterval(() => this.tick(), 1000);
    if (!navigator.locks) this.fail(error('UNSUPPORTED', 'Web Locks is unavailable'));
    else {this.elect();this.broadcast({type:'probe'});}
  }
  get state() { return this.stateValue; }
  stats() { return { route:this.generation?'owner-local':this.direct?.ready?'direct-worker':this.port?'shared-router-page':'broadcast-page',directPending:!!this.direct&&!this.direct.ready, state: this.stateValue, adapter:this.generation?.adapter??this.adapter, owner: this.owner?.id, ownerEpoch:this.owner?.epoch, ownsWorker: !!this.generation, pending: this.pending.size, pendingBytes: this.pendingBytes, ownerOperations: this.inflight.size, ownerBytes: this.inflightBytes, listeners:this.changeListeners.size,changeScopes:this.lastChanges.size,changeBytes:this.lastChangeBytes }; }
  private setState(state: SQLiteBrokerState) {
    if (state === this.stateValue) return;
    this.stateValue = state;
    try { this.options.onState?.(state); } catch { /* Observer cannot alter ownership. */ }
  }
  private connectHub() {
    if ((!this.options.hubURL&&!this.options.createHub) || typeof SharedWorker === 'undefined') return;
    try {
      this.port?.close();
      this.hub = this.options.createHub?.()??new SharedWorker(this.options.hubURL!, { type: 'module', name: 'gonvex-sqlite-router-v1', extendedLifetime:true } as WorkerOptions);
      this.port = this.hub.port;
      this.port.onmessage = event => {
        const m = event.data;
        if (m.type === 'pong') { this.lastHubReply = Date.now(); return; }
        if (m.type === 'attach' && event.ports[0]) {
          const g = this.generation;
          if (g?.ready && m.token === g.token) g.worker.postMessage({ type: 'attach', tab: m.from, token: g.token, port: event.ports[0] }, [event.ports[0]]);
          else event.ports[0].close();
        } else if (m.type === 'request') void this.serve(m);
        else this.receiveReply(m);
      };
      this.port.start(); this.lastHubReply = Date.now();
      this.port.postMessage({ type: 'hello', tab: this.tabID, protocol });
      this.hub.onerror = () => { this.port?.close(); this.port = undefined; };
    } catch { this.port = undefined; }
  }
  private broadcast(message: object) { if (!this.closed) this.control.postMessage({ ...message, protocol }); }
  private receiveControl(m: any) {
    if(this.closed)return;
    if(m?.protocol!==protocol){if(m?.type==='owner'&&m?.owner)this.fail(error('PROTOCOL','Storage worker protocol changed; reload this client'));return;}
    if (m.type === 'owner' && m.owner?.id !== this.tabID) {
      const o = m.owner as Owner;
      if (!Number.isSafeInteger(o.epoch) || o.epoch < 0 || typeof o.id !== 'string' ||!o.id||o.id.length>128|| typeof o.token !== 'string'||!o.token||o.token.length>128 || !Number.isFinite(o.seen)) return;
      if (!this.owner || o.epoch > this.owner.epoch || o.epoch === this.owner.epoch && o.seen >= this.owner.seen) {
        const changed=this.owner?.token!==o.token;if(changed){if(this.owner)this.fenceSingleShots(this.owner.token);this.heartbeatToken=undefined;}
        this.owner = { ...o, seen: Math.min(Date.now(), o.seen) };
        if (!o.opening) this.setState('ready');
        this.connectDirect();if(changed)this.sendHeartbeat(); this.resend();
      }
    }
    if (m.type === 'gone' && this.owner?.token === m.token) { this.fenceSingleShots(m.token);this.owner = undefined; this.setState('pending'); this.resend(); }
    if (m.type === 'probe' && this.generation?.ready) void this.pulse(true);
    if(['tab-heartbeat','tab-bye','change-ack'].includes(m.type)&&this.generation?.ready&&m.token===this.generation.token&&typeof m.tab==='string'&&m.tab.length<=128)this.generation.worker.postMessage(m);
    if(m.type==='change'&&m.to!==undefined&&m.to!==this.tabID)return;
    if (m.type === 'change' && typeof m.scope === 'string' && m.scope.length <= 512 && Number.isSafeInteger(m.sequence)&&m.sequence>=0) {
      try{if(m.epoch!==undefined){
        if(!Number.isSafeInteger(m.epoch)||m.epoch<0||this.owner&&m.epoch<this.owner.epoch)return;
        const changeKey=(m.reset?'r:':'s:')+m.scope;const previous=this.lastChanges.get(changeKey);
        const freshControl=(m.scope==='@control'||m.scope==='@preview'||m.reset===true)&&Number.isSafeInteger(m.notification)&&m.notification>0&&m.notification>(previous?.notification??0);
        if(previous&&(m.epoch<previous.epoch||m.epoch===previous.epoch&&(m.sequence<previous.sequence||m.sequence===previous.sequence&&!freshControl)))return;
        if(previous){this.lastChanges.delete(changeKey);this.lastChangeBytes-=previous.bytes;}
        const bytes=m.scope.length*2+160;
        while(this.lastChanges.size>=128||this.lastChangeBytes+bytes>262144){const key=this.lastChanges.keys().next().value!;this.lastChangeBytes-=this.lastChanges.get(key)!.bytes;this.lastChanges.delete(key);}
        this.lastChanges.set(changeKey,{epoch:m.epoch,sequence:m.sequence,bytes,...(Number.isSafeInteger(m.notification)?{notification:m.notification}:{})});this.lastChangeBytes+=bytes;
      }
      const preview=m.scope==='@preview'&&m.token===this.owner?.token&&validSQLitePrecommitDisplay(m.preview)?m.preview:undefined;
      this.wakeBlocked();
      try { this.options.onChange?.(m.scope, m.sequence,m.reset===true); } catch { /* Observer only. */ }
      for(const listener of this.changeListeners)try{listener(m.scope,m.sequence,m.reset===true,preview);}catch{/* Observer only. */}
      }finally{this.acknowledgeChange(m);}
    }
  }
  private noteOwner(sentAt:unknown){if(this.owner&&Number.isSafeInteger(sentAt)&&Number(sentAt)>=0&&Number(sentAt)<=Date.now()+1000){this.owner.seen=Math.max(this.owner.seen,Math.min(Date.now(),Number(sentAt)));if(Date.now()-this.owner.seen<=5000&&!this.closed&&!this.failed)this.setState('ready');}}
  private routeWorkerEvent(message:any){
    if(typeof message.to!=='string'){if(message.type==='change'){this.receiveControl(message);this.broadcast(message);}return;}
    if(message.to===this.tabID)this.receiveReply(message);
    else{const channel=new BroadcastChannel(controlName+':reply:'+message.to);channel.postMessage(message);channel.close();}
  }
  private sendHeartbeat(){
    const owner=this.owner;if(!owner||owner.opening||this.closed||this.heartbeatToken===owner.token)return;
    this.heartbeatToken=owner.token;const message={type:'tab-heartbeat',protocol,tab:this.tabID,from:this.tabID,token:owner.token};
    if(this.generation?.ready)this.generation.worker.postMessage(message);
    else if(this.direct?.ready)this.direct.port.postMessage(message);else this.broadcast(message);
  }
  private acknowledgeChange(message:any){
    if(!Number.isSafeInteger(message.notification)||message.notification<1||!Number.isSafeInteger(message.epoch)||message.epoch<0||!this.owner)return;
    const ack={type:'change-ack',protocol,tab:this.tabID,from:this.tabID,token:this.owner.token,notification:message.notification,epoch:message.epoch,sequence:message.sequence};
    if(this.generation?.ready)this.generation.worker.postMessage(ack);else if(this.direct?.ready)this.direct.port.postMessage(ack);else this.broadcast(ack);
  }
  private sendBye(){if(this.owner)this.broadcast({type:'tab-bye',tab:this.tabID,token:this.owner.token});}
  private connectDirect() {
    if (this.heartbeatToken===this.owner?.token||!this.port || !this.owner || this.owner.id === this.tabID || this.direct?.token === this.owner.token) return;
    this.direct?.port.close();
    const channel = new MessageChannel();
    const direct: NonNullable<SQLiteReplicaBroker['direct']> = this.direct = { port: channel.port1, token: this.owner.token, ready: false, started: Date.now(), seen: Date.now() };
    channel.port1.onmessage = event => {
      if (this.direct !== direct) return;
      direct.seen = Date.now();
      if (event.data?.type === 'pong') return;
      if (event.data?.type === 'attached') { direct.ready = true; this.resend(); }
      else if(event.data?.type==='tab-pong'){this.heartbeatToken=undefined;this.noteOwner(event.data.sentAt);}
      else if (event.data?.type === 'change') this.receiveControl(event.data);
      else this.receiveReply(event.data);
    };
    channel.port1.start();
    this.port.postMessage({ type: 'attach', to: this.owner.id, token: this.owner.token }, [channel.port2]);
  }
  private announce(g: Generation,force=false) {
    if (this.generation !== g || !g.ready) return;
    this.owner = { id: this.tabID, token: g.token, epoch: g.epoch, seen: g.lastProgress };
    this.setState('ready');if(force||!g.announced){g.announced=true;this.broadcast({ type: 'owner', owner: this.owner });}
  }
  private progress(g: Generation, sentAt: unknown) {
    if (this.generation !== g || !g.ready || !Number.isSafeInteger(sentAt)) return;
    const now = Date.now();
    if ((sentAt as number) <= g.lastProgress || (sentAt as number) > now + 1000) return;
    g.lastProgress = Math.min(now, sentAt as number); this.announce(g);
  }
  private async pulse(force=false) {
    const g = this.generation; if (!g?.ready || this.workerCalls.size >= 65) return;
    // A regular queued response is itself a progress heartbeat. Avoid adding
    // heartbeat calls faster than a busy worker can consume them.
    if (this.workerCalls.size){if(force&&Date.now()-g.lastProgress<2000)this.announce(g,true);return;}
    try { await this.workerCall(g, 'ping', {}); this.announce(g,force); }
    catch { if (this.generation === g) this.stop(g); }
  }
  private tick() {
    if (this.closed || this.failed) return;
    if (this.direct && Date.now() - this.direct.seen > 15000) { this.direct.port.close(); this.direct = undefined; this.connectDirect(); }
    if(!this.owner&&!this.generation)this.broadcast({type:'probe'});
    this.sendHeartbeat();
    if (this.port) {
      if (Date.now() - this.lastHubReply > 6000) { this.port.close(); this.port = undefined; }
      else this.port.postMessage({ type: 'ping' });
    }
    if (this.generation) {
      if (Date.now() - this.generation.lastProgress > 30000) { this.setState('pending'); this.stop(this.generation); }
      else void this.pulse();
    } else if (this.owner && Date.now() - this.owner.seen > 15000) {
      this.setState('pending'); void this.recover();
    }
    for (const call of this.pending.values()) if (Date.now() - call.lastSent > 2000) this.send(call);
  }
  private elect(steal = false) {
    if (this.closed || this.failed || this.election && !steal) return;
    this.election?.abort();
    const controller = this.election = new AbortController();
    const options: LockOptions = steal ? { mode: 'exclusive', steal: true } : { mode: 'exclusive', signal: controller.signal };
    void navigator.locks.request(ownershipLock, options, async () => {
      if (this.closed || this.failed) return;
      let release!: () => void;
      const released = new Promise<void>(resolve => { release = resolve; });
      const adapter=this.adapter,worker = this.options.createWorker?.()??new Worker(this.options.workerURL!, { type: 'module' });
      const token = crypto.randomUUID(), channel = new BroadcastChannel(controlName + ':owner:' + token);
      const g: Generation = { token, worker, adapter, channel, release, ready: false, epoch: 0, lastProgress: Date.now() };
      this.generation = g;
      channel.onmessage = event => { void this.serve(event.data); };
      worker.onmessage = event => {
        if (this.generation !== g) return;
        const m = event.data;
        if (m.type === 'progress') { this.progress(g, m.sentAt); return; }
        if(m.type==='change'||m.type==='tab-pong'){this.progress(g,m.sentAt);this.routeWorkerEvent(m);return;}
        if (m.type === 'storage-fault') { this.cooldown = Date.now() + 1000; this.setState('pending'); this.stop(g); return; }
        const p = this.workerCalls.get(m.id);
        if (!p || p.generation !== g) return;
        this.workerCalls.delete(m.id); g.lastProgress = Math.min(Date.now(), Number(m.sentAt) || 0);
        if (m.error) p.reject(error(m.error.code ?? 'SQLITE', m.error.message));
        else { p.resolve(m.value); if (g.ready) this.announce(g); }
      };
      worker.onerror = () => { this.setState('pending'); this.stop(g); };
      try {
        const opened = await this.workerCall(g, 'open', { ...(this.options.schema?{schema:this.options.schema}:{}),...(this.options.schemaVersion?{schemaVersion:this.options.schemaVersion}:{}),owner:token,protocol });
        if (this.generation === g) {
          g.epoch = opened.epoch; g.ready = true;this.heartbeatToken=undefined; this.announce(g);this.sendHeartbeat(); this.resend();
        }
      } catch (e) {
        if (permanent.has((e as SQLiteBrokerError).code)) this.fail(e);
        else { this.setState('pending'); this.cooldown = Date.now() + 1000; }
        this.stop(g);
      }
      await released;
      this.stop(g);
    }).catch(e => { if (e?.name !== 'AbortError' && !this.closed) { this.setState('pending'); if (this.generation) this.stop(this.generation); } }).finally(() => {
      if (this.election === controller) {
        this.election = undefined;
        if (!this.closed && !this.failed) setTimeout(() => this.elect(), Math.max(100, this.cooldown - Date.now()));
      }
    });
  }
  private async recover() {
    if (this.recovering || this.closed) return;
    this.recovering = true;
    try {
      await navigator.locks.request(ownershipLock + ':recovery', { ifAvailable: true }, async lock => {
        if (!lock || this.generation || !this.owner) return;
        const token = this.owner.token, seen = this.owner.seen;
        this.broadcast({ type: 'probe' });
        await new Promise(resolve => setTimeout(resolve, 1500));
        if (this.closed || this.generation || this.owner?.token !== token || this.owner.seen > seen) return;
        // Stealing a lock does not unlock OPFS. The new worker must acquire
        // exclusive handles and a durable epoch before it may serve traffic.
        this.elect(true);
      });
    } finally { this.recovering = false; }
  }
  private workerCall(g: Generation, op: string, args: unknown, requestID?:string,tabID?:string): Promise<any> {
    if (this.generation !== g) return Promise.reject(error('FENCED', 'Storage owner changed', 'unknown'));
    const id = this.tabID + ':w:' + ++this.serial;
    return new Promise((resolve, reject) => {
      this.workerCalls.set(id, { generation: g, resolve, reject });
      try { const outgoing=sqliteTransferRequest(op,args);g.worker.postMessage({ id, op, requestID:requestID??id,tabID:tabID??this.tabID, args:outgoing.args },outgoing.transfer); }
      catch (e) { this.workerCalls.delete(id); reject(e); }
    });
  }
  private stop(g: Generation) {
    if (this.generation !== g) return;
    this.generation = undefined;this.fenceSingleShots(g.token);
    g.worker.terminate(); g.channel.close();
    for (const [id, call] of this.workerCalls) if (call.generation === g) { call.reject(error('FENCED', 'Storage owner changed', 'unknown')); this.workerCalls.delete(id); }
    this.inflight.clear(); this.inflightBytes = 0;
    this.broadcast({ type: 'gone', token: g.token });
    if (this.owner?.token === g.token) this.owner = undefined;
    g.release();
  }
  handoff() {
    this.cooldown = Date.now() + 5000;
    this.election?.abort();
    if (this.generation) this.stop(this.generation);
    if (!this.closed) this.setState('pending');
  }
  private rejectSingleShot(call:Call){
    if(!this.pending.has(call.id))return;this.pending.delete(call.id);this.pendingBytes-=call.bytes;clearTimeout(call.timer);call.reject(error('FENCED','This request was interrupted after dispatch; its outcome is unknown','unknown'));this.wakeBlocked();
  }
  private fenceSingleShots(token:string){for(const call of this.pending.values())if(!call.replay&&call.sentToken===token)this.rejectSingleShot(call);}
  private send(call: Call) {
    if (!this.owner || this.owner.opening || this.closed) return;
    if(!call.replay&&call.sentToken){if(call.sentToken!==this.owner.token)this.rejectSingleShot(call);return;}
    if(call.sentToken!==this.owner.token||call.busy){
      let active=0;for(const pending of this.pending.values())if(pending.sentToken===this.owner.token&&!pending.busy)active++;
      if(active>=sqliteOwnerRequestCapacity)return;
    }
    call.busy=false;call.sentToken=this.owner.token;call.lastSent = Date.now();
    const m = { type: 'request', protocol, from: this.tabID, token: this.owner.token, id: call.id, op: call.op, args: call.args };
    if (this.owner.id === this.tabID) { void this.serve(m); return; }
    if (this.direct?.ready && this.direct.token === this.owner.token) { const outgoing=sqliteTransferRequest(call.op,call.args);this.direct.port.postMessage({...m,args:outgoing.args},outgoing.transfer); }
    else if (this.port) { const outgoing=sqliteTransferRequest(call.op,call.args);this.port.postMessage({ ...m,args:outgoing.args, to: this.owner.id },outgoing.transfer); }
    else {
      const channel = new BroadcastChannel(controlName + ':owner:' + this.owner.token);
      channel.postMessage(m); channel.close();
    }
  }
  private resend() { for (const call of this.pending.values()) if (Date.now() - call.lastSent >= 100) this.send(call); }
  private reply(m: any, value?: unknown, failure?: unknown) {
    const e = failure as SQLiteBrokerError | undefined;
    const reply = { type: 'reply', protocol, id: m.id, to: m.from, token: m.token, ...(failure ? { error: { code: e?.code ?? 'SQLITE', message: String(e?.message ?? failure).slice(0, 512), outcome: e?.outcome ?? 'unknown' } } : { value }) };
    if (m.from === this.tabID) this.receiveReply(reply);
    else if (this.port) this.port.postMessage(reply);
    else { const channel = new BroadcastChannel(controlName + ':reply:' + m.from); channel.postMessage(reply); channel.close(); }
  }
  private async serve(m: any) {
    const g = this.generation;
    if (!g?.ready || m?.protocol !== protocol || m.token !== g.token || typeof m.from !== 'string' ||!m.from||m.from.length>128|| typeof m.id !== 'string' || m.id.length > 256 || !publicOperations.has(m.op)) return;
    const key = m.from + ':' + m.id;
    if (this.inflight.has(key)) return;
    let bytes: number;
    try {
      bytes = sqliteRequestBytes(m.op,m.args,262144);
      if (this.inflight.size >= sqliteOwnerRequestCapacity || this.inflightBytes + bytes > 2097152) throw error('BUSY', 'Storage queue is full');
    } catch (e) { this.reply(m, undefined, e); return; }
    this.inflight.set(key, { generation: g, bytes }); this.inflightBytes += bytes;
    try {
      const value = await this.workerCall(g, m.op, m.args,m.id,m.from);
      if (this.generation !== g) return;
      sqlitePayloadBytes(value, 262144); this.reply(m, value);
      // The worker publishes the fenced global clock after commit on every route.
    } catch (e) { if (this.generation === g) this.reply(m, undefined, e); }
    finally { const retained = this.inflight.get(key); if (retained?.generation === g) { this.inflightBytes -= retained.bytes; this.inflight.delete(key); } }
  }
  private wakeBlocked() {
    if(this.closed||this.failed)return;
    let waiting=false;for(const call of this.pending.values())if(call.busy||call.sentToken!==this.owner?.token){waiting=true;break;}if(!waiting)return;
    // Completion/progress gives one unsent or BUSY request a capacity opportunity.
    // BUSY alone never schedules retries, and the heartbeat remains recovery
    // for lost messages or an owner that has stopped reporting progress.
    queueMicrotask(()=>{if(this.closed||this.failed)return;for(const call of this.pending.values())if(call.busy||call.sentToken!==this.owner?.token){this.send(call);break;}});
  }
  private receiveReply(m: any) {
    if (m?.protocol !== protocol || m.to !== this.tabID) return;
    if(m.type==='change'||m.type==='tab-pong'){if(m.token!==this.owner?.token)return;this.noteOwner(m.sentAt);if(m.type==='change')this.receiveControl(m);else this.heartbeatToken=undefined;return;}
    const call = this.pending.get(m.id); if (!call) return;
    if (m.token !== this.owner?.token) return;this.noteOwner(m.sentAt);
    if (m.error?.code === 'BUSY' || m.error?.code === 'FENCED') { if(!call.replay){this.rejectSingleShot(call);return;}call.busy=m.error.code==='BUSY';call.lastSent = Date.now(); return; }
    this.pending.delete(call.id); this.pendingBytes -= call.bytes; clearTimeout(call.timer);
    if (m.error) call.reject(error(m.error.code, m.error.message, m.error.outcome));
    else call.resolve(m.value);
    this.wakeBlocked();
  }
  request<T = unknown>(op: string, args: unknown = {}, timeoutMs = 90000,requestID?:string): Promise<T> {
    if (this.closed || this.failed) return Promise.reject(error('UNAVAILABLE', 'Storage is unavailable'));
    if (!publicOperations.has(op)) return Promise.reject(error('OPERATION', 'Unsupported storage operation'));
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 300000) return Promise.reject(error('BUDGET', 'Invalid storage deadline'));
    let bytes: number;
    try { bytes = sqliteRequestBytes(op,args,262144); }
    catch (e) { return Promise.reject(e); }
    if(requestID!==undefined&&(typeof requestID!=='string'||!/^[A-Za-z0-9:_-]{1,256}$/.test(requestID)||op!=='clientCall'||!args||typeof args!=='object'||(args as {method?:unknown}).method!=='reducer'))return Promise.reject(error('OPERATION','Stable request IDs require a durable reducer call'));
    if(requestID!==undefined){const previous=this.pending.get(requestID);if(previous){if(previous.op!==op||JSON.stringify(previous.args)!==JSON.stringify(args))return Promise.reject(error('INTENT_CONFLICT','Pending intent ID reused with different content'));return previous.promise??Promise.reject(error('BUSY','Intent request admission is completing'));}}
    if (this.pending.size >= 256 || this.pendingBytes + bytes > 2097152) return Promise.reject(error('BUDGET', 'Storage request budget exceeded'));
    // Immutable replay payload. Public durable writes additionally require an
    // immutable intent ID, checked by the worker's transactional receipt ledger.
    const admitted = structuredClone(args), id = requestID??this.tabID + ':r:' + ++this.serial;
    const promise=new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        const call = this.pending.get(id); if (!call) return;
        this.pending.delete(id); this.pendingBytes -= call.bytes;
        this.setState('pending');
        const method=op==='clientCall'&&admitted&&typeof admitted==='object'?String((admitted as {method?:unknown}).method):op;
        const readOnly=new Set(['ping','stats','integrity','controls','status','entities','replicaPage','collectionPage','localLivePage','livePage','replicaPendingPage','livePendingPage','controlState']).has(method);
        const failure=error('TIMEOUT',readOnly?'Storage read timed out; retry the read':'Storage commit outcome is unknown; retry the same intent ID',readOnly?'rejected':'unknown');
        Object.assign(failure,{operation:method,...(requestID?{intentId:requestID}:{})});
        reject(failure);this.wakeBlocked();
      }, timeoutMs);
      const replay=!(op==='clientCall'&&!!admitted&&typeof admitted==='object'&&['action','token'].includes(String((admitted as {method?:unknown}).method)));
      const call: Call = { id, op, args: admitted, bytes, replay,lastSent: 0, timer, resolve, reject };
      this.pending.set(id, call); this.pendingBytes += bytes; this.send(call);
    });const admittedCall=this.pending.get(id);if(admittedCall)admittedCall.promise=promise;return promise;
  }
  private fail(reason: unknown) {
    this.failed = true; this.setState('unavailable');
    for (const call of this.pending.values()) { clearTimeout(call.timer); call.reject(reason); }
    this.pending.clear(); this.pendingBytes = 0;
  }
  close() {
    if (this.closed) return;
    this.sendBye();this.fail(error('CLOSED', 'Storage broker closed', 'unknown'));
    this.election?.abort(); if (this.generation) this.stop(this.generation);
    this.closed = true; this.changeListeners.clear();this.lastChanges.clear();this.lastChangeBytes=0; clearInterval(this.timer);
    this.direct?.port.postMessage({ type: 'bye' }); this.direct?.port.close();
    this.port?.postMessage({ type: 'bye' }); this.port?.close(); this.control.close(); this.replies.close();
    globalThis.removeEventListener('pagehide', this.onPageHide); globalThis.removeEventListener('pageshow', this.onPageShow);
    document.removeEventListener('freeze', this.onFreeze); document.removeEventListener('resume', this.onResume);
    this.setState('closed');
  }
}
