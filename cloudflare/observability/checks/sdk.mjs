import { requireCheck } from './contract.mjs';

// An explicit server-execution port is still a proposal. A generic adapter may
// neither erase genuine generated execution metadata nor select that port.
const serverRef = ref => {
  requireCheck(typeof ref?.path==='string' && ref.path.length>0
    && (ref.kind===undefined || ref.kind==='reducer'),'sdk-write-kind','blocked');
  requireCheck(ref?.localExecution===undefined && ref?.optimistic===undefined,
    'sdk-server-execution-unavailable','blocked');
  return ref;
};

/** Adapter over the existing public GonvexClient surface (query/reducer/watchReplica).
 * connect(principal,{signal,fresh:true}) performs REAL app authentication, disables telemetry/outbox
 * and returns a new SDK client. No operator principal, token discovery or raw frame capture.
 * App mappings/receipt evidence are mandatory inputs, never fabricated from reducer's returned value.
 * Public reducer() returns the app result, not committedRevision; the missing evidence hook is explicit.
 */
export function sdkApplication({ connect, refs, argumentsFor, matches, durableEvidence, readEvidence }) {
  requireCheck(typeof connect==='function' && typeof argumentsFor==='function' && typeof matches==='function' && typeof durableEvidence==='function' && typeof readEvidence==='function', 'sdk-bindings-missing','blocked');
  requireCheck(['create','update','remove','read','collection'].every(k=>refs?.[k]?.path), 'sdk-functions-missing','blocked');
  // This adapter's authoritative read uses query.call. A generated query-kind ref can
  // still declare replica/live delivery; do not erase that mismatch or infer eligibility
  // from kind alone. A separate audited read binding is required before connecting.
  requireCheck((refs.read.kind===undefined || refs.read.kind==='query') && refs.read.delivery==null,
    'sdk-read-delivery','blocked');
  for (const role of ['create','update','remove']) serverRef(refs[role]);
  const fresh = async (principal, opts, fn) => {
    const client=await connect(principal,{signal:opts.signal,fresh:true});
    try { requireCheck(client?.localSessionState() === 'authenticated','sdk-auth');return await fn(client); }
    finally { await client.close(); }
  };
  const authoritative = async (principal,opts) => fresh(principal,opts,async client=>{
    const result=await client.query(refs.read,argumentsFor('read',opts),{timeoutMs:10000});
    const proof=await readEvidence(result,opts);
    requireCheck(proof?.storage==='tenant-sqlite' && proof?.fresh===true && Number.isSafeInteger(proof.revision),'sdk-read-proof');
    return {authenticated:true,storage:proof.storage,fresh:true,expected:proof.expected===true,matches:matches(result,opts),revision:proof.revision};
  });
  return {
    applicationRead:authoritative,
    freshRead:authoritative,
    realtimeAuth:(principal,opts)=>fresh(principal,opts,async()=>({authenticated:true,protocolReady:true})),
    async mutate(principal,opts) {
      const ref=serverRef(refs[opts.kind]);
      return fresh(principal,opts,async client=>{
        // Existing server execution and visibility semantics. Never inject trace IDs into wire/idempotency.
        const args=argumentsFor(opts.kind,opts);
        const result=await client.reducer(serverRef(ref),args,{offline:'reject',optimistic:[],timeoutMs:10000});
        const proof=await durableEvidence(client,result,opts);
        requireCheck(proof?.durable===true && Number.isSafeInteger(proof.revision),'sdk-durable-proof');
        return {durable:true,revision:proof.revision,operationId:opts.operationId};
      });
    },
    async openReader(principal,opts) {
      const client=await connect(principal,{signal:opts.signal,fresh:true});
      if(client?.localSessionState()!=='authenticated') {await client?.close();requireCheck(false,'sdk-reader-auth');}
      const view=client.watchReplica(refs.collection,argumentsFor('collection',opts));
      let closed=false;
      return {principal,
        waitApplied:request=>new Promise((resolve,reject)=>{
          let stop=()=>{};
          const finish=(error,result)=>{stop();request.signal.removeEventListener('abort',abort);error?reject(error):resolve(result);};
          const abort=()=>finish(new Error('aborted'));
          const inspect=()=>{
            try {
              const state=view.localReplicaState();
              if(state?.isUpToDate && state.completeness==='complete' && state.computedRevision>=request.revision && matches(view.localReplicaResult(),request)) finish(null,{principal,revision:request.revision,matches:true,applied:true});
            } catch { finish(new Error('sdk-apply')); }
          };
          if(request.signal.aborted) {reject(new Error('aborted'));return;}
          request.signal.addEventListener('abort',abort,{once:true});stop=view.onUpdate(inspect);inspect();
        }),
        async close(){if(!closed){closed=true;await client.close();}},
      };
    },
    cleanup:(principal,opts)=>{
      const ref=serverRef(refs.remove);
      return fresh(principal,opts,async client=>{
        const args=argumentsFor('remove',opts);
        await client.reducer(serverRef(ref),args,{offline:'reject',optimistic:[],timeoutMs:opts.timeoutMs});
        return true;
      });
    },
  };
}
