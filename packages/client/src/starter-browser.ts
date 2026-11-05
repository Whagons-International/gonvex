import type { StarterInflater } from "./starter-bootstrap.js";

const PIECE = 256 * 1024;
const TAIL = new Uint8Array([0, 0, 255, 255, 1, 0, 0, 255, 255]);
const LENGTH = [3,4,5,6,7,8,9,10,11,13,15,17,19,23,27,31,35,43,51,59,67,83,99,115,131,163,195,227,258];
const LEXTRA = [0,0,0,0,0,0,0,0,1,1,1,1,2,2,2,2,3,3,3,3,4,4,4,4,5,5,5,5,0];
const DIST = [1,2,3,4,5,7,9,13,17,25,33,49,65,97,129,193,257,385,513,769,1025,1537,2049,3073,4097,6145,8193,12289,16385,24577];
const DEXTRA = [0,0,0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,8,8,9,9,10,10,11,11,12,12,13,13];
const ORDER = [16,17,18,0,8,7,9,6,10,5,11,4,12,3,13,2,14,1,15];
function fail(): never { throw new Error("starter-browser-format"); }
function aborted(signal?: AbortSignal) { if (signal?.aborted) throw signal.reason ?? new DOMException("Aborted", "AbortError"); }

/** Canonical counts, ordered symbols. No expanded lookup tables or output. */
class Tree {
  readonly count = new Uint16Array(16);
  readonly symbols = new Uint16Array(288);
  readonly offsets = new Uint16Array(16);
  build(lengths: Uint8Array, kind: "codes" | "data" | "distance") {
    this.count.fill(0); this.offsets.fill(0);
    let used = 0, max = 0;
    for (const n of lengths) { if (n > 15) fail(); if (n) { this.count[n]++; used++; max = Math.max(max,n); } }
    let left = 1;
    for (let n=1;n<=15;n++) { left = left*2-this.count[n]; if (left<0) fail(); }
    // RFC1951/zlib permit a single one-bit data code, and no distances
    // when the block contains literals only. Code-length trees must be full.
    if (!used) { if (kind!=="distance") fail(); }
    else if (left && !(kind!=="codes" && used===1 && max===1)) fail();
    const offsets = this.offsets;
    for (let n=1;n<15;n++) offsets[n+1]=offsets[n]+this.count[n];
    lengths.forEach((n,s) => { if (n) this.symbols[offsets[n]++]=s; });
  }
}

/** Validate the complete restored SyncFlush stream and count output BEFORE
 * invoking native decompression. Bounds CPU by input bits + output limit;
 * each loop consumes bits, a symbol, or a bounded (<=318) tree description.
 * RFC7692 7.2.2 supplies the removed LEN/NLEN bytes. The final block is ours.
 */
function predictedSize(input: Uint8Array, limit: number, signal?: AbortSignal): number {
  let bit=0, output=0, work=0;
  const lit=new Tree(), dist=new Tree(), codeTree=new Tree();
  const lengths=new Uint8Array(320), codes=new Uint8Array(19);
  const end=(input.length+4)*8;
  const read=(n:number) => {
    if (bit+n>end) fail();
    let value=0;
    for(let i=0;i<n;i++,bit++) {
      const pos=bit>>>3;
      const b=pos<input.length?input[pos]:TAIL[pos-input.length];
      value |= ((b>>>(bit&7))&1)<<i;
    }
    return value;
  };
  const tick=()=>{ if ((++work&255)===0) aborted(signal); };
  const add=(n:number)=>{ if (n>limit-output) throw new Error("starter-browser-output-limit"); output+=n; };
  const symbol=(tree:Tree) => {
    let code=0, first=0, index=0;
    for(let len=1;len<=15;len++) {
      code=(code<<1)|read(1);
      const count=tree.count[len];
      if(code>=first && code<first+count) return tree.symbols[index+code-first];
      index+=count; first=(first+count)*2;
    }
    return fail();
  };
  while(bit<end) {
    tick(); if(read(1)!==0) fail(); // v1 stored pieces never contain BFINAL
    const type=read(2);
    if(type===0) {
      bit=Math.ceil(bit/8)*8;
      const lengthPosition=bit;
      const len=read(16), inv=read(16);
      if((len^inv)!==65535) fail();
      add(len); if(bit+len*8>end) fail(); bit+=len*8;
      if(bit===end) {
        // The removed four bytes must be exactly the last empty stored
        // block's LEN/NLEN, never borrowed as literals or a Huffman suffix.
        if(len!==0 || lengthPosition!==input.length*8) fail();
        aborted(signal); return output;
      }
      continue;
    }
    if(type===3) fail();
    if(type===1) {
      lengths.fill(0);
      lengths.fill(8,0,144); lengths.fill(9,144,256); lengths.fill(7,256,280); lengths.fill(8,280,288);
      lit.build(lengths.subarray(0,288),"data"); lengths.fill(5,0,32); dist.build(lengths.subarray(0,32),"distance");
    } else {
      const nl=read(5)+257, nd=read(5)+1, nc=read(4)+4;
      if(nl>286 || nd>30) fail();
      codes.fill(0);
      for(let i=0;i<nc;i++) codes[ORDER[i]]=read(3);
      codeTree.build(codes,"codes"); lengths.fill(0);
      for(let i=0;i<nl+nd;) {
        tick(); const s=symbol(codeTree);
        if(s<16) { lengths[i++]=s; continue; }
        const count=s===16?read(2)+3:s===17?read(3)+3:read(7)+11;
        if(s>18 || (s===16 && i===0) || i+count>nl+nd) fail();
        lengths.fill(s===16?lengths[i-1]:0,i,i+count); i+=count;
      }
      if(!lengths[256]) fail();
      lit.build(lengths.subarray(0,nl),"data"); dist.build(lengths.subarray(nl,nl+nd),"distance");
    }
    for(;;) {
      tick(); const s=symbol(lit);
      if(s<256) { add(1); continue; }
      if(s===256) break;
      if(s>285) fail();
      const length=LENGTH[s-257]+read(LEXTRA[s-257]), d=symbol(dist);
      if(d>29) fail();
      const distance=DIST[d]+read(DEXTRA[d]);
      if(distance>4096 || distance>output) fail();
      add(length);
    }
  }
  return fail();
}

async function decode(owned: Uint8Array<ArrayBuffer>, predicted: number, signal?: AbortSignal) {
  aborted(signal);
  const ds=new DecompressionStream("deflate-raw");
  const reader=ds.readable.getReader(), writer=ds.writable.getWriter();
  const cancel=()=> { void reader.cancel(signal?.reason).catch(()=>{}); void writer.abort(signal?.reason).catch(()=>{}); };
  signal?.addEventListener("abort",cancel,{once:true});
  // Both ends must run concurrently: waiting for write before reading deadlocks
  // TransformStream backpressure. One bounded write is pending at a time.
  const writing=(async()=>{
    for(let i=0;i<owned.length;i+=1024) { aborted(signal); await writer.write(owned.subarray(i,i+1024)); }
    await writer.close();
  })();
  void writing.catch(()=>{});
  try {
    const output=new Uint8Array(predicted); let offset=0, chunks=0;
    for(;;) {
      aborted(signal); const {value,done}=await reader.read(); aborted(signal);
      if(done) break;
      if(++chunks>predicted+1 || !value.length || value.length>predicted-offset) fail();
      output.set(value,offset); offset+=value.length;
    }
    await writing; aborted(signal);
    if(offset!==predicted) fail();
    return output;
  } finally {
    signal?.removeEventListener("abort",cancel);
    await Promise.allSettled([reader.cancel(),writer.abort()]);
    await writing.catch(()=>{}); reader.releaseLock(); writer.releaseLock();
  }
}

/** Optional browser codec only; it confers no snapshot authority or storage
 * readiness. Caller must retain existing SDK opt-in and durable-storage gates.
 * SDK must reserve 64KiB fixed codec workspace per pending stage in addition
 * to raw*12 + encoded*8; retain the charge until the promise settles.
 * No Node imports, polyfills, dependencies, or implicit capability activation.
 */
export async function createBrowserStarterInflater(options: { signal?: AbortSignal; maxInputBytes?: number } = {}): Promise<StarterInflater | undefined> {
  const {signal}=options, maxInput=options.maxInputBytes??PIECE;
  if(!Number.isSafeInteger(maxInput)||maxInput<1||maxInput>PIECE) throw new Error("starter-browser-input-limit");
  aborted(signal);
  if(typeof DecompressionStream!=="function") return undefined;
  try {
    // Actual native EOF/format probe, rather than constructor availability.
    const empty=new Uint8Array([0,...TAIL]);
    await decode(empty,0,signal);
  } catch { aborted(signal); return undefined; }
  return async (encoded,limits) => {
    aborted(signal);
    const length=encoded.byteLength, maxOutput=limits.maxOutputBytes;
    if(!(encoded instanceof Uint8Array)||length>maxInput||!Number.isSafeInteger(maxOutput)||maxOutput<0||maxOutput>PIECE||limits.windowBits!==12||limits.flush!=="sync") throw new Error("starter-browser-limit");
    // Copy synchronously before the first await, including SAB-backed inputs.
    // A racing SAB writer can alter the copy, but proof and decode use the
    // SAME private copy; later caller mutations cannot invalidate the proof.
    const owned=new Uint8Array(length+TAIL.length); owned.set(new Uint8Array(encoded.buffer,encoded.byteOffset,length)); owned.set(TAIL,length);
    const predicted=predictedSize(owned.subarray(0,length),maxOutput,signal);
    return decode(owned,predicted,signal);
  };
}
