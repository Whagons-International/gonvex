/** Runs unchanged in Node's Web APIs and in native Chromium. */
export async function qualify(create, corpus) {
  let assertions=0;
  const check=(ok,label)=>{assertions++;if(!ok)throw new Error(label);};
  const equal=(a,b)=>a.length===b.length&&a.every((v,i)=>v===b[i]);
  const limits=n=>({maxOutputBytes:n,windowBits:12,flush:'sync'});
  const inflate=await create(); check(!!inflate,'native probe');
  for(const c of corpus) {
    const encoded=Uint8Array.from(c.encoded), raw=Uint8Array.from(c.raw);
    const actual=await inflate(encoded,limits(raw.length)); check(equal(actual,raw),'crossdecode '+c.name);
    const mutable=encoded.slice(), pending=inflate(mutable,limits(raw.length));mutable.fill(255);
    check(equal(await pending,raw),'private input copy '+c.name);
    if(raw.length) {let denied=false;try{await inflate(encoded,limits(raw.length-1));}catch{denied=true;}check(denied,'output bound '+c.name);}
  }
  for(const bad of [[],[1],[7],[3,0],[0,0,0,0,0],...corpus.slice(0,30).flatMap(c=>[c.encoded.slice(0,-1),[...c.encoded,0],[...c.encoded,1,0,0,255,255]])]) {
    let denied=false;try{await inflate(Uint8Array.from(bad),limits(262144));}catch{denied=true;}check(denied,'malformed/truncated/trailing');
  }
  for(const n of [-1,NaN,Infinity,1.5,262145,Number.MAX_SAFE_INTEGER]) {
    let denied=false;try{await inflate(Uint8Array.of(0),limits(n));}catch{denied=true;}check(denied,'invalid limit');
  }
  const saved=globalThis.DecompressionStream;
  const malformed=[];
  const bits=(fn)=>{const a=[];const w=(n,v)=>{for(let i=0;i<n;i++)a.push((v>>>i)&1);};const h=(n,v)=>{for(let i=n-1;i>=0;i--)w(1,(v>>>i)&1);};fn(w,h);return Uint8Array.from({length:Math.ceil(a.length/8)},(_,i)=>a.slice(i*8,i*8+8).reduce((v,b,j)=>v+(b<<j),0));};
  for(const codes of [[1,1,1,0],[0,0,0,2],[1,0,0,1],[0,0,1,1]]) {
    malformed.push(bits((w,h)=>{w(1,0);w(2,2);w(5,0);w(5,0);w(4,0);for(const n of codes)w(3,n);h(1,1);w(7,127);h(1,1);w(7,127);}));
  }
  malformed.push(bits((w,h)=>{w(1,0);w(2,1);h(8,198);})); // reserved literal 286
  malformed.push(bits((w,h)=>{w(1,0);w(2,1);h(7,1);h(5,0);})); // backref before any output
  malformed.push(Uint8Array.of(0,1,0,0,0)); // stored LEN/NLEN
  malformed.push(bits((w,h)=>{w(1,0);w(2,0);w(5,0);w(16,4097);w(16,65535^4097);for(let i=0;i<4097;i++)w(8,65);w(1,0);w(2,1);h(7,1);h(5,24);w(11,0);}));
  try {
    let calls=0;globalThis.DecompressionStream=class {constructor(){calls++;throw new Error('native must not see malformed');}};
    for(const bytes of malformed){let rejected=false;try{await inflate(bytes,limits(262144));}catch{rejected=true;}check(rejected,'malformed syntax');}
    check(calls===0,'all malformed rejected by preflight');
  }finally{globalThis.DecompressionStream=saved;}

  try {
    globalThis.DecompressionStream=undefined; check(await create()===undefined,'unsupported API');
    globalThis.DecompressionStream=class {constructor(){throw new Error('unsupported format');}};
    check(await create()===undefined,'unsupported format');
  } finally {globalThis.DecompressionStream=saved;}
  const controller=new AbortController(), abortable=await create({signal:controller.signal});
  const pending=abortable(Uint8Array.from(corpus.at(-1).encoded),limits(corpus.at(-1).raw.length));controller.abort();
  let denied=false;try{await pending;}catch{denied=true;}check(denied,'abort during read');
  // Preflight must reject BEFORE constructing any native decoder.
  let calls=0;
  try {
    globalThis.DecompressionStream=class {constructor(){calls++;throw new Error('unexpected native call');}};
    try{await inflate(Uint8Array.from(corpus.at(-1).encoded),limits(0));}catch{}
    check(calls===0,'no native allocation on overbudget');
  }finally{globalThis.DecompressionStream=saved;}
  return {assertions,corpus:corpus.length};
}
