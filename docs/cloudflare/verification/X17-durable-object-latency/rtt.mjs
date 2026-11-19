// RTT: client -> CF edge (ping auto-response) vs client -> gateway Durable Object (unauthenticated query.call -> query.error, no I/O).
const url=process.argv[2]||'wss://gonvex-cf-dev-integration-20261117.whagons-international.workers.dev/ws';const N=+(process.argv[3]||25);
const ws=new WebSocket(url);const t0=Date.now();let open;
const waiters=[];ws.onmessage=e=>{const w=waiters.shift();if(w)w(e.data);};
await new Promise((r,j)=>{ws.onopen=()=>{open=Date.now()-t0;r();};ws.onerror=j;});
const rt=async(msg)=>{const s=performance.now();const p=new Promise(r=>waiters.push(r));ws.send(msg);const d=await p;return [performance.now()-s,String(d).slice(0,40)];};
await new Promise(r=>setTimeout(r,300));
while(waiters.length)waiters.shift();
const ping=[],q=[];let sample;
for(let i=0;i<N;i++){ping.push((await rt('{"type":"ping"}'))[0]);const [ms,d]=await rt(JSON.stringify({type:'query.call',id:'r'+i,path:'x.y',args:{}}));q.push(ms);sample=d;await new Promise(r=>setTimeout(r,50));}
const med=a=>{const s=[...a].sort((x,y)=>x-y);return +s[s.length>>1].toFixed(1)};
console.log(JSON.stringify({at:new Date().toISOString(),openMs:open,pingMed:med(ping),pingMin:+Math.min(...ping).toFixed(1),gatewayMed:med(q),gatewayMin:+Math.min(...q).toFixed(1),n:N,sample}));ws.close();
