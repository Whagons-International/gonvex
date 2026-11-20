// X17 backpressure repro client (arch3): holds the target's WebSocket like a browser (deflate offered), reads every frame,
// then asks the Worker to blast frames to it and time ping RPCs to the target from another object.
import WebSocket from "ws";
const base = "gonvex-cf-dev-x17-repro.whagons-international.workers.dev";
const [n, size, rand, target] = [process.argv[2] || "20", process.argv[3] || "262144", process.argv[4] || "0", process.argv[5] || "bp-target-" + Date.now()];
const ws = new WebSocket(`wss://${base}/ws?o=${target}`, { perMessageDeflate: true });
let frames = 0, bytes = 0, first = 0, last = 0;
ws.on("message", (d) => { frames++; bytes += d.length; const t = Date.now(); if (!first) first = t; last = t; });
ws.on("open", async () => {
  await new Promise((r) => setTimeout(r, 500));
  const t0 = Date.now();
  const res = await (await fetch(`https://${base}/bp?o=${target}&n=${n}&size=${size}&rand=${rand}`)).json();
  await new Promise((r) => setTimeout(r, 4000));
  console.log(JSON.stringify({ n: +n, size: +size, rand: rand === "1", ext: ws.extensions, res, client: { frames, bytes, firstMs: first - t0, lastMs: last - t0 } }));
  ws.close(); process.exit(0);
});
