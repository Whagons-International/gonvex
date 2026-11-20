import WebSocket from "ws";
const base = "gonvex-cf-dev-x17-repro.whagons-international.workers.dev"; const target = "seq-" + Date.now();
const ws = new WebSocket(`wss://${base}/ws?o=${target}`, { perMessageDeflate: true });
let frames = 0, bytes = 0; ws.on("message", (d) => { frames++; bytes += d.length; });
ws.on("open", async () => {
  for (const rows of [500, 1000, 1000]) { const t = Date.now(); const r = await (await fetch(`https://${base}/seq?o=${target}&n=20&rows=${rows}`)).text(); console.log(rows, Date.now() - t, r.slice(0, 400)); }
  await new Promise((r) => setTimeout(r, 2000)); console.log("client", frames, bytes); ws.close(); process.exit(0);
});
