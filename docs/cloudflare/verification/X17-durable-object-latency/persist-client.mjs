import WebSocket from "ws";
const base = "gonvex-cf-dev-x17-repro.whagons-international.workers.dev"; const target = "pers-" + Date.now();
const ws = new WebSocket(`wss://${base}/ws?o=${target}`, { perMessageDeflate: true });
ws.on("open", async () => {
  for (const [size, k] of [[4096, 1], [65536, 4], [131072, 8]]) console.log(size, k, JSON.stringify(await (await fetch(`https://${base}/persist?o=${target}&size=${size}&k=${k}`)).json()));
  ws.close(); process.exit(0);
});
