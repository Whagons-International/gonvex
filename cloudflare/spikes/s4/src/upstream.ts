// Test upstream for the S4 spike. Stands in for the services Whagons Actions
// call, so the spike never touches a paid API:
//   /sse       an OpenRouter-style Server-Sent Events stream
//   /slow      a media worker that answers only after N seconds
//   /download  a deterministic N MB body (a Telnyx recording stand-in)
//   /sink      an upload target that counts and hashes what it receives
//   /poll      a sandbox status endpoint that turns done after a deadline
import { block, BLOCK_BYTES, hex } from "./bytes";

interface Env {
  S4_TOKEN?: string;
}

const json = (value: unknown, status = 200) =>
  new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json" } });

const number = (url: URL, name: string, fallback: number, max: number) => {
  const value = Number(url.searchParams.get(name) ?? fallback);
  return Number.isFinite(value) ? Math.min(Math.max(value, 0), max) : fallback;
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function sse(seconds: number, intervalMs: number): Response {
  const encoder = new TextEncoder();
  const started = Date.now();
  let n = 0;
  const body = new ReadableStream<Uint8Array>({
    async pull(controller) {
      if (Date.now() - started >= seconds * 1000) {
        controller.enqueue(encoder.encode(`event: done\ndata: {"events":${n}}\n\n`));
        controller.close();
        return;
      }
      if (n > 0) await sleep(intervalMs);
      n += 1;
      // A token-sized delta, like an OpenRouter chat completion chunk.
      controller.enqueue(
        encoder.encode(`id: ${n}\ndata: {"n":${n},"t":${Date.now()},"delta":"tok${n} "}\n\n`),
      );
    },
  });
  return new Response(body, {
    headers: { "content-type": "text/event-stream", "cache-control": "no-store" },
  });
}

function download(mb: number): Response {
  const blocks = Math.floor((mb * 1024 * 1024) / BLOCK_BYTES);
  let index = 0;
  const body = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (index >= blocks) {
        controller.close();
        return;
      }
      controller.enqueue(block(index));
      index += 1;
    },
  }, { highWaterMark: 0 });
  // A plain ReadableStream body is sent chunked; FixedLengthStream keeps the
  // Content-Length header, as a real file server sends it.
  const fixed = new FixedLengthStream(blocks * BLOCK_BYTES);
  void body.pipeTo(fixed.writable);
  return new Response(fixed.readable, {
    headers: {
      "content-type": "application/octet-stream",
      "content-length": String(blocks * BLOCK_BYTES),
    },
  });
}

async function sink(request: Request): Promise<Response> {
  const digest = new crypto.DigestStream("SHA-256");
  const writer = digest.getWriter();
  let bytes = 0;
  const reader = request.body!.getReader();
  for (;;) {
    const part = await reader.read();
    if (part.done) break;
    bytes += part.value.byteLength;
    await writer.write(part.value);
  }
  await writer.close();
  return json({ bytes, sha256: hex(await digest.digest) });
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (env.S4_TOKEN && request.headers.get("authorization") !== `Bearer ${env.S4_TOKEN}`) {
      return json({ error: "unauthorized" }, 401);
    }
    const url = new URL(request.url);
    switch (url.pathname) {
      case "/sse":
        return sse(number(url, "seconds", 300, 3600), number(url, "intervalMs", 200, 60_000));
      case "/slow": {
        const seconds = number(url, "seconds", 800, 3600);
        const started = Date.now();
        await sleep(seconds * 1000);
        return json({ waitedMs: Date.now() - started, video: { size: 1234 } });
      }
      case "/download":
        return download(number(url, "mb", 64, 4096));
      case "/sink":
        return sink(request);
      case "/poll": {
        const until = number(url, "until", 0, Number.MAX_SAFE_INTEGER);
        return json({ done: Date.now() >= until, now: Date.now() });
      }
      default:
        return json({ error: "not found" }, 404);
    }
  },
};
