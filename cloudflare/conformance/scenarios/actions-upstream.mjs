/**
 * The HTTP upstream the `actions` scenario's Actions fetch (package K12).
 * Not a scenario: `scenarios/index.mjs` does not list it.
 *
 * `upstream(request)` is written against web `Request`/`Response` only, so the
 * same answers serve the Rust runtime (through `startUpstream`, a Node HTTP
 * server on the fixture's fixed origin) and the kernel's workerd parity test
 * (`cloudflare/actions/test`, which routes the origin to this function).
 *
 * Every answer is deterministic. Event streams are written in several pieces,
 * one of them splitting a two-byte UTF-8 character, so a client that reads
 * them as they arrive sees more than one chunk.
 */
export const UPSTREAM_PORT = 47812;
export const UPSTREAM_ORIGIN = `http://127.0.0.1:${UPSTREAM_PORT}`;

const encoder = new TextEncoder();
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function piecewise(pieces, delayMs) {
  return new ReadableStream({
    async start(controller) {
      for (const piece of pieces) {
        controller.enqueue(piece);
        await sleep(delayMs);
      }
      controller.close();
    },
  });
}

function bytes(n, chunked) {
  const chunk = new Uint8Array(65536).fill(0x61);
  let left = n;
  const body = new ReadableStream({
    pull(controller) {
      if (left <= 0) { controller.close(); return; }
      const size = Math.min(left, chunk.length);
      controller.enqueue(chunk.subarray(0, size));
      left -= size;
    },
  });
  const headers = { 'content-type': 'text/plain' };
  if (!chunked) headers['content-length'] = String(n);
  return new Response(body, { headers });
}

export async function upstream(request) {
  const url = new URL(request.url);
  const path = url.pathname;
  if (path === '/json') return Response.json({ ok: true, n: 1 });
  if (path === '/text') return new Response('hello', { headers: { 'content-type': 'text/plain; charset=utf-8' } });
  if (path === '/binary') return new Response(new Uint8Array([0xff, 0x41, 0xc3]), { headers: { 'content-type': 'application/octet-stream' } });
  if (path.startsWith('/status/')) {
    const status = Number(path.slice('/status/'.length));
    const empty = status === 204 || status === 304;
    return new Response(empty ? null : `status ${status}`, { status, headers: { 'content-type': 'text/plain' } });
  }
  if (path === '/echo') {
    return Response.json({
      method: request.method,
      contentType: request.headers.get('content-type'),
      xk12: request.headers.get('x-k12'),
      body: await request.text(),
    });
  }
  if (path === '/redirect') return new Response(null, { status: 302, headers: { location: url.searchParams.get('to') ?? '/text' } });
  if (path === '/bytes') return bytes(Number(url.searchParams.get('n') ?? 0), url.searchParams.get('chunked') === '1');
  if (path === '/sse') {
    const events = Number(url.searchParams.get('events') ?? 5);
    const delayMs = Number(url.searchParams.get('delayMs') ?? 20);
    const pieces = [];
    for (let n = 1; n <= events; n++) {
      // "é" is 0xc3 0xa9: the first event's character is split across two pieces.
      const event = encoder.encode(`data: {"n":${n},"text":"café"}\n\n`);
      if (n === 1) {
        const cut = event.indexOf(0xc3) + 1;
        pieces.push(event.subarray(0, cut), event.subarray(cut));
      } else pieces.push(event);
    }
    return new Response(piecewise(pieces, delayMs), { headers: { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' } });
  }
  if (path === '/slow') {
    // No response headers until the delay is over (a media worker, S4 scenario 4).
    await sleep(Number(url.searchParams.get('ms') ?? 0));
    return new Response(`slept ${url.searchParams.get('ms') ?? 0}`, { headers: { 'content-type': 'text/plain' } });
  }
  if (path === '/sse-big') {
    const line = encoder.encode(`data: ${'x'.repeat(65_000)}\n\n`);
    const pieces = Array.from({ length: 140 }, () => line); // about 9.1 MB in total
    return new Response(piecewise(pieces, 0), { headers: { 'content-type': 'text/event-stream' } });
  }
  return new Response('not found', { status: 404, headers: { 'content-type': 'text/plain' } });
}

/** Serves `upstream` on 127.0.0.1:47812 for the Rust runtime. Resolves with `close()`. */
export async function startUpstream() {
  const { createServer } = await import('node:http');
  const { Readable } = await import('node:stream');
  const server = createServer(async (incoming, outgoing) => {
    try {
      const chunks = [];
      for await (const chunk of incoming) chunks.push(chunk);
      const headers = new Headers();
      for (const [name, value] of Object.entries(incoming.headers)) if (typeof value === 'string') headers.set(name, value);
      const method = incoming.method ?? 'GET';
      const request = new Request(`${UPSTREAM_ORIGIN}${incoming.url}`, {
        method, headers, body: method === 'GET' || method === 'HEAD' ? undefined : Buffer.concat(chunks),
      });
      const response = await upstream(request);
      const out = {};
      response.headers.forEach((value, name) => { out[name] = value; });
      outgoing.writeHead(response.status, out);
      if (!response.body) { outgoing.end(); return; }
      Readable.fromWeb(response.body).on('error', () => outgoing.destroy()).pipe(outgoing);
    } catch (error) {
      outgoing.writeHead(500); outgoing.end(String(error));
    }
  });
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(UPSTREAM_PORT, '127.0.0.1', resolve); });
  return { close: () => new Promise((resolve) => { server.closeAllConnections?.(); server.close(() => resolve()); }) };
}
