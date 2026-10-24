import { encode, decode } from '@msgpack/msgpack';
import * as flex from 'flatbuffers/js/flexbuffers.js';
import protobuf from 'protobufjs';
import { decompress as unzstd } from 'fzstd';
import unbrotli from 'brotli/decompress.js';
import { LocalReplica } from '../../packages/client/src/local-replica.ts';
import { IndexedDBEngineReplicaStorage } from '../../packages/client/src/indexeddb-engine.ts';
import { applyPatch } from './patch-model.mjs';

const canonical = v => JSON.stringify(v, (_k, value) => value && !Array.isArray(value) && typeof value === 'object' ? Object.fromEntries(Object.keys(value).sort().map(k => [k, value[k]])) : value);
const textDecoder = new TextDecoder();
const textEncoder = new TextEncoder();
const source = await (await fetch('/fixtures.json')).json();
function stats(values) {
  const samples = [...values].sort((a, b) => a - b);
  const p = q => samples[Math.max(0, Math.ceil(q * samples.length) - 1)];
  return { samples: samples.length, p50: p(.5), p95: p(.95), p99: p(.99), mean: samples.reduce((a, b) => a + b) / samples.length };
}
const pbRoot = protobuf.Root.fromJSON({ nested: { Value: { oneofs: { kind: { oneof: ['nullValue', 'numberValue', 'stringValue', 'boolValue', 'structValue', 'listValue'] } }, fields: {
  nullValue: { type: 'int32', id: 1 }, numberValue: { type: 'double', id: 2 }, stringValue: { type: 'string', id: 3 }, boolValue: { type: 'bool', id: 4 }, structValue: { type: 'Struct', id: 5 }, listValue: { type: 'ListValue', id: 6 },
} }, Struct: { fields: { fields: { keyType: 'string', type: 'Value', id: 1 } } }, ListValue: { fields: { values: { rule: 'repeated', type: 'Value', id: 1 } } } } });
const pbValue = pbRoot.lookupType('Value');
function toProto(value) {
  if (value === null) return { nullValue: 0 };
  if (typeof value === 'string') return { stringValue: value };
  if (typeof value === 'number') return { numberValue: value };
  if (typeof value === 'boolean') return { boolValue: value };
  if (Array.isArray(value)) return { listValue: { values: value.map(toProto) } };
  return { structValue: { fields: Object.fromEntries(Object.entries(value).map(([k, v]) => [k, toProto(v)])) } };
}
function fromProto(value) {
  if (Object.hasOwn(value, 'nullValue')) return null;
  for (const k of ['numberValue', 'stringValue', 'boolValue']) if (Object.hasOwn(value, k)) return value[k];
  if (Object.hasOwn(value, 'listValue')) return value.listValue.values.map(fromProto);
  return Object.fromEntries(Object.entries(value.structValue.fields).map(([k, v]) => [k, fromProto(v)]));
}
function measure(run, count) {
  for (let i = 0; i < 5; i++) run();
  const samples = [];
  for (let i = 0; i < count; i++) { const start = performance.now(); run(); samples.push((performance.now() - start) * 1000); }
  return stats(samples);
}
window.codecBench = async () => {
  const results = [];
  for (const fixture of source.fixtures.filter(f => f.name !== 'reconnect-expired-cursor')) {
    const frames = fixture.variants.baseline;
    const count = fixture.after.length > 1000 ? 8 : 100;
    const codecs = {
      json: [v => textEncoder.encode(JSON.stringify(v)), b => JSON.parse(textDecoder.decode(b))],
      msgpack: [v => encode(v), b => decode(b)],
      protobufValue: [v => pbValue.encode(toProto(v)).finish(), b => fromProto(pbValue.decode(b))],
      flexbuffers: [v => flex.encode(v), b => flex.toObject(b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength))],
    };
    for (const [name, [enc, dec]] of Object.entries(codecs)) {
      try {
      const bytes = frames.map(enc);
      // Equality guards the actual eager conversion, including null/arrays.
      if (canonical(bytes.map(dec)) !== canonical(frames)) throw new Error(`${name} round-trip failed`);
      const compressed = [];
      for (const bytesPart of bytes) {
        const stream = new Blob([bytesPart]).stream().pipeThrough(new CompressionStream('deflate-raw'));
        compressed.push((await new Response(stream).arrayBuffer()).byteLength);
      }
      results.push({ workload: fixture.name, codec: name, rawBytes: bytes.reduce((s, b) => s + b.length, 0),
        browserDefaultDeflateBytes: compressed.reduce((a, b) => a + b, 0), encodeUs: measure(() => frames.map(enc), count),
        decodeUs: measure(() => bytes.map(dec), count) });
      } catch (error) { results.push({ workload: fixture.name, codec: name, error: String(error) }); }
    }
    try {
    const bytes = flex.encode(frames.at(0));
    results.push({ workload: fixture.name, codec: 'flexbuffers-lazy-envelope-only',
      decodeUs: measure(() => flex.toReference(bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength)).get('type').stringValue(), count),
      limitation: 'Reads envelope type only. Does not decode rows or apply storage; not comparable to eager decoding.' });
    } catch (error) { results.push({ workload: fixture.name, codec: 'flexbuffers-lazy-envelope-only', error: String(error) }); }
  }
  await window.contractBench();
  return results;
};

window.contractBench = async () => {
  const serverContract = decode(new Uint8Array(await (await fetch('/contract.msgpack')).arrayBuffer()), { useBigInt64: true });
  if (serverContract.signed !== -(2n ** 63n) || serverContract.unsigned !== 2n ** 64n - 1n || serverContract.unsafeInteger !== 9007199254740993n || serverContract.nullable !== null || serverContract.type !== 'contract') throw new Error('Rust/browser MessagePack integer contract');
  const binary = new Uint8Array([0, 128, 255]);
  const exact = { signed: -(2n ** 63n), unsigned: 2n ** 64n - 1n, nullable: null, binary };
  const decoded = decode(encode(exact, { useBigInt64: true }), { useBigInt64: true });
  if (decoded.signed !== exact.signed || decoded.unsigned !== exact.unsigned || decoded.binary[2] !== 255 || decoded.nullable !== null) throw new Error('MessagePack value contract');
  return { rustInt64: true, rustUint64: true, rustUnsafeInteger: true, browserBinary: true, nullable: true };
};

window.pipelineBench = async (options) => {
  const fixture = source.fixtures.find(f => f.name === options.workload);
  const socket = new WebSocket(`${window.benchWsBase ?? location.origin.replace('http', 'ws')}/ws?pmd=${options.compression === 'deflate1-1024' ? 1 : 0}`);
  socket.binaryType = 'arraybuffer';
  await new Promise((resolve, reject) => { socket.onopen = resolve; socket.onerror = reject; });
  if (options.compression === 'deflate1-1024' && !socket.extensions.includes('permessage-deflate')) throw new Error('PMD not negotiated');
  const negotiatedExtensions = socket.extensions;
  const metrics = { latencyMs: [], decodeUs: [], applyUs: [], serverEncodeUs: [], serverCompressSendUs: [], sampledHeapBytes: [] };
  let maxHeap = 0;
  try {
    for (let trial = -(options.warmups ?? 5); trial < options.samples; trial++) {
      const namespace = 'gonvex-protocol-benchmark';
      const storage = options.storage === 'indexeddb' ? new IndexedDBEngineReplicaStorage(namespace) : undefined;
      const replica = new LocalReplica(storage, { maxResidentRows: 20000, maxResidentBytes: 128 * 1024 * 1024 });
      await replica.activateScope('bench');
      if (replica.entityRows('tasks').length !== 0) throw new Error('benchmark database is not empty');
      if (fixture.before.length) await replica.replaceWindow({ signature: 'tasks', kind: 'replica', entity: 'tasks', key: 'id',
        rows: fixture.before, cursor: { epoch: 'benchmark-epoch', revision: 1 }, completeness: 'complete', source: 'server' });
      const bases = new Map(fixture.before.map(row => [row.id, { value: row, revision: 1 }]));
      const deltaBases = new Map(bases);
      let decodeUs = 0, applyUs = 0, seen = 0, elapsed, trailer;
      let staged = [];
      const start = performance.now();
      await new Promise((resolve, reject) => {
        let queue = Promise.resolve();
        socket.onmessage = event => {
          queue = queue.then(async () => {
            const decoding = performance.now();
            let data = event.data;
            if (typeof data !== 'string' && options.compression === 'zstd3') data = unzstd(new Uint8Array(data));
            if (typeof data !== 'string' && options.compression === 'brotli4') data = unbrotli(new Uint8Array(data));
            const message = typeof data === 'string' ? JSON.parse(data) : options.codec === 'json' ? JSON.parse(typeof data === 'string' ? data : textDecoder.decode(data)) : decode(new Uint8Array(data));
            if (message.type === 'bench.done') { trailer = message; resolve(); return; }
            decodeUs += (performance.now() - decoding) * 1000;
            const applying = performance.now();
            if (message.type === 'replica.transaction') {
              const changes = message.changes.map(change => {
                if (change.operation !== 'patch') return change;
                const next = applyPatch(bases.get(change.id), change); bases.set(change.id, next);
                return { entity: change.entity, id: change.id, operation: 'update', newValue: next.value };
              });
              await replica.applyTransaction({ ...message, changes });
            } else if (message.type === 'replica.delta') {
              const upserts = message.patches ? message.patches.map(patch => {
                const next = applyPatch(deltaBases.get(patch.id), patch); deltaBases.set(patch.id, next); return next.value;
              }) : message.upserts;
              await replica.applyWindowDelta({ signature: 'tasks', kind: 'replica', entity: 'tasks', key: 'id', upserts,
                deleted: message.deleted, cursor: message.cursor, completeness: 'complete', source: 'server' });
            } else if (message.type === 'replica.snapshot' || message.type === 'replica.snapshotPage' || message.type === 'replica.snapshotEncodedPage') {
              staged.push(...(message.result ?? message.canonicalRows.map(row => JSON.parse(row))));
              if (message.type === 'replica.snapshot' || message.last) {
                await replica.replaceWindow({ signature: 'tasks', kind: 'replica', entity: 'tasks', key: 'id', rows: staged,
                  cursor: message.cursor, completeness: 'complete', source: 'server' });
              }
            }
            applyUs += (performance.now() - applying) * 1000;
            seen++;
            maxHeap = Math.max(maxHeap, performance.memory?.usedJSHeapSize ?? 0);
            if (seen === fixture.variants[options.variant].length) elapsed = performance.now() - start;
          }).catch(reject);
        };
        socket.onerror = reject;
        socket.send(JSON.stringify(options));
      });
      // Read the real replica after both consumers have applied the payload.
      if (canonical(replica.entity('tasks', fixture.after[0].id)) !== canonical(fixture.after[0])) throw new Error('replica mismatch');
      if (trial >= 0) {
        metrics.latencyMs.push(elapsed); metrics.decodeUs.push(decodeUs); metrics.applyUs.push(applyUs);
        metrics.serverEncodeUs.push(trailer.encodeUs); metrics.serverCompressSendUs.push(trailer.compressSendUs);
        metrics.sampledHeapBytes.push(performance.memory?.usedJSHeapSize ?? 0);
      }
      replica.dispose(); storage?.close();
      if (storage) {
        const owned = (await indexedDB.databases()).filter(database => database.name?.startsWith(namespace));
        if (owned.length !== 1) throw new Error('unexpected benchmark database namespace');
        for (const { name } of owned) await new Promise((resolve, reject) => { const request = indexedDB.deleteDatabase(name); request.onsuccess = resolve; request.onerror = reject; request.onblocked = () => reject(new Error('database cleanup blocked')); });
      }
      staged = [];
    }
  } finally { socket.close(); }
  return { ...options, negotiatedExtensions, ...Object.fromEntries(Object.entries(metrics).map(([name, samples]) => [name, stats(samples)])),
    sampledPeakHeapBytes: maxHeap, updatesPerSecond: fixture.after.length * 1000 / stats(metrics.latencyMs).mean,
    limitation: 'Owned loopback Rust transport to real LocalReplica, optional IndexedDB. Excludes SQL/feed/permissions, SDK dispatch, digest verification, TLS/WAN and reconnect handshake. Snapshots staged in JS; native direct ingestion is not simulated.' };
};
window.benchmarkReady = true;
