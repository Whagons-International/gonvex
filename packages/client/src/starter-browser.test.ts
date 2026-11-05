import { describe, it, expect } from 'vitest';
import { deflateRawSync, constants } from 'node:zlib';
import { createBrowserStarterInflater } from './starter-browser.js';
// The same qualification checks are executed in native Chromium by the fixture.
// @ts-expect-error JavaScript browser fixture has no declaration file.
import { qualify } from '../test/starter-browser/codec-fixture.mjs';

describe('optional bounded browser starter codec', () => {
  it('cross-decodes real SyncFlush streams and rejects malformed/oversized data before native work', async () => {
    const corpus=[];
    for(const raw of [Buffer.alloc(0),Buffer.from('a'),Buffer.from('😀é\\u0000\n{"id":"x","body":"escaped\\n"}\n'.repeat(200)),Buffer.alloc(256*1024,65)]) {
      for(const strategy of [constants.Z_DEFAULT_STRATEGY,constants.Z_FIXED,constants.Z_HUFFMAN_ONLY]) {
        const encoded=deflateRawSync(raw,{level:1,windowBits:12,strategy,finishFlush:constants.Z_SYNC_FLUSH}).subarray(0,-4);
        corpus.push({name:String(corpus.length),raw:[...raw],encoded:[...encoded]});
      }
    }
    const result=await qualify(createBrowserStarterInflater,corpus);
    expect(result.corpus).toBe(12);
  });
  it('copies shared input before yielding and rejects oversized input and invalid factory caps', async () => {
    const codec=(await createBrowserStarterInflater())!;
    const raw=Buffer.from('shared buffer isolation '.repeat(100));
    const encoded=deflateRawSync(raw,{level:1,windowBits:12,finishFlush:constants.Z_SYNC_FLUSH}).subarray(0,-4);
    const bytes=new Uint8Array(new SharedArrayBuffer(encoded.length));bytes.set(encoded);
    const pending=codec(bytes,{maxOutputBytes:raw.length,windowBits:12,flush:'sync'});bytes.fill(255);
    expect(Buffer.from(await pending)).toEqual(raw);
    await expect(codec(new Uint8Array(262145),{maxOutputBytes:1,windowBits:12,flush:'sync'})).rejects.toThrow(/limit/);
    await expect(createBrowserStarterInflater({maxInputBytes:Infinity})).rejects.toThrow(/limit/);
    const c=new AbortController();c.abort();await expect(createBrowserStarterInflater({signal:c.signal})).rejects.toBeDefined();
  });
});
