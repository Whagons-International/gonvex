import { expect, it } from 'vitest';
import { utf8ByteLength } from './utf8-size.js';

it('counts UTF-8 bytes exactly for ASCII, Unicode, surrogate pairs and malformed UTF-16', () => {
  const encoder = new TextEncoder();
  for (const value of ['', 'plain ASCII', 'ação', '任务界', '🙂🎉', '\u0000\u007f\u0080\u07ff\u0800', '\ud800', '\udc00', '\ud800A', '\ud800\ud800\udc00']) {
    expect(utf8ByteLength(value)).toBe(encoder.encode(value).byteLength);
  }
  let state = 17;
  for (let sample = 0; sample < 100; sample++) {
    let text = '';
    for (let i = 0; i < 200; i++) { state = (Math.imul(state, 1664525) + 1013904223) >>> 0; text += String.fromCharCode(state & 0xffff); }
    expect(utf8ByteLength(text)).toBe(encoder.encode(text).byteLength);
  }
});
