import { assert, encode, prefixEnd } from "./codec.js";
import type { KeyValue } from "./database.js";

export function* postingBits(body: Uint8Array): Generator<number> {
  assert(body.length >= 1, "Corrupt posting bitmap");
  if (body[0] === 0) {
    assert(body.length === 33, "Corrupt dense posting length");
    for (let i = 0; i < 256; i++)
      if (body[1 + (i >>> 3)]! & (1 << (i & 7))) yield i;
  } else {
    assert(body[0] === 1 && body.length <= 33, "Corrupt sparse posting length");
    let previous = -1;
    for (let i = 1; i < body.length; i++) {
      const bit = body[i]!;
      assert(bit > previous, "Corrupt sparse posting order");
      previous = bit;
      yield bit;
    }
  }
}
function compress(bitmap: Uint8Array) {
  const bits: number[] = [];
  for (let i = 0; i < 256; i++)
    if (bitmap[i >>> 3]! & (1 << (i & 7))) bits.push(i);
  return {
    bytes:
      bits.length < 32
        ? Uint8Array.from([1, ...bits])
        : Uint8Array.from([0, ...bitmap]),
    count: bits.length,
  };
}
/** Merge partial posting blocks using OR. A block covers 256 input ordinals,
 * so dense common terms and sparse rare terms both stay small. */
export function* postingGroups(input: Iterable<KeyValue>): Generator<KeyValue> {
  let group = "",
    key = "",
    bitmap: Uint8Array | undefined,
    count = 0;
  const finishBlock = (): KeyValue | undefined => {
    if (!bitmap) return;
    const result = compress(bitmap);
    count += result.count;
    bitmap = undefined;
    return { key, value: result.bytes };
  };
  for (const item of input) {
    const split = item.key.startsWith("g/") ? item.key.lastIndexOf("/") + 1 : 0,
      next = split ? item.key.slice(0, split) : "";
    if (item.key !== key) {
      const block = finishBlock();
      if (block) yield block;
    }
    if (group !== next) {
      if (group) yield { key: prefixEnd(group), value: encode(String(count)) };
      count = 0;
      group = next;
    }
    key = item.key;
    if (!group) {
      yield item;
      continue;
    }
    assert(item.value.length === 32, "Corrupt staging posting");
    if (!bitmap) bitmap = item.value.slice();
    else for (let i = 0; i < 32; i++) bitmap[i] = bitmap[i]! | item.value[i]!;
  }
  const block = finishBlock();
  if (block) yield block;
  if (group) yield { key: prefixEnd(group), value: encode(String(count)) };
}
