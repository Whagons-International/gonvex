/** Node-only optional subpath. Never imported by the browser SDK entry. */
import { inflateRawSync, constants } from "node:zlib";
/** @type {import("./starter-bootstrap.js").StarterInflater} */
export const nodeStarterInflater = async (encoded, limits) => {
  if(encoded.byteLength>256*1024||!Number.isSafeInteger(limits.maxOutputBytes)||limits.maxOutputBytes<1||limits.maxOutputBytes>256*1024||limits.windowBits!==12||limits.flush!=="sync")throw new Error("starter-codec-limit");
  const framed=new Uint8Array(encoded.byteLength+4);framed.set(encoded);framed.set([0,0,255,255],encoded.byteLength);
  return inflateRawSync(framed,{windowBits:12,finishFlush:constants.Z_SYNC_FLUSH,maxOutputLength:limits.maxOutputBytes});
};
