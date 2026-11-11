/**
 * The decoder's accept/close boundary. Every expectation was measured against
 * the Rust runtime at 0bf3ad13 (conformance scenario `session-frames`, step
 * "decoder accept/close boundary"): `true` = Rust accepted the frame, `false`
 * = Rust closed the socket with 1003.
 */
import { describe, expect, it } from "vitest";
import { decodeClientFrame, MAX_DEPTH } from "../decode";

const RUST: Array<[string, string, boolean]> = [
  ["not json", "hello", false],
  ["json scalar", "5", false],
  ["unknown type", '{"type":"nope","id":"x"}', false],
  ["missing type", '{"id":"x"}', false],
  ["type not a string", '{"type":5}', false],
  ["ping frame", '{"type":"ping"}', false],
  ["unsubscribe ok", '{"type":"query.unsubscribe","id":"x"}', true],
  ["unsubscribe extra field", '{"type":"query.unsubscribe","id":"x","extra":[1,{"a":null}]}', true],
  ["unsubscribe missing id", '{"type":"query.unsubscribe"}', false],
  ["unsubscribe id number", '{"type":"query.unsubscribe","id":5}', false],
  ["unsubscribe id null", '{"type":"query.unsubscribe","id":null}', false],
  ["duplicate id", '{"type":"query.unsubscribe","id":"a","id":"b"}', false],
  ["duplicate type", '{"type":"query.unsubscribe","type":"query.unsubscribe","id":"a"}', false],
  ["sequence form", '["query.unsubscribe","x"]', true],
  ["sequence form too long", '["query.unsubscribe","x","y"]', false],
  ["sequence form missing", '["query.unsubscribe"]', false],
  ["presence.unsubscribe empty", '{"type":"presence.unsubscribe"}', true],
  ["presence.set free string", '{"type":"presence.set","state":"gone"}', true],
  ["presence.set missing state", '{"type":"presence.set"}', false],
  ["auth contract -1", '{"type":"auth","id":"v","clientContract":-1}', false],
  ["auth contract 1.5", '{"type":"auth","id":"v","clientContract":1.5}', false],
  ["auth contract 1.0", '{"type":"auth","id":"v","clientContract":1.0}', false],
  ["auth contract 1e0", '{"type":"auth","id":"v","clientContract":1e0}', false],
  ["auth contract u64 max", '{"type":"auth","id":"v","clientContract":18446744073709551615}', true],
  ["auth contract u64 max + 1", '{"type":"auth","id":"v","clientContract":18446744073709551616}', false],
  ["auth contract string", '{"type":"auth","id":"v","clientContract":"1"}', false],
  ["auth contract null", '{"type":"auth","id":"v","clientContract":null}', true],
  ["auth capability 2", '{"type":"auth","id":"v","capabilities":{"presence":2}}', true],
  ["auth capability 255", '{"type":"auth","id":"v","capabilities":{"presence":255}}', true],
  ["auth capability 256", '{"type":"auth","id":"v","capabilities":{"presence":256}}', false],
  ["auth capability -0", '{"type":"auth","id":"v","capabilities":{"presence":-0}}', false],
  ["auth capability string", '{"type":"auth","id":"v","capabilities":{"presence":"1"}}', false],
  ["auth capability null", '{"type":"auth","id":"v","capabilities":{"presence":null}}', true],
  ["auth capability unknown", '{"type":"auth","id":"v","capabilities":{"somethingNew":"x"}}', true],
  ["auth capabilities array", '{"type":"auth","id":"v","capabilities":[]}', true],
  ["auth capabilities array of 1", '{"type":"auth","id":"v","capabilities":[1]}', true],
  ["auth capabilities number", '{"type":"auth","id":"v","capabilities":5}', false],
  ["auth capabilities null", '{"type":"auth","id":"v","capabilities":null}', true],
  ["auth controlOnly null", '{"type":"auth","id":"v","controlOnly":null}', false],
  ["auth controlOnly 1", '{"type":"auth","id":"v","controlOnly":1}', false],
  ["auth public string", '{"type":"auth","id":"v","public":"false"}', false],
  ["auth token number", '{"type":"auth","id":"v","token":5}', false],
  ["auth token null", '{"type":"auth","id":"v","token":null}', true],
  ["auth device viewport 1.5", '{"type":"auth","id":"v","device":{"viewportWidth":1.5}}', false],
  ["auth device viewport u32 max", '{"type":"auth","id":"v","device":{"viewportWidth":4294967295}}', true],
  ["auth device viewport u32 max + 1", '{"type":"auth","id":"v","device":{"viewportWidth":4294967296}}', false],
  ["auth device memory int", '{"type":"auth","id":"v","device":{"deviceMemory":8}}', true],
  ["auth device memory string", '{"type":"auth","id":"v","device":{"deviceMemory":"8"}}', false],
  ["auth device userAgent number", '{"type":"auth","id":"v","device":{"userAgent":5}}', false],
  ["auth device null", '{"type":"auth","id":"v","device":null}', true],
  ["auth id missing", '{"type":"auth","controlOnly":true}', false],
  ["query.call no args", '{"type":"query.call","id":"q","path":"p"}', true],
  ["query.call scope control", '{"type":"query.call","id":"q","path":"p","scope":"control"}', true],
  ["query.call scope other", '{"type":"query.call","id":"q","path":"p","scope":"other"}', false],
  ["query.call scope null", '{"type":"query.call","id":"q","path":"p","scope":null}', true],
  ["query.call path missing", '{"type":"query.call","id":"q"}', false],
  ["query.subscribe windowRevision number", '{"type":"query.subscribe","id":"q","path":"p","windowRevision":5}', false],
  ["subscribeMany null", '{"type":"query.subscribeMany","subscribes":null}', false],
  ["subscribeMany item missing path", '{"type":"query.subscribeMany","subscribes":[{"id":"q"}]}', false],
  ["reducer.call contract float", '{"type":"reducer.call","id":"r","path":"p","clientContract":1e3}', false],
  ["reducer.call trace string", '{"type":"reducer.call","id":"r","path":"p","trace":{"clientSentAtMs":"1"}}', false],
  ["reducer.call trace int", '{"type":"reducer.call","id":"r","path":"p","trace":{"clientSentAtMs":1}}', true],
  ["reducer.callMany not array", '{"type":"reducer.callMany","calls":{}}', false],
  ["reducer.callMany empty", '{"type":"reducer.callMany","calls":[]}', true],
  ["replica.open cursor revision string", '{"type":"replica.open","id":"o","path":"p","cursor":{"epoch":"e","revision":"5"}}', false],
  ["replica.open cursor sequence form", '{"type":"replica.open","id":"o","path":"p","cursor":["e",5]}', true],
  ["replica.open fullIntegrity null", '{"type":"replica.open","id":"o","path":"p","fullIntegrity":null}', false],
  ["replica.open hashes number value", '{"type":"replica.open","id":"o","path":"p","hashes":{"a":5}}', false],
  ["replica.open keys number", '{"type":"replica.open","id":"o","path":"p","keys":[1]}', false],
  ["replica.openMany missing", '{"type":"replica.openMany"}', false],
  ["replica.snapshotAck sequence -1", '{"type":"replica.snapshotAck","id":"o","snapshotId":"s","sequence":-1}', false],
  ["telemetry.event missing clientReceivedAtMs", '{"type":"telemetry.event","id":"t","kind":"query","path":"p","outcome":"ok"}', false],
  ["telemetry.event ok", '{"type":"telemetry.event","id":"t","kind":"anything","path":"p","outcome":"ok","clientReceivedAtMs":1.5}', true],
  ["telemetry.batch null events", '{"type":"telemetry.batch","events":null}', false],
  ["error.heartbeat id number", '{"type":"error.heartbeat","id":1}', false],
  ["lone surrogate in id", '{"type":"query.unsubscribe","id":"\\ud800"}', false],
  ["escaped pair in id", '{"type":"query.unsubscribe","id":"\\ud83d\\ude00"}', true],
  ["nesting 126", `{"type":"query.call","id":"q","path":"p","args":${"[".repeat(125)}${"]".repeat(125)}}`, true],
  ["nesting 127", `{"type":"query.call","id":"q","path":"p","args":${"[".repeat(126)}${"]".repeat(126)}}`, true],
  ["nesting 128", `{"type":"query.call","id":"q","path":"p","args":${"[".repeat(127)}${"]".repeat(127)}}`, false],
  ["trailing garbage", '{"type":"query.unsubscribe","id":"x"} x', false],
  ["trailing whitespace", '{"type":"query.unsubscribe","id":"x"} \n', true],
];

describe("decodeClientFrame matches serde's accept/close boundary (Rust, measured)", () => {
  for (const [label, text, accepted] of RUST) {
    it(`${accepted ? "accepts" : "refuses"}: ${label}`, () => {
      const result = decodeClientFrame(text);
      expect(result.ok, result.ok ? "accepted" : result.reason).toBe(accepted);
    });
  }
});

describe("decoded frames", () => {
  it("fills serde defaults and keeps the u64 contract exact", () => {
    const r = decodeClientFrame('{"type":"auth","id":"a","clientContract":18446744073709551615,"capabilities":[1,null,2]}');
    expect(r.ok && r.frame).toEqual({
      type: "auth", id: "a", clientContract: 18446744073709551615n, controlOnly: false, public: false,
      capabilities: { replicaNoOldValues: 1, replicaSetDigest: 2 },
    });
  });

  it("reads the sequence form positionally", () => {
    const r = decodeClientFrame('["replica.open","o","p",{"x":1},["e",7],["k"]]');
    expect(r.ok && r.frame).toEqual({ type: "replica.open", id: "o", path: "p", args: { x: 1 }, cursor: { epoch: "e", revision: 7 }, keys: ["k"], hashes: {}, fullIntegrity: false });
  });

  it("keeps Value subtrees as JSON, duplicates in them included (last wins, as serde_json::Value)", () => {
    const r = decodeClientFrame('{"type":"query.call","id":"q","path":"p","args":{"a":1,"a":2,"b":[1.5,"x",null]}}');
    expect(r.ok && r.frame).toEqual({ type: "query.call", id: "q", path: "p", args: { a: 2, b: [1.5, "x", null] } });
  });

  it("refuses non-finite numbers anywhere, as serde_json does", () => {
    expect(decodeClientFrame('{"type":"query.call","id":"q","path":"p","args":[1e400]}').ok).toBe(false);
    expect(decodeClientFrame('{"type":"query.call","id":"q","path":"p","args":[123456789012345678901234567890]}').ok).toBe(true);
  });

  it(`allows nesting to depth ${MAX_DEPTH} in every container, not only args`, () => {
    const deep = (n: number) => `${"{\"a\":".repeat(n)}1${"}".repeat(n)}`;
    expect(decodeClientFrame(`{"type":"error.envelope","id":"e","events":[${deep(MAX_DEPTH - 2)}]}`).ok).toBe(true);
    expect(decodeClientFrame(`{"type":"error.envelope","id":"e","events":[${deep(MAX_DEPTH - 1)}]}`).ok).toBe(false);
  });

  it("decodes a 4 MiB replica.openMany (Value subtrees go to the native parser)", () => {
    const keys = Array.from({ length: 50_000 }, (_, i) => `row-${i}`);
    const hashes = Object.fromEntries(keys.map(k => [k, "a".repeat(64)]));
    const text = JSON.stringify({ type: "replica.openMany", opens: [{ id: "o", path: "p", args: { filter: { status: "open" } }, keys, hashes }] });
    expect(text.length).toBeGreaterThan(4_000_000);
    const r = decodeClientFrame(text);
    expect(r.ok && r.frame.type === "replica.openMany" && r.frame.opens[0]!.keys.length).toBe(50_000);
  });
});
