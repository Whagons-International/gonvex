#!/usr/bin/env node
// A stand-in for `gonvex-sandbox-worker` where the Rust binary is not built (CI):
// it speaks the worker's protocol (one JSON request on stdin, one response on
// stdout) so the manager's policy and request can be tested. The real worker
// is used whenever K20_SANDBOX_BIN or K20_SANDBOX_URL is set (test/global-setup.ts).
//   __echo           answers the request it received (code removed)
//   __sleep:<ms>     waits, then answers 1
//   __fail:<text>    answers ok:false with that error
//   __big:<bytes>    answers a string of that many bytes
//   anything else    answers { code }
import { readFileSync } from "node:fs";

const request = JSON.parse(readFileSync(0, "utf8"));
const reply = (value) => process.stdout.write(JSON.stringify(value));
const code = request.code;
if (request.timeoutMs === 0) {
  reply({ ok: false, result: null, error: "worker timeout is required", logs: [] });
} else if (code === "__echo") {
  const { code: _code, ...rest } = request;
  reply({ ok: true, result: rest, error: "", logs: [] });
} else if (code.startsWith("__sleep:")) {
  setTimeout(() => reply({ ok: true, result: 1, error: "", logs: [] }), Number(code.slice(8)));
} else if (code.startsWith("__fail:")) {
  reply({ ok: false, result: null, error: code.slice(7), logs: [] });
} else if (code.startsWith("__big:")) {
  reply({ ok: true, result: "x".repeat(Number(code.slice(6))), error: "", logs: [] });
} else {
  reply({ ok: true, result: { code }, error: "", logs: [{ level: "log", message: "stub" }] });
}
