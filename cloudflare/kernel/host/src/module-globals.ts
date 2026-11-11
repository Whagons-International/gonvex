// ES module the app-bundle prelude imports (see `bundle.ts`): each export is a
// module-scope binding that shadows the global of the same name inside the
// bundle, and only there.
import { moduleGlobals } from "./globals.js";

const g = moduleGlobals as Record<string, unknown>;

const Date = g["Date"] as DateConstructor;
const Math = g["Math"] as Math;
const crypto = g["crypto"] as Crypto;
const console = g["console"] as Console;
const globalThis = g["globalThis"];
const self = g["self"];
const fetch = undefined;
const WebSocket = undefined;
const Request = undefined;
const caches = undefined;
const structuredClone = undefined;
const queueMicrotask = undefined;
const performance = undefined;
const Blob = undefined;
const FormData = undefined;
const EventTarget = undefined;
const navigator = undefined;
const scheduler = undefined;

export {
  Date, Math, crypto, console, globalThis, self, fetch, WebSocket, Request, caches, structuredClone, queueMicrotask,
  performance, Blob, FormData, EventTarget, navigator, scheduler,
};
