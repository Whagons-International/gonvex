import clone from '@ungap/structured-clone';
import { createLocalReducerWorker, type LocalExecutor, type LocalWorkerEndpoint } from './worker-client.js';

/** The native view is only an execution sandbox. Durable state stays in the SDK replica/outbox. */
export class NativeReducerBridge implements LocalWorkerEndpoint {
  private listeners = new Map<string, Array<(event: any) => void>>();
  private sender?: (message: unknown) => void;
  private stopped = false;
  readonly executor: LocalExecutor;
  constructor(readonly id: number, private readonly onClose: () => void) {
    this.executor = createLocalReducerWorker(this);
  }
  addEventListener(type: string, listener: (event: any) => void): void {
    this.listeners.set(type, [...this.listeners.get(type) ?? [], listener]);
  }
  private emit(type: string, event: unknown): void {
    for (const listener of this.listeners.get(type) ?? []) listener(event);
  }
  attach(sender: (message: unknown) => void): void { this.sender = sender; }
  receive(encoded: string): void {
    if (this.stopped) return;
    try {
      const message = JSON.parse(encoded);
      if (Array.isArray(message.result?.rejected)) {
        for (const rejected of message.result.rejected) {
          if (rejected.error) rejected.error = Object.assign(new Error(rejected.error.message), {name: rejected.error.name});
        }
      }
      this.emit('message', {data: message});
    } catch { this.emit('messageerror', {}); }
  }
  fail(message: string): void { this.emit('error', {message}); }
  postMessage(message: unknown): void {
    if (this.stopped || !this.sender) throw new Error('Native Reducer host is unavailable');
    this.sender(message);
  }
  terminate(): void {
    if (this.stopped) return;
    this.stopped = true;
    this.sender = undefined;
    this.onClose();
  }
}

/** Generated bindings create this registry; generated UI mounts one sandbox per client. */
export function nativeReducerHosts() {
  let nextId = 0;
  let hosts: readonly NativeReducerBridge[] = [];
  const listeners = new Set<() => void>();
  const emit = () => { for (const listener of listeners) listener(); };
  return {
    snapshot: () => hosts,
    subscribe(listener: () => void) { listeners.add(listener); return () => { listeners.delete(listener); }; },
    create() {
      const id = ++nextId;
      const host = new NativeReducerBridge(id, () => { hosts = hosts.filter(entry => entry.id !== id); emit(); });
      hosts = [...hosts, host];
      emit();
      return host.executor;
    },
  };
}

/** Serialize the bridge envelope as data for WebView's script injection transport. */
export function nativeMessageScript(message: unknown): string {
  const encoded = JSON.stringify(JSON.stringify(message)).replaceAll('\u2028','\\u2028').replaceAll('\u2029','\\u2029');
  return `window.dispatchEvent(new MessageEvent('message',{data:JSON.parse(${encoded})}));true;`;
}

/** Called by generated Expo bindings before constructing the client. */
export function installNativeRuntimeGlobals(random: {getRandomValues(array: Uint8Array): Uint8Array; randomUUID(): string}): void {
  if (typeof globalThis.structuredClone !== 'function') Object.defineProperty(globalThis,'structuredClone',{value:(value: unknown)=>clone(value,{lossy:false}),configurable:true});
  if (typeof globalThis.crypto?.getRandomValues !== 'function') {
    Object.defineProperty(globalThis,'crypto',{value:{getRandomValues:random.getRandomValues,randomUUID:random.randomUUID},configurable:true});
  }
}
