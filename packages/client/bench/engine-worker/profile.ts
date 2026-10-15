type Profile = { requests: Record<string, number>; rowsRead: Record<string, number>; requestMs: Record<string, number>; rpcPackets: number; rpcCalls: number; hostAdmissionMs: number[] };
let profile: Profile;
export function resetProfile() {
  profile = { requests: {}, rowsRead: {}, requestMs: {}, rpcPackets: 0, rpcCalls: 0, hostAdmissionMs: [] };
}
resetProfile();
export function readProfile() { return structuredClone(profile); }
export function installProfile() {
  for (const prototype of [IDBObjectStore.prototype, IDBIndex.prototype]) {
    for (const name of ["get", "getAll", "openCursor", "put", "add", "delete", "clear"] as const) {
      const original = (prototype as any)[name];
      if (!original) continue;
      (prototype as any)[name] = function(this: IDBObjectStore | IDBIndex, ...args: any[]) {
        const store = "objectStore" in this ? `${this.objectStore.name}:${this.name}` : this.name;
        const key = `${store}.${name}`, current = profile, started = performance.now();
        current.requests[key] = (current.requests[key] ?? 0) + 1;
        const request: IDBRequest = original.apply(this, args);
        let first = true;
        request.addEventListener("success", () => {
          if (first) { first = false; current.requestMs[key] = (current.requestMs[key] ?? 0) + performance.now() - started; }
          if (["get", "getAll", "openCursor"].includes(name)) {
            const result = request.result;
            const count = name === "getAll" ? result.length : result ? 1 : 0;
            current.rowsRead[key] = (current.rowsRead[key] ?? 0) + count;
          }
        });
        return request;
      };
    }
  }
}
export function installWorkerProfile() {
  installProfile();
  const starts = new Map<string, number>();
  const post = globalThis.postMessage.bind(globalThis);
  globalThis.postMessage = ((data: any, ...args: any[]) => {
    const start = starts.get(data?.id);
    if (start !== undefined && ("result" in data || "error" in data)) {
      profile.hostAdmissionMs.push(performance.now() - start); starts.delete(data.id);
    }
    return (post as any)(data, ...args);
  }) as typeof globalThis.postMessage;
  globalThis.addEventListener("message", ({ data }) => {
    if (data.benchProfile) {
      if (data.benchProfile === "reset") resetProfile();
      post({ benchProfile: data.benchProfile, value: readProfile() }); return;
    }
    const calls = data.admissions ?? (data.method === "reducer" ? [data] : []);
    if (calls.length) { profile.rpcPackets++; profile.rpcCalls += calls.length; }
    for (const call of calls) starts.set(call.id, performance.now());
  });
}
