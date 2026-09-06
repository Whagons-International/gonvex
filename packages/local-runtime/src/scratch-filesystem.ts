import { OpfsAhpFS } from "@electric-sql/pglite/opfs-ahp";

const rootName = "gonvex-reducer-scratch-v1";
const lockName = (name: string) => `${rootName}/${name}`;

/** Each worker owns an empty execution directory. The replica and outbox never live here. */
export async function createScratchFilesystem(): Promise<OpfsAhpFS | undefined> {
  if (typeof navigator === "undefined" || !navigator.storage?.getDirectory || !navigator.locks) return undefined;
  let release: (() => void) | undefined;
  try {
    const root = await (await navigator.storage.getDirectory()).getDirectoryHandle(rootName, { create: true });
    // Terminating a worker releases its Web Lock, including crashes and tab closes.
    // Never remove a directory still owned by another tab's worker.
    let removed = 0;
    for await (const name of (root as FileSystemDirectoryHandle & { keys(): AsyncIterableIterator<string> }).keys()) {
      if (!/^[0-9a-f-]{36}$/.test(name)) continue;
      await navigator.locks.request(lockName(name), { ifAvailable: true }, async lock => {
        if (lock) { await root.removeEntry(name, { recursive: true }); removed++; }
      });
      if (removed >= 8) break;
    }
    const name = crypto.randomUUID();
    const held = new Promise<void>(resolve => { release = resolve; });
    await new Promise<void>((resolve, reject) => {
      void navigator.locks.request(lockName(name), async () => { resolve(); await held; }).catch(reject);
    });
    return new OpfsAhpFS(`${rootName}/${name}`, { initialPoolSize: 1000, maintainedPoolSize: 100 });
  } catch {
    release?.();
    // Private browsing and older browsers may not support sync file handles.
    return undefined;
  }
}
