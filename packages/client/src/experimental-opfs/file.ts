/** Direct synchronous OPFS I/O. The SDK's dedicated owner worker holds the
 * exclusive access handle; this module never opens a handle on the UI thread. */
export interface ReplicaFile {
  size(): number;
  read(at: number, bytes: Uint8Array): void;
  write(at: number, bytes: Uint8Array): void;
  truncate(bytes: number): void;
  flush(): void;
  close(): void;
}
export type SyncHandle = {
  getSize(): number;
  read(buffer: Uint8Array, options: { at: number }): number;
  write(buffer: Uint8Array, options: { at: number }): number;
  truncate(size: number): void;
  flush(): void;
  close(): void;
};
export function opfsFile(handle: SyncHandle): ReplicaFile {
  const transfer = (write: boolean, at: number, bytes: Uint8Array) => {
    for (let done = 0; done < bytes.length; ) {
      const n = write
        ? handle.write(bytes.subarray(done), { at: at + done })
        : handle.read(bytes.subarray(done), { at: at + done });
      if (!Number.isSafeInteger(n) || n <= 0 || n > bytes.length - done)
        throw new Error("Incomplete OPFS I/O");
      done += n;
    }
  };
  return {
    size: () => handle.getSize(),
    read: (at, bytes) => transfer(false, at, bytes),
    write: (at, bytes) => transfer(true, at, bytes),
    truncate: (n) => handle.truncate(n),
    flush: () => handle.flush(),
    close: () => handle.close(),
  };
}

/** Coalesce sequential frame headers/bodies without weakening flush ordering.
 * Reads drain to the underlying file, not to stable storage; flush is the only
 * durability barrier. One fixed 64 KiB buffer, independent of database size. */
export function bufferedFile(file: ReplicaFile): ReplicaFile {
  const buffer = new Uint8Array(65536);
  let at = 0,
    used = 0;
  const drain = () => {
    if (used) {
      file.write(at, buffer.subarray(0, used));
      used = 0;
    }
  };
  return {
    size: () => Math.max(file.size(), at + used),
    read: (offset, bytes) => {
      if (offset >= at && offset + bytes.length <= at + used)
        bytes.set(buffer.subarray(offset - at, offset - at + bytes.length));
      else {
        drain();
        file.read(offset, bytes);
      }
    },
    write: (offset, bytes) => {
      if (used && offset !== at + used) drain();
      let done = 0;
      while (done < bytes.length) {
        if (!used) at = offset + done;
        const n = Math.min(buffer.length - used, bytes.length - done);
        buffer.set(bytes.subarray(done, done + n), used);
        used += n;
        done += n;
        if (used === buffer.length) drain();
      }
    },
    truncate: (size) => {
      used = 0;
      at = 0;
      file.truncate(size);
    },
    flush: () => {
      drain();
      file.flush();
    },
    close: () => {
      used = 0;
      file.close();
    },
  };
}
