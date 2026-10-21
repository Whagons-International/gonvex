import fs from "node:fs";
export function nativeFile(path) {
  const fd = fs.openSync(path, "a+"); // Positional writes cannot use O_APPEND.
  fs.closeSync(fd);
  const file = fs.openSync(path, "r+");
  let closed = false;
  return {
    size: () => fs.fstatSync(file).size,
    read(at, bytes) {
      let done = 0;
      while (done < bytes.length) {
        const n = fs.readSync(
          file,
          bytes,
          done,
          bytes.length - done,
          at + done,
        );
        if (!n) throw Error("Short read");
        done += n;
      }
    },
    write(at, bytes) {
      let done = 0;
      while (done < bytes.length) {
        const n = fs.writeSync(
          file,
          bytes,
          done,
          bytes.length - done,
          at + done,
        );
        if (!n) throw Error("Short write");
        done += n;
      }
    },
    truncate: (n) => fs.ftruncateSync(file, n),
    flush: () => fs.fsyncSync(file),
    close() {
      if (!closed) {
        closed = true;
        fs.closeSync(file);
      }
    },
  };
}
export class MemoryFile {
  data;
  buffer;
  durable;
  operations = 0;
  failAt = Infinity;
  failBytes = 0;
  closed = false;
  constructor(bytes = new Uint8Array()) {
    this.buffer = bytes.slice();
    this.data = this.buffer;
    this.durable = bytes.slice();
  }
  resize(n) {
    const prior = this.data.length;
    if (n > this.buffer.length) {
      const next = new Uint8Array(Math.max(n, this.buffer.length * 2, 8192));
      next.set(this.data);
      this.buffer = next;
    }
    if (n > prior) this.buffer.fill(0, prior, n);
    this.data = this.buffer.subarray(0, n);
  }
  step() {
    if (++this.operations === this.failAt)
      throw Error("Injected storage failure");
  }
  size() {
    this.step();
    return this.data.length;
  }
  read(at, bytes) {
    this.step();
    if (at + bytes.length > this.data.length) throw Error("Short read");
    bytes.set(this.data.subarray(at, at + bytes.length));
  }
  write(at, bytes) {
    this.operations++;
    const n =
      this.operations === this.failAt
        ? Math.min(bytes.length, this.failBytes)
        : bytes.length;
    if (at + n > this.data.length) this.resize(at + n);
    this.data.set(bytes.subarray(0, n), at);
    if (this.operations === this.failAt) throw Error("Injected torn write");
  }
  truncate(n) {
    this.step();
    this.resize(n);
  }
  flush() {
    this.operations++;
    if (this.operations === this.failAt) {
      // Some/all prior writes may reach disk even when flush reports failure.
      const next = new Uint8Array(
        Math.max(
          this.durable.length,
          Math.min(this.data.length, this.failBytes),
        ),
      );
      next.set(this.durable);
      next.set(this.data.subarray(0, this.failBytes));
      this.durable = next;
      throw Error("Injected unknown flush");
    }
    this.durable = this.data.slice();
  }
  crash() {
    return new MemoryFile(this.durable);
  }
  close() {
    this.closed = true;
  }
}
