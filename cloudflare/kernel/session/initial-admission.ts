// 0065 provisional admission envelope for UNSETTLED initial operations only.
// Installed subscriptions and legal argument/frame limits are unchanged.
export const MAX_PENDING_INITIALS = 4096;
const ISOLATE_COUNT = 16_384;
const ISOLATE_BYTES = 32 * 1024 * 1024;
const owners = new Set<WeakRef<InitialAdmission>>();
let count = 0, bytes = 0;
export const initialPressure = () => ({ count, bytes });

export class InitialAdmission {
  private readonly entries = new Map<number, number>();
  private readonly owner = new WeakRef(this);
  private retained = 0;
  private sweep(): void {
    // Dead holders have no outstanding server enqueue authority here. Private
    // durable owner metadata has its separate unresolved lifecycle gate.
    for (const ref of owners) {
      if (!ref.deref()) owners.delete(ref);
    }
    count = 0; bytes = 0;
    for (const ref of owners) { const live = ref.deref(); if (live) { count += live.entries.size; bytes += live.retained; } }
  }
  reserve(generation: number, retained: number): boolean {
    if (this.entries.has(generation)) throw new Error("initial generation reused");
    if (!Number.isSafeInteger(retained) || retained < 0 || this.entries.size >= MAX_PENDING_INITIALS) return false;
    if (count >= ISOLATE_COUNT || bytes + retained > ISOLATE_BYTES) this.sweep();
    const singleton = count === 0 && !this.entries.size;
    if (count >= ISOLATE_COUNT || (!singleton && bytes + retained > ISOLATE_BYTES)) return false;
    this.entries.set(generation, retained); this.retained += retained;
    owners.add(this.owner); count++; bytes += retained;
    return true;
  }
  release(generation: number): void {
    const retained = this.entries.get(generation);
    if (retained === undefined) return;
    this.entries.delete(generation); this.retained -= retained; count--; bytes -= retained;
    if (!this.entries.size) owners.delete(this.owner);
  }
  clear(): void { for (const generation of this.entries.keys()) this.release(generation); }
}
