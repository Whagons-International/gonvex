// Release plans: the transformed, chunked DDL one release applies to a tenant
// object (0016 G0-10), and where objects read them from. Plans are built once
// per release, outside the tenant object (build.ts): the A01 transform over the
// whole Whagons history takes about 8 s of CPU and more than 128 MB of heap,
// which no tenant object can spend. Each object then fetches and applies only
// the chunks it still needs (migrator.ts). This module has no transform import,
// so the tenant object's bundle does not carry the transform.

import type { AddedObject } from "../schema-transform/index.ts";

export interface PlannedPrelude {
  kind: "prelude";
  name: string;
  chunks: number;
}

export interface PlannedMigration {
  kind: "migration";
  name: string;
  checksum: string;
  noTransaction: boolean;
  foreignKeyRebuild: boolean;
  /** Set when Rust would refuse this migration when it reaches it; the object fails there with this text. */
  refusal: string | null;
  statements: number;
  chunks: number;
}

export type PlannedUnit = PlannedPrelude | PlannedMigration;

/** A plan without its SQL: what a tenant object reads first. */
export interface PlanManifest {
  id: string;
  format: string;
  transformVersion: string;
  prelude: PlannedPrelude[];
  migrations: PlannedMigration[];
  /** Objects the transform added (M02, P03 and checksum tools exclude exactly these). */
  addedObjects: AddedObject[];
}

export interface ReleasePlan extends PlanManifest {
  /** `prelude/<name>` or `migration/<name>` -> statement chunks, one storage transaction each. */
  chunks: Record<string, string[][]>;
}

export const unitKey = (unit: PlannedUnit) => `${unit.kind}/${unit.name}`;

export function manifestOf(plan: ReleasePlan): PlanManifest {
  const { chunks: _chunks, ...manifest } = plan;
  return manifest;
}

/** Where a tenant object reads a plan from. K19 stores plans next to the release bundle. */
export interface PlanStore {
  manifest(id: string): Promise<PlanManifest | null>;
  chunk(id: string, unit: string, index: number): Promise<string[]>;
}

export class MemoryPlanStore implements PlanStore {
  private readonly plans = new Map<string, ReleasePlan>();
  constructor(...plans: ReleasePlan[]) {
    for (const p of plans) this.add(p);
  }
  add(plan: ReleasePlan): void {
    this.plans.set(plan.id, plan);
  }
  async manifest(id: string): Promise<PlanManifest | null> {
    const plan = this.plans.get(id);
    return plan ? manifestOf(plan) : null;
  }
  async chunk(id: string, unit: string, index: number): Promise<string[]> {
    const chunk = this.plans.get(id)?.chunks[unit]?.[index];
    if (!chunk) throw new Error(`plan ${id}: no chunk ${index} of ${unit}`);
    return chunk;
  }
}

/** Minimal R2 surface, so the store also works with a fake in tests. */
export interface R2Like {
  get(key: string): Promise<{ text(): Promise<string> } | null>;
  put(key: string, value: string): Promise<unknown>;
}

/** Plans in R2: `<prefix><id>/manifest.json` and `<prefix><id>/<unit>/<index>.json`. One read per chunk applied. */
export class R2PlanStore implements PlanStore {
  private readonly bucket: R2Like;
  private readonly prefix: string;
  constructor(bucket: R2Like, prefix = "plans/") {
    this.bucket = bucket;
    this.prefix = prefix;
  }

  async write(plan: ReleasePlan): Promise<void> {
    for (const [unit, list] of Object.entries(plan.chunks)) {
      for (let i = 0; i < list.length; i += 1) await this.bucket.put(`${this.prefix}${plan.id}/${unit}/${i}.json`, JSON.stringify(list[i]));
    }
    // The manifest last: a plan is readable only once every chunk is.
    await this.bucket.put(`${this.prefix}${plan.id}/manifest.json`, JSON.stringify(manifestOf(plan)));
  }

  async manifest(id: string): Promise<PlanManifest | null> {
    const object = await this.bucket.get(`${this.prefix}${id}/manifest.json`);
    return object ? (JSON.parse(await object.text()) as PlanManifest) : null;
  }

  async chunk(id: string, unit: string, index: number): Promise<string[]> {
    const object = await this.bucket.get(`${this.prefix}${id}/${unit}/${index}.json`);
    if (!object) throw new Error(`plan ${id}: no chunk ${index} of ${unit}`);
    return JSON.parse(await object.text()) as string[];
  }
}
