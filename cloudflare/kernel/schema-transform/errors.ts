import type { LimitName } from "./limits.ts";

/** Why the transform refused: a limit its output would still break, or a construct it cannot handle. */
export type TransformFailure = LimitName | "unhandled-construct" | "catalog" | "syntax";

/**
 * Raised for anything the transform cannot make fit (0014, rule 2). It names
 * the migration, the object and the limit; provisioning and migration stop.
 */
export class TransformError extends Error {
  override readonly name = "TransformError";
  readonly migration: string;
  readonly object: string;
  readonly limit: TransformFailure;
  readonly detail: string;

  constructor(migration: string, object: string, limit: TransformFailure, detail: string) {
    super(`schema transform: migration ${migration}: ${object}: ${limit}: ${detail}`);
    this.migration = migration;
    this.object = object;
    this.limit = limit;
    this.detail = detail;
  }
}
