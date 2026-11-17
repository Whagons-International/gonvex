// Empty bootstrap catalog. K19 replaces this alias with a generated release module.
import type { ModuleExports } from "../host/src/index";
export interface BakedRelease {
  project: string;
  hash: string;
  generation: number;
  manifest: Record<string, unknown>;
  exports: ModuleExports;
}
export const releases: Record<string, BakedRelease> = {};
export const version = "bootstrap";
