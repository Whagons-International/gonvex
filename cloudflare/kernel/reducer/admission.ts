import { errors, type ReducerFailure } from "./errors";

/** All application write entries share the gate across migration chunks. */
export function writeAdmissionError(gateClosed?: () => boolean): ReducerFailure | undefined {
  return gateClosed?.() ? errors.migrationInProgress() : undefined;
}
