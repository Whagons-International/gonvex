import type { CapacityPage, CapacitySlot } from './fixtures/k22-contract.js';
/** Compile-time correspondence to the immutable951 native wire, not a second serving contract. */
export function nativeFacts(page: CapacityPage) {
  return {
    rateUnits: page.slots.map((slot: CapacitySlot) => ({ sourceAt: slot.cached.sample?.at,
      framesPerSecond: slot.cached.sample?.framesPerSecond, bytesPerSecond: slot.cached.sample?.bytesPerSecond,
      windowInstance: slot.cached.sample?.windowInstance, windowSequence: slot.cached.sample?.windowSequence })),
    eventLoss: page.coverage?.events.loss,
    attempts: page.provenance.catalogIdentityCalls + page.provenance.controllerStatusCalls,
    codeVersion: page.identity.objectCodeVersions,
  };
}
