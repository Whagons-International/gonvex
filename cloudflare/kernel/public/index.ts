/**
 * Public functions (package K16): anonymous sockets' limits and admission.
 * See README.md. The session layer (K05) calls in through `PublicGuard`;
 * the Worker entry exports `PublicLimiterObject`, bound as `PUBLIC_LIMITER`.
 */
export { addressBucket, clientAddress, UNKNOWN_ADDRESS } from "./address";
export {
  ADMISSION_MAX_AGE_MS, CALL_WINDOW_MS, DEFAULT_PUBLIC_LIMITS, PublicConfigError, type PublicLimits, type PublicLimitsEnv, publicLimits,
} from "./config";
export { ADMISSION_ERROR, frameCost, publicAdmissionErrors, publicLimitErrors, RATE_LIMIT_ERROR, RATE_LIMIT_ID, runsTenantCode } from "./frames";
export { admitUpgrade, LIMITER_UNAVAILABLE, type PublicGuard, type PublicGuardEnv, publicGuard, validatePublicScope } from "./guard";
export {
  AddressBudget, kvSlotStore, limiterFor, limiterName, MemorySlotStore, PENDING_LIMIT_ERROR, type SlotStore, type StoredSlot, PUBLIC_LIMIT_ERROR, PublicLimiterObject, type PublicLimiterRpc, type SlotKind,
} from "./limiter";
export { allowCalls, type CallWindow, newWindow } from "./window";
