/**
 * Public functions (package K16): anonymous sockets' limits and admission.
 * See README.md. The session layer (K05) calls in through `PublicGuard`;
 * the Worker entry exports `PublicLimiterObject`, bound as `PUBLIC_LIMITER`.
 */
export { addressBucket, clientAddress, UNKNOWN_ADDRESS } from "./address";
export {
  ACTION_TIMEOUT_MS, ADMISSION_MAX_AGE_MS, CALL_WINDOW_MS, DEFAULT_PUBLIC_LIMITS, PublicConfigError, type PublicLimits,
  type PublicLimitsEnv, publicActionDeadline, publicLimits,
} from "./config";
export { ADMISSION_ERROR, frameCost, publicAdmissionErrors, publicLimitErrors, RATE_LIMIT_ERROR, RATE_LIMIT_ID, runsTenantCode } from "./frames";
export { admitUpgrade, type PublicGuard, type PublicGuardEnv, publicGuard, validatePublicScope } from "./guard";
export {
  AddressBudget, limiterFor, limiterName, PENDING_LIMIT_ERROR, PUBLIC_LIMIT_ERROR, PublicLimiterObject, type PublicLimiterRpc, type SlotKind,
} from "./limiter";
export { allowCalls, type CallWindow, newWindow } from "./window";
