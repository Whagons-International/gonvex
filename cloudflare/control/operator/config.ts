/**
 * Operator configuration, read from the Worker's vars and secrets with the
 * reference's names (rust/crates/runtime/src/config.rs).
 */

export interface OperatorEnv {
  GONVEX_ADMIN_KEY?: string;
  GONVEX_DASHBOARD_SESSION_SECRET?: string;
  GONVEX_DEV_SYNC_KEY?: string;
  GONVEX_REQUIRE_AUTH?: string;
  DASHBOARD_AUTH_ACCOUNT?: string;
  DASHBOARD_AUTH_PASSWORD?: string;
  GONVEX_DASHBOARD_AUTH_PROJECT_ID?: string;
  GONVEX_RUNTIME_VERSION?: string;
  GONVEX_AUTH_URL?: string;
  GONVEX_PUBLIC_URL?: string;
  GONVEX_GOOGLE_CLIENT_ID?: string;
  GONVEX_GOOGLE_CLIENT_SECRET?: string;
  /**
   * Kernel only: Rust's "Local" operator (no credential configured, every
   * request an admin) must be impossible in a deployed Worker (CONTRACT D.3).
   * It is available only when this is exactly `"true"`, which only local
   * development configurations set, and `scripts/deploy-preview.mjs`-style
   * deploys must never set it.
   */
  GONVEX_LOCAL_OPERATOR?: string;
  /** Rollout deadline for a held-open `/dev/sync` (0012 R4), default 10 minutes. */
  GONVEX_SYNC_DEADLINE_MS?: string;
  /** P03 dev transfer: the target deployment's name (must start with `gonvex-cf-dev-`). */
  GONVEX_DEV_TRANSFER_TARGET?: string;
  /** P03 dev transfer: comma-separated exact `devcopy-*` tenant object names. */
  GONVEX_DEV_TRANSFER_TENANTS?: string;
  [binding: string]: unknown;
}

export interface OperatorConfig {
  adminKey: string | undefined;
  controlSecret: string | undefined;
  devSyncKey: string | undefined;
  requireAuth: boolean;
  dashboardAccount: string | undefined;
  dashboardPassword: string | undefined;
  dashboardAuthProjectId: string | undefined;
  runtimeVersion: string;
  authPublicUrl: string | undefined;
  googleClient: boolean;
  localAllowed: boolean;
  syncDeadlineMs: number;
  /** P03 dev-transfer gate (see `devTransferAllowed`). */
  devTransferTarget: string;
  devTransferTenants: string;
}

/**
 * The P03 dev-transfer gate, shared by `/__tenant-transfer` and the dev-only
 * operator routes (contract-changes/0091): a `gonvex-cf-dev-*` deployment, a
 * `devcopy-*` tenant name, and that exact name listed in
 * `GONVEX_DEV_TRANSFER_TENANTS`. Staging and production set neither variable.
 */
export function devTransferAllowed(gate: { target?: string | undefined; tenants?: string | undefined }, tenant: string): boolean {
  return !!gate.target?.startsWith("gonvex-cf-dev-") &&
    /^devcopy-[A-Za-z0-9_-]{1,100}$/.test(tenant) &&
    (gate.tenants ?? "").split(",").includes(tenant);
}

const nonEmpty = (value: unknown): string | undefined => (typeof value === "string" && value.trim() !== "" ? value : undefined);

/** config.rs `boolean`: 1/true/yes/on and 0/false/no/off. */
function flag(value: unknown, fallback: boolean): boolean {
  const text = String(value ?? "").trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(text)) return true;
  if (["0", "false", "no", "off"].includes(text)) return false;
  return fallback;
}

export function operatorConfig(env: OperatorEnv): OperatorConfig {
  const deadline = Number(env.GONVEX_SYNC_DEADLINE_MS);
  return {
    adminKey: nonEmpty(env.GONVEX_ADMIN_KEY),
    controlSecret: nonEmpty(env.GONVEX_DASHBOARD_SESSION_SECRET),
    devSyncKey: nonEmpty(env.GONVEX_DEV_SYNC_KEY),
    requireAuth: flag(env.GONVEX_REQUIRE_AUTH, false),
    // config.rs lower-cases the configured account.
    dashboardAccount: nonEmpty(env.DASHBOARD_AUTH_ACCOUNT)?.toLowerCase(),
    dashboardPassword: typeof env.DASHBOARD_AUTH_PASSWORD === "string" ? env.DASHBOARD_AUTH_PASSWORD : undefined,
    dashboardAuthProjectId: nonEmpty(env.GONVEX_DASHBOARD_AUTH_PROJECT_ID),
    runtimeVersion: nonEmpty(env.GONVEX_RUNTIME_VERSION) ?? "development",
    authPublicUrl: nonEmpty(env.GONVEX_AUTH_URL) ?? nonEmpty(env.GONVEX_PUBLIC_URL),
    googleClient: !!nonEmpty(env.GONVEX_GOOGLE_CLIENT_ID) && !!nonEmpty(env.GONVEX_GOOGLE_CLIENT_SECRET),
    localAllowed: env.GONVEX_LOCAL_OPERATOR === "true",
    syncDeadlineMs: Number.isFinite(deadline) && deadline > 0 ? deadline : 10 * 60 * 1000,
    devTransferTarget: typeof env.GONVEX_DEV_TRANSFER_TARGET === "string" ? env.GONVEX_DEV_TRANSFER_TARGET : "",
    devTransferTenants: typeof env.GONVEX_DEV_TRANSFER_TENANTS === "string" ? env.GONVEX_DEV_TRANSFER_TENANTS : "",
  };
}
