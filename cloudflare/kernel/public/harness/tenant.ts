/// <reference path="../../host/src/shims.d.ts" />
/**
 * Test-only harness (never deployed): K07's tenant object with tenant-scope
 * `query.call` and `action.call` answered, so K16 can show anonymous calls
 * that pass, not only refusals (0045). The tenant-object composition is the
 * assembly package's (0045 "Assembly"); this harness composes only what the
 * public calls need, from the merged packages, unchanged:
 *
 *  - `query.call`: K08's `callQuery` (Rust's checks, texts and frame), with
 *    the public-handler branch run by K04's module host;
 *  - `action.call`: K12's `ActionRuntime.execute` inline, its tool and
 *    `ctx.functions` calls run by the same host, with K16's
 *    `GONVEX_PUBLIC_ACTION_TIMEOUT_SECONDS` as `publicActionTimeoutMs`.
 *    Agent-profile Actions run inline here too (no Workflow): the deadline
 *    rule is K12's either way.
 *
 * Every other frame goes to K07's dispatch. Queries and Actions take the
 * Reducer path's serial queue, as K07's own calls do.
 */
import { ModuleRegistry, TenantModuleHost, validateValue, type ModuleExports } from "../../host/src/index.js";
import { callQuery, moduleFromManifest, snapshotRunner, type CallPorts, type Manifest, type QueryModule, type QueryVisibility } from "../../query/index";
import type { ClientFrame } from "../../session/decode";
import type { SocketView, TenantSession } from "../../session/ports";
import { frames } from "../../session/protocol";
import { HarnessTenant, type HarnessEnv } from "../../reducer/harness/index.ts";
import { moduleArtifact } from "../../reducer/module.ts";
import { identityOf as memberIdentity, invocationInfoOf } from "../../reducer/path.ts";
import { MODULE_KEY, type StoredModule } from "../../reducer/tenant.ts";
import { ActionRuntime } from "../../../actions/src/runtime.js";
import type { TenantActionPort, TenantCallRequest } from "../../../actions/src/ports.js";
import { identityOf as actionIdentity, isAnonymous, type ActionSession } from "../../../actions/src/session.js";
import { publicInfo } from "../../../actions/src/provenance.js";
import { publicLimits, type PublicLimitsEnv } from "../config";

export type PublicHarnessEnv = HarnessEnv & PublicLimitsEnv;

const q = JSON.stringify;

/** The session an Action runs as (K12's `ActionSession`), from K05's tenant session. */
export function actionSession(session: TenantSession): ActionSession {
  const a = session.identity.account, m = session.member;
  return {
    projectId: session.route.projectId, tenantId: session.route.tenantId,
    account: { id: a.id, email: a.email, name: a.name, avatarUrl: a.avatarUrl },
    member: { id: m.id, accountId: m.accountId, status: m.status, role: m.role, displayName: m.displayName, permissions: m.permissions },
    delegation: session.delegation, developerSession: (session.developerSession ?? null) as Record<string, unknown> | null,
  };
}

/** Only members' plan Queries use visibility (K09); the public calls this harness serves never reach it. */
const NO_VISIBILITY = new Proxy({}, { get: () => { throw new Error("visibility is not composed in the K16 harness"); } }) as QueryVisibility;

const unsupported = (what: string) => async (): Promise<never> => { throw new Error(`${what} is not composed in the K16 harness`); };

export abstract class PublicCallsTenant extends HarnessTenant {
  private mirror: { hash: string; registry: ModuleRegistry; host: TenantModuleHost; queries: QueryModule } | undefined;
  private generations = 0;

  protected abstract override bundle(artifactHash: string): ModuleExports | undefined;

  override async dispatch(socket: SocketView, frame: ClientFrame): Promise<string[]> {
    if (frame.type === "query.call" && frame.scope !== "control") {
      return [await callQuery(this.queryPorts(), socket.tenant, frame.id, frame.path, frame.args)];
    }
    if (frame.type === "action.call" && frame.scope !== "control" && socket.tenant) return [await this.action(socket.tenant, frame)];
    return super.dispatch(socket, frame);
  }

  /** The active generation, loaded into this harness's own registry and host (K04), rebuilt when it changes. */
  private active() {
    const module = this.kernel.module;
    if (!module) return undefined;
    if (this.mirror?.hash !== module.artifactHash) {
      const exports = this.bundle(module.artifactHash);
      const stored = this.ctx.storage.kv.get<StoredModule>(MODULE_KEY);
      if (!exports || !stored) return undefined;
      const registry = new ModuleRegistry(stored.projectId);
      registry.install(moduleArtifact(module, ++this.generations, exports));
      const host = new TenantModuleHost({ storage: this.kernel.storage, registry, permissions: this.kernel.permissions });
      this.mirror = { hash: module.artifactHash, registry, host, queries: moduleFromManifest(stored.projectId, stored.manifest as Manifest, module.artifactHash) };
    }
    return this.mirror;
  }

  private queryPorts(): CallPorts {
    const active = this.active();
    return {
      visibility: NO_VISIBILITY,
      module: () => active?.queries ?? null,
      run: snapshotRunner(this.kernel.storage),
      validateArgs: (schema, args) => { try { validateValue(schema, args); return null; } catch (error) { return error instanceof Error ? error.message : String(error); } },
      handler: async (session, path, args) => {
        if (!active) return { ok: false, error: `project ${q(session.identity.projectId)} has no active TypeScript module` };
        const report = await this.kernel.reducers.serial(() => active.host.run({
          function: path, kind: "query", args, identity: memberIdentity(session),
          invocation: invocationInfoOf(session, crypto.randomUUID(), active.hash), anonymous: session.anonymous,
        }));
        return report.ok ? { ok: true, text: report.value ?? "null" } : { ok: false, error: report.error?.message ?? "query failed" };
      },
    };
  }

  private async action(session: TenantSession, frame: Extract<ClientFrame, { type: "action.call" }>): Promise<string> {
    const active = this.active();
    if (!active) return frames.actionError(frame.id, frame.path, `project ${q(session.identity.projectId)} has no active TypeScript module`);
    const host = active.host;
    const tenant: TenantActionPort = {
      call: async (request: TenantCallRequest) => {
        const report = await this.kernel.reducers.serial(() => host.run({
          function: request.function, kind: request.kind, args: request.args, identity: actionIdentity(request.session),
          invocation: publicInfo(request.provenance), anonymous: isAnonymous(request.session),
          ...(request.provenance.deadlineUnixMs === null ? {} : { deadlineUnixMs: request.provenance.deadlineUnixMs }),
        }));
        return report.ok ? { ok: true, value: report.value ?? "null" } : { ok: false, error: report.error?.message ?? "call failed", transient: report.error?.class === "transient" };
      },
      claimCall: unsupported("claimCall"), renewCall: unsupported("renewCall"), completeCall: unsupported("completeCall"),
    };
    const runtime = new ActionRuntime({ registry: active.registry, tenant, publicActionTimeoutMs: publicLimits(this.env as PublicHarnessEnv).actionTimeoutMs });
    const report = await runtime.execute({ session: actionSession(session), path: frame.path, args: frame.args });
    if (!report.ok) return frames.actionError(frame.id, frame.path, report.error);
    return `{"type":"action.result","id":${q(frame.id)},"path":${q(frame.path)},"result":${report.value}`
      + (report.committedRevision === undefined ? "" : `,"committedRevision":${report.committedRevision}`) + "}";
  }
}
