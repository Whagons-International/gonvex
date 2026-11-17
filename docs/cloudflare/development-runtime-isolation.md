# Stable sockets during application publication

Application publication replaces the Runtime and Actions Workers. It never deploys the kernel Worker
that owns Gateway, GatewayController, Control, PublicLimiter and Ingress. Control also owns operator
WebSockets, so moving Control alongside the application would retain the original problem.

| Worker | Ownership | Application deployment |
| --- | --- | --- |
| kernel | Existing Control, Gateway, GatewayController, PublicLimiter, Ingress namespaces | Never |
| runtime | Existing AssemblyTenant SQLite namespace; private RuntimeCatalog service | Yes; immediate DO adoption |
| actions | ActionWorker, ActionWorkflow, private ActionCatalog service | Yes |
| sandbox | Existing sandbox Containers and registry | No |
| builder | Existing builder Container and fenced PublicationExecutor | No |

The kernel remains the public application endpoint. The new runtime's default fetch returns 404,
`workers_dev` and preview URLs are disabled, and RuntimeCatalog is a service binding. Other
companions retain their existing exposure and authentication. The runtime exports no client
socket owner: AssemblyTenant and its Reducer/Replica base classes have no WebSocket accept handlers.
Gateway keeps the native sockets, Session records, ordering, recovery, cancellation and security
timers. Its logical routing identity and X01 controller protocol remain unchanged.

Cloudflare applies a DO code-update strategy to every class exported by a Worker. An external DO
binding uses the owning Worker's strategy. Therefore immediate adoption belongs only on runtime;
the kernel has no immediate policy. See the [code-update API](https://developers.cloudflare.com/durable-objects/deployments/durable-objects-code-updates/).

## Publication and recovery

Control keeps the existing durable whole-deployment fence from preparation through completed
publication. Concurrent projects cannot capture competing whole-catalog snapshots. The Node builder
stages the complete immutable catalog and rechecks its fence in PublicationExecutor before any
deployment. WranglerTarget validates both configurations before writing either Worker, and its
only allowed targets are `${name}-actions` and `${name}-runtime`.

The generated Actions entry exports the same version as Runtime. Stable Control observes
RuntimeCatalog over its private binding; RuntimeCatalog checks both its own version and the
Actions catalog's version. Tenant activation must additionally acknowledge the exact expected
artifact hash and generation. Control then updates its active metadata and awaits the actual
Gateway reload RPC for tenant and control-only sockets before completing the publication.

A durable target-attempt row is written before a deployment grant returns. A lost builder or
partial component deployment retains the publishing fence. Same-ID recovery reconstructs that
same catalog from R2 and retries it; another project cannot pass it. Abort refuses an unfinished
target attempt. A completed target barrier clears the attempt, and ordinary failures known to
precede mutation can terminate normally. No caller timeout or lost process is treated as proof
that a provider write did not happen. A successful publication clears the attempt and owner.
This procedure does not roll back application DDL or rewrite imported tenant data.

The private `/__assembly/version` response retains `worker` for the kernel identity and adds
`runtimeWorker`; `version` and `releases` come from the serving runtime. `ready` includes Actions
readiness. This metadata does not substitute for the tenant activation acknowledgment.

## One-time namespace transition

Existing monolithic deployments must use `migrateExistingAssembly` in
`cloudflare/kernel/assembly/namespace-transfer.mjs`. `deployAssembly` refuses to create a new
runtime namespace over an existing monolith. Namespace transfer is planned framework maintenance;
it does not promise continuity for sockets during the framework transition. Subsequent module
publications do not run this procedure or deploy the socket-owning Worker.

Inputs are the exact approved account/name, existing admin and storage secret references, matching
immutable builder/sandbox images, captured active catalog tuples and source-generated Runtime and
Actions entry files. Generate both entries with the current K19 builder from the retained R2 release
pieces; an older Actions entry without a version label is refused. Include every active project.
Quiesce legacy publishers for this maintenance operation. Never abort a pending publication or
edit its SQL to obtain a freeze; complete it through its ordinary same-ID protocol first.

The helper uses the documented [declarative namespace transfer](https://developers.cloudflare.com/durable-objects/reference/durable-objects-migrations/#transfer-a-durable-object-class-between-workers):

- The source bridge still exports AssemblyTenant and carries the captured complete catalog. Its
  router refuses new preparations while allowing staged same-ID sync recovery through the normal
  hash/key checks. Before any Tenant restore, the bridge requires the retained active hash and
  generation to match its captured release. A stale capture or an uninstalled Tenant refuses
  the wake before module code or metadata can change. A matching bridge restores the retained
  manifest without auto-activation; release plans, changed pins and provisioning wait for maintenance
  to finish. An already-active exact pin can still acknowledge a same-ID publication without mutation.
  These guards also apply to the runtime source during reverse transfer. If a legacy publication
  still needs a different Tenant pin, finish its ordinary same-ID recovery on its compatible serving
  framework before retrying maintenance with a fresh capture; never abort it or rewrite its rows.
  Control's durable freeze must report no deployment owner, and its actual
  active catalog must match the capture. An interrupted preparation may expire normally; a staged
  publication cannot expire. Framework code adoption can take the platform's default interval;
  the helper's separate maintenance retry does not change SDK or conformance publication deadlines.
- The Actions bridge installs ActionCatalog while its Tenant binding still points at the source.
  The target declares `expecting-transfer` with the source name and has no Tenant self-binding.
  The retained storage secret is installed before handoff.
- The source declares `transferred`, retains the class in code, and points its Tenant binding at
  the target. The provider's namespace ID must equal the original ID after commit.
- The target binds the transferred class locally. Actions and Sandbox are repointed; the matching
  builder image and stable kernel are installed. Only then may the source tombstone be removed.
  Thaw requires the actual version in Runtime and Actions, the exact active project/hash set,
  and matching namespace-derived Tenant IDs through Control and Runtime bindings.

All provider writes verify the exact account and the owned Worker allowlist. Private configs and
the resumable local journal use mode 0600. The journal pins the original namespace and captured
inputs, plus all five stable kernel namespace IDs. Each phase checks that stable mapping, including
after the final kernel deployment. Post-commit retry skips the transfer and resumes binding/companions. A changed capture may
be explicitly refreshed only while the actual source remains durably frozen and still owns the
original namespace; a committed handoff cannot be reinterpreted with new inputs. Errors retain
maintenance/freeze. There is no namespace delete, reimport, SQL rewrite or account-wide change.

Rollback uses `reverse:true`, the same public transfer protocol and a new reverse journal. It moves
the original namespace back and repoints companion bindings without deleting data. It deliberately
remains frozen: the isolated builder must not deploy a new Runtime namespace while Tenant ownership
is back on the monolith. Returning to a publishing monolithic framework requires a separately
approved compatible framework/builder release. Neither direction undoes application migrations.
Steady kernel and Runtime configurations explicitly set the transition flag to `0`, so normal
`--keep-vars` deployment cannot accidentally retain a bridge's activation guard.

## Local qualification and limits

`node --test cloudflare/kernel/assembly/local.test.mjs` boots the actual kernel/runtime/Actions/sandbox
composition and Node builder, then publishes three complete catalogs through HTTP. Wrangler's local
multi-config reload can restart the whole local process; this test does not certify connected
continuity during that reload.

The workerd publication test uses distinct stable, runtime and Actions isolates, actual Control,
Tenant, Gateway, Session, SQLite/R2 and native WebSockets. A controlled baked-catalog switch plus
socket-free Tenant reconstruction exercises v1→v2→v3 on the original sockets, awaited reload,
control-only notification, failure/same-ID recovery, stale pins, contract refusal and namespace
validation. The mutation endpoints and injected faults exist only in test Workers. They are not
deployed runtime loaders or an assertion about managed adoption.

Native Miniflare transfer regressions load the actual Tenant and Control classes, switch source code
over retained SQLite, and inject stale hash or generation captures in both directions. They check
failed freeze, real durable freeze, reconstruction, refused activation/provisioning and a frozen
refresh interrupted before receiving. A temporary read-only inspection class checks that refused
wakes have not changed the retained module or synthetic row. Provider namespace responses in these
tests remain controlled inputs; no managed namespace transfer is inferred.
Node config/transfer tests also check target selection, namespace identity, partial failure, resume and
reverse plans. A coordinated dev transfer and the
unchanged original remote connected scenarios remain necessary observations after review/merge.
Full V01, terminal/OWNER acceptance and load remain separate; no latency, cost or load result is
inferred from these local tests. Runtime keeps CPU30000; Actions and Sandbox retain their existing
CPU and role caps. The new Worker adds a separate isolate; runtime heap/cost delta is unmeasured.
