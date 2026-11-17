# AGENTS.md

## Before you open a PR

- Before opening or pushing, run every generator checked by CI and commit its output: Gonvex codegen, `agent-api.ndjson`, i18n and other artifacts where present. On this branch, `pnpm --filter @gonvex/docs typecheck` regenerates docs metadata; verify other commands against the relevant `package.json` and CI.
- Resolve generated-file conflicts by rerunning generators on the merged source; never hand-merge generated output.
- Never add a runtime import cycle. Keep imports one-way when splitting files; compare runtime import cycles with the PR base using the repo's cycle tool (e.g. madge/dpdm) before opening or pushing.

Source: 2026-10-18 code-quality comparison F8 (client stale generators: 35/51 first-CI failures, 69%); F13 (client #1398 split: runtime cycles 6 → 64).

## Pull requests and deployment

- Target the active development branch requested for the task; never target `production`.
- Keep `production` a fast-forward of the approved `main` commit. Do not create a production merge commit; both branches must identify the same release SHA after promotion.
- Include clearly labeled before and after screenshots in the PR description for UI changes.
- Open ready-for-review PRs. Under the standing PR-speed authorization, merge non-UI PRs when required CI is green. UI scope comes only from the complete changed-file diff through `.github/scripts/ui-diff-classifier.cjs`, including rename originals. Tests, docs, workflows, server, bridge, backend and Rust skip UI labels, browser/media/AI visual requirements and the veto window. UI PRs require current-head browser evidence, a passing AI visual check and a 24-hour veto window; Gabriel’s genuine approval ends the wait early. There is no reviewer-approval or owner-review gate. Agents never add or remove UI labels by hand; trusted automation reconciles them. Preserve explicit holds, product-fit flags and drafts; never bypass required protection. Deployment still requires authorization.
- Verify CI and the exact healthy dev deployment before promoting production. Follow the release checks in `scripts/release-gates.mjs` and the ordered rollout in `scripts/deploy-coolify.mjs`.

## Error triage

- Resolve confirmed, deployed fixes and preserve their event history. Do not delete groups to clear the inbox.
- New accepted occurrences reopen resolved groups as regressions. Ignore only intentional noise.
- Whagons application functions belong in the client repository's `gonvex/` directory, not this runtime framework.

## Schema and relationships

Read `apps/docs/content/docs/schema.mdx` and `reference-integrity.mdx` beside it before changing schema behavior.
Keep one authoritative fact; derived copies need a concrete access path, maintenance owner and rebuild path.
Gonvex owns generic discovery, enforcement and diagnostics. Application names, domain semantics and declarations stay in the application.
Prefer simple native SQL constraints; shared transactional runtime validation is also valid. Cover target changes/deletes, concurrency, atomicity and JSON/polymorphic references; metadata or an offline audit is not enforcement. Avoid scattered Reducer checks and expensive generated SQL.
Discover from actual FKs and explicit metadata. ID-like names produce warnings, never invented targets or policies. Design/cost findings are advisory deltas with locations and actionable explanations; genuine integrity failures remain errors. Do not use arbitrary object limits or one complexity score.
Versioned migrations are the current authoring source. A declarative schema compiler generating storage/migrations/types is the target, not an existing capability. Keep bounded application repairs separate from framework redesign.
