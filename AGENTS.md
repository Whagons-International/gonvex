# AGENTS.md

## Client allocation work

- `gonvex perf allocations <hot-path> --json` reports syntactic allocation hints using the CLI's existing parser. Use `--marked-only` for `@allocation-hot` functions in otherwise ordinary UI code. Pair findings with browser evidence; a copy can be necessary for ownership, and a shared buffer view can retain more memory than a small copy.
- The runtime-regression workflow checks the client hot-path baseline. Review new findings rather than accepting them by regenerating the baseline. Generic bounded buffers and storage/query helpers belong in this SDK, with no application-specific task logic or additional frontend dependency.
- Mark measured allocation-sensitive accessors/scans with `/** @allocation-hot */`. Run `gonvex perf allocations packages/client/src packages/react/src --strict-hot --check`: strict annotations cannot use a baseline. Explain necessary cold/error allocations individually with `allocation-allow hot-allocation: reason`. This checks known syntax, not transitive allocations; keep the browser memory guard as a separate check.

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
