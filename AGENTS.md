# AGENTS.md

## Before you open a PR

- Before opening or pushing, run every generator checked by CI and commit its output: Gonvex codegen, `agent-api.ndjson`, i18n and other artifacts where present. On this branch, `pnpm --filter @gonvex/docs typecheck` regenerates docs metadata; verify other commands against the relevant `package.json` and CI.
- Resolve generated-file conflicts by rerunning generators on the merged source; never hand-merge generated output.
- Never add a runtime import cycle. Keep imports one-way when splitting files; compare runtime import cycles with the PR base using the repo's cycle tool (e.g. madge/dpdm) before opening or pushing.

Source: 2026-10-18 code-quality comparison F8 (client stale generators: 35/51 first-CI failures, 69%); F13 (client #1398 split: runtime cycles 6 → 64).

## Pull requests and deployment

- Target all pull requests at `main`, never `production`.
- Keep `production` a fast-forward of the approved `main` commit. Do not create a production merge commit; both branches must identify the same release SHA after promotion.
- Include clearly labeled before and after screenshots in the PR description for UI changes.
- Open ready-for-review PRs. Merge or deploy only when the user authorizes it.
- Verify CI and the exact healthy dev deployment before promoting production. Follow the release checks in `scripts/release-gates.mjs` and the ordered rollout in `scripts/deploy-coolify.mjs`.

## Error triage

- Resolve confirmed, deployed fixes and preserve their event history. Do not delete groups to clear the inbox.
- New accepted occurrences reopen resolved groups as regressions. Ignore only intentional noise.
- Whagons application functions belong in the client repository's `gonvex/` directory, not this runtime framework.
