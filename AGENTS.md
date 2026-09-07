# AGENTS.md

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
