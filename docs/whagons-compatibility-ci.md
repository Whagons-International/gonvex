# Whagons release compatibility gate

`.github/workflows/whagons-compatibility.yml` invokes the Whagons-owned reusable workflow for every PR to main/staging and every push to those branches. It passes the candidate Gonvex commit and the pinned `WHAGONS_COMPAT_REF` client commit. Tests run on isolated GitHub runners, not the Coolify staging database.

The canonical setup and coverage guide is in the client repository at `apps/docs/content/docs/internal/release-compatibility-ci.md`. The client owns `compatibility/app-support-policy.json`, mobile inventories, bridge adapters, and live scenarios.

Before activation:

- Land the client reusable workflow on staging. Pin the caller's `uses` reference to that reviewed commit after landing.
- Set `WHAGONS_COMPAT_REF` to the full client commit SHA.
- Share the documented test/checkout secrets with this repository and permit private reusable-workflow access.
- Require the `Whagons compatibility` check in branch protection.

Production promotion now requires this check on the exact candidate commit. Missing configuration, a skipped run, or a failing app version blocks promotion. CI configuration alone does not prove that the current bridge supports the candidate runtime.

The suite tests released SDKs through the bridge. Full mobile UI testing and Coolify deployment smoke/rollback testing remain separate.
