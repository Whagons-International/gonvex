# 0003: P01 scaffold decisions

Date: 2026-11-10. Status: decided (lead). Affects: P01 and every `cloudflare/` package.

1. **`@cloudflare/vitest-plugin`, not `@cloudflare/vitest-pool-workers`.** The pool-workers package is
   deprecated in favor of the plugin. The plugin pins one wrangler version exactly, so tests and deploys run
   the same workerd. Accepted. Upgrade both together.
2. **Shared files touched by P01.** `pnpm-workspace.yaml` gains `cloudflare` and `workerd: true` under
   `allowBuilds` (pnpm 11 fails install without it). Accepted.
3. **Lockfile churn in `apps/docs`.** Any lockfile write re-resolves the `latest` specifiers in `apps/docs`
   (fumadocs 16.17.5 → 16.18.4, next 16.4.2 → 16.4.6). The docs build and typecheck pass. Accepted for the
   integration branch; the lead calls this out when `cloudflare/integration` is merged into `staging`, and
   pinning `apps/docs` is a separate non-Cloudflare change.
4. **`done_when` and CI secrets.** "Deploys from CI" needs the repository secrets
   `CLOUDFLARE_DEV_ACCOUNT_ID` and `CLOUDFLARE_DEV_API_TOKEN`, ideally the scoped token the brief promises.
   Only Gabriel can add them. Until then the preview and cleanup jobs skip with a notice. The same deploy
   script was proven by hand from arch3 (deploy, smoke test, delete). P01 is merged with this one item open;
   it is not a stage-0 blocker because no package depends on CI previews before stage 1.
