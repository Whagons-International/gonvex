# Whagons release compatibility gate

Compatibility runs in the private `Whagons-International/whagons5-client` repository. It checks out an explicit Gonvex commit and runs against isolated runner services. Public Gonvex workflows cannot call private reusable workflows, so there is no cross-repository `uses` call here.

The client workflow runs on client staging pushes and PRs. For a Gonvex-only candidate, dispatch it in the private repository:

```sh
gh workflow run release-compatibility.yml --repo Whagons-International/whagons5-client --ref staging -f gonvex_ref=<full-framework-sha> -f client_ref=<full-client-sha>
```

The private repository variable `GONVEX_COMPAT_REF` pins the framework for ordinary client runs. Test and mobile-checkout credentials remain private. Each run creates and cleans up a unique Firebase test actor. Reports record both source revisions, mobile commits, SDK versions, and live scenario outcomes.

The canonical coverage and setup guide is `apps/docs/content/docs/internal/release-compatibility-ci.md` in the client repository. The client owns the support policy, inventories, bridge, and tests. Full mobile UI and Coolify rollout tests remain separate.

Production promotion remains fail-closed: `scripts/release-gates.mjs` still requires successful `Whagons compatibility` evidence on the exact Gonvex candidate. Private CI execution alone does not publish that public check. A dedicated, narrowly scoped cross-repository result publisher must be configured before that production gate can pass. Do not remove the gate or substitute a workflow dispatch acknowledgment for a passing test result.
