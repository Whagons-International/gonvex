# Staging SDK and runtime release

Prepare the coordinated prerelease on the feature branch before its staging PR:

```sh
pnpm release:staging --prepare --version 0.5.2-staging.30
```

Commit the eight manifests, regenerated artifacts and `releases/vVERSION.md` in
that PR. Run the CI commands locally, then merge through the normal staging
lane. Once CI passes on the staging merge commit, check out that exact SHA in a
clean release worktree, install frozen dependencies, build `pnpm build:packages`
and run `pnpm verify:package-tarballs`. Arch3 builds use build-headroom and the
machine's compile wrapper.

```sh
pnpm release:staging --publish --version 0.5.2-staging.30 --sha FULL_STAGING_SHA --dry-run
pnpm release:staging --publish --version 0.5.2-staging.30 --sha FULL_STAGING_SHA
```

The publisher requires all versions committed and successful CI on that exact
origin/staging SHA. It packs every package before publishing, rejects a newer
staging version or an existing version with different integrity, and can resume
a partial publication with identical tarballs. It only updates npm's staging
tag, pushes the release tag and creates a GitHub prerelease. It never commits or
pushes a branch. Existing npm and GitHub credentials are required.

Coolify's staging auto-deployment ships the same runtime commit. Verify its
`/healthz` version and module-host readiness before running read-only application
checks. Production promotion remains a separate authorized workflow.
