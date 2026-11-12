# 0066: Credential-free hosted image builds, Vault-only publishing

Date: 2026-11-12. Status: decided (lead); actual hosted artifacts/publishing/deployment remain unproved.

## Observed blocker

PR400's Cloudflare run43212000513 at a957d17b concluded successfully, but checkout, dependency install,
image build/deploy and identity upload in the preview job were all **skipped**. The required Cloudflare dev
account/token secrets were unavailable. A green job conclusion is not build or deployment evidence.
No Vault token was retrieved/copied to repository secrets to work around this, and no broader CI credential
exposure is authorized by the missing-secret condition.

## Approved lane

Separate the same-repository hosted source-image build from credential-gated preview/deletion. The build
requires no Cloudflare/Vault secret. Use the two fixed sandbox/builder recipes and a clean tracked git-archive
context without `.git`; a Docker environment whitelist excludes CI/Vault/Cloudflare credentials. Resolve and
pin base/frontend digests, hash exact inputs and recipes, and retain local image/config identities.

K22's prepared envelope is a 90-minute job, uncompressed Docker-save archives named `sandbox.docker.tar`
and `builder.docker.tar`, artifact `gonvex-k22-container-archives`, seven-day retention, 8 GiB per image and
12 GiB combined limits. Record actual byte/runtime/storage use; this is not measured cost or verified account
artifact headroom. No paid runner, account, subscription or quota increase is implied. Upload failures remain
failures. Record the actual checkout/merge SHA and its relationship to the PR head; never label a synthetic
merge build as a different source SHA.

`images.json`/`inputs.json` identify source, tracked inputs, pinned recipes/bases, image/config digests,
archive hashes/bytes/tags. Local publication verifies artifact provenance, archive hash and image/config
identity before pushing. An archive is not deployment proof, nor is an unused build recipe a build result.

Publish only `gonvex-cf-dev-*` images/resources through the existing approved Vault wrapper. Use a pinned
daemonless OCI client; no fresh arch3 Docker build/cache or daemon/cgroup workaround. Obtain short-lived
registry permissions only inside the wrapper, keep credentials out of arguments/logs/artifacts, isolate any
standard tool auth store and remove it on exit. Do not copy the broad Vault credential into GitHub secrets.
Cloudflare documents temporary registry credentials with expiry and push/pull permissions in its
[registry credential API](https://developers.cloudflare.com/api/resources/containers/subresources/registries/subresources/credentials/methods/generate/).

Lead prepared `crane v0.20.6` under an admitted `/mnt` task directory. A synthetic Docker archive was read,
pushed to an ephemeral local registry, checked for digest/config preservation, and a corrupt archive rejected
without using the Docker daemon or Cloudflare credentials. This proves local transport mechanics only.
Evidence: `/mnt/data/work/gonvex-cloudflare-lead/oci-tools/`; actual registry publishing and deployed images
remain pending real immutable CI artifacts. The original K20 image input-provenance gap is not erased.

The lead-owned standalone implementation is `.github/workflows/cloudflare-images.yml` and
`cloudflare/scripts/export-container-images.mjs`; see `verification/container-image-lane.md`. K22's duplicate
unpublished image job is deferred. Initial integration-source archives are not final K22 runtime images.
