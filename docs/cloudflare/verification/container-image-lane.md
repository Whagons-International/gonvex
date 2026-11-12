# Credential-free Container archive lane

Status: implemented source; actual hosted archives, quota/cost, publication and deployment pending.
Owner: lead. K22 retains serving composition, runtime fixes and the secret-gated preview helper.

`.github/workflows/cloudflare-images.yml` runs only for relevant same-repository PR changes targeting
`cloudflare/integration`. It checks out the exact PR head, runs Node integrity tests, then invokes:

```sh
node cloudflare/scripts/export-container-images.mjs "$IMAGE_NAME" "$RUNNER_TEMP/gonvex-container-export"
```

This command refuses arch3/self-hosted builds. Two fixed recipes use a clean `git archive` context without
untracked files or `.git`; `inputs.json` records Git blobs and actual archived files/link identities, including
Docker ignore rules. Pinned recipe/base/frontend digests, image/config identities, archive SHA256/bytes,
build metadata hashes, tool versions, source/run identities and disk observations are retained with both
Docker-save archives in `gonvex-k22-container-archives`. No Cloudflare/Vault token, push or deploy is involved.

The job is limited to 90 minutes, 8 GiB per archive, 12 GiB combined and seven-day artifact retention.
These are safeguards, not verified artifact quota, cost or capacity. Failures remain failures and retain
diagnostics. Node tests use real Git/tar fixtures; they do not build images or prove deployment.

The first integration-source builder is infrastructure evidence only: its `COPY cloudflare` includes the
runtime, so a final K22 builder must be rebuilt from the final runtime source. Compare sandbox inputs exactly
before reusing any digest. Download and verify actual run/artifact/source/context/config/archive identities
before the separately approved Vault-only daemonless publisher; never infer provenance from tags or time.
Original CPU/quarantine, SDK deploy/recovery, lifecycle, parity and autoscaling/cost gates remain open.

Tool references: [Docker build metadata/load](https://docs.docker.com/reference/cli/docker/buildx/build/)
and [GitHub runner/source variables](https://docs.github.com/en/actions/reference/workflows-and-actions/variables).

## First hosted failure and identity correction

Run43229865575 at093338e426641fdae93a2e18c719e032989704e3 built/exported sandbox but failed validation:
`archive config differs from built image`. The retained failure artifact13952610217 has verified API ZIP
digest5811d9d6121af0674db43a42259067d06205ad4b36dcc2807786b70c1aa3bb83. Docker29.4.2/Buildx0.38.2
reported inspect.Id equal to the OCI **index** digest (049e99ad0f87 prefix), also equal to Buildx's descriptor;
it was not a config digest. The producer's original Id=config equality was invalid for this image store.

The corrected verifier preserves classic config-Id validation and, for index/image Ids, requires a verified
archive descriptor chain from the exact built digest to one platform manifest and the exact archive config.
It validates descriptor bytes/sizes, platform, rootfs diff ids and config integrity; tag/source-label equality
cannot substitute. Index, platform-manifest and config digests are recorded separately. Bounded archive
identity diagnostics survive failures. Synthetic but real-format OCI/Docker-save positive/negative fixtures
cover index/attestation layouts and foreign/tampered configs; pinned crane independently reads the fixture.
Actual hosted rerun/archives remain required. Layer-content verification by the daemonless importer and
registry mapping must finish before publication; do not equate a published platform digest with the index.
