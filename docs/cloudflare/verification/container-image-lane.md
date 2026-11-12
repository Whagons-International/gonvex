# Credential-free Container archive lane

Status: hosted archives verified and published to development registry; no Worker/Container deployed.
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
At this repair checkpoint, actual hosted rerun/archives and layer-content/import verification were still
required before publication. The verified outcome below records their completion; a published platform
digest must not be equated with the built index.

## Verified run and development publication, 2026-11-12

PR404 head `2b32e9eb09d873df43c84fc4f733bddef0a46574` produced actual successful
[run43241519837](https://github.com/Whagons-International/gonvex/actions/runs/43241519837), from
14:51:25 to 15:23:36 UTC. Artifact13958510830 is a 323,113,097-byte ZIP with API SHA256
`f4916d1a47ed0a539432d156c830dc0ef13ad5f4979209fa079fe953964356f4`. Download verification matched.
Reproduced `git archive`, all 2,234 tracked/effective inputs and context manifest matched exactly.
Both pinned recipes, frontend/base materials, build metadata, index/platform/config graph and archive
hashes matched. Independently hashed all blobs and 21 compressed/uncompressed layers against config
diffIDs; checked actual SLSA-v1 attestation subjects/materials and full pinned crane v0.20.6 validation.
The first local attestation check assumed v0.2 and failed; that failed log is retained, then the actual v1
schema, subjects and pins were verified. Producer integrity guards were unchanged.

Both `gonvex-cf-dev-artifact-pr404-*` images were published through the approved Vault wrapper, using
15-minute push/pull registry credentials and a private standard tool auth store removed on exit. Remote
full validation, config/rootfs/layer identity and equality with the local import manifest passed. No broad
Cloudflare token entered CI, a command argument or a retained auth store.

| Image | Archive bytes | Published import manifest | Config digest |
|---|---:|---|---|
| sandbox | 129,944,576 | `sha256:efad3409656342f27ab179c547786cf15eed8fe797decd1c8d3685d5034d3aea` | `sha256:552f8ea53e04211d6384265cbd3f5ba1454f21d7615096360425f18ad5da20f4` |
| builder | 192,269,312 | `sha256:d9b31f5b02e23f22d7b3a9485963f2008f43e603f68c99b21b6f78e81e207cbe` | `sha256:06ebe0208b03754b719bf2ea25a5c0100c7fbc66c7920f9d7fa074f12dec1b56` |

These import manifests differ from the built OCI indexes and platform manifests; the verified mapping
preserves config and layer bytes. Exact index/platform/archive/base digests, commands and raw receipts are
under `/mnt/data/work/gonvex-cloudflare-lead/container-export-lane/artifacts/43241519837/`:
`download-receipt.json`, `source-archive-verification.json`, `layer-import-verification.json` and
`publication.json`; rerun validation via `verify.mjs` and `verify-layers.py` under normal resource admission.
Publication uses `publish-dev.py` only within the approved Vault wrapper.

PR404 squash-merged into integration as `ca1ae8a04c0adc5b2564c74ae3f7441a4b1e0119`; reviewed and merged
trees match. This closes the source-image build/publication unblock, not the runtime gates. The builder
contains the integration scaffold, not K22. A final K22 full-context build is still mandatory; sandbox reuse
also requires exact relevant-input comparison. Upload success proves this artifact was accepted, not account
quota headroom or measured billing. No serving deployment, CPU enforcement or assembled capacity is claimed.
