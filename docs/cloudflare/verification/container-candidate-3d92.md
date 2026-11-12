# Frozen K22 candidate images: verified and published

Source `3d92e1339b2d65dbc47ac2f5713f56bd9e57d738`, runtime `18afb02a5cccde933c571f7448a930ebafe574a0`.
Status: development image publication verified. No assembled Worker/Container deployment yet.
Owner: lead; K22 owns the next bounded private-platform proof from this clean frozen source.

[Hosted run43251641388](https://github.com/Whagons-International/gonvex/actions/runs/43251641388)
completed successfully; the actual image-build step in job131298712493 ran. Artifact13962905071 contains
both archives, recipes, metadata and input/provenance receipts. Its323,327,347-byte ZIP matches API SHA256
`949c39e703692fdf21fcf5a77c6acbdc638d9452aed96aa3e4c8223cd773c01e`. The first download returned HTTP503
before extraction; its zero-byte file/failure receipt are retained. One bounded GET retry succeeded. The
build was not rerun and no integrity guard changed.

Independent admitted validation reproduced all2,303 tracked/effective inputs and the exact git-archive/
context manifest. Pinned recipes/frontend/bases, build metadata, built index→platform→raw config, archive
sizes/hashes, every blob and21 compressed/uncompressed layers/config diffIDs passed. Actual SLSA-v1
subjects/materials and full pinned crane v0.20.6 import validation passed. Index, platform, config and Docker
import-manifest digest domains remain distinct; the verified mapping preserves config/rootfs/layer bytes.

Both `gonvex-cf-dev-artifact-pr400-*` images were published via the approved Vault wrapper with15-minute
push/pull credentials. The isolated standard tool auth store was removed on exit. Full remote validation
and equality of local-import/published manifests, config, rootfs and layers passed. No broad token entered
GitHub secrets, command arguments or retained logs/auth stores. No local Docker build/daemon workaround.

| Image | Published import manifest | Config |
|---|---|---|
| sandbox | `sha256:304864ae1880b67a696c8ac5edcfc6d2db11dec0656b7ce5901f9bdd8b24f903` | `sha256:9cfb3c63612b99c974a76cf4d805d4a6b96116e690390cfa4afdc6263db5fcb3` |
| builder | `sha256:dcf301c14329e3de0f6e177e01cdd937f47c5c3dbfaf55fa2e187c2d32da892f` | `sha256:2fcbfaa071ebfaf4cf05c062603d951bcb6252bbb99a42bde49212b863eb4a19` |

The adjacent `container-candidate-3d92.json` records exact source/context/archive/base/descriptor/layer/tool
digests. Raw receipts, private immutable registry references and runnable validators are under
`/mnt/data/work/gonvex-cloudflare-lead/container-export-lane/artifacts/43251641388/`:
`download-receipt.json`, `source-archive-verification.json`, `layer-import-verification.json`,
`publication.json`, `verify.mjs`, `verify-layers.py`. Publication uses `publish-dev.py` only inside the approved
Vault wrapper. GitHub artifact retention is seven days; retained local artifacts are separate evidence.
Validation/publication job memory and CPU are whole-job observations, not the128MiB serving-isolate gate.
Neither upload nor publication proves quota headroom, billing or capacity.

This builder contains frozen18afb serving source; it does **not** contain the later uncommitted owner-lifecycle
prototype. Any final full-cloudflare COPY input change requires another source-image campaign. Use a clean
exact3d92 source archive for the bounded private CPU/quarantine/rollback/quiet-neighbor/SDK/deploy probe;
never label a bundle built from dirty owner source as3d92. Record actual Worker/Container image/config/
deployment IDs, data and commands. No probe pass or deployment IDs exist in this receipt.

Keep the full34PASS/4FAIL local campaign, errors.spin local blocker, owner/N6/stranded OWNER and all eight
package-domain gaps, real enabled files, X01 automatic26tenant/1,000user, security/cost and actual-duration
gates open. Images are a dependency for platform proof, not approval to merge K22/X01 or switch staging.
No staging/production load/write, monitor/DNS/publication or purchase occurred.
