# 0059: oversized legal identity uses verified holder recovery storage

Date: 2026-11-12. Status: conditional implementation authorization; fit/behavior proof pending.

Member.permissions is unrestricted Json; legal identity can exceed 16-KiB attachment space even after
subscription maps are removed. Do not invent a smaller public identity/permission limit. K22 must record
actual legal worst-case serialized sizes, then may move oversized auth/scope identity into the SAME private
bounded/chunked verified generation authorized in 0058. Attachment may retain connectionId, generation,
scope/epoch digest and version. A digest/reference is a locator/integrity fence, never authorization.

Restore the complete verified identity before Session.restore; bind generation to connection/account/tenant/
Member/subscription epoch. Preserve existing revalidation/public-admission deadlines, token rotation and
revocation/scope fences. Missing/corrupt/wrong-owner/partial state fails closed with bounded recovery, not
stale identity. Publish atomically or detect mismatches; fence late close cleanup and retain old generations
only across the publish transition. Auth metadata is credential-sensitive: no public/module/operator/export
exposure or log/trace leakage. Bound cleanup/storage without per-delivered-frame writes or silently pruning
live authority. Prove hibernation, legal large permissions, identity/scope switches, corruption and cleanup.

K22 owns the recovery-store/Host change; X01's immutable PR is not edited. This authorizes the minimal fit fix,
not a new authentication model, weakened authorization, an attachment fit claim or completed staging gate.
