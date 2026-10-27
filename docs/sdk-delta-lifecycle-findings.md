# SDK delta continuation lifetime

A delta can be awaiting storage when its client closes or changes authorization
scope. The active database operation may finish, but the old continuation must
not publish through a disposed subscription or restore retired replay metadata.

On staging `afc85d5e`, public-client fixtures reproduced four callbacks after
explicit close, including explicit reconnect and batched-open variants. A fifth
fixture reproduced old-scope snapshot metadata replay to a newly added listener.
This was stale replay metadata, not a demonstrated cross-tenant row disclosure.
Seven compatibility fixtures passed on that baseline.

The fix captures the subscription's routing ID before hashing or storage can
suspend the frame. Publication requires the same owned subscription, routing ID
and authorized scope. Before storage admission, the existing verification
generation check still applies. After an admitted write commits, transport loss
alone does not cancel its valid delivery to an active offline observer.

The fixtures preserve ordinary disconnect and explicit reconnect while a committed
storage result is held. They require revision-11 delivery and a ready-only resume
at the same cursor, with no later snapshot masking missed delivery. Other controls
cover current owners, replacement owners, returning listeners and commits during
a retained interval with no listeners. The retention timer in that fixture exceeds
the test timeout and is cancelled by cleanup.

These are Node tests using the real public GonvexClient, controlled WebSocket
frames and the actual memory adapter. Only the existing frame-lane promise is
read privately to join delivered frames. The new public fixtures do not mutate
a private handler or subscription map. The fixture is source-derived, not a captured runtime session.

Close remains synchronous and does not cancel or join an active storage operation.
The guard addresses an obsolete continuation before it begins publication. It
does not establish a universal no-callback guarantee if a listener closes the
client during an emission already in progress. It does not change the cursor-only
watermark path or establish native Expo, Hermes, browser or whole-SDK proofs.
Effective optimistic-ack corruption and delta-triggered reconnect claims were
rejected because the source did not support them.

Validation passed with 625 client tests, 19 OPFS checks, client typechecking,
all package builds, docs and function-binding generation, and 31 repository
script checks. One optional runtime integration check was skipped. Existing
private-handler integrity tests now register their fixture subscriptions and
scopes; the chunked-hash test also asserts that reconnect invalidation ran.
Runtime import cycles are unchanged against staging. The bounded validation
used one CPU, a 1 GiB hard memory cap and no swap.
