# 0004: The tenant arrives in the first `auth` frame, not in the URL

Date: 2026-11-10. Status: **open, lead recommendation below; needs S3 numbers and Gabriel's approval**.
Affects: K05, K06, X01, M01.

## Finding

`/ws` carries no project or tenant (`rust/crates/runtime/src/lib.rs:1565-1641`). The only query parameter
read is `public=1`. Project and tenant arrive in the first `auth` frame (`lib.rs:2714`, `2775`), and a later
`auth` on the same socket may switch tenant (`lib.rs:2967`). The SDK takes the URL as an opaque string from the
app (`new GonvexClient(url, …)`), so the router cannot choose the tenant Durable Object from the upgrade
request, and a Durable Object cannot hand an accepted WebSocket to another object.

## Options

1. **Router proxy.** The router Worker accepts the client socket, reads the first `auth` frame, opens a
   socket to the tenant object (or the control object for `controlOnly`) and pipes frames. A tenant switch
   reconnects the inner socket. The tenant object can still hibernate on the inner socket; the Worker cannot
   hibernate, every frame costs Worker CPU twice, and a Worker deploy drops every proxied socket.
2. **Gateway object per connection group.** The upgrade goes to a gateway Durable Object chosen without the
   tenant (for example by a hash of the client address or a random shard). The gateway holds the socket with
   the hibernation API and talks to tenant objects by RPC. This is the X01 placement made the default.
3. **Tenant in the URL.** Requires an SDK or app change, so it is out under the brief.

## Recommendation

Option 2 with a fixed shard count in stage 1, because the brief already requires delivery to be written
against one interface with two placements, and the gateway is the placement X01 needs anyway. Option 1 is
the fallback if S3 shows gateway-to-tenant RPC fan-out costs more than the proxy. K05 must not start until
this note is decided. Conformance is unaffected: frames are identical in both options.
