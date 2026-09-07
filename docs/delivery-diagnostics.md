# Diagnosing slow live updates

A September 6 production alert for `bulk.workspaceTaskCounts` measured 5,879 ms from commit to the browser acknowledgment. The result was ready after 413 ms. Existing traces could not separate the remaining time into socket queueing, socket writing, browser processing, and network delay.

## Measurements

- `trace.serverSubscriptionSentAtMs` retains its existing meaning: result ready for delivery, before batching and acquiring the socket lock.
- `trace.serverSocketWriteStartedAtMs` is stamped immediately before encoding and physically writing the WebSocket frame, after acquiring that lock. Nested query batches receive their own copied traces. The difference is socket queue time, including batching.
- A `slow websocket delivery` runtime log records physical writes with queue or write duration of at least 200 ms. It includes connection/project/tenant, frame type, byte size, queue/write milliseconds, failure status, and up to eight subscription IDs. It excludes query arguments and results. Write duration includes JSON encoding and ends when Go's WebSocket write returns; it does not measure arrival at the browser or transit through the supervisor/proxy.
- Browser telemetry retains browser name/version and adds `device.deliveryDiagnostics`: event-creation-to-handler delay when timestamps are compatible, JSON decoding duration, processing time up to telemetry submission, socket `bufferedAmount`, online/visibility state, and recent long-task count/maximum/total.
- Long-task context keeps at most 32 entries ending within the last 30 seconds. It is context, not proof that those tasks delayed this particular update. `longTaskSupported=false` means unavailable, not zero main-thread activity. The observer runs only when telemetry is enabled, has no polling timer, and disconnects when disabled or the client closes.

Browser durations use a monotonic clock. Epoch-style, future, or more-than-60-second-old event timestamps are omitted rather than mixed with `performance.now()`. Event timestamps represent event creation, not necessarily network receipt, so handler delay alone cannot identify all browser scheduling delay. See the [DOM event timestamp definition](https://dom.spec.whatwg.org/#dom-event-timestamp) and [Long Tasks API draft](https://w3c.github.io/longtasks/).

## Retention and correlation

`/dev/metrics` and the JSONL ledger expose `serverSocketWriteStartedAtMs` and `serverSocketQueueMs` on browser events. Durable `telemetry_events.device` preserves:

- `deliveryDiagnostics`, from the SDK;
- `serverDelivery.socketWriteStartedAtMs` and `serverDelivery.socketQueueMs`, derived from the returned trace;
- `serverDelivery.changeToAckMs`, which previously disappeared from the durable SQL row.

No table/schema migration is needed. Find a slow acknowledgment by project/tenant, operation ID and event time, then match any slow-write log's subscription IDs. The Telegram alert now includes socket queue time when available and the browser version. Threshold and cooldown behavior are unchanged.

These are diagnostic client reports, not trusted audit evidence. An old SDK may omit browser diagnostics while still returning the new server trace. A new SDK works with an old server, but socket timings will be absent. Browser context requires publishing the updated protocol/client packages and upgrading consuming apps. Runtime deployment alone supplies the socket-side timings.

Do not subtract browser and server wall clocks to estimate one-way network latency. The server-clock commit-to-ack measure remains an upper bound, and includes the return hop. If socket queue/write measurements are small and browser processing/long-task evidence is inconclusive, network delay remains unresolved.
