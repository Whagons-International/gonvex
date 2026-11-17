# 0075: Owner control admission and bounded reply credit

Date: 2026-11-12. Status: bounded interface permitted; activation, fit and lifecycle acceptance unproved.
Continues 0056/0074. A reserve-charged control entry is not a whole-recovery completion.

## Same queue, separate completion roles

Permit this optional local outlet interface:

```ts
interface OwnerControlLease {
  readonly receipt: ReplicaReceipt | undefined;
  release(): void;
}
ownerControl?(host: string, payload: ReplicaDelivery): Promise<OwnerControlLease>;
```

Accept only strict0074 challenge/empty-bootstrap shapes. Missing support fails before retaining/RPC;
there is no ordinary `send`/`reset` fallback. Keep one actual RPC per host, the same ordered queue, global
eight-RPC scheduling/fairness and existing host metadata admission. An internal normal/owner-control/
whole-recovery role is permitted; distinguish byte-pool ownership from permission to clear the fence.

Owner control preserves `recoveryRequired` exactly, including on failure. A control-admission refusal
is not normal frame loss: classify it distinctly and do not invent a lagged-reset reason or retry credit. It never supplies or requires a
fabricated `recoveryAck`. Normal sends remain fenced. Only actual universal recovery through `reset`, with
literal `recoveryAck:1`, all original validation and the exact owner tuple ACK when owner-marked, can clear
the outlet fence. A successful control step cannot admit normal catch-up through an existing capacity
fence; genuine universal recovery must clear it, or the host remains honestly blocked. Engine suppression
still lasts through real ordered catch-up.

Do not cancel an in-flight RPC, replace its payload or overtake queued universal recovery. At most one
owner-control entry is pending/in-flight per host. Caller singleflight is required; exact tuple/phase/credit
duplicates may share actual work only with correct per-consumer lease ownership, or fail admission. A
different tuple/phase cannot coalesce into an earlier receipt. Preserve reset's actual captured tuple.
While fenced, queued normals cannot dispatch: retain them blocked within bounds or discard through the
existing undefined-receipt/credit-release path, never accepted/delivered progress. Unfenced normal order
is unchanged. A refused control admission cannot silently discard otherwise valid normal work.

## Unknown challenge replies require an upfront grant

A restarted producer does not know the holder's connection keys. A tuple-size estimate, autoscale MAX,
configured pool size or guessed socket count cannot bound `scopeRequired`.

The new pre-application receiver takes a mandatory server-only credit envelope:

```ts
deliverReplicaOwner(
  tenantId: string,
  payload: ReplicaDelivery,
  credit: {v: 1; replyCreditBytes: number},
): Promise<ReplicaReceipt>;
```

The outlet generates this envelope; it is not auth. The lease/release function stays local and never
crosses Workers RPC; only the plain credit envelope and plain receipt do. For owner control, reserve a provisional full1MiB
reply allowance **plus** the complete request/envelope/copy charge upfront from the same shared8MiB
recovery reserve, before retaining or calling RPC. The allowance uses the conservative retained/copy-byte
credit unit, not a heap measurement. No oversized singleton exception, separate pool or reserve expansion.
If the combined charge cannot fit, fail honest capacity admission before RPC. This is an internal metadata
guard, not a public socket/subscription/argument/frame limit or measured safe capacity.

The receiver validates envelope version and positive safe-integer credit within its hard ceiling before
any Session effect. Bind preflight to the actual recipient snapshot; later changes cannot add unreserved
entries or make a partial map appear complete. Preflight the entire possible response, including every actual applicable socket key,
epoch, echo/ACK/map/ref/envelope and copy allowance, before materializing/sending it. Never omit sockets,
truncate a map, return a partial challenge or fabricate ACK to fit. Over-credit replies produce a bounded
typed failure with no admission, durable rebuild context or application. Missing credit/unsupported method
fails before effects. No automatic credit-fishing/retry loop is permitted.

For owner-marked universal recovery, a smaller reply bound may be derived from the exact offered0070 refs
and literal ACKs, with conservative envelope/copy charge. It cannot admit a new unreserved holder-wide
map or unknown response fields. Preflight its worst possible receipt before effects; retain exact-subset
processing rules and the unchanged fence-clearing semantics. All owner receiver responses obey their grant.

## Credit lifetime includes caller retention

Release the RPC scheduling slot after the actual RPC settles, so waiting for receipt consumption does not
deadlock the next same-queue operation. Keep reply credit held while the caller retains the response. The
engine must consume/drop it or transfer retained epochs/refs into separately admitted bounded state/next
payload **before** releasing the local lease. Use idempotent release in `finally`; coalesced consumers need
separate ownership/refcounts. A caller-side timeout does not release an actually running RPC's credit.
Do not assume timely GC/finalizers or clear UNKNOWN/fence authority merely to recover memory credits.
Native abort/hibernate/wake and permanently stranded credits remain availability/fit proof requirements.

Measure count/byte bookkeeping, transient copies and other DO state against the shared128MiB isolate.
The1MiB grant and8MiB reserve are safeguards, not fit/cost/throughput proof. Original V03/V04 and public
contracts must pass; a failed internal guard needs an evidence-backed fix, not a waived scenario.

Required tests: fenced challenge/bootstrap without fake recovery ACK, universal recovery alone clearing,
pending/in-flight order, exact/mismatched coalescing, old receiver/credit skew before effects, insufficient
reserve before RPC, oversized complete unknown-holder reply with zero omission/effects, multi-outlet shared
credits, lease retention/transfer/error/duplicate release, timeout/abort/wake and zero leaked idle work.
Keep the partial prototype's owner-marked paths fail-closed until receiver, accounting and engine are wired.
No new channel/placement rewrite, retry credit, source approval or K22/X01 acceptance follows.
