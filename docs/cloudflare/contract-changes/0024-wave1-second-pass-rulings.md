# 0024: Rulings from the second wave-1 review pass

Date: 2026-11-11. Status: decided (lead).
Inputs: #345 (A01 part 2) and its note 0023, #344 (harness flake), #341 (K06) and its note (renumbered 0025
on merge), #338 (K04), #339 (K01).

## 1. A01 complete

- **Strict hosts.** No strict host was found during A01 part 2: 52 fresh objects and 15 tenant objects
  were probed with S1's 22 failing views. Acceptance rests on the static guarantee that no output statement
  reads more than one union branch, plus 1,908/1,908 write statements compiling on the platform.
  `verify/strict-probe.mjs` is re-run whenever an object reports "nested too deep". A failure there is a
  stop-and-report under rule 7.
- **Cold wake.** With T6's 1,152 added objects the largest tenant wakes in **1,058 ms** median (830 ms
  without T6; S1 997 ms). R03's target is under 300 ms, and R02's wake budget hides it during working hours.
- **Rows written.** A reference-changing write writes more rows with T6 (41 against 17 on
  `workplanItems`). Accepted; R03 and R05 revisit.
- **Note 0023** (Rust's hook capture reports a column added by `ALTER TABLE … DEFAULT` as NULL on rows
  written before the ALTER, which makes a spurious change row on their first update): **the kernel's
  behavior is intended.** Plain SQL cannot reproduce the quirk, and Whagons does not hit it (record capture
  reads through views). The P02 fixture's `board.label` case is the only known instance. Conformance masks
  that one row with a named, unit-tested rule. M02 knows that change rows can differ for `gonvex_capture`
  tables altered after rows exist.
- The drift job's prelude moves out of `cloudflare/spikes/s1/` into the kernel when K02 lands; K02 owns that.
- Gabriel: the `WHAGONS_CLIENT_DEPLOY_KEY` secret (read-only deploy key on `whagons5-client`). Without it the
  job warns "not checked". The nightly schedule starts once the workflow is on `main`.

## 2. Harness

- The `thin-sdk-parity` flake is fixed (#344). Only the answered integrity reopen's cursor revision is
  masked, and ranks come from exactly-compared frames.
- **Gap the harness already had:** watermark values are compared only as present or absent. **K10 adds a
  per-backend watermark self-check**, like the digest check:
  - watermarks are monotonic per socket;
  - a watermark never exceeds the highest committed revision delivered to that socket;
  - a watermark never falls below the client's applied cursor.

## 3. K06 findings (note 0025)

| # | Finding | Ruling |
|---|---|---|
| a | On the deployed platform `node:crypto` PBKDF2 is also capped at 100,000 rounds, so 0007's fallback applies. A WASM PBKDF2 built from the reference's own crates verifies 210k rounds in about 125 ms of CPU (pure JS 253–417 ms) | **Accepted** (under 0007's 500 ms line). Logins serialize on the control object at roughly 8 password logins per second. **Follow-up before V03:** verify the password in a stateless caller (the router or the gateway) and send the control object only the verified result. Refresh-token sign-ins, which is what a reconnect wave uses, do no PBKDF2. |
| b | `ControlChanged` is delivered once, without Rust's database-listener echo | Accepted; no client sees the echo. |
| c | Process presence (`activeSessions`, live support sessions) is reported by socket owners | Accepted, consistent with 0016 G0-1. |
| d | The Firebase emulator decoder is gated by configuration instead of a debug build | Accepted, on one condition: the configuration key is refused in any deployment whose name does not start with `gonvex-cf-dev-`. |
| e | The tenant port is the interface between K06 and K01/K02/K04 | Accepted. K02 and K07 build on it. |
| f | `last_seen_at` is not written (Rust writes it at most every 5 minutes) | **Deviation recorded.** Operator and dashboard views that show last-seen times read the socket owners' presence reports instead. K19 confirms nothing else reads the column. |

## 4. K01 and K04

- K04 carries one commit to `kernel/storage/catalog.ts` (record schemas loaded once per generation: 92 %
  fewer rows read on the committed Whagons Reducers). It is approved as an exception to 0022 §1, with the K01
  owner's review.
- The FK model costs 5,256 rows read once per object wake, and the record-schema JSON (about 400 schemas) stays
  resident per object. Both are recorded for **R04** (rows) and **R03/R04** (heap per object).

## 5. Note numbering

Three packages proposed a note numbered 0020. The merged numbering is: 0020 K05 session findings; 0021 K04
module host; 0023 A01 capture quirk; 0025 K06 control object. From now on, a package proposing a note
takes the next free number on `cloudflare/integration` when it opens its PR, and the lead renumbers on
merge if two collide.
