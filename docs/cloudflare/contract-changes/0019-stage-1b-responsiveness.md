# 0019: Stage 1b, responsiveness before any customer moves

Date: 2026-11-11. Status: decided (Gabriel, via the coordinator; recorded by the lead).
Affects: `AGENT_BRIEF.md` (stages, rule 6), `work-packages.json` (stages, gates, packages).

## Change

The new backend must feel responsive before any customer moves. A new stage **1b** sits between stage 1
and stage 2, with gate **G3b** in front of G4.

| ID | Package | Target |
|---|---|---|
| R01 | Outside-in synthetic Playwright probe against current staging and production on a test tenant, run from the Americas. It measures cold and warm boot, confirmation time, feed delay and reconnect. No app change. It can start right after G0. | Baseline for G3b |
| R02 | Wake budget: keep each tenant warm during its working hours, and pre-warm on login | No cold wake felt in working hours; first sync adds under 100 ms after a cold wake |
| R03 | Server-side schema diet: hot-path trigger and view logic moves into kernel code | Cold wake under 300 ms; warm Reducer commit under 10 ms |
| R04 | In-memory catalog cache | Rows read per Reducer down 90 % |
| R05 | Change log without row images; fan-out reads each changed row once per batch | Feed delay median under 150 ms |
| R06 | Kernel timers instead of polling crons | No per-minute wakes |

**G3b:** on the same scenarios, the kernel equals or beats R01's baseline on every user-facing number.

R02–R06 replace **L02–L05**, which are removed from stage 3: L02 timers (now R06), L03 schema diet (now R03),
L04 lean change log (now R05), L05 session parking (covered by R02). The client-side items **L06** (packed
pages) and **L07** (columnar replica) stay in stage 3, built in parallel behind capability flags, and ship after
migration.

## Lead notes

- R01 is the only package allowed to touch live staging and production. The touch is read-only: synthetic
  sessions on a designated test tenant, with one user's load and no app change (brief rule 6).
- Today's measurements set the starting points: S1's cold wake median is 997 ms against R03's 300 ms target.
  S2's warm commit is 11–23 ms locally against 10 ms. S2's rows read are 95–99 % catalog lookups, which R04
  targets. S3's single-object delivery p50 is 123 ms, against R05's 150 ms median.
- Order: R01 starts now. R04 can follow K04 directly. R06 follows K11, R05 follows K03 and K10, R02 follows
  K05, K06 and K11, and R03 follows A01, K02, K03 and K07.
- Shadow (M02) and cutover tooling (M03) may be built and rehearsed before G3b. No tenant cuts over before
  G3b passes.
