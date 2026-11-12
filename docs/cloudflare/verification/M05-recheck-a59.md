# M05 foundationa59 recheck: offset timing defects remain

Date:2026-11-12. NEW independent [COMMENT5882133354](https://github.com/Whagons-International/gonvex/pull/414#pullrequestreview-5882133354)
pins `a59c76316e0949ee36de4f85c7a0dedb4f8a9432`. Source-clear NO; finite disabled foundation changes
required. Full M05 acceptance is separately UNMET. Exact prior1b findings are fixed; earlier445/493
fixes and byte-unchanged100 current tests remain verified.

| P2 | Causal reproduction and required behavior |
|---|---|
| R1: failed offset skips wall validation without completion timing | started0/finished5/duration100/U0, failed operation offset80/deadline10 with absent completion fields validates. Actual CLI at cutoff10 counts attempted1/failed1 although attempt80 lies after observation5. Validate the offset itself against both clocks regardless of success/completion fields; malformed input is integrity UNKNOWN, not a published accounting object. |
| R2: offset-only expiry and eligibility use enclosing start | Consistent0..100 observation, offset80/deadline10, cutoff85 becomes failed1/pending0 while equivalent timestamp80 yields failed0/pending1. At cutoff10 the future attempt80 is incorrectly counted too. Use the recorded offset/clock bounds consistently for eligibility, expiry and proof cutoff; retain valid failures and conservative unknown/legacy behavior. |

Independent100 foundation/29 conformance, generators/types/docs and cycles1→1/417→430 pass. All48
files, source/tool/raw/historical hashes and84 prior tests verified. Actual8 offset cases span create/
update, first/retry and offsets20/80;4 impossible-offset cases plus zero-offset/expired/observed controls
are retained. These are local input/CLI/adapter/SQLite fixtures, not deployed application or service proof.

Report/receipt/reproductions: `/mnt/data/work/gonvex-m05-recheck-a59c7631/`. Finite jobs settled and
checkout stayed clean; no fixes, installs, account or merge activity by verifier. Harness/audit/tool-path
collection mistakes and old implementation failures remain disclosed, not relabelled successful proof.

Lead owns the bounded correction and a NEW immutable independent recheck. No completed reviewer reuse,
assertion weakening, foundation merge or activation from green CI. Full original P01-only dependency,
five-drill done_when/source-definition gap, real app/hosted/trace/heartbeat/alert/status/security/cost/
capacity and actual14d/30d gates remain. Current local correction results cannot replace fresh review.
