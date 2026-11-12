# M05 foundation1b recheck: two P2 defects remain

Date: 2026-11-12. NEW independent COMMENT review pinned to
`1b3455bb46132fd4b6cd658d38fb1796cd0d01a9`:
[review5881664350](https://github.com/Whagons-International/gonvex/pull/414#pullrequestreview-5881664350).
Finite foundation changes required; source-clear NO. Full M05 acceptance separately remains UNMET.

Report/receipt/reproductions: `/mnt/data/work/gonvex-m05-recheck-1b3455bb/`.
All three exact493 counterexamples are fixed and prior445 fixes remain. New adjacent defects:

| P2 | Actual causal reproduction and required behavior |
|---|---|
| N1: exception inspection escapes sanitizer/pause | Adapter rejects through real dispatch with a proxy whose getPrototypeOf trap throws a private synthetic Error, or CheckError status/code accessors throw. contract.mjs77–80 inspects instanceof/status/code before its protected raw boundary, leaks that original Error and leaves WriteGuard unpaused. Classify without invoking untrusted exception behavior; emit only fixed safe outcomes and pause unsafe execution. This is a synthetic privacy-boundary repro, not a deployed credential-leak claim. |
| N2: legacy timing manufactures early application | started0/finished5/duration100/uncertainty0, create durable80/applied90/deadline100 without attemptedAt (also with offset-only0) validates. Actual CLI window[0,10)/asOf10 reports success1/bounds1/1, although application cannot fit observation/cutoff. Preserve valid legacy data while rejecting contradictions or withholding unsupported success under declared clock tolerance. |

Independent84 foundation/29 conformance tests, generators/docs and cycles1→1 pass. All45 files and22
implementation files/144,484 source bytes match6ac; old62 tests/evidence remain unchanged. These source
defects block foundation merge despite those passes. Reviewer made no fixes; finite jobs settled and
checkout stayed clean. Local SQLite/adaptor/CLI evidence is not SDK/workerd/Whagons/hosted proof.

Lead owns the bounded correction and subsequent NEW immutable independent recheck; do not retask this
reviewer or relabel1b as clear. Preserve the raw failures, old reports and original assertions. P1–P5
source definitions, real application/serving traces/three-region/provider/heartbeat/alerts/status, security/
cost/capacity and actual14-day/30-day gates remain open. No activation or staging-readiness approval.
