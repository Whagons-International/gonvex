# 0029: K06 merged over a known external CI failure

Date: 2026-11-11. Status: decided (coordinator ruling, recorded by the lead).

PR #341 (K06, head `91c4642d`) was squash-merged as `7f86b6a7` while `Runtime tests` (and the `CI` aggregate)
were red.

- **Failing step:** `Install Playwright Chromium` in the Rust `Runtime tests` job. Its apt step could not
  resolve Ubuntu's mirrors ("Temporary failure resolving 'azure.archive.ubuntu.com' / 'archive.ubuntu.com' /
  'security.ubuntu.com'").
- It failed on 3 attempts of run 43034576204, and runs on other branches hung at the same step at the
  same time.
- **Why it is external:** GitHub's runners had a DNS and package-mirror outage. K06 changes nothing in that job's
  scope: no lockfile, root `package.json`, `packages/` or Rust. The same K06 content passed `Runtime tests`
  at 13:02, before the lead's integration merge.
- **Checks that cover `cloudflare/` all passed:** typecheck, workerd tests, conformance unit tests, and the
  schema-transform drift job.

Rule for later cases: a red check may be merged over only when the failing step is outside the PR's scope,
the cause is shown to be external, and every check covering the PR's paths is green. The coordinator or
the lead records each case here. The next PR's `Runtime tests` run is the confirmation that
`cloudflare/integration` is healthy.
