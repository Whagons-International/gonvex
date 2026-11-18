# 0090: DEV e2e member seed takes an optional role and permissions, and has a read (X12b, BUG-8)

Raised by X12 on 2026-11-18 and decided by the lead (option 2 of the BUG-8 triage, PR #465,
`docs/cloudflare/verification/BUG-8-apikeys-list-triage.md`). **Diverge, dev tooling only.** No product route, frame,
`control.*` function, SDK or visibility behavior changes.

## Why

`apiKeys.list` and `webhooks` admit rows by the Member's direct `members.permissions` key `config.integrations`. The
walkthrough admin was seeded by `POST /dev/internal/e2e/members`, which writes `role='admin', permissions={"e2e":true}`,
so it could create keys but never list or revoke them. Every other path was closed to the walkthrough:

- `members` is an internal table, hidden from `/dev/data`.
- Invitations need Users access.
- Setting a password on a real copied admin would change a real Firebase account.

## Reference

Rust `lib.rs:1156-1360` (`provision_e2e_member`), with the row at `lib.rs:1315-1317`. It ignores any other body field,
always writes `'admin'` and `'{"e2e":true}'`, and has no read.

## Kernel behavior

`POST /dev/internal/e2e/members` accepts two optional fields:

- `role`: a non-empty string of at most 64 bytes, after trimming.
- `permissions`: a JSON object (not null, not an array) of at most 64 KiB serialized.

How they apply:

- **Both absent:** the row is exactly today's and Rust's, `'admin'` and `'{"e2e":true}'`, on insert and on re-seed.
- **One present:** that column takes the given value, and the other keeps its default.
- **Re-seed:** it updates the same Member row and bumps `membership_revision`, as before. Sending neither field again
  restores the defaults.
- **Malformed field:** 400. A field sent with `accountOnly: true` is also 400, because those writes create no Member.

`GET /dev/internal/e2e/members?project=&tenant=` plus exactly one of `accountId`, `email` or `memberId` is new and
read-only. It returns `{projectId, tenantId, accountId, memberId, status, role, permissions}`:

- `accountId` and `email` read that account's active Member, through the existing `admit` port.
- `memberId` reads the row whatever its status, through `memberById`.
- An unknown selector is 404 `member not found`.
- A wrong selector count is 400.

This read lets a walkthrough copy a real copied admin's grants without inventing any.

## Unchanged

- **Gate:** both methods check only `adminKeyMatches` (the runtime admin key), the same check and message as today and
  as Rust. They are served from the same operator route table entry. A project key, a dashboard session or no token
  gets 403.
- The command id (`e2e-member:<account>`), the Control `account_tenant_index` write and membership projection.
- Every non-dev path that writes `members` (`provision`, invitations, `control.*`).

## Conformance

No scenario sends the new fields, so the Rust and kernel runs stay identical. A Rust backend ignores the fields, which
means a walkthrough that needs grants must run against the kernel. Tests:

- `router/dev/test/e2e-members.test.ts`: gate, validation, routing.
- `kernel/assembly/test/composition.spec.ts`, "X12b": real Control to real `AssemblyTenant` defaults, overrides,
  restore and read.
