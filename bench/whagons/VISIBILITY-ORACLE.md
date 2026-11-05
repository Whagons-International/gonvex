# Visibility oracle regression

The original historical Mac run remains failed. Its four visibility errors did
not save the compared principals, window cursors or readiness states. The stopped
synthetic database preserves transaction history, but that history cannot recover
unsaved client observations. A revision mismatch is therefore not established as
the original run's cause.

The strongest correlation is `tasks.recentWorkspaceTasks`, args `{}`, synthetic
user291 in tenant2, member `member_1a574c7e9f22494d9571613bc6f8f5f6`.
The historical `members.removeMemberTeam` at tenant revision800 removes that
member's primary team. An isolated synthetic copy was rewound from846 to799
using the retained full old/new records, with the original triggers restored.
Normal verified authentication and the real shipping app reducer then produced:

| Complete window | Warm thin | Warm SDK | Fresh SDK |
| --- | ---: | ---: | ---: |
| revision799 | 1489 | 1489 | — |
| revision800 after removal | 418 | 418 | 418 |

This reproduces the exact1071-row difference across revisions. At matched800,
all three agree with zero extra, missing or changed rows. It does not establish
which revisions the original failed comparison observed.

`fixtures/visibility-team-revoke-799-800.json.gz` contains the real synthetic
snapshot pages, ready, needHashes, watermark batch and1071-delete delta from this
probe, plus the fresh authoritative pages. Its metadata pins the source frame
SHA, runtime2b7e2ff9 and matching shipping module. The test sends these frames
through each client's normal WebSocket handlers and public watch API.

Run with the approved SDK artifact, without a live server:

```sh
LOAD_TEST_SDK=/mnt/data/work/gonvex-sdk-composition/candidate-2/dist/index.js \
  node --test bench/whagons/visibility-oracle.test.mjs
```

The regression also drops the invalidation/delta while advancing a thin window
to800. The oracle must detect the resulting1071 stale extras at a matched cursor.
It rejects corrupted row values, different principals and incomplete needHashes
state even when both provisional row lists contain the same418 IDs. The latter
reproduces the old misleading `0 leaked, 0 missing` diagnostic without treating
pending readiness as success. It does not prove why the original three timeouts
occurred.

The harness now requires complete current windows, exact authority and cursor,
full row membership and a digest validated against actual projected row values.
A certified mismatch is terminal. Missing readiness, authority or revision
alignment fails qualification; none receives an exemption. Records retain
principal, args, mutation, cursors and readiness in durable worker receipts.
The original schedule, SDK/runtime artifacts, subscriptions and SLOs are unchanged.
