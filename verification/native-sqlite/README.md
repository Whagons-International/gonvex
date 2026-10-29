# Native SQLite verification

This standalone Expo fixture executes the repository's actual app SDK storage
source through Expo SQLite 16.0.10 and Hermes on Android API 36. It has its own
frozen npm lock and is outside the published pnpm workspace. It does not connect
to a backend, account or tenant.

The check first reproduces three defects from pinned staging commit `08e45898`:
an overlapping outbox rolls back replica rows while leaving checkpoint 8, two
outboxes both reject, and a second replica reads uncommitted data. It then checks
the fixed source for those schedules, rollback recovery, concurrent migrations
and independent handles. The baseline must exhibit the expected wrong state;
a generic tool error does not count as a reproduced defect.

The host next kills the native process after a successful entity SQL statement
inside an uncommitted replica transaction. A previously committed intent must
survive, the rows and checkpoint must roll back to 7, recovery must keep the
intent's ID, key and arguments, and the next ID must be 2. A retry commits 8.
A second process restart must retain those committed rows and both intents.
Process IDs must change across each stop. SQLite must report FULL synchronization
and an on-disk journal. This is app-process failure evidence, not a power-loss,
physical-phone, iOS or complete SDK protocol proof.

## CI

`runtime-regression.yml` calls `native-sqlite-verification.yml`. Changes to SDK
execution inputs, locks, this fixture or the coordinating workflows require the
Android job. The immutable PR diff includes both old and new rename paths.
Other changes still run the receipt/scope guard tests. Pushes and manual runs
execute Android unconditionally. A failed scope job or native job blocks `CI`.
The existing real Chromium IndexedDB check remains a separate required check
inside the runtime test job.

The Android job uses a fresh CI emulator, the checksum-pinned official Expo Go
APK and a frozen fixture install. It uploads `receipt.json`, source/bundle hashes,
native observations, process events and Metro/device logs. `complete: true`
requires every phase and removal of the test app. Unit vectors exercise the host
validator using earlier native observations; they do not substitute for Android
execution.

## Run on a dedicated local emulator

Apply the machine's build-resource and emulator rules first. Use `device_list`
and `device_open`, then save the returned `agentDevice` launcher configuration
as JSON. The runner uses that exact launcher for install/open/close/uninstall.
It refuses to replace an existing `host.exp.exponent` installation. Reserve
enough device storage for the official APK; the runner does not change Android
storage limits or delete other applications.

From the repository root, inside the appropriate resource grant:

```sh
pnpm install --frozen-lockfile
pnpm build:packages
npm --prefix verification/native-sqlite ci --ignore-scripts --no-audit --no-fund
python3 verification/native-sqlite/run.py \
  --serial emulator-5554 \
  --agent-device-json /absolute/path/device-open.json \
  --output /mnt/data/work/native-sqlite-fresh-result
```

The result directory must not exist. `--apk` accepts a cached copy of the same
checksum-pinned official APK. Do not set `CI=true` to bypass local device control.
The CI-only adb lifecycle is for the workflow's disposable emulator. The runner
leaves the emulator running and removes only its test app and port reversals.

For fast validator checks without a device:

```sh
python3 -m unittest discover -s verification/native-sqlite -p 'test_*.py' -v
```

`prepare.mjs` copies current adapter source, verifies the old baseline hashes,
bundles the actual queue source and records all bundle input hashes. Generated
files live in ignored `.work/`. Native versions and the old baseline are pinned
in `toolchain.json`. Change those pins with fresh native evidence and review
the validator expectations when intentionally upgrading the engine.
