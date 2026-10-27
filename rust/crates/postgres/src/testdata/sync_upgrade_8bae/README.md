# Sync upgrade baseline

These fixtures reproduce the native sync installer at commit
`8bae698880d6a7e63472596b1a22c5dc1ced0ebc`. They are inputs to
`sync_bootstrap_upgrades_real_8bae_receipts_atomically`.

`infrastructure.sql` contains the exact bytes inside that commit's
`SYNC_INFRASTRUCTURE_SQL` raw string. This includes its leading and trailing
newlines and the old finalizer that uses `gonvex.sync_revision` as its guard.

`tasks.sql` contains the exact output of the old `install_table_trigger`
statement template for `CREATE TABLE tasks (id text PRIMARY KEY, title text)`.
It has no final newline. Its old stage function has no finalization guard.

`derive_fixtures.py` reads the pinned source through `git show`. It extracts the
raw statement template and the Rust format strings for artifact names, quoted
identifiers, literals, JSON pairs and JSON objects. It substitutes `tasks`,
`id`, and the ordered columns `id,title`, following the old projection code.
Both columns fit in the old 50-column JSON object chunk. All artifact names
fit in 63 bytes, so the name-shortening hash branch is unused. There is no
SQL-body merge, current-source substitution, whitespace normalization or Rust
execution.

The Python formatter is used only on the extracted old templates. These
specific templates use positional arguments or named arguments without format
specifiers. Their ordinary string escapes are shared by Rust and JSON.
The literal escape rule copies the old `value.replace('\'', "''")` operation.
No input identifier contains a quote or apostrophe.

`manifest.json` records the immutable source commit, source hash, derivation
script hash, old installer and raw stage-template hashes, substitution values,
and each SQL fixture's byte count and SHA-256 hash. The fixture hashes are also
the two old bootstrap receipt values, since `bootstrap_digest` uses SHA-256
over the exact UTF-8 source bytes. The test installs these strings and calls
the real `record_bootstrap` helper with those same strings. It never inserts
an invented receipt digest.

To regenerate these files from a checkout containing the pinned commit, run
from the repository root:

```sh
python3 rust/crates/postgres/src/testdata/sync_upgrade_8bae/derive_fixtures.py .
```

Keep the fixture bytes unchanged unless intentionally replacing this baseline.
The current SQL remains owned by the production installer. The test checks
its infrastructure hash directly, and the table hash through the real warm
installer pass: the installer rebuilds the current table statement and can
skip all DDL only when the installed receipt matches that statement's hash.
