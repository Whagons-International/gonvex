# @gonvex/expo-sqlite

Transactional Expo SQLite persistence for the Gonvex Local Replica.

```ts
import { openDatabaseAsync } from "expo-sqlite";
import { expoSQLite } from "@gonvex/expo-sqlite";

const database = await openDatabaseAsync("gonvex.db");
const client = new GonvexClient(url, {
  localReplica: { storage: expoSQLite(database) },
});
```

The SDK reads cold rows by primary key or a scalar secondary index. Unindexed
reads use 256-row pages and retain only the requested result window. The adapter
keeps normalized rows and query memberships on disk; it does not hydrate the
whole database to execute a Reducer. Incoming projections update only their fields.
Schema upgrades, cursor changes, row authority, and tombstones are transactional.

Keep this database handle dedicated to Gonvex. Replica and outbox instances that
receive the same handle object share one queue, so their reads, writes and
schema upgrades cannot interrupt each other's transactions. Raw application SQL
on that connection is outside this queue. Use a separate connection for it.

## Durable reducer outbox

`expoSQLiteOutbox(database)` stores queued reducer intents one row per intent,
with every change in its own SQLite transaction and a persisted id sequence.
It implements the same `OutboxStore` contract as the browser storage, so the
SDK keeps owning ordering, retries and crash recovery.

```ts
import AsyncStorage from "@react-native-async-storage/async-storage";
import { expoSQLite, expoSQLiteOutbox, migrateLegacyOutbox } from "@gonvex/expo-sqlite";

const database = await openDatabaseAsync("gonvex.db");
const outbox = expoSQLiteOutbox(database);
// One-time import of an older whole-list AsyncStorage outbox. Safe on every
// launch; the key is removed only after the import committed.
await migrateLegacyOutbox({ store: outbox, storage: AsyncStorage, key: "wh_gonvex_v2_reducer_outbox" });

const client = new GonvexClient(url, {
  localReplica: { storage: expoSQLite(database) },
  outbox: { store: outbox },
});
```

Use the same `database` object for both adapters. Read-view callbacks must await
their reads and use the supplied view without reentering another SDK operation
on that handle. See [the transaction contract](../../docs/verification/sqlite-transactions.md)
for the reproduced failures and evidence limits.

Run the migration before constructing the client. Imported entries keep their
ids and idempotency keys, so a replay is still deduplicated by the runtime.
