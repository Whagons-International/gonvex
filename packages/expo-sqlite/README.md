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
