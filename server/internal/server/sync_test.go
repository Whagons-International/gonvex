package server

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/config"
	"github.com/gorilla/websocket"
)

func TestSyncValueMatchesEqualityArguments(t *testing.T) {
	definition := manifest.SyncDefinition{
		EqualFilters:   map[string]string{"workspaceId": "workspaceId"},
		ExcludeWhenSet: []string{"deletedAt"},
	}
	args := map[string]json.RawMessage{
		"workspaceId": json.RawMessage(`"workspace-a"`),
	}

	if !syncValueMatches(json.RawMessage(`{"id":"task-a","workspaceId":"workspace-a"}`), definition, args) {
		t.Fatal("expected matching workspace row")
	}
	if syncValueMatches(json.RawMessage(`{"id":"task-b","workspaceId":"workspace-b"}`), definition, args) {
		t.Fatal("did not expect another workspace row")
	}
	if syncValueMatches(json.RawMessage(`null`), definition, args) {
		t.Fatal("did not expect a deleted row to match")
	}
	if syncValueMatches(json.RawMessage(`{"id":"task-c","workspaceId":"workspace-a","deletedAt":1720000000000}`), definition, args) {
		t.Fatal("did not expect a soft-deleted row to match")
	}
}

func TestSyncSnapshotHonorsRowAndByteBudgets(t *testing.T) {
	result := []map[string]any{
		{"id": "a", "title": "small"},
		{"id": "b", "title": "also-small"},
		{"id": "c", "title": "third"},
	}
	rows, truncated, err := syncSnapshotRows(result, manifest.SyncDefinition{
		Key:      "id",
		MaxRows:  2,
		MaxBytes: 1_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected row budget to keep two rows, got %d", len(rows))
	}
	if !truncated {
		t.Fatal("expected row budget to report remaining rows as truncated")
	}

	rows, truncated, err = syncSnapshotRows(result, manifest.SyncDefinition{
		Key:      "id",
		MaxBytes: int64(len(rows[0])),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected byte budget to keep one row, got %d", len(rows))
	}
	if !truncated {
		t.Fatal("expected byte budget to report remaining rows as truncated")
	}

	rows, truncated, err = syncSnapshotRows(result[:2], manifest.SyncDefinition{
		Key:     "id",
		MaxRows: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || truncated {
		t.Fatalf("collection exactly at its row budget must be complete: rows=%d truncated=%t", len(rows), truncated)
	}

	rows, truncated, err = syncSnapshotRows(result[:1], manifest.SyncDefinition{
		Key:     "id",
		MaxRows: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || truncated {
		t.Fatalf("collection below its row budget must be complete: rows=%d truncated=%t", len(rows), truncated)
	}
}

func TestSyncReadyReportsTruncationAndClearsAfterBoundedDelta(t *testing.T) {
	definition := manifest.SyncDefinition{Table: "tasks", Key: "id", MaxRows: 2}
	subscription := &syncSubscription{
		id:         "sync-a",
		path:       "tasks.sync",
		definition: definition,
	}
	cursor := syncCursor{Epoch: "epoch-a", Revision: 41}

	_, truncated, err := syncSnapshotRows([]map[string]any{
		{"id": "a"},
		{"id": "b"},
		{"id": "c"},
	}, definition)
	if err != nil {
		t.Fatal(err)
	}
	subscription.truncated = truncated
	assertSyncReadyTruncated(t, syncReadyServerMessage(subscription, cursor), true)

	changes := []syncLogChange{
		{revision: 42, ordinal: 1, table: "tasks", rowID: "b", operation: "delete"},
		{revision: 42, ordinal: 2, table: "tasks", rowID: "c", operation: "delete"},
	}
	if !syncNeedsAuthoritativeReconcile(definition, changes) {
		t.Fatal("a bounded delta must recompute the authoritative collection and its truncation state")
	}
	cursor.Revision = 42
	_, truncated, err = syncSnapshotRows([]map[string]any{
		{"id": "a"},
	}, definition)
	if err != nil {
		t.Fatal(err)
	}
	subscription.truncated = truncated
	assertSyncReadyTruncated(t, syncReadyServerMessage(subscription, cursor), false)
}

func assertSyncReadyTruncated(t *testing.T, message serverMessage, want bool) {
	t.Helper()
	payload, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	var frame map[string]json.RawMessage
	if err := json.Unmarshal(payload, &frame); err != nil {
		t.Fatal(err)
	}
	raw, ok := frame["truncated"]
	if !ok {
		t.Fatalf("sync.ready frame omitted truncated: %s", payload)
	}
	var got bool
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("sync.ready truncated=%t, want %t: %s", got, want, payload)
	}
}

func TestSyncProtocolLogCarriesSnapshotAndClientContext(t *testing.T) {
	connection := &wsConn{
		id:      "conn-000042",
		project: "project-a",
		tenant:  "tenant-a",
		user:    &gonvex.User{ID: "user-a", Email: "ada@example.test"},
		device: clientDeviceInfo{
			BrowserName: "Chrome", BrowserVersion: "140", DeviceType: "desktop", Platform: "Linux",
		},
	}
	entry := connection.syncProtocolLog(clientMessage{
		ID: "sync-1", Path: "tasks.recentSync", Args: json.RawMessage(`{"workspaceId":"workspace-a"}`),
	}, "snapshot", 18, 25*time.Millisecond, nil)

	if entry.Kind != "sync" || entry.Path != "tasks.recentSync" || entry.Reason != "snapshot" || entry.Outcome != "ok" {
		t.Fatalf("unexpected sync log identity: %#v", entry)
	}
	if entry.ExecutionID == "" || entry.OperationID != "sync-1" {
		t.Fatalf("sync log did not separate the unique attempt from the subscription id: %#v", entry)
	}
	if entry.ConnectionID != "conn-000042" || entry.Browser != "Chrome 140" || entry.DeviceType != "desktop" || entry.Platform != "Linux" {
		t.Fatalf("sync log lost client attribution: %#v", entry)
	}
	if entry.UserID != "user-a" || entry.UserEmail != "ada@example.test" || entry.Tenant != "tenant-a" {
		t.Fatalf("sync log lost caller attribution: %#v", entry)
	}
	if string(entry.Request) != `{"workspaceId":"workspace-a"}` || entry.ResultCount == nil || *entry.ResultCount != 18 {
		t.Fatalf("sync log lost snapshot context: %#v", entry)
	}
}

func TestSyncSnapshotRejectsDuplicateKeys(t *testing.T) {
	_, _, err := syncSnapshotRows([]map[string]any{
		{"id": "duplicate", "title": "first"},
		{"id": "duplicate", "title": "second"},
	}, manifest.SyncDefinition{Key: "id"})
	if err == nil {
		t.Fatal("duplicate keys would make the collection digest ambiguous")
	}
}

func TestSyncRowKeyMatchesJavaScriptScalarStringification(t *testing.T) {
	tests := map[string]string{
		`{"id":100000000000000000000}`: "100000000000000000000",
		`{"id":1e-7}`:                  "1e-7",
		`{"id":-0}`:                    "0",
		`{"id":true}`:                  "true",
	}
	for raw, want := range tests {
		if got := syncRowKey(json.RawMessage(raw), "id"); got != want {
			t.Fatalf("syncRowKey(%s) = %q, want %q", raw, got, want)
		}
	}
}

func TestSyncRetentionUsesLongestDeclaredWindow(t *testing.T) {
	current := manifest.Manifest{Functions: map[string]manifest.FunctionEntry{
		"tasks.sync": {
			Kind: manifest.FunctionKindSync,
			Sync: &manifest.SyncDefinition{RetentionMilliseconds: (14 * 24 * time.Hour).Milliseconds()},
		},
		"statuses.sync": {
			Kind: manifest.FunctionKindSync,
			Sync: &manifest.SyncDefinition{RetentionMilliseconds: (2 * 24 * time.Hour).Milliseconds()},
		},
	}}

	if got := syncRetentionForManifest(current); got != 14*24*time.Hour {
		t.Fatalf("expected longest retention window, got %s", got)
	}
	if got := syncRetentionForManifest(manifest.Manifest{}); got != defaultSyncRetention {
		t.Fatalf("expected default retention window, got %s", got)
	}
}

func TestSyncCursorForClockSharesRevisionButIsolatesDefinitionsAndScopes(t *testing.T) {
	clock := syncClock{DatabaseEpoch: "database-a", Revision: 42, RetainedRevision: 7}
	tasks := manifest.SyncDefinition{Table: "tasks", Key: "id", Columns: []string{"id", "title"}}
	statuses := manifest.SyncDefinition{Table: "statuses", Key: "id", Columns: []string{"id", "name"}}

	taskCursor := syncCursorForClock(clock, tasks, "scope-a")
	statusCursor := syncCursorForClock(clock, statuses, "scope-a")
	otherScopeCursor := syncCursorForClock(clock, tasks, "scope-b")

	if taskCursor.Revision != clock.Revision || statusCursor.Revision != clock.Revision {
		t.Fatalf("expected every definition in a batch to share revision %d", clock.Revision)
	}
	if taskCursor.Epoch == statusCursor.Epoch {
		t.Fatal("different sync definitions must not share an epoch")
	}
	if taskCursor.Epoch == otherScopeCursor.Epoch {
		t.Fatal("different visibility scopes must not share an epoch")
	}
}

func TestSyncDefinitionTableIntersectionIncludesVisibilityDependencies(t *testing.T) {
	definition := manifest.SyncDefinition{
		Table:            "tasks",
		VisibilityTables: []string{"taskAcks", "memberships"},
	}
	for _, table := range []string{"tasks", "taskAcks", "memberships"} {
		if !syncDefinitionIntersectsTables(definition, []string{table}) {
			t.Fatalf("definition did not intersect relevant table %q", table)
		}
	}
	if syncDefinitionIntersectsTables(definition, []string{"statuses"}) {
		t.Fatal("definition intersected an unrelated table")
	}
}

func TestSyncDefinitionsForSchemaDurablyLogsVisibilityDependencies(t *testing.T) {
	definitions := map[string]manifest.SyncDefinition{
		"tasks": {
			Table:            "tasks",
			Key:              "id",
			Columns:          []string{"id", "title"},
			VisibilityTables: []string{"taskAcks"},
		},
	}
	schema := manifest.Schema{Tables: map[string]manifest.Table{
		"tasks": {Columns: map[string]manifest.Column{
			"id":    {Type: "id", PrimaryKey: true},
			"title": {Type: "string"},
		}},
		"taskAcks": {Columns: map[string]manifest.Column{
			"_id":    {Type: "id", PrimaryKey: true},
			"status": {Type: "string"},
		}},
	}}

	got, err := syncDefinitionsForSchema(definitions, schema)
	if err != nil {
		t.Fatal(err)
	}
	dependency, ok := got["taskAcks"]
	if !ok {
		t.Fatal("visibility dependency must have a durable sync-log trigger for offline reconnects")
	}
	if dependency.Key != "_id" || len(dependency.Columns) != 1 || dependency.Columns[0] != "_id" {
		t.Fatalf("unexpected visibility dependency definition: %#v", dependency)
	}
}

func TestSyncReadDependenciesAutomaticallyBecomeVisibilityDependencies(t *testing.T) {
	definition := effectiveSyncDefinition(manifest.FunctionEntry{
		Kind: manifest.FunctionKindSync,
		Sync: &manifest.SyncDefinition{
			Table: "tasks", Key: "id", VisibilityTables: []string{"taskAcks"},
		},
		Dependencies: manifest.FunctionDependencies{Reads: []manifest.ReadDependency{
			{Table: "tasks"},
			{Table: "taskApprovalInstances"},
			{Table: "taskAcks"},
		}},
	})
	if got, want := strings.Join(definition.VisibilityTables, ","), "taskAcks,taskApprovalInstances"; got != want {
		t.Fatalf("effective visibility dependencies = %q, want %q", got, want)
	}
}

func TestSyncVisibilityChangesAreIncludedInReconnectReconciliation(t *testing.T) {
	changes := []syncLogChange{
		{revision: 41, table: "tasks", rowID: "task-a"},
		{revision: 42, table: "taskAckReads", rowID: "read-a"},
	}
	if !syncVisibilityChanged(changes, "tasks") {
		t.Fatal("a dependency-table change must be reconciled before sync.ready")
	}
	if syncVisibilityChanged(changes[:1], "tasks") {
		t.Fatal("source-table changes must remain delta-replayable")
	}
}

func TestBoundedEagerSyncReconcilesItsAuthoritativeWindow(t *testing.T) {
	changes := []syncLogChange{{revision: 42, table: "statuses", rowID: "status-new"}}
	if !syncNeedsAuthoritativeReconcile(manifest.SyncDefinition{
		Table: "statuses", Mode: "eager", MaxRows: 100,
	}, changes) {
		t.Fatal("direct replay cannot determine which row follows a full bounded window")
	}
	if syncNeedsAuthoritativeReconcile(manifest.SyncDefinition{
		Table: "statuses", Mode: "eager",
	}, changes) {
		t.Fatal("an unbounded source-shaped eager collection should retain direct delta replay")
	}
}

func TestSyncValueMatchesAppliesExclusionsWithoutEqualityFilters(t *testing.T) {
	definition := manifest.SyncDefinition{ExcludeWhenSet: []string{"deletedAt"}}
	if syncValueMatches(json.RawMessage(`{"id":"deleted","deletedAt":123}`), definition, nil) {
		t.Fatal("a soft-deleted row must not reappear merely because the sync has no equality filters")
	}
	if !syncValueMatches(json.RawMessage(`{"id":"active","deletedAt":null}`), definition, nil) {
		t.Fatal("an active row should remain visible")
	}
}

func TestProgressiveSyncKeepsContextRoutedRowsInBothCollections(t *testing.T) {
	current := map[string]json.RawMessage{
		"task-a": json.RawMessage(`{"id":"task-a","workspaceId":"approval-workspace"}`),
	}
	visible := map[string]bool{"task-a": true}
	currentHashes := syncRowsHashes([]json.RawMessage{current["task-a"]}, "id")
	visibleHashes := map[string]string{"task-a": "stale-hash"}
	changes := []syncLogChange{{
		revision: 42,
		table:    "tasks",
		rowID:    "task-a",
		// The physical source task remains in its default workspace. The computed
		// handler includes it in the approval/acknowledgment workspace as well.
		newValue: json.RawMessage(`{"id":"task-a","workspaceId":"default-workspace"}`),
	}}

	upserts, deleted, _ := progressiveSyncDiff(current, currentHashes, visible, visibleHashes, changes)
	if _, ok := upserts["task-a"]; !ok {
		t.Fatal("handler-authorized context row must be updated in the action workspace")
	}
	if deleted["task-a"] {
		t.Fatal("context-routed task must not be removed merely because its physical workspace differs")
	}
}

func TestProgressiveSyncSendsOnlyRowsWhoseAuthoritativeHashChanged(t *testing.T) {
	rows := map[string]json.RawMessage{
		"same":    json.RawMessage(`{"id":"same","value":1}`),
		"changed": json.RawMessage(`{"id":"changed","value":2}`),
	}
	hashes := syncRowsHashes([]json.RawMessage{rows["same"], rows["changed"]}, "id")
	visible := map[string]bool{"same": true, "changed": true, "deleted": true}
	visibleHashes := map[string]string{
		"same":    hashes["same"],
		"changed": "old-hash",
		"deleted": "deleted-hash",
	}

	upserts, deleted, _ := progressiveSyncDiff(rows, hashes, visible, visibleHashes, nil)
	if len(upserts) != 1 || upserts["changed"] == nil {
		t.Fatalf("expected only changed row delta, got %#v", upserts)
	}
	if len(deleted) != 1 || !deleted["deleted"] {
		t.Fatalf("expected only missing row deletion, got %#v", deleted)
	}
}

func TestAuthoritativeSyncDiffConvergesAcrossRandomCorruption(t *testing.T) {
	random := rand.New(rand.NewSource(0x5eed))
	for iteration := 0; iteration < 2_000; iteration++ {
		currentRows := map[string]json.RawMessage{}
		currentHashes := map[string]string{}
		visibleKeys := map[string]bool{}
		visibleHashes := map[string]string{}
		for index := 0; index < 40; index++ {
			key := fmt.Sprintf("row-%d", index)
			if random.Intn(3) != 0 {
				row := json.RawMessage(fmt.Sprintf(`{"id":%q,"value":%d}`, key, random.Intn(20)))
				currentRows[key] = row
				currentHashes[key] = syncRowHash(row)
			}
			if random.Intn(3) != 0 {
				visibleKeys[key] = true
				switch random.Intn(4) {
				case 0:
					// Missing metadata is a valid corruption case.
				case 1:
					visibleHashes[key] = "corrupted"
				default:
					visibleHashes[key] = currentHashes[key]
				}
			}
		}

		upserts, deleted, _ := progressiveSyncDiff(
			currentRows,
			currentHashes,
			visibleKeys,
			visibleHashes,
			nil,
		)
		for key := range deleted {
			delete(visibleKeys, key)
			delete(visibleHashes, key)
		}
		for key := range upserts {
			visibleKeys[key] = true
			visibleHashes[key] = currentHashes[key]
		}

		if len(visibleKeys) != len(currentRows) || len(visibleHashes) != len(currentHashes) {
			t.Fatalf("iteration %d did not converge collection cardinality", iteration)
		}
		for key, hash := range currentHashes {
			if !visibleKeys[key] || visibleHashes[key] != hash {
				t.Fatalf("iteration %d left %q stale", iteration, key)
			}
		}
	}
}

func TestSyncHashesDigestMatchesClientCanonicalEncoding(t *testing.T) {
	const want = "db8992cf941b185a1c5bbcfbdc3f43204fe09d42b1a57ff584f6ad2e5e6dfdbd"
	if got := syncHashesDigest(map[string]string{"a": "b"}); got != want {
		t.Fatalf("digest = %q, want %q", got, want)
	}
	if got := syncHashesDigest(map[string]string{}); got != "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945" {
		t.Fatalf("empty digest = %q", got)
	}
	const unicodeWant = "829a3fd7e2d0b8a181fc05f90729311dee80aa61e3a94723a1cc4553dd4dd7c9"
	if got := syncHashesDigest(map[string]string{"< ": "one", "": "two", "𐀀": "three"}); got != unicodeWant {
		t.Fatalf("unicode-key digest = %q, want %q", got, unicodeWant)
	}
}

func TestSyncRowHashMatchesClientCanonicalEncoding(t *testing.T) {
	row := json.RawMessage(`{
		"nested":{"z":1,"a":" ","key ":"value","":"bmp","𐀀":"supplementary"},
		"minusZero":-0,
		"id":"row-a",
		"amp":"<&>"
	}`)
	const want = "ba54453d2ecfb596b1cedeb64d8753675128ae801cf18da01a6cdb1005c5d71b"
	if got := syncRowHash(row); got != want {
		t.Fatalf("canonical row %s hash = %q, want %q", canonicalSyncJSON(row), got, want)
	}
}

func TestSyncBatchProtocolPreservesIndependentResumeState(t *testing.T) {
	var message clientMessage
	if err := json.Unmarshal([]byte(`{
		"type":"sync.openMany",
		"opens":[
			{"id":"one","path":"sync.tasks","args":{"workspaceId":"a"},"cursor":{"epoch":"e1","revision":7},"digest":"digest-one"},
			{"id":"two","path":"sync.statuses","args":{},"cursor":{"epoch":"e2","revision":9},"keys":["s1","s2"],"fullIntegrity":true}
		]
	}`), &message); err != nil {
		t.Fatal(err)
	}
	if len(message.Opens) != 2 {
		t.Fatalf("expected two independent opens, got %d", len(message.Opens))
	}
	if message.Opens[0].Cursor == nil || message.Opens[0].Cursor.Revision != 7 {
		t.Fatalf("first cursor was not preserved: %#v", message.Opens[0].Cursor)
	}
	if got := len(message.Opens[1].Keys); got != 2 {
		t.Fatalf("expected progressive keys to survive batching, got %d", got)
	}
	if message.Opens[0].Digest != "digest-one" || !message.Opens[1].FullIntegrity {
		t.Fatal("compact and expanded integrity resume metadata must survive batching")
	}
}

func TestSyncOpenManyStorageFailureIsRoutedToEverySubscription(t *testing.T) {
	connection, peer := newSyncReadyTestConnection(t, false)
	connection.server = New(config.Config{
		ProjectDatabases: map[string]string{
			connection.project: "postgres://127.0.0.1:1/missing",
		},
	})
	connection.openSyncMany(context.Background(), []syncOpenRequest{
		{ID: "priorities", Path: "sync.priorities", Args: json.RawMessage(`{"tenantId":"tenant-a"}`)},
		{ID: "templates", Path: "sync.templates", Args: json.RawMessage(`{"tenantId":"tenant-a"}`)},
	})

	frames := readSyncTestFrames(t, peer, 2)
	for index, want := range []struct{ id, path string }{
		{id: "priorities", path: "sync.priorities"},
		{id: "templates", path: "sync.templates"},
	} {
		if frames[index].Type != "sync.error" || frames[index].ID != want.id || frames[index].Path != want.path || frames[index].Error == "" {
			t.Fatalf("frame %d = %#v, want addressed sync.error for %s", index, frames[index], want.path)
		}
	}
}

func TestSyncSubscriptionReadyFanoutIsCapabilityGated(t *testing.T) {
	const subscriptionCount = 5
	for _, test := range []struct {
		name              string
		syncReadyMany     bool
		wantReadyFrames   int
		wantReadyManySize int
	}{
		{name: "capability-on", syncReadyMany: true, wantReadyFrames: 0, wantReadyManySize: subscriptionCount},
		{name: "capability-off", syncReadyMany: false, wantReadyFrames: subscriptionCount},
	} {
		t.Run(test.name, func(t *testing.T) {
			connection, peer := newSyncReadyTestConnection(t, test.syncReadyMany)
			runtime := New(config.Config{})
			connection.server = runtime
			listenerKey := tenantListenerKey{project: connection.project, tenant: connection.tenant}
			runtime.subscriptions.listeners.active[listenerKey] = &tenantListener{
				key: listenerKey, connected: true,
			}
			connection.write(serverMessage{
				Type: "sync.delta", ID: "sync-0", Path: "sync.tasks",
				Cursor: &syncCursor{Epoch: "epoch-a", Revision: 2},
			})
			for index := 0; index < subscriptionCount; index++ {
				subscription := &syncSubscription{
					conn: connection, id: fmt.Sprintf("sync-%d", index), path: fmt.Sprintf("sync.table%d", index),
					project: connection.project, tenant: connection.tenant,
				}
				connection.syncs[subscription.id] = subscription
				if !runtime.writeSyncReady(subscription, syncReadyTestMessage(index)) {
					t.Fatalf("subscription %q lost listener freshness while writing ready", subscription.id)
				}
			}

			frames := readSyncTestFrames(t, peer, 1+max(test.wantReadyFrames, 1))
			if frames[0].Type != "sync.delta" {
				t.Fatalf("first frame = %q, want sync.delta", frames[0].Type)
			}
			readyFrames := 0
			readyManyFrames := 0
			covered := map[string]bool{}
			for _, frame := range frames[1:] {
				switch frame.Type {
				case "sync.ready":
					readyFrames++
					covered[frame.ID] = true
				case "sync.readyMany":
					readyManyFrames++
					if len(frame.Ready) != test.wantReadyManySize {
						t.Fatalf("sync.readyMany covered %d subscriptions, want %d", len(frame.Ready), test.wantReadyManySize)
					}
					for _, ready := range frame.Ready {
						covered[ready.ID] = true
						if ready.Path == "" || ready.Cursor == nil || ready.Mode == "" || ready.Digest == "" {
							t.Fatalf("sync.readyMany entry lost ready metadata: %#v", ready)
						}
					}
				default:
					t.Fatalf("unexpected frame after delta: %#v", frame)
				}
			}
			if readyFrames != test.wantReadyFrames {
				t.Fatalf("plain sync.ready frames = %d, want %d", readyFrames, test.wantReadyFrames)
			}
			if test.syncReadyMany && readyManyFrames != 1 {
				t.Fatalf("sync.readyMany frames = %d, want 1", readyManyFrames)
			}
			if len(covered) != subscriptionCount {
				t.Fatalf("ready coverage = %d subscriptions, want %d", len(covered), subscriptionCount)
			}
		})
	}
}

func TestAuthStoresSyncCapabilities(t *testing.T) {
	connection, peer := newSyncReadyTestConnection(t, false)
	connection.server = New(config.Config{})
	connection.handle(context.Background(), clientMessage{
		Type: "auth", ID: "auth-1", Project: connection.project,
		Capabilities: &clientCapabilities{SyncReadyMany: 1, SyncWatermark: 1},
	})

	frames := readSyncTestFrames(t, peer, 1)
	if frames[0].Type != "auth.result" {
		t.Fatalf("auth response = %q, want auth.result", frames[0].Type)
	}
	connection.mu.Lock()
	readyMany := connection.syncReadyMany
	watermark := connection.syncWatermark
	connection.mu.Unlock()
	if !readyMany || !watermark {
		t.Fatalf("auth stored syncReadyMany=%t syncWatermark=%t, want both", readyMany, watermark)
	}
}

func TestSyncWatermarkWaitsForChangedSubscriptionReady(t *testing.T) {
	connection, peer := newSyncReadyTestConnection(t, true)
	connection.syncWatermark = true
	connection.syncs["sync-1"] = &syncSubscription{id: "sync-1", conn: connection}
	connection.writeSyncWatermark(5, []string{"sync-1"})
	connection.write(serverMessage{
		Type: "sync.delta", ID: "sync-1", Path: "sync.tasks",
		Cursor: &syncCursor{Epoch: "epoch-a", Revision: 5},
	})
	ready := syncReadyTestMessage(1)
	ready.Cursor.Revision = 5
	connection.writeSyncReady(ready)

	frames := readSyncTestFrames(t, peer, 3)
	if frames[0].Type != "sync.delta" || frames[1].Type != "sync.ready" || frames[2].Type != "sync.watermark" {
		t.Fatalf("frame order = [%s, %s, %s], want delta, ready, watermark", frames[0].Type, frames[1].Type, frames[2].Type)
	}
	if frames[2].Revision != 5 {
		t.Fatalf("watermark revision = %d, want 5", frames[2].Revision)
	}
}

func TestSyncReadyCoalescerFlushesBeforeOtherFrames(t *testing.T) {
	connection, peer := newSyncReadyTestConnection(t, true)
	connection.writeSyncReady(syncReadyTestMessage(1))
	connection.write(serverMessage{Type: "sync.delta", ID: "sync-1", Path: "sync.tasks"})

	frames := readSyncTestFrames(t, peer, 2)
	if frames[0].Type != "sync.ready" || frames[1].Type != "sync.delta" {
		t.Fatalf("frame order = [%s, %s], want [sync.ready, sync.delta]", frames[0].Type, frames[1].Type)
	}
}

func TestSyncReadyCoalescerFlushesOnClose(t *testing.T) {
	connection, peer := newSyncReadyTestConnection(t, true)
	connection.writeSyncReady(syncReadyTestMessage(1))
	connection.close()

	frames := readSyncTestFrames(t, peer, 1)
	if frames[0].Type != "sync.ready" {
		t.Fatalf("close flushed %q, want sync.ready", frames[0].Type)
	}
}

func syncReadyTestMessage(index int) serverMessage {
	truncated := index%2 == 0
	return serverMessage{
		Type: "sync.ready", ID: fmt.Sprintf("sync-%d", index), Path: fmt.Sprintf("sync.table%d", index),
		Cursor: &syncCursor{Epoch: "epoch-a", Revision: uint64(index + 2)}, Mode: "eager",
		Digest: fmt.Sprintf("digest-%d", index), Truncated: &truncated,
	}
}

func newSyncReadyTestConnection(t *testing.T, syncReadyMany bool) (*wsConn, *websocket.Conn) {
	t.Helper()
	serverConn := make(chan *websocket.Conn, 1)
	release := make(chan struct{})
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connection, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		serverConn <- connection
		<-release
		_ = connection.Close()
	}))
	peer, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(httpServer.URL, "http"), nil)
	if err != nil {
		httpServer.Close()
		t.Fatal(err)
	}
	connection := &wsConn{
		conn: peerConnection(t, serverConn), id: "conn-ready-test", project: "project-a", tenant: "tenant-a",
		syncReadyMany: syncReadyMany, subs: map[string]querySubscription{}, syncs: map[string]*syncSubscription{},
	}
	t.Cleanup(func() {
		if connection.server != nil {
			connection.cancelSubscriptions()
		}
		connection.close()
		_ = peer.Close()
		close(release)
		httpServer.Close()
	})
	return connection, peer
}

func peerConnection(t *testing.T, connections <-chan *websocket.Conn) *websocket.Conn {
	t.Helper()
	select {
	case connection := <-connections:
		return connection
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for server websocket")
		return nil
	}
}

func readSyncTestFrames(t *testing.T, connection *websocket.Conn, count int) []serverMessage {
	t.Helper()
	if err := connection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	frames := make([]serverMessage, 0, count)
	for len(frames) < count {
		var frame serverMessage
		if err := connection.ReadJSON(&frame); err != nil {
			t.Fatal(err)
		}
		frames = append(frames, frame)
	}
	return frames
}

func TestSyncDeliveryStormCoalescesToOneRunningAndOnePendingPass(t *testing.T) {
	subscription := &syncSubscription{}
	if !beginSyncDelivery(subscription) {
		t.Fatal("first notification must start delivery")
	}

	for index := 0; index < 10_000; index++ {
		if beginSyncDelivery(subscription) {
			t.Fatalf("notification %d started a duplicate concurrent delivery", index)
		}
	}

	if !finishSyncDelivery(subscription) {
		t.Fatal("coalesced notifications must request exactly one follow-up delivery")
	}
	if finishSyncDelivery(subscription) {
		t.Fatal("follow-up delivery must drain the pending state")
	}

	subscription.mu.Lock()
	running := subscription.deliveryRunning
	pending := subscription.deliveryPending
	subscription.mu.Unlock()
	if running || pending {
		t.Fatalf("delivery state did not drain: running=%t pending=%t", running, pending)
	}
}

func TestSyncAuthRevocationCannotDeadlockDelivery(t *testing.T) {
	runtime := New(config.Config{RequireAuth: true})
	connection := &wsConn{
		server:  runtime,
		project: "project-a",
		tenant:  "tenant-a",
		syncs:   map[string]*syncSubscription{},
		subs:    map[string]querySubscription{},
	}
	subscription := &syncSubscription{
		conn: connection,
		id:   "sync-a",
		path: "tasks.sync",
	}
	connection.syncs[subscription.id] = subscription

	done := make(chan error, 1)
	go func() {
		done <- runtime.deliverSync(subscription)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("sync delivery deadlocked while clearing revoked authentication")
	}

	connection.mu.Lock()
	remaining := len(connection.syncs)
	connection.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("revoked authentication left %d sync subscriptions attached", remaining)
	}
}

func TestFailedSyncOpenCapturesOneAttributedOccurrence(t *testing.T) {
	metrics := newRuntimeMetrics()
	var captured []runtimeLogEntry
	metrics.onFunctionError = func(entry runtimeLogEntry) { captured = append(captured, entry) }
	connection := &wsConn{server: &Server{metrics: metrics}, project: "project-a", tenant: "tenant-a", id: "connection-a", user: &gonvex.User{ID: "user-a"}}
	// A malformed open fails before storage access but still exercises the same
	// deferred reporting used for membership and snapshot failures.
	connection.openSyncWithClock(context.Background(), clientMessage{Path: "sync.tasks", Args: json.RawMessage(`{"password":"secret","workspaceId":"workspace-a"}`)}, "", syncClock{})
	if len(captured) != 1 {
		t.Fatalf("one failed open produced %d captured errors, want 1", len(captured))
	}
	entry := captured[0]
	if entry.Tenant != "tenant-a" || entry.UserID != "user-a" || entry.ConnectionID != "connection-a" || entry.Release == "" || entry.RuntimeInstance == "" {
		t.Fatalf("lost sync error attribution: %+v", entry)
	}
	if strings.Contains(string(entry.Request), "secret") {
		t.Fatal("sync error leaked request secret")
	}
	if calls := metrics.functions["sync.tasks"].Calls; calls != 1 {
		t.Fatalf("function calls = %d, want 1", calls)
	}
	if errors := metrics.functions["sync.tasks"].Errors; errors != 1 {
		t.Fatalf("function errors = %d, want 1", errors)
	}
}

func TestOperationalErrorIncludesRuntimeDeployment(t *testing.T) {
	metrics := newRuntimeMetrics()
	var captured runtimeLogEntry
	metrics.onFunctionError = func(entry runtimeLogEntry) { captured = entry }
	metrics.recordOperationalLog(runtimeLogEntry{Project: "project-a", Kind: "sync", Path: "sync.tasks", Outcome: "error", Error: "clock unavailable"}, time.Now())
	if captured.Release == "" || captured.RuntimeInstance == "" {
		t.Fatalf("operational failure has no deployment context: %+v", captured)
	}
	if len(metrics.functions) != 0 {
		t.Fatal("operational logging must not increment function calls")
	}
}
