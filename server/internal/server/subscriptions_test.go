package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/config"
)

type testResultRowIDProvider []string

func (ids testResultRowIDProvider) GonvexResultRowIDs() []string { return ids }

func TestSubscriptionTokensAreDistinctMapKeys(t *testing.T) {
	tokens := make(map[*subscriptionToken]struct{}, 10_000)
	for range 10_000 {
		tokens[newSubscriptionToken()] = struct{}{}
	}
	if len(tokens) != 10_000 {
		t.Fatalf("distinct subscription tokens = %d, want 10000", len(tokens))
	}
}

func TestResultRowIDsAcceptsConvexStyleIDs(t *testing.T) {
	ids := resultRowIDs([]map[string]any{
		{"id": "postgres-id"},
		{"_id": "convex-id"},
		{"id": "preferred-id", "_id": "fallback-id"},
	})
	for _, id := range []string{"postgres-id", "convex-id", "preferred-id"} {
		if !ids[id] {
			t.Fatalf("missing result row id %q from %#v", id, ids)
		}
	}
	if ids["fallback-id"] {
		t.Fatalf("used _id even though id was available: %#v", ids)
	}
}

func TestResultRowIDsAcceptsPageEnvelopes(t *testing.T) {
	ids := resultRowIDs(map[string]any{
		"rows": []any{
			map[string]any{"id": "task-1"},
			map[string]any{"_id": "task-2"},
		},
	})
	for _, id := range []string{"task-1", "task-2"} {
		if !ids[id] {
			t.Fatalf("missing page result row id %q from %#v", id, ids)
		}
	}
	itemIDs := resultRowIDs(map[string]any{"items": []map[string]any{{"id": "item-1"}}})
	if !itemIDs["item-1"] {
		t.Fatalf("missing items envelope id from %#v", itemIDs)
	}
}

func TestResultRowIDsAcceptsStructuralProvider(t *testing.T) {
	ids := resultRowIDs(testResultRowIDProvider{"task-1", " task-2 ", ""})
	if !ids["task-1"] || !ids["task-2"] || len(ids) != 2 {
		t.Fatalf("structural result row ids = %#v, want task-1 and task-2", ids)
	}
}

func TestWindowedDependencyDoesNotUseOldRowsAfterOrderingChange(t *testing.T) {
	group := &sharedSubscription{
		reads:  []manifest.ReadDependency{{Table: "tasks", Columns: []string{"title"}, OrdersBy: []string{"updatedAt"}, Windowed: true}},
		rowIDs: map[string]bool{"task-visible": true},
	}
	change := tableChange{
		table: "tasks", operation: "update", changedColumns: []string{"updatedAt"}, rowIDs: map[string]bool{"task-outside-window": true},
		details: map[string]tableChangeDetail{"tasks": {
			operation: "update", changedColumns: []string{"updatedAt"}, rowIDs: map[string]bool{"task-outside-window": true}, precise: true,
		}},
	}
	if !group.matches(change) {
		t.Fatal("ordering change outside the old window must rerun because it can enter the result")
	}
}

func TestCallerIDPredicateSelectsOnlyAffectedUser(t *testing.T) {
	detail := tableChangeDetail{precise: true, userIDs: map[string]bool{"user-a": true}}
	callerA := callerContext{user: &gonvex.User{ID: "user-a"}}
	callerB := callerContext{user: &gonvex.User{ID: "user-b"}}
	if !readPredicateMatches("callerIdColumn:userId", nil, callerA, detail, nil) {
		t.Fatal("affected caller did not match committed userId")
	}
	if readPredicateMatches("callerIdColumn:userId", nil, callerB, detail, nil) {
		t.Fatal("unaffected caller matched committed userId")
	}
	if !readPredicateMatches("callerIdColumn:userId", nil, callerB, tableChangeDetail{precise: true}, nil) {
		t.Fatal("missing userId metadata must fail open")
	}
}

func TestColumnArgumentPredicateSelectsOldAndNewWorkspace(t *testing.T) {
	detail := tableChangeDetail{precise: true, workspaceIDs: map[string]bool{"ws-old": true, "ws-new": true}}
	for _, workspace := range []string{"ws-old", "ws-new"} {
		args := json.RawMessage(fmt.Sprintf(`{"workspaceId":%q}`, workspace))
		if !readPredicateMatches("columnArg:workspaceId", args, callerContext{}, detail, nil) {
			t.Fatalf("affected workspace %q did not match", workspace)
		}
	}
	if readPredicateMatches("columnArg:workspaceId", json.RawMessage(`{"workspaceId":"ws-other"}`), callerContext{}, detail, nil) {
		t.Fatal("unaffected workspace matched committed workspace IDs")
	}
	if !readPredicateMatches("columnArg:workspaceId", json.RawMessage(`{"workspaceId":"all"}`), callerContext{}, detail, nil) {
		t.Fatal("all-workspaces query must fail open")
	}
}

func TestResultTaskPredicateDoesNotFanOutAcrossUnrelatedPages(t *testing.T) {
	change := tableChange{
		table: "taskAckReads", operation: "insert", taskIDs: map[string]bool{"task-a": true},
		details: map[string]tableChangeDetail{"taskAckReads": {
			operation: "insert", taskIDs: map[string]bool{"task-a": true}, precise: true,
		}},
	}
	for _, test := range []struct {
		name   string
		rowIDs map[string]bool
		want   bool
	}{
		{name: "affected page", rowIDs: map[string]bool{"task-a": true}, want: true},
		{name: "unrelated page", rowIDs: map[string]bool{"task-b": true}, want: false},
		{name: "empty page", rowIDs: map[string]bool{}, want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			group := &sharedSubscription{
				reads:  []manifest.ReadDependency{{Table: "taskAckReads", Predicate: "resultTaskIds"}},
				rowIDs: test.rowIDs,
			}
			if got := group.matches(change); got != test.want {
				t.Fatalf("matches = %v, want %v", got, test.want)
			}
		})
	}
	group := &sharedSubscription{
		reads:  []manifest.ReadDependency{{Table: "taskAckReads", Predicate: "resultTaskIds"}},
		rowIDs: map[string]bool{"task-b": true},
	}
	missingMetadata := tableChange{
		table: "taskAckReads", operation: "insert",
		details: map[string]tableChangeDetail{"taskAckReads": {operation: "insert", precise: true}},
	}
	if !group.matches(missingMetadata) {
		t.Fatal("missing taskId metadata must fail open")
	}
}

func TestResultTaskOrWorkspacePredicatePreservesMembershipChanges(t *testing.T) {
	group := &sharedSubscription{
		args:   json.RawMessage(`{"workspaceId":"workspace-b"}`),
		reads:  []manifest.ReadDependency{{Table: "taskWorkspaceContexts", Predicate: "resultTaskIdsOrColumnArg:workspaceId"}},
		rowIDs: map[string]bool{"task-a": true},
	}

	contentChange := tableChange{
		table: "taskWorkspaceContexts", operation: "update",
		details: map[string]tableChangeDetail{"taskWorkspaceContexts": {
			operation: "update", taskIDs: map[string]bool{"task-a": true}, workspaceIDs: map[string]bool{"workspace-c": true}, precise: true,
		}},
	}
	if !group.matches(contentChange) {
		t.Fatal("change for a task already in the page must match")
	}

	membershipChange := tableChange{
		table: "taskWorkspaceContexts", operation: "insert",
		details: map[string]tableChangeDetail{"taskWorkspaceContexts": {
			operation: "insert", taskIDs: map[string]bool{"task-new": true}, workspaceIDs: map[string]bool{"workspace-b": true}, precise: true,
		}},
	}
	if !group.matches(membershipChange) {
		t.Fatal("change that can add a task to the subscribed workspace must match")
	}

	unrelatedChange := tableChange{
		table: "taskWorkspaceContexts", operation: "insert",
		details: map[string]tableChangeDetail{"taskWorkspaceContexts": {
			operation: "insert", taskIDs: map[string]bool{"task-new": true}, workspaceIDs: map[string]bool{"workspace-c": true}, precise: true,
		}},
	}
	if group.matches(unrelatedChange) {
		t.Fatal("unrelated task and workspace must not match")
	}
}

func installTestTenantListener(manager *tenantListenerManager, project, tenant string, connected bool) {
	manager.mu.Lock()
	manager.active[tenantListenerKey{project: project, tenant: tenant}] = &tenantListener{
		key: tenantListenerKey{project: project, tenant: tenant}, connected: connected,
	}
	manager.mu.Unlock()
}

func indexedTestGroup(manager *subscriptionManager, key, table string, executions *atomic.Int32) *sharedSubscription {
	token := newSubscriptionToken()
	group := &sharedSubscription{
		manager: manager, key: key, project: "project-a", tenant: "tenant-a", path: key,
		ctx: context.Background(), reads: []manifest.ReadDependency{{Table: table}},
		listeners: map[*subscriptionToken]querySubscription{
			token: {token: token, ctx: context.Background(), caller: callerContext{user: &gonvex.User{ID: "user-a"}}},
		},
	}
	manager.mu.Lock()
	manager.groups[key] = group
	manager.indexGroupLocked(group)
	manager.mu.Unlock()
	return group
}

func TestPreciseTriggerTablesOverrideDeclaredWritesForSubscriptions(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0, SharedResultMaxBytes: 1 << 20})
	manager := server.subscriptions
	installTestTenantListener(manager.listeners, "project-a", "tenant-a", true)
	var taskExecutions atomic.Int32
	var logExecutions atomic.Int32
	manager.execute = func(_ context.Context, group *sharedSubscription, _ querySubscription, _ string, _ float64) (any, error) {
		if group.key == "tasks.list" {
			taskExecutions.Add(1)
		} else {
			logExecutions.Add(1)
		}
		return []map[string]any{{"id": "task-1"}}, nil
	}
	indexedTestGroup(manager, "tasks.list", "tasks", &taskExecutions)
	indexedTestGroup(manager, "taskLogs.list", "task_logs", &logExecutions)

	const commitID = "precise-commit"
	server.scheduleTableChange(tableChange{
		project: "project-a", tenant: "tenant-a", commitID: commitID, broad: true,
		tables: map[string]bool{"tasks": true, "task_logs": true}, changedAtMS: 10,
	})
	server.scheduleTableChange(tableChange{
		project: "project-a", tenant: "tenant-a", commitID: commitID, table: "tasks",
		operation: "update", changedColumns: []string{"title"}, rowIDs: map[string]bool{"task-1": true},
		triggerObserved: true, changedAtMS: 11,
	})

	eventually(t, time.Second, func() bool { return taskExecutions.Load() == 1 })
	time.Sleep(subscriptionRerunCooldown + 25*time.Millisecond)
	if got := logExecutions.Load(); got != 0 {
		t.Fatalf("task_logs executions = %d, want 0 for tasks-only observed commit", got)
	}
	if got := server.metrics.snapshot(manifest.Manifest{}, 0, 0, "").Reactive.SubscriptionsSkippedByTable; got == 0 {
		t.Fatal("subscriptions skipped by table = 0, want the declared-only task_logs dependency counted")
	}
}

func TestCommitBatchKeepsMutationCommitTimestamp(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0, SharedResultMaxBytes: 1 << 20})
	installTestTenantListener(server.subscriptions.listeners, "project-a", "tenant-a", true)
	observed := make(chan float64, 1)
	group := indexedTestGroup(server.subscriptions, "tasks.list", "tasks", nil)
	server.subscriptions.execute = func(_ context.Context, _ *sharedSubscription, _ querySubscription, _ string, changedAtMS float64) (any, error) {
		observed <- changedAtMS
		return []map[string]any{}, nil
	}
	_ = group
	server.scheduleTableChange(tableChange{
		project: "project-a", tenant: "tenant-a", commitID: "commit-a", broad: true,
		tables: map[string]bool{"tasks": true}, changedAtMS: 100,
	})
	server.scheduleTableChange(tableChange{
		project: "project-a", tenant: "tenant-a", commitID: "commit-a", table: "tasks",
		triggerObserved: true, changedAtMS: 125,
	})
	select {
	case got := <-observed:
		if got != 100 {
			t.Fatalf("rerun changedAtMS = %v, want mutation commit timestamp 100", got)
		}
	case <-time.After(time.Second):
		t.Fatal("subscription rerun did not execute")
	}
}

func TestAdjacentTriggerNotificationsForCommitBatchAcrossTables(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0, SharedResultMaxBytes: 1 << 20})
	installTestTenantListener(server.subscriptions.listeners, "project-a", "tenant-a", true)
	var executions atomic.Int32
	group := indexedTestGroup(server.subscriptions, "tasks.list", "tasks", nil)
	group.reads = append(group.reads, manifest.ReadDependency{Table: "taskUsers"})
	server.subscriptions.mu.Lock()
	server.subscriptions.indexGroupLocked(group)
	server.subscriptions.mu.Unlock()
	server.subscriptions.execute = func(context.Context, *sharedSubscription, querySubscription, string, float64) (any, error) {
		executions.Add(1)
		return []map[string]any{{"id": "task-1"}}, nil
	}
	for _, table := range []string{"tasks", "taskUsers"} {
		server.scheduleTableChange(tableChange{
			project: "project-a", tenant: "tenant-a", commitID: "commit-batch", table: table,
			triggerObserved: true, changedAtMS: 100,
		})
	}
	eventually(t, time.Second, func() bool { return executions.Load() == 1 })
	time.Sleep(subscriptionRerunCooldown + 20*time.Millisecond)
	if got := executions.Load(); got != 1 {
		t.Fatalf("executions = %d, want one rerun for adjacent committed notifications", got)
	}
}

func TestUnhealthyListenerFallsBackToDeclaredWrites(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0, SharedResultMaxBytes: 1 << 20})
	manager := server.subscriptions
	installTestTenantListener(manager.listeners, "project-a", "tenant-a", false)
	var executions atomic.Int32
	manager.execute = func(context.Context, *sharedSubscription, querySubscription, string, float64) (any, error) {
		executions.Add(1)
		return []map[string]any{{"id": "task-1"}}, nil
	}
	indexedTestGroup(manager, "tasks.list", "tasks", &executions)
	indexedTestGroup(manager, "taskLogs.list", "task_logs", &executions)

	const commitID = "fallback-commit"
	server.scheduleTableChange(tableChange{
		project: "project-a", tenant: "tenant-a", commitID: commitID, broad: true,
		tables: map[string]bool{"tasks": true, "task_logs": true}, changedAtMS: 10,
	})
	server.scheduleTableChange(tableChange{
		project: "project-a", tenant: "tenant-a", commitID: commitID, table: "tasks",
		operation: "update", triggerObserved: true, changedAtMS: 11,
	})

	eventually(t, time.Second, func() bool { return executions.Load() == 2 })
}

func TestHealthyListenerSuppressesDeclaredOnlyNoOpCommit(t *testing.T) {
	redisServer := miniredis.RunT(t)
	server := New(config.Config{
		TenantListenerLimit:  0,
		SharedResultMaxBytes: 1 << 20,
		ValkeyURL:            "redis://" + redisServer.Addr(),
		RowsCacheTTL:         time.Minute,
	})
	t.Cleanup(func() { _ = server.cache.close() })
	manager := server.subscriptions
	installTestTenantListener(manager.listeners, "project-a", "tenant-a", true)
	var executions atomic.Int32
	manager.execute = func(context.Context, *sharedSubscription, querySubscription, string, float64) (any, error) {
		executions.Add(1)
		return []map[string]any{{"id": "task-1"}}, nil
	}
	indexedTestGroup(manager, "tasks.list", "tasks", &executions)
	ctx := context.Background()
	beforeRows := server.cache.rowsGeneration(ctx, "project-a", "tenant-a", "tasks")
	beforeQueries, ok := server.cache.queryGeneration(ctx, "project-a", "tenant-a", []string{"tasks"})
	if !ok {
		t.Fatal("query cache is not enabled")
	}

	server.scheduleTableChange(tableChange{
		project: "project-a", tenant: "tenant-a", commitID: "no-op-commit",
		broad: true, tables: map[string]bool{"tasks": true}, changedAtMS: 10,
	})
	time.Sleep(tableChangeDebounce + 25*time.Millisecond)
	if got := executions.Load(); got != 0 {
		t.Fatalf("declared-only healthy commit executions = %d, want 0", got)
	}
	afterRows := server.cache.rowsGeneration(ctx, "project-a", "tenant-a", "tasks")
	afterQueries, _ := server.cache.queryGeneration(ctx, "project-a", "tenant-a", []string{"tasks"})
	if afterRows != beforeRows || afterQueries != beforeQueries {
		t.Fatalf("no-op commit invalidated caches: rows %q -> %q, queries %q -> %q", beforeRows, afterRows, beforeQueries, afterQueries)
	}

	// A notification delayed beyond the declared-write debounce still starts a
	// precise run, so suppressing the no-op candidate cannot lose a real write.
	server.scheduleTableChange(tableChange{
		project: "project-a", tenant: "tenant-a", commitID: "no-op-commit", table: "tasks",
		operation: "update", changedColumns: []string{"title"}, rowIDs: map[string]bool{"task-1": true},
		triggerObserved: true, changedAtMS: 11,
	})
	eventually(t, time.Second, func() bool { return executions.Load() == 1 })
	if got := server.cache.rowsGeneration(ctx, "project-a", "tenant-a", "tasks"); got == beforeRows {
		t.Fatal("observed task write did not invalidate the row cache")
	}
}

func TestHealthyListenerInvalidatesOnlyObservedWriteTables(t *testing.T) {
	redisServer := miniredis.RunT(t)
	server := New(config.Config{
		TenantListenerLimit:  0,
		SharedResultMaxBytes: 1 << 20,
		ValkeyURL:            "redis://" + redisServer.Addr(),
		RowsCacheTTL:         time.Minute,
	})
	t.Cleanup(func() { _ = server.cache.close() })
	installTestTenantListener(server.subscriptions.listeners, "project-a", "tenant-a", true)
	ctx := context.Background()
	beforeTasks := server.cache.rowsGeneration(ctx, "project-a", "tenant-a", "tasks")
	beforeLogs := server.cache.rowsGeneration(ctx, "project-a", "tenant-a", "taskLogs")

	const commitID = "actual-write-commit"
	server.scheduleTableChange(tableChange{
		project: "project-a", tenant: "tenant-a", commitID: commitID, broad: true,
		tables: map[string]bool{"tasks": true, "taskLogs": true}, changedAtMS: 10,
	})
	server.scheduleTableChange(tableChange{
		project: "project-a", tenant: "tenant-a", commitID: commitID, table: "tasks",
		operation: "update", triggerObserved: true, changedAtMS: 11,
	})
	time.Sleep(tableChangeDebounce + 25*time.Millisecond)

	if got := server.cache.rowsGeneration(ctx, "project-a", "tenant-a", "tasks"); got == beforeTasks {
		t.Fatal("observed task write did not invalidate tasks")
	}
	if got := server.cache.rowsGeneration(ctx, "project-a", "tenant-a", "taskLogs"); got != beforeLogs {
		t.Fatalf("unobserved declared taskLogs write invalidated cache: %q -> %q", beforeLogs, got)
	}
}

func TestObservedWriteInvalidatesCacheBeforeTriggerBatchFlush(t *testing.T) {
	redisServer := miniredis.RunT(t)
	server := New(config.Config{
		TenantListenerLimit:  0,
		SharedResultMaxBytes: 1 << 20,
		ValkeyURL:            "redis://" + redisServer.Addr(),
		RowsCacheTTL:         time.Minute,
	})
	t.Cleanup(func() { _ = server.cache.close() })
	ctx := context.Background()
	beforeRows := server.cache.rowsGeneration(ctx, "project-a", "tenant-a", "tasks")
	beforeQueries, ok := server.cache.queryGeneration(ctx, "project-a", "tenant-a", []string{"tasks"})
	if !ok {
		t.Fatal("query cache is not enabled")
	}

	const commitID = "immediate-cache-invalidation"
	server.tableChangeMu.Lock()
	scheduled := make(chan struct{})
	go func() {
		server.scheduleTableChange(tableChange{
			project: "project-a", tenant: "tenant-a", commitID: commitID, table: "tasks",
			operation: "update", triggerObserved: true, changedAtMS: 11,
		})
		close(scheduled)
	}()

	// Holding tableChangeMu prevents the batching timer from even being
	// scheduled. The authoritative trigger must still invalidate both cache
	// generations before scheduleTableChange reaches that batching step.
	invalidated := false
	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		afterRows := server.cache.rowsGeneration(ctx, "project-a", "tenant-a", "tasks")
		afterQueries, _ := server.cache.queryGeneration(ctx, "project-a", "tenant-a", []string{"tasks"})
		if afterRows != beforeRows && afterQueries != beforeQueries {
			invalidated = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	server.tableChangeMu.Unlock()
	<-scheduled

	const key = "project-a:tenant-a:commit\x1fimmediate-cache-invalidation"
	server.tableChangeMu.Lock()
	timer := server.tableChangeWait[key]
	if timer != nil {
		timer.Stop()
	}
	delete(server.tableChangeWait, key)
	delete(server.tableChanges, key)
	server.tableChangeMu.Unlock()

	if !invalidated {
		t.Fatal("observed write did not invalidate caches before entering the trigger batching section")
	}
}

func TestHealthyTenantListenerDoesNotSuppressLandlordWrite(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0, SharedResultMaxBytes: 1 << 20})
	if err := server.runtime.SyncManifest(manifest.Manifest{
		Project: "project-a",
		Schema: manifest.Schema{LandlordTables: map[string]manifest.Table{
			"users": {},
		}},
	}); err != nil {
		t.Fatalf("sync manifest: %v", err)
	}
	manager := server.subscriptions
	installTestTenantListener(manager.listeners, "project-a", "tenant-a", true)
	var executions atomic.Int32
	manager.execute = func(context.Context, *sharedSubscription, querySubscription, string, float64) (any, error) {
		executions.Add(1)
		return []map[string]any{{"id": "user-1"}}, nil
	}
	indexedTestGroup(manager, "users.list", "users", &executions)

	server.scheduleTableChange(tableChange{
		project: "project-a", tenant: "tenant-a", commitID: "landlord-commit",
		broad: true, tables: map[string]bool{"users": true}, changedAtMS: 10,
	})
	eventually(t, time.Second, func() bool { return executions.Load() == 1 })
}

func TestLateAdditionalTableForPreciseCommitUsesCommittedSnapshot(t *testing.T) {
	oldCooldown := subscriptionRerunCooldown
	subscriptionRerunCooldown = 10 * time.Millisecond
	t.Cleanup(func() { subscriptionRerunCooldown = oldCooldown })

	server := New(config.Config{TenantListenerLimit: 0, SharedResultMaxBytes: 1 << 20})
	manager := server.subscriptions
	var executions atomic.Int32
	manager.execute = func(context.Context, *sharedSubscription, querySubscription, string, float64) (any, error) {
		executions.Add(1)
		return []map[string]any{{"id": "task-1"}}, nil
	}
	token := newSubscriptionToken()
	group := &sharedSubscription{
		manager: manager, key: "combined.list", project: "project-a", tenant: "tenant-a", path: "combined.list",
		ctx: context.Background(), reads: []manifest.ReadDependency{{Table: "tasks"}, {Table: "task_logs"}},
		listeners: map[*subscriptionToken]querySubscription{
			token: {token: token, ctx: context.Background(), caller: callerContext{user: &gonvex.User{ID: "user-a"}}},
		},
	}
	manager.mu.Lock()
	manager.groups[group.key] = group
	manager.indexGroupLocked(group)
	manager.mu.Unlock()

	const commitID = "late-table-commit"
	manager.requestChange(tableChange{
		project: "project-a", tenant: "tenant-a", commitID: commitID, table: "tasks",
		tables: map[string]bool{"tasks": true}, details: map[string]tableChangeDetail{"tasks": {precise: true, broad: true}},
	})
	eventually(t, time.Second, func() bool { return executions.Load() == 1 })
	manager.requestChange(tableChange{
		project: "project-a", tenant: "tenant-a", commitID: commitID, table: "task_logs",
		tables: map[string]bool{"task_logs": true}, details: map[string]tableChangeDetail{"task_logs": {precise: true, broad: true}},
	})
	time.Sleep(subscriptionRerunCooldown + 25*time.Millisecond)
	if got := executions.Load(); got != 1 {
		t.Fatalf("executions = %d, want 1 because the first post-commit query observes every table in the transaction", got)
	}
}

func TestSubscriptionCountsDoNotTraverseGroups(t *testing.T) {
	blocked := &sharedSubscription{}
	manager := &subscriptionManager{
		groups:        map[string]*sharedSubscription{"blocked": blocked},
		listenerCount: 42,
	}
	blocked.mu.Lock()
	type counts struct{ groups, listeners int }
	done := make(chan counts, 1)
	go func() {
		manager.mu.Lock()
		groups, listeners := manager.countsLocked()
		manager.mu.Unlock()
		done <- counts{groups: groups, listeners: listeners}
	}()

	select {
	case got := <-done:
		blocked.mu.Unlock()
		if got.groups != 1 || got.listeners != 42 {
			t.Fatalf("counts = %+v, want groups=1 listeners=42", got)
		}
	case <-time.After(50 * time.Millisecond):
		blocked.mu.Unlock()
		<-done
		t.Fatal("counting subscriptions blocked on an individual group")
	}
}

func TestSubscriptionRunnerSerializesAndCoalescesBurst(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0, SharedResultMaxBytes: 1 << 20})
	manager := server.subscriptions
	var running atomic.Int32
	var maximum atomic.Int32
	var executions atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	type executionChange struct {
		reason      string
		changedAtMS float64
	}
	changes := make(chan executionChange, 2)
	manager.execute = func(_ context.Context, _ *sharedSubscription, _ querySubscription, reason string, changedAtMS float64) (any, error) {
		changes <- executionChange{reason: reason, changedAtMS: changedAtMS}
		current := running.Add(1)
		for current > maximum.Load() && !maximum.CompareAndSwap(maximum.Load(), current) {
		}
		call := executions.Add(1)
		if call == 1 {
			close(started)
			<-release
		}
		running.Add(-1)
		return []map[string]any{{"id": "task-1", "title": "same"}}, nil
	}
	group := &sharedSubscription{
		manager: manager, project: "project-a", tenant: "tenant-a", path: "tasks.list",
		ctx: context.Background(), listeners: map[*subscriptionToken]querySubscription{},
	}
	token := newSubscriptionToken()
	group.listeners[token] = querySubscription{token: token, ctx: context.Background(), caller: callerContext{user: &gonvex.User{ID: "user-a"}}}

	group.request("initial", 0)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first execution did not start")
	}
	for index := 0; index < 20; index++ {
		group.request("invalidate", float64(index+1))
	}
	close(release)
	eventually(t, time.Second, func() bool {
		group.mu.Lock()
		defer group.mu.Unlock()
		return !group.running && executions.Load() == 2
	})
	if maximum.Load() != 1 {
		t.Fatalf("maximum concurrent executions = %d, want 1", maximum.Load())
	}
	if got := server.metrics.snapshot(manifest.Manifest{}, 0, 0, "").Reactive.RerunsCoalesced; got != 20 {
		t.Fatalf("reruns coalesced = %d, want 20", got)
	}
	first, second := <-changes, <-changes
	if first.reason != "initial" || first.changedAtMS != 0 {
		t.Fatalf("first execution change = %#v", first)
	}
	if second.reason != "invalidate" || second.changedAtMS != 20 {
		t.Fatalf("coalesced execution change = %#v, want latest revision 20", second)
	}
}

func TestSubscriptionCooldownBoundsDistinctCommitBurstAndDeliversFinalState(t *testing.T) {
	oldCooldown := subscriptionRerunCooldown
	subscriptionRerunCooldown = 25 * time.Millisecond
	t.Cleanup(func() { subscriptionRerunCooldown = oldCooldown })

	server := New(config.Config{TenantListenerLimit: 0, SharedResultMaxBytes: 1 << 20})
	manager := server.subscriptions
	var state atomic.Int32
	var executions atomic.Int32
	var delivered atomic.Int32
	manager.execute = func(context.Context, *sharedSubscription, querySubscription, string, float64) (any, error) {
		executions.Add(1)
		value := state.Load()
		delivered.Store(value)
		return map[string]any{"value": value}, nil
	}
	token := newSubscriptionToken()
	group := &sharedSubscription{
		manager: manager, key: "tasks.latest", project: "project-a", tenant: "tenant-a", path: "tasks.latest",
		ctx: context.Background(), reads: []manifest.ReadDependency{{Table: "tasks"}},
		listeners: map[*subscriptionToken]querySubscription{
			token: {token: token, ctx: context.Background(), caller: callerContext{user: &gonvex.User{ID: "user-a"}}},
		},
	}
	group.request("initial", 0)
	eventually(t, time.Second, func() bool {
		group.mu.Lock()
		defer group.mu.Unlock()
		return !group.running && executions.Load() == 1
	})

	for index := 1; index <= 20; index++ {
		state.Store(int32(index))
		group.requestForCommit("invalidate", float64(index), "commit-"+strconv.Itoa(index))
	}
	eventually(t, time.Second, func() bool {
		group.mu.Lock()
		defer group.mu.Unlock()
		return !group.running && delivered.Load() == 20
	})
	if reruns := executions.Load() - 1; reruns > 3 {
		t.Fatalf("executions for 20-commit cooldown burst = %d, want at most 3", reruns)
	}
}

func TestSubscriptionRerunDispatcherBoundsConcurrency(t *testing.T) {
	server := New(config.Config{
		TenantListenerLimit: 0, SharedResultMaxBytes: 1 << 20, SubscriptionRerunConcurrency: 2,
	})
	manager := server.subscriptions
	var running atomic.Int32
	var maximum atomic.Int32
	var executions atomic.Int32
	release := make(chan struct{})
	manager.execute = func(context.Context, *sharedSubscription, querySubscription, string, float64) (any, error) {
		current := running.Add(1)
		for current > maximum.Load() && !maximum.CompareAndSwap(maximum.Load(), current) {
		}
		<-release
		running.Add(-1)
		executions.Add(1)
		return []map[string]any{{"id": "task-1"}}, nil
	}
	groups := make([]*sharedSubscription, 0, 10)
	for index := 0; index < 10; index++ {
		group := indexedTestGroup(manager, "tasks."+strconv.Itoa(index), "tasks", &executions)
		groups = append(groups, group)
		group.request("recover", 1)
	}
	eventually(t, time.Second, func() bool { return running.Load() == 2 })
	eventually(t, time.Second, func() bool {
		return server.metrics.snapshot(manifest.Manifest{}, 0, 0, "").Reactive.SubscriptionRerunQueueDepth == 8
	})
	if got := server.metrics.snapshot(manifest.Manifest{}, 0, 0, "").Reactive.SubscriptionRerunQueueDepth; got != 8 {
		t.Fatalf("rerun queue depth = %d, want 8", got)
	}
	close(release)
	eventually(t, time.Second, func() bool { return executions.Load() == 10 })
	if got := maximum.Load(); got != 2 {
		t.Fatalf("maximum concurrent reruns = %d, want 2", got)
	}
	for _, group := range groups {
		group.mu.Lock()
		running := group.running
		group.mu.Unlock()
		if running {
			t.Fatal("rerun group remained active after dispatcher drained")
		}
	}
	if got := server.metrics.snapshot(manifest.Manifest{}, 0, 0, "").Reactive.SubscriptionRerunQueueDepth; got != 0 {
		t.Fatalf("final rerun queue depth = %d, want 0", got)
	}
}

func TestDeclaredAndPhysicalInvalidationsForCommitExecuteSubscriptionOnce(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0, SharedResultMaxBytes: 1 << 20})
	manager := server.subscriptions
	var executions atomic.Int32
	manager.execute = func(context.Context, *sharedSubscription, querySubscription, string, float64) (any, error) {
		executions.Add(1)
		return []map[string]any{{"id": "task-1", "title": "latest"}}, nil
	}
	groupCtx, groupCancel := context.WithCancel(context.Background())
	token := newSubscriptionToken()
	group := &sharedSubscription{
		manager: manager, key: "tasks-group", project: "project-a", tenant: "tenant-a", path: "tasks.list",
		ctx: groupCtx, cancel: groupCancel, reads: []manifest.ReadDependency{{Table: "tasks"}},
		listeners: map[*subscriptionToken]querySubscription{
			token: {token: token, ctx: context.Background(), caller: callerContext{user: &gonvex.User{ID: "user-a"}}},
		},
	}
	manager.mu.Lock()
	manager.groups[group.key] = group
	manager.indexGroupLocked(group)
	manager.mu.Unlock()

	const commitID = "mutation-one"
	server.scheduleTableChange(tableChange{
		project: "project-a", tenant: "tenant-a", tables: map[string]bool{"tasks": true, "taskLogs": true},
		broad: true, changedAtMS: 10, commitID: commitID,
	})
	server.scheduleTableChange(tableChange{
		project: "project-a", tenant: "tenant-a", table: "tasks", operation: "update",
		changedColumns: []string{"title"}, rowIDs: map[string]bool{"task-1": true}, changedAtMS: 11, commitID: commitID,
	})
	eventually(t, time.Second, func() bool {
		group.mu.Lock()
		defer group.mu.Unlock()
		return !group.running && executions.Load() == 1
	})

	// A delayed physical notification for an already executed commit is still
	// harmless: commit-aware subscription deduplication prevents a second run.
	server.scheduleTableChange(tableChange{
		project: "project-a", tenant: "tenant-a", table: "tasks", operation: "update",
		changedColumns: []string{"title"}, changedAtMS: 12, commitID: commitID,
	})
	time.Sleep(tableChangeDebounce + 25*time.Millisecond)
	if got := executions.Load(); got != 1 {
		t.Fatalf("subscription executions for one declared+physical commit = %d, want 1", got)
	}
	reactive := server.metrics.snapshot(manifest.Manifest{}, 0, 0, "").Reactive
	if reactive.SubscriptionCommitsObserved != 1 || reactive.CommitQueryExecutions != 1 ||
		reactive.ExecutionsPerSubscriptionCommit != 1 || reactive.MaxExecutionsPerSubscriptionCommit != 1 ||
		reactive.DuplicateCommitQueryExecutions != 0 {
		t.Fatalf("commit execution telemetry = %+v, want exactly one execution for one subscription/commit", reactive)
	}
}

func TestRapidCommitsCoalesceToLatestResultAndAdvanceRevision(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0, SharedResultMaxBytes: 1 << 20})
	connection, peer := newSyncReadyTestConnection(t, false)
	connection.server = server
	connection.project = "project-a"
	connection.tenant = "tenant-a"

	manager := server.subscriptions
	var state atomic.Int32
	var executions atomic.Int32
	manager.execute = func(context.Context, *sharedSubscription, querySubscription, string, float64) (any, error) {
		executions.Add(1)
		return map[string]any{"value": state.Load()}, nil
	}
	groupCtx, groupCancel := context.WithCancel(context.Background())
	token := newSubscriptionToken()
	sub := querySubscription{
		conn: connection, id: "query-1", project: "project-a", tenant: "tenant-a", path: "tasks.latest",
		token: token, ctx: context.Background(), caller: callerContext{user: &gonvex.User{ID: "user-a"}},
	}
	connection.mu.Lock()
	connection.subs[sub.id] = sub
	connection.mu.Unlock()
	group := &sharedSubscription{
		manager: manager, key: "latest-group", project: sub.project, tenant: sub.tenant, path: sub.path,
		ctx: groupCtx, cancel: groupCancel, reads: []manifest.ReadDependency{{Table: "tasks"}},
		listeners: map[*subscriptionToken]querySubscription{token: sub},
	}
	manager.mu.Lock()
	manager.groups[group.key] = group
	manager.indexGroupLocked(group)
	manager.mu.Unlock()

	group.request("initial", 0)
	initial := readSyncTestFrames(t, peer, 1)[0]
	if initial.Type != "query.result" || initial.SubscriptionRevision == nil || initial.SubscriptionRevision.Sequence != 1 {
		t.Fatalf("initial query frame = %+v, want result at revision 1", initial)
	}

	state.Store(1)
	manager.requestChange(tableChange{project: "project-a", tenant: "tenant-a", table: "tasks", broad: true, changedAtMS: 20, commitID: "commit-one"})
	state.Store(2)
	manager.requestChange(tableChange{project: "project-a", tenant: "tenant-a", table: "tasks", broad: true, changedAtMS: 21, commitID: "commit-two"})
	eventually(t, time.Second, func() bool {
		group.mu.Lock()
		defer group.mu.Unlock()
		return !group.running && executions.Load() == 2
	})

	latest := readSyncTestFrames(t, peer, 1)[0]
	if latest.Type != "query.result" || latest.SubscriptionRevision == nil || latest.SubscriptionRevision.Sequence != 2 {
		t.Fatalf("coalesced query frame = %+v, want result at revision 2", latest)
	}
	if len(latest.MutationIDs) != 2 || latest.MutationIDs[0] != "commit-one" || latest.MutationIDs[1] != "commit-two" {
		t.Fatalf("coalesced query mutation IDs = %v, want both commits", latest.MutationIDs)
	}
	var payload struct {
		Value int32 `json:"value"`
	}
	encodedResult, err := json.Marshal(latest.Result)
	if err != nil {
		t.Fatalf("encode latest result: %v", err)
	}
	if err := json.Unmarshal(encodedResult, &payload); err != nil {
		t.Fatalf("decode latest result: %v", err)
	}
	if payload.Value != 2 {
		t.Fatalf("coalesced query result value = %d, want final state 2", payload.Value)
	}
	group.mu.Lock()
	revision := group.revision
	group.mu.Unlock()
	if revision != 2 {
		t.Fatalf("subscription revision = %d, want initial + one coalesced rerun = 2", revision)
	}
	reactive := server.metrics.snapshot(manifest.Manifest{}, 0, 0, "").Reactive
	if reactive.SubscriptionCommitsObserved != 2 || reactive.CommitQueryExecutions != 1 ||
		reactive.ExecutionsPerSubscriptionCommit != 0.5 || reactive.MaxExecutionsPerSubscriptionCommit != 1 ||
		reactive.DuplicateCommitQueryExecutions != 0 {
		t.Fatalf("rapid-commit execution telemetry = %+v, want both commits covered by one latest-state execution", reactive)
	}
}

func TestSingleListenerGroupKeepsHashWithoutRetainingResultPayload(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0, SharedResultMaxBytes: 1 << 20})
	token := newSubscriptionToken()
	group := &sharedSubscription{
		manager: server.subscriptions,
		path:    "tasks.list",
		ctx:     context.Background(),
		listeners: map[*subscriptionToken]querySubscription{
			token: {token: token, ctx: context.Background()},
		},
	}
	result := []map[string]any{{"id": "task-1", "title": "same"}}

	group.completeResult(result, "initial", 0, time.Now())
	if !group.hasHash {
		t.Fatal("single-listener group did not retain the result hash")
	}
	if len(group.lastResult) != 0 {
		t.Fatalf("single-listener group retained %d result bytes", len(group.lastResult))
	}

	group.completeResult(result, "invalidate", 0, time.Now())
	if got := server.metrics.snapshot(manifest.Manifest{}, 0, 0, "").Reactive.UnchangedResultsSuppressed; got != 1 {
		t.Fatalf("unchanged results suppressed = %d, want 1", got)
	}
	if group.revision != 1 {
		t.Fatalf("unchanged invalidation advanced revision to %d, want acknowledged revision 1", group.revision)
	}

	group.listeners[newSubscriptionToken()] = querySubscription{ctx: context.Background()}
	group.completeResult([]map[string]any{{"id": "task-1", "title": "changed"}}, "invalidate", 0, time.Now())
	if len(group.lastResult) == 0 {
		t.Fatal("shared group did not retain a replayable result")
	}
}

func TestWindowedSingleListenerRetainsSnapshotForKeyedPatches(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0, SharedResultMaxBytes: 1 << 20})
	token := newSubscriptionToken()
	group := &sharedSubscription{
		manager: server.subscriptions, path: "tasks.window", ctx: context.Background(), retainSnapshot: true,
		listeners: map[*subscriptionToken]querySubscription{
			token: {token: token, ctx: context.Background()},
		},
	}
	before := make([]map[string]any, 10)
	for index := range before {
		before[index] = map[string]any{"id": fmt.Sprintf("task-%d", index), "title": "before", "body": strings.Repeat("x", minimumPatchResultBytes)}
	}
	group.completeResult(before, "initial", 0, time.Now())
	if len(group.lastResult) == 0 {
		t.Fatal("windowed single-listener group did not retain its patch baseline")
	}
	after := append([]map[string]any(nil), before...)
	after[0] = map[string]any{"id": "task-0", "title": "after", "body": strings.Repeat("x", minimumPatchResultBytes)}
	group.completeResult(after, "invalidate", 0, time.Now())
	if got := server.metrics.snapshot(manifest.Manifest{}, 0, 0, "").Reactive.Patches; got != 1 {
		t.Fatalf("patches = %d, want 1", got)
	}
}

func TestVisibilityReconvergencePatchesFromEachPartitionBaseline(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0, SharedResultMaxBytes: 1 << 20})
	firstToken, secondToken := newSubscriptionToken(), newSubscriptionToken()
	first := querySubscription{token: firstToken, ctx: context.Background()}
	second := querySubscription{token: secondToken, ctx: context.Background()}
	group := &sharedSubscription{
		manager: server.subscriptions, path: "tasks.window", ctx: context.Background(), retainSnapshot: true,
		listeners: map[*subscriptionToken]querySubscription{firstToken: first, secondToken: second},
	}
	result := func(title string) *visibilitySharedResult {
		rows := make([]map[string]any, 10)
		for index := range rows {
			rowTitle := "stable"
			if index == 0 {
				rowTitle = title
			}
			rows[index] = map[string]any{"id": fmt.Sprintf("task-%d", index), "title": rowTitle, "body": strings.Repeat("x", minimumPatchResultBytes)}
		}
		payload, err := json.Marshal(rows)
		if err != nil {
			t.Fatal(err)
		}
		hash, perf := queryResultSemantics(payload)
		return &visibilitySharedResult{payload: payload, hash: hash, queryPerf: perf}
	}

	group.completeResult(result("shared"), "initial", 0, time.Now())
	group.completePartitionedResult(&visibilityPartitionedResult{partitions: []visibilityResultPartition{
		{key: "scope-a", listeners: []querySubscription{first}, result: result("first")},
		{key: "scope-b", listeners: []querySubscription{second}, result: result("second")},
	}}, "invalidate", 1, time.Now())
	afterSplit := server.metrics.snapshot(manifest.Manifest{}, 0, 0, "").Reactive
	if afterSplit.Patches != 2 || afterSplit.FullResults != 1 {
		t.Fatalf("split metrics = %+v, want two patches after initial full result", afterSplit)
	}

	group.completeResult(result("shared-again"), "invalidate", 2, time.Now())
	afterConvergence := server.metrics.snapshot(manifest.Manifest{}, 0, 0, "").Reactive
	if afterConvergence.Patches != 4 || afterConvergence.FullResults != 1 {
		t.Fatalf("reconvergence metrics = %+v, want one patch per prior partition and no recovery full result", afterConvergence)
	}
	if len(group.partitionBaselines) != 0 || !group.hasHash || len(group.lastResult) == 0 {
		t.Fatal("reconvergence did not restore the shared baseline")
	}
}

func TestSubscriptionResultSuppressionIgnoresTopLevelPerformanceMetadata(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0, SharedResultMaxBytes: 1 << 20})
	token := newSubscriptionToken()
	group := &sharedSubscription{
		manager: server.subscriptions,
		path:    "bulk.tasksByWorkspace",
		ctx:     context.Background(),
		listeners: map[*subscriptionToken]querySubscription{
			token: {token: token, ctx: context.Background()},
		},
	}
	result := func(title string, durationMS float64) map[string]any {
		return map[string]any{
			"page":  []map[string]any{{"id": "task-1", "title": title}},
			"total": 1,
			"perf": map[string]any{
				"source":                   "tasksSQL",
				"serverFunctionDurationMs": durationMS,
			},
		}
	}

	group.completeResult(result("same", 1.25), "initial", 0, time.Now())
	group.completeResult(result("same", 8.75), "invalidate", 0, time.Now())

	reactive := server.metrics.snapshot(manifest.Manifest{}, 0, 0, "").Reactive
	if reactive.FullResults != 1 || reactive.UnchangedResultsSuppressed != 1 || reactive.ProgressMessages != 0 {
		t.Fatalf("volatile-only rerun metrics = %+v, want one full result followed by a delivery-free suppression", reactive)
	}

	group.completeResult(result("changed", 3.5), "invalidate", 0, time.Now())
	reactive = server.metrics.snapshot(manifest.Manifest{}, 0, 0, "").Reactive
	if reactive.FullResults != 2 || reactive.UnchangedResultsSuppressed != 1 {
		t.Fatalf("semantic-change metrics = %+v, want a second full result", reactive)
	}
}

func TestDependencyIndexSelectsOnlyMatchingTenantTableAndColumns(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0})
	manager := server.subscriptions
	tasks := &sharedSubscription{manager: manager, project: "p", tenant: "a", path: "tasks.list", ctx: context.Background(), listeners: map[*subscriptionToken]querySubscription{}, reads: []manifest.ReadDependency{{Table: "tasks", Columns: []string{"title"}}}}
	users := &sharedSubscription{manager: manager, project: "p", tenant: "a", path: "users.list", ctx: context.Background(), listeners: map[*subscriptionToken]querySubscription{}, reads: []manifest.ReadDependency{{Table: "users"}}}
	otherTenant := &sharedSubscription{manager: manager, project: "p", tenant: "b", path: "tasks.list", ctx: context.Background(), listeners: map[*subscriptionToken]querySubscription{}, reads: []manifest.ReadDependency{{Table: "tasks"}}}
	manager.mu.Lock()
	manager.indexGroupLocked(tasks)
	manager.indexGroupLocked(users)
	manager.indexGroupLocked(otherTenant)
	manager.mu.Unlock()

	manager.requestChange(tableChange{project: "p", tenant: "a", table: "tasks", operation: "update", changedColumns: []string{"description"}})
	if tasks.requested != 0 || users.requested != 0 || otherTenant.requested != 0 {
		t.Fatalf("irrelevant column selected a subscription: tasks=%d users=%d other=%d", tasks.requested, users.requested, otherTenant.requested)
	}
	manager.requestChange(tableChange{project: "p", tenant: "a", table: "tasks", operation: "update", changedColumns: []string{"title"}})
	if tasks.requested != 1 || users.requested != 0 || otherTenant.requested != 0 {
		t.Fatalf("dependency selection mismatch: tasks=%d users=%d other=%d", tasks.requested, users.requested, otherTenant.requested)
	}
}

func TestSubscriptionDependencyIDArgumentSkipsUnrelatedRow(t *testing.T) {
	group := &sharedSubscription{
		args:  json.RawMessage(`{"taskId":"task-1"}`),
		reads: []manifest.ReadDependency{{Table: "tasks", Predicate: "idArg:taskId"}},
	}
	unrelated := tableChange{
		table: "tasks", operation: "insert", rowIDs: map[string]bool{"task-2": true},
		details: map[string]tableChangeDetail{"tasks": {operation: "insert", rowIDs: map[string]bool{"task-2": true}, precise: true}},
	}
	if group.matches(unrelated) {
		t.Fatal("unrelated inserted task matched an idArg dependency")
	}
	related := unrelated
	related.rowIDs = map[string]bool{"task-1": true}
	related.details = map[string]tableChangeDetail{"tasks": {operation: "insert", rowIDs: map[string]bool{"task-1": true}, precise: true}}
	if !group.matches(related) {
		t.Fatal("matching inserted task did not match an idArg dependency")
	}
}

func TestSharedKeyRequiresExplicitPermissionSharing(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0})
	server.runtime.SyncManifest(manifest.Manifest{Project: "p", Functions: map[string]manifest.FunctionEntry{
		"tasks.list": {Kind: manifest.FunctionKindQuery, Dependencies: manifest.FunctionDependencies{Reads: []manifest.ReadDependency{{Table: "tasks"}}, ShareByPermissions: true}},
	}, Schema: manifest.EmptySchema()})
	base := querySubscription{project: "p", tenant: "a", path: "tasks.list", args: json.RawMessage(`{"status":"open"}`), caller: callerContext{user: &gonvex.User{ID: "one"}, permissions: map[string]any{"role": "member"}}}
	other := base
	other.caller.user = &gonvex.User{ID: "two"}
	base.cacheScope = "browser-user-one"
	other.cacheScope = "browser-user-two"
	firstKey, _, _ := server.subscriptions.groupKeyAndDependencies(base)
	secondKey, _, _ := server.subscriptions.groupKeyAndDependencies(other)
	if firstKey != secondKey {
		t.Fatal("same permission scope should share when explicitly enabled")
	}
	other.tenant = "b"
	thirdKey, _, _ := server.subscriptions.groupKeyAndDependencies(other)
	if firstKey == thirdKey {
		t.Fatal("different tenants must never share")
	}
}

func TestSharedKeySeparatesUsersAndBundleVersionsByDefault(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0})
	current := manifest.Manifest{
		Project: "p",
		Functions: map[string]manifest.FunctionEntry{
			"tasks.list": {Kind: manifest.FunctionKindQuery, Dependencies: manifest.FunctionDependencies{Reads: []manifest.ReadDependency{{Table: "tasks"}}}},
		},
		Schema: manifest.EmptySchema(),
		Bundle: &manifest.SourceBundle{Hash: "bundle-a"},
	}
	if err := server.runtime.SyncManifest(current); err != nil {
		t.Fatal(err)
	}
	base := querySubscription{project: "p", tenant: "a", path: "tasks.list", args: json.RawMessage(`{"status":"open"}`), caller: callerContext{user: &gonvex.User{ID: "one"}, permissions: map[string]any{"role": "member"}}}
	otherUser := base
	otherUser.caller.user = &gonvex.User{ID: "two"}
	firstKey, _, _ := server.subscriptions.groupKeyAndDependencies(base)
	secondKey, _, _ := server.subscriptions.groupKeyAndDependencies(otherUser)
	if firstKey == secondKey {
		t.Fatal("different users must not share unless permission-only sharing is explicit")
	}

	current.Bundle.Hash = "bundle-b"
	if err := server.runtime.SyncManifest(current); err != nil {
		t.Fatal(err)
	}
	afterDeploy, _, _ := server.subscriptions.groupKeyAndDependencies(base)
	if firstKey == afterDeploy {
		t.Fatal("bundle deployment must create a distinct shared key")
	}
}

func TestSharedKeySeparatesQueryCacheScopes(t *testing.T) {
	server := New(config.Config{TenantListenerLimit: 0})
	if err := server.runtime.SyncManifest(manifest.Manifest{
		Project: "p",
		Functions: map[string]manifest.FunctionEntry{
			"tasks.list": {
				Kind: manifest.FunctionKindQuery,
				Dependencies: manifest.FunctionDependencies{
					Reads: []manifest.ReadDependency{{Table: "tasks"}},
				},
			},
		},
		Schema: manifest.EmptySchema(),
		Bundle: &manifest.SourceBundle{Hash: "same-bundle"},
	}); err != nil {
		t.Fatal(err)
	}

	first := querySubscription{
		project: "p", tenant: "a", path: "tasks.list",
		args: json.RawMessage(`{"status":"open"}`),
		caller: callerContext{
			user:        &gonvex.User{ID: "one"},
			permissions: map[string]any{"role": "member"},
		},
		cacheScope: "scope-before-manifest-change",
	}
	second := first
	second.cacheScope = "scope-after-manifest-change"

	firstKey, _, _ := server.subscriptions.groupKeyAndDependencies(first)
	secondKey, _, _ := server.subscriptions.groupKeyAndDependencies(second)
	if firstKey == secondKey {
		t.Fatal("different query cache scopes must not share a subscription group")
	}
}

func TestKeyedResultPatch(t *testing.T) {
	patch, ok := keyedResultPatch(
		json.RawMessage(`[{"id":"a","title":"old"},{"id":"b","title":"keep"}]`),
		json.RawMessage(`[{"id":"b","title":"keep"},{"id":"a","title":"new"},{"id":"c","title":"added"}]`),
	)
	if !ok || len(patch.Inserted) != 1 || len(patch.Updated) != 1 || len(patch.Deleted) != 0 {
		t.Fatalf("unexpected patch: %#v", patch)
	}
	if got := patch.Order; len(got) != 3 || got[0] != "b" || got[2] != "c" {
		t.Fatalf("unexpected order: %v", got)
	}
}

func TestKeyedResultPatchSupportsPageEnvelope(t *testing.T) {
	patch, ok := keyedResultPatch(
		json.RawMessage(`{"page":[{"_id":"a","name":"before"}],"total":1,"perf":{"duration":1}}`),
		json.RawMessage(`{"page":[{"_id":"a","name":"after"},{"_id":"b","name":"new"}],"total":2,"perf":{"duration":2}}`),
	)
	if !ok || patch.Type != "query.pagePatch" || len(patch.Inserted) != 1 || len(patch.Updated) != 1 {
		t.Fatalf("unexpected page patch: %#v", patch)
	}
	metadata, ok := patch.Result.(map[string]json.RawMessage)
	if !ok || metadata["page"] != nil || string(metadata["total"]) != "2" {
		t.Fatalf("unexpected page metadata: %#v", patch.Result)
	}
}

func TestKeyedResultPatchOmitsUnchangedOrder(t *testing.T) {
	patch, ok := keyedResultPatch(
		json.RawMessage(`[{"id":"a","title":"before"},{"id":"b","title":"keep"}]`),
		json.RawMessage(`[{"id":"a","title":"after"},{"id":"b","title":"keep"}]`),
	)
	if !ok || len(patch.Updated) != 1 {
		t.Fatalf("unexpected patch: %#v", patch)
	}
	if patch.Order != nil {
		t.Fatalf("unchanged order should be omitted, got %v", patch.Order)
	}
}

func TestKeyedResultPatchCompactsPrependOrder(t *testing.T) {
	patch, ok := keyedResultPatch(
		json.RawMessage(`[{"id":"b"},{"id":"a"}]`),
		json.RawMessage(`[{"id":"c"},{"id":"b"},{"id":"a"}]`),
	)
	if !ok || len(patch.Prepend) != 1 || patch.Prepend[0] != "c" || patch.Order != nil {
		t.Fatalf("unexpected prepend patch: %#v", patch)
	}
}

func TestKeyedResultPatchSupportsObjectCollections(t *testing.T) {
	patch, ok := keyedResultPatch(
		json.RawMessage(`{"taskUsers":[{"id":"u1","taskId":"a"}],"taskTags":[],"taskCustomFieldValues":[]}`),
		json.RawMessage(`{"taskUsers":[{"id":"u1","taskId":"b"},{"id":"u2","taskId":"c"}],"taskTags":[],"taskCustomFieldValues":[]}`),
	)
	if !ok || patch.Type != "query.objectPatch" || len(patch.Collections) != 1 {
		t.Fatalf("unexpected object patch: %#v", patch)
	}
	users := patch.Collections["taskUsers"]
	if len(users.Inserted) != 1 || len(users.Updated) != 1 || len(users.Append) != 1 || users.Append[0] != "u2" {
		t.Fatalf("unexpected taskUsers patch: %#v", users)
	}
}

func TestKeyedResultPatchRejectsChangedObjectMetadata(t *testing.T) {
	_, ok := keyedResultPatch(
		json.RawMessage(`{"rows":[],"total":1}`),
		json.RawMessage(`{"rows":[],"total":2}`),
	)
	if ok {
		t.Fatal("changed scalar metadata must fall back to a full result")
	}
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not satisfied")
}

// An invalidation that arrives while a shard has no listeners (the detach
// grace window) cannot rerun the query — but the retained snapshot now
// predates that commit. A listener joining before the grace expires used to be
// served that stale snapshot as its first (and, for one-shot consumers like
// the legacy bridge, only) result: a mutation committed between one client
// closing and the next opening was invisible. The join must instead see no
// servable snapshot until a fresh execution completes.
func TestGraceIdleInvalidationMarksRetainedSnapshotUnservable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	group := &sharedSubscription{
		ctx:        ctx,
		cancel:     cancel,
		listeners:  map[*subscriptionToken]querySubscription{},
		lastResult: json.RawMessage(`[{"id":"stale"}]`),
	}
	group.requested++
	group.pendingReason = "invalidate"
	group.running = true
	group.run()

	group.mu.Lock()
	stale := group.staleWhileIdle
	servable := len(group.lastResult) > 0 && !group.staleWhileIdle
	group.mu.Unlock()
	if !stale {
		t.Fatal("an invalidation with no listeners must mark the retained snapshot stale")
	}
	if servable {
		t.Fatal("a stale snapshot must not be servable to a joining listener")
	}
}
