package server

import (
	"encoding/json"
	"testing"

	"github.com/gonvex/gonvex/pkg/manifest"
)

func TestScopedSyncChangesDropsOnlyProvablyUnrelatedRows(t *testing.T) {
	definition := manifest.SyncDefinition{
		Table:        "tasks",
		EqualFilters: map[string]string{"workspaceId": "workspaceId"},
		VisibilityPredicates: map[string]string{
			"taskViews":             "resultTaskIds",
			"taskUsers":             "resultTaskIdsOrColumnArg:workspaceId",
			"taskWorkspaceContexts": "resultTaskIdsOrColumnArg:workspaceId",
		},
	}
	args := map[string]json.RawMessage{"workspaceId": json.RawMessage(`"ws-a"`)}
	visible := map[string]bool{"task-a1": true}
	j := func(s string) json.RawMessage { return json.RawMessage(s) }
	cases := []struct {
		name   string
		change syncLogChange
		keep   bool
	}{
		{"other workspace task update", syncLogChange{table: "tasks", rowID: "task-b1", operation: "update", oldValue: j(`{"workspaceId":"ws-b"}`), newValue: j(`{"workspaceId":"ws-b"}`)}, false},
		{"other workspace task insert", syncLogChange{table: "tasks", rowID: "task-b2", operation: "insert", newValue: j(`{"workspaceId":"ws-b"}`)}, false},
		{"task moved into this workspace", syncLogChange{table: "tasks", rowID: "task-b1", operation: "update", oldValue: j(`{"workspaceId":"ws-b"}`), newValue: j(`{"workspaceId":"ws-a"}`)}, true},
		{"task moved out of this workspace", syncLogChange{table: "tasks", rowID: "task-x", operation: "update", oldValue: j(`{"workspaceId":"ws-a"}`), newValue: j(`{"workspaceId":"ws-b"}`)}, true},
		{"visible task (overlay) in other workspace", syncLogChange{table: "tasks", rowID: "task-a1", operation: "update", oldValue: j(`{"workspaceId":"ws-b"}`), newValue: j(`{"workspaceId":"ws-b"}`)}, true},
		{"insert into this workspace beyond window", syncLogChange{table: "tasks", rowID: "task-new", operation: "insert", newValue: j(`{"workspaceId":"ws-a"}`)}, true},
		{"task row without workspace column", syncLogChange{table: "tasks", rowID: "task-q", operation: "update", oldValue: j(`{"name":"x"}`), newValue: j(`{"name":"y"}`)}, true},
		{"unparsable task row", syncLogChange{table: "tasks", rowID: "task-q", operation: "update", newValue: j(`[1]`)}, true},
		{"view of an unseen task", syncLogChange{table: "taskViews", rowID: "v1", operation: "insert", newValue: j(`{"taskId":"task-b1"}`)}, false},
		{"view of a visible task", syncLogChange{table: "taskViews", rowID: "v2", operation: "insert", newValue: j(`{"taskId":"task-a1"}`)}, true},
		{"view without taskId", syncLogChange{table: "taskViews", rowID: "v3", operation: "insert", newValue: j(`{"userId":"u"}`)}, true},
		{"assignment in other workspace", syncLogChange{table: "taskUsers", rowID: "tu1", operation: "insert", newValue: j(`{"taskId":"task-b1","workspaceId":"ws-b"}`)}, false},
		{"assignment in this workspace for unseen task", syncLogChange{table: "taskUsers", rowID: "tu2", operation: "insert", newValue: j(`{"taskId":"task-new","workspaceId":"ws-a"}`)}, true},
		{"assignment with null workspace", syncLogChange{table: "taskUsers", rowID: "tu3", operation: "insert", newValue: j(`{"taskId":"task-b1","workspaceId":null}`)}, true},
		{"assignment without workspace column", syncLogChange{table: "taskUsers", rowID: "tu4", operation: "insert", newValue: j(`{"taskId":"task-b1"}`)}, true},
		{"context into this workspace", syncLogChange{table: "taskWorkspaceContexts", rowID: "c1", operation: "insert", newValue: j(`{"taskId":"task-b1","workspaceId":"ws-a"}`)}, true},
		{"undeclared visibility table", syncLogChange{table: "userTeams", rowID: "ut", operation: "insert", newValue: j(`{"userId":"u"}`)}, true},
	}
	for _, tc := range cases {
		got := len(scopedSyncChanges(definition, args, visible, []syncLogChange{tc.change})) == 1
		if got != tc.keep {
			t.Errorf("%s: keep=%v, want %v", tc.name, got, tc.keep)
		}
	}
}

func TestScopedSyncChangesWithoutEqualFiltersKeepsSourceChanges(t *testing.T) {
	definition := manifest.SyncDefinition{Table: "statuses"}
	change := syncLogChange{table: "statuses", rowID: "s", operation: "update", newValue: json.RawMessage(`{"name":"x"}`)}
	if len(scopedSyncChanges(definition, nil, map[string]bool{}, []syncLogChange{change})) != 1 {
		t.Fatal("a sync without EqualFilters must keep every source change")
	}
}

func TestSyncVisibilityPredicatesConflictingReadsStayUnscoped(t *testing.T) {
	predicates := syncVisibilityPredicates("tasks", []manifest.ReadDependency{
		{Table: "tasks", Predicate: "columnArg:workspaceId"},
		{Table: "taskViews", Predicate: "resultTaskIds"},
		{Table: "taskTags", Predicate: "resultTaskIds"},
		{Table: "taskTags"},
		{Table: "taskUsers", Predicate: "resultTaskIds"},
		{Table: "taskUsers", Predicate: "resultTaskIdsOrColumnArg:workspaceId"},
	})
	if predicates["taskViews"] != "resultTaskIds" || len(predicates) != 1 {
		t.Fatalf("predicates = %v", predicates)
	}
}
