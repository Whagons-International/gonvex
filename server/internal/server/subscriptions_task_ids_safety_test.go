package server

import (
	"encoding/json"
	"testing"

	"github.com/gonvex/gonvex/pkg/manifest"
)

func TestTaskIDsArgConservativeMetadata(t *testing.T) {
	precise := tableChangeDetail{precise: true, operation: "update", rowIDs: map[string]bool{"unlisted": true}, changedColumns: []string{"spotId"}}
	broad := precise
	broad.broad = true
	merged := mergeTableChangeDetail(precise, tableChangeDetail{broad: true})
	for _, test := range []struct {
		name, table string
		detail      tableChangeDetail
		want        bool
	}{
		{"child row ids are not task ids", "taskTags", precise, true},
		{"broad task retains unrelated ids", "tasks", broad, true},
		{"merged broad task retains precise ids", "tasks", merged, true},
		{"unlisted task insert", "tasks", tableChangeDetail{precise: true, operation: "insert", rowIDs: map[string]bool{"unlisted": true}}, false},
		{"listed task delete", "tasks", tableChangeDetail{precise: true, operation: "delete", rowIDs: map[string]bool{"listed": true}}, true},
		{"child reparent keeps old and new task ids", "taskTags", tableChangeDetail{precise: true, operation: "update", rowIDs: map[string]bool{"pivot": true}, taskIDs: map[string]bool{"listed": true, "unlisted": true}}, true},
		{"known unrelated child", "taskTags", tableChangeDetail{precise: true, operation: "insert", rowIDs: map[string]bool{"pivot": true}, taskIDs: map[string]bool{"unlisted": true}}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			group := &sharedSubscription{args: json.RawMessage(`{"taskId":"listed"}`), reads: []manifest.ReadDependency{{Table: test.table, Predicate: "taskIdsArg:taskId"}}}
			change := tableChange{table: test.table, details: map[string]tableChangeDetail{test.table: test.detail}}
			if got := group.matches(change); got != test.want {
				t.Fatalf("matches=%v, want %v", got, test.want)
			}
		})
	}
}
