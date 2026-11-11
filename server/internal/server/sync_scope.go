package server

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gonvex/gonvex/pkg/manifest"
)

// scopedSyncChanges removes logged changes that cannot affect one verified
// subscription. Without it a progressive or budgeted sync re-runs its handler
// for every subscriber after any change to its source or visibility tables,
// e.g. every open workspace window after a write in another workspace.
//
// A change is dropped only when that is provable from the change log and the
// subscription's authoritative visible keys:
//
//   - source table: the row is not visible to this subscription, and neither
//     its old nor its new value satisfies the sync's EqualFilters.
//   - a dependency table whose Reads declare resultTaskIds: the change's
//     taskId (old and new) is not visible to this subscription.
//   - resultTaskIdsOrColumnArg:<col> / columnArg:<col>: additionally the
//     change's <col> (old and new) differs from the subscription argument.
//
// Missing columns, unparsable values, absent arguments, or undeclared
// predicates keep the change (the conservative full reconcile).
func scopedSyncChanges(definition manifest.SyncDefinition, args map[string]json.RawMessage, visibleKeys map[string]bool, changes []syncLogChange) []syncLogChange {
	if len(changes) == 0 {
		return changes
	}
	kept := changes[:0:0]
	for _, change := range changes {
		if syncChangeOutOfScope(definition, args, visibleKeys, change) {
			syncStat("drop."+change.table, 1)
			continue
		}
		syncStat("keep."+change.table, 1)
		kept = append(kept, change)
	}
	return kept
}

func syncChangeOutOfScope(definition manifest.SyncDefinition, args map[string]json.RawMessage, visibleKeys map[string]bool, change syncLogChange) bool {
	values := [][]byte{change.oldValue, change.newValue}
	if change.table == definition.Table {
		if len(definition.EqualFilters) == 0 || visibleKeys[change.rowID] {
			return false
		}
		for _, value := range values {
			row, present := decodeSyncRow(value)
			if !present {
				continue
			}
			if row == nil {
				return false
			}
			for column, argument := range definition.EqualFilters {
				if columnMatchesArg(row, column, args, argument) {
					return false
				}
			}
		}
		return true
	}
	predicate := definition.VisibilityPredicates[change.table]
	taskScoped := false
	column := ""
	switch {
	case predicate == "resultTaskIds":
		taskScoped = true
	case strings.HasPrefix(predicate, "resultTaskIdsOrColumnArg:"):
		taskScoped = true
		column = strings.TrimSpace(strings.TrimPrefix(predicate, "resultTaskIdsOrColumnArg:"))
	case strings.HasPrefix(predicate, "columnArg:"):
		column = strings.TrimSpace(strings.TrimPrefix(predicate, "columnArg:"))
	default:
		return false
	}
	if column != "" {
		if _, ok := args[column]; !ok {
			return false
		}
	}
	sawRow := false
	for _, value := range values {
		row, present := decodeSyncRow(value)
		if !present {
			continue
		}
		if row == nil {
			return false
		}
		sawRow = true
		if taskScoped {
			taskID, ok := rowString(row, "taskId")
			if !ok || visibleKeys[taskID] {
				return false
			}
		}
		if column != "" && columnMatchesArg(row, column, args, column) {
			return false
		}
	}
	return sawRow
}

// syncPredicateColumns lists the logged columns a declared predicate reads.
func syncPredicateColumns(predicate string) []string {
	predicate = strings.TrimSpace(predicate)
	switch {
	case predicate == "resultTaskIds":
		return []string{"taskId"}
	case strings.HasPrefix(predicate, "resultTaskIdsOrColumnArg:"):
		return []string{"taskId", strings.TrimSpace(strings.TrimPrefix(predicate, "resultTaskIdsOrColumnArg:"))}
	case strings.HasPrefix(predicate, "columnArg:"):
		return []string{strings.TrimSpace(strings.TrimPrefix(predicate, "columnArg:"))}
	}
	return nil
}

// decodeSyncRow returns (nil, true) for a value that is present but not a
// JSON object, and (nil, false) for an absent/null side of an insert/delete.
func decodeSyncRow(value []byte) (map[string]json.RawMessage, bool) {
	trimmed := bytes.TrimSpace(value)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, false
	}
	row := map[string]json.RawMessage{}
	if json.Unmarshal(trimmed, &row) != nil {
		return nil, true
	}
	return row, true
}

func rowString(row map[string]json.RawMessage, column string) (string, bool) {
	raw, ok := row[column]
	if !ok {
		return "", false
	}
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return "", false
	}
	return value, true
}

// columnMatchesArg is true when the row's column equals the argument, or when
// equality cannot be ruled out (column missing or null in the logged row).
func columnMatchesArg(row map[string]json.RawMessage, column string, args map[string]json.RawMessage, argument string) bool {
	left, ok := row[column]
	if !ok || bytes.Equal(bytes.TrimSpace(left), []byte("null")) {
		// Missing or null (e.g. an unbackfilled denormalized column): unknown.
		return true
	}
	right, ok := args[argument]
	if !ok {
		return true
	}
	return jsonScalarEqual(left, right)
}

// Lab-only delivery accounting (GONVEX_SYNC_STATS=1): why sync deliveries
// re-ran their handler, and which changes the scope check kept or dropped.
var syncStats = struct {
	sync.Mutex
	counts map[string]int
	once   sync.Once
}{counts: map[string]int{}}

func syncStat(key string, n int) {
	if os.Getenv("GONVEX_SYNC_STATS") != "1" || n == 0 {
		return
	}
	syncStats.once.Do(func() {
		go func() {
			for range time.Tick(10 * time.Second) {
				syncStats.Lock()
				keys := make([]string, 0, len(syncStats.counts))
				for k := range syncStats.counts {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				parts := make([]any, 0, 2*len(keys))
				for _, k := range keys {
					parts = append(parts, k, syncStats.counts[k])
				}
				syncStats.counts = map[string]int{}
				syncStats.Unlock()
				if len(parts) > 0 {
					slog.Info("sync stats (10s)", parts...)
				}
			}
		}()
	})
	syncStats.Lock()
	syncStats.counts[key] += n
	syncStats.Unlock()
}
