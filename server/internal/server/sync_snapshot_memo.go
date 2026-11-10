package server

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"sync"

	"github.com/gonvex/gonvex/pkg/manifest"
)

// A sync delivery marshals the handler result, parses every row back, then
// canonicalizes and hashes each row. Apps that share one result across many
// subscribers (ShareByVisibility / snapshot-shared handlers) hand every
// subscriber the same immutable slice, so that analysis is identical for all
// of them; at 55 users it was ~40% of runtime CPU. Memoize it per result
// object. The memo holds a reference to the result, so its backing array
// cannot be reused for a different value while the entry exists.
type syncSnapshotAnalysis struct {
	result    any
	rows      []json.RawMessage
	truncated bool
	byKey     map[string]json.RawMessage // read-only
	hashes    map[string]string          // read-only; clone before storing on a subscription
	digest    string
}

type syncSnapshotMemoKey struct {
	pointer  uintptr
	length   int
	key      string
	maxRows  int
	maxBytes int64
}

const syncSnapshotMemoMax = 32

var syncSnapshotMemo = struct {
	sync.Mutex
	entries map[syncSnapshotMemoKey]*syncSnapshotAnalysis
	order   []syncSnapshotMemoKey
}{entries: map[syncSnapshotMemoKey]*syncSnapshotAnalysis{}}

func analyzeSyncSnapshotUncached(result any, definition manifest.SyncDefinition) (*syncSnapshotAnalysis, error) {
	return analyzeSyncSnapshotWith(result, definition, false)
}

func analyzeSyncSnapshot(result any, definition manifest.SyncDefinition) (*syncSnapshotAnalysis, error) {
	return analyzeSyncSnapshotWith(result, definition, true)
}

func analyzeSyncSnapshotWith(result any, definition manifest.SyncDefinition, memo bool) (analysis *syncSnapshotAnalysis, err error) {
	memoKey, memoizable := syncSnapshotIdentity(result, definition)
	memoizable = memo && memoizable
	if memoizable {
		syncSnapshotMemo.Lock()
		cached := syncSnapshotMemo.entries[memoKey]
		syncSnapshotMemo.Unlock()
		if cached != nil {
			return cached, nil
		}
	}
	analysis, err = computeSyncSnapshotAnalysis(result, definition)
	if err != nil {
		return nil, err
	}
	if memoizable {
		syncSnapshotMemo.Lock()
		if _, exists := syncSnapshotMemo.entries[memoKey]; !exists {
			syncSnapshotMemo.order = append(syncSnapshotMemo.order, memoKey)
		}
		syncSnapshotMemo.entries[memoKey] = analysis
		for len(syncSnapshotMemo.order) > syncSnapshotMemoMax {
			delete(syncSnapshotMemo.entries, syncSnapshotMemo.order[0])
			syncSnapshotMemo.order = syncSnapshotMemo.order[1:]
		}
		syncSnapshotMemo.Unlock()
	}
	return analysis, nil
}

// syncSnapshotIdentity identifies a non-empty slice result by its backing
// array. Only slices qualify: a handler that builds a fresh slice per call
// never hits, so unshared handlers keep their previous behavior.
func syncSnapshotIdentity(result any, definition manifest.SyncDefinition) (syncSnapshotMemoKey, bool) {
	value := reflect.ValueOf(result)
	if !value.IsValid() || value.Kind() != reflect.Slice || value.Len() == 0 {
		return syncSnapshotMemoKey{}, false
	}
	return syncSnapshotMemoKey{
		pointer: value.Pointer(), length: value.Len(), key: definition.Key,
		maxRows: definition.MaxRows, maxBytes: int64(definition.MaxBytes),
	}, true
}

func cloneSyncKeys(source map[string]json.RawMessage) map[string]bool {
	keys := make(map[string]bool, len(source))
	for key := range source {
		keys[key] = true
	}
	return keys
}

func cloneSyncHashes(source map[string]string) map[string]string {
	return maps.Clone(source)
}

// syncResultIsShared reports whether the sync function declares a shared
// result contract (ShareByVisibility or ShareResultFrom): its handler may hand
// the same immutable result to many subscribers.
func (s *Server) syncResultIsShared(project, path string) bool {
	if s == nil || s.runtime == nil {
		return false
	}
	entry, ok := s.runtime.ManifestForProject(project).Functions[path]
	if !ok {
		return false
	}
	return strings.TrimSpace(entry.Dependencies.ShareByVisibility) != "" || strings.TrimSpace(entry.Dependencies.ShareResultFrom) != ""
}

// computeSyncSnapshotAnalysis is syncSnapshotRows + syncRowKey + syncRowsHashes
// in one pass: each row is decoded once and canonically encoded once (the
// separate helpers decoded every row five times). Errors, truncation, keys and
// hashes are identical to those helpers.
func computeSyncSnapshotAnalysis(result any, definition manifest.SyncDefinition) (*syncSnapshotAnalysis, error) {
	payload, err := json.Marshal(explicitNull(result))
	if err != nil {
		return nil, err
	}
	var rows []json.RawMessage
	if err := json.Unmarshal(payload, &rows); err != nil {
		return nil, fmt.Errorf("sync handler must return an array of keyed rows")
	}
	analysis := &syncSnapshotAnalysis{
		result: result, rows: make([]json.RawMessage, 0, len(rows)),
		byKey: make(map[string]json.RawMessage, len(rows)), hashes: make(map[string]string, len(rows)),
	}
	totalBytes := int64(0)
	for _, row := range rows {
		var decoded any
		decodeErr := json.Unmarshal(row, &decoded)
		key := ""
		if object, ok := decoded.(map[string]any); decodeErr == nil && ok {
			key = syncDecodedKey(object[definition.Key])
		}
		if key == "" {
			return nil, fmt.Errorf("sync row is missing key %q", definition.Key)
		}
		if _, seen := analysis.byKey[key]; seen {
			return nil, fmt.Errorf("sync handler returned duplicate key %q", key)
		}
		if definition.MaxRows > 0 && len(analysis.rows) >= definition.MaxRows {
			analysis.truncated = true
			break
		}
		canonical := encodeCanonicalSyncValue(decoded, row)
		if definition.MaxBytes > 0 && totalBytes+int64(len(canonical)) > definition.MaxBytes {
			analysis.truncated = true
			break
		}
		totalBytes += int64(len(canonical))
		analysis.rows = append(analysis.rows, row)
		analysis.byKey[key] = row
		sum := sha256.Sum256(canonical)
		analysis.hashes[key] = hex.EncodeToString(sum[:])
	}
	analysis.digest = syncHashesDigest(analysis.hashes)
	return analysis, nil
}

// syncDecodedKey mirrors syncRowKey for an already decoded key value.
func syncDecodedKey(value any) string {
	switch current := value.(type) {
	case string:
		return current
	case float64, bool:
		return string(encodeCanonicalSyncValue(current, nil))
	}
	return ""
}

// encodeCanonicalSyncValue is canonicalSyncJSON for an already decoded value
// (fallback is returned if encoding fails, as canonicalSyncJSON does).
func encodeCanonicalSyncValue(decoded any, fallback json.RawMessage) []byte {
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(normalizeSyncJSON(decoded)); err != nil {
		return fallback
	}
	return bytes.TrimSpace(output.Bytes())
}
