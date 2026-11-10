package server

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/gonvex/gonvex/pkg/manifest"
)

func referenceSyncAnalysis(t *testing.T, result any, definition manifest.SyncDefinition) (*syncSnapshotAnalysis, error) {
	t.Helper()
	rows, truncated, err := syncSnapshotRows(result, definition)
	if err != nil {
		return nil, err
	}
	byKey := map[string]json.RawMessage{}
	for _, row := range rows {
		if key := syncRowKey(row, definition.Key); key != "" {
			byKey[key] = row
		}
	}
	hashes := syncRowsHashes(rows, definition.Key)
	return &syncSnapshotAnalysis{rows: rows, truncated: truncated, byKey: byKey, hashes: hashes, digest: syncHashesDigest(hashes)}, nil
}

func TestComputeSyncSnapshotAnalysisMatchesReferenceHelpers(t *testing.T) {
	rows := []map[string]any{
		{"_id": "b", "name": "<b>&</b>", "n": -0.0, "nested": map[string]any{"z": 1, "a": []any{2.5, "x y"}}},
		{"_id": "a", "count": 12345678901234, "flag": true, "nil": nil},
		{"_id": "c", "emoji": "✓ ñ", "list": []any{map[string]any{"k": 1e21}}},
	}
	numeric := []map[string]any{{"id": 7.0, "v": "x"}, {"id": 8.5, "v": "y"}}
	cases := []struct {
		name       string
		result     any
		definition manifest.SyncDefinition
	}{
		{"string keys", rows, manifest.SyncDefinition{Key: "_id"}},
		{"numeric keys", numeric, manifest.SyncDefinition{Key: "id"}},
		{"row budget", rows, manifest.SyncDefinition{Key: "_id", MaxRows: 2}},
		{"byte budget", rows, manifest.SyncDefinition{Key: "_id", MaxBytes: 120}},
		{"empty", []map[string]any{}, manifest.SyncDefinition{Key: "_id"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want, wantErr := referenceSyncAnalysis(t, tc.result, tc.definition)
			got, gotErr := computeSyncSnapshotAnalysis(tc.result, tc.definition)
			if (wantErr == nil) != (gotErr == nil) {
				t.Fatalf("errors differ: got %v want %v", gotErr, wantErr)
			}
			if wantErr != nil {
				return
			}
			if got.truncated != want.truncated || got.digest != want.digest ||
				!reflect.DeepEqual(got.hashes, want.hashes) || !reflect.DeepEqual(got.byKey, want.byKey) ||
				len(got.rows) != len(want.rows) {
				t.Fatalf("analysis differs:\n got %+v\nwant %+v", got, want)
			}
		})
	}
	for _, bad := range []any{
		[]map[string]any{{"name": "no key"}},
		[]map[string]any{{"_id": "dup"}, {"_id": "dup"}},
		map[string]any{"not": "an array"},
	} {
		_, wantErr := referenceSyncAnalysis(t, bad, manifest.SyncDefinition{Key: "_id"})
		_, gotErr := computeSyncSnapshotAnalysis(bad, manifest.SyncDefinition{Key: "_id"})
		if (wantErr == nil) != (gotErr == nil) {
			t.Fatalf("error mismatch for %v: got %v want %v", bad, gotErr, wantErr)
		}
	}
}

func TestSharedSyncSnapshotMemoReusesAnalysisForTheSameResult(t *testing.T) {
	rows := []map[string]any{{"_id": "memo-1"}, {"_id": "memo-2"}}
	definition := manifest.SyncDefinition{Key: "_id"}
	first, err := analyzeSyncSnapshot(rows, definition)
	if err != nil {
		t.Fatal(err)
	}
	second, err := analyzeSyncSnapshot(rows, definition)
	if err != nil || first != second {
		t.Fatalf("same shared result was analyzed twice (err=%v)", err)
	}
	other := []map[string]any{{"_id": "memo-1"}, {"_id": "memo-2"}}
	third, err := analyzeSyncSnapshot(other, definition)
	if err != nil || third == first {
		t.Fatalf("a different result object reused another result's analysis (err=%v)", err)
	}
	if uncached, _ := analyzeSyncSnapshotUncached(rows, definition); uncached == first {
		t.Fatal("unshared sync paths must not use the memo")
	}
}
