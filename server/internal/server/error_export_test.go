package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/gonvex/gonvex/server/internal/config"
)

func TestErrorExportPaginatesEveryGroup(t *testing.T) {
	s := New(config.Config{})
	for i := 0; i < 503; i++ {
		s.errorTracker.capture(capturedError{Project: "project-a", EventID: fmt.Sprint(i), Message: "failure", Culprit: fmt.Sprintf("function_%04x", i)})
	}
	assertCompleteErrorExport(t, s, "project-a")
}

func assertCompleteErrorExport(t *testing.T, s *Server, project string) {
	t.Helper()
	seen := map[string]bool{}
	cursor := ""
	for page := 0; page < 2; page++ {
		r := httptest.NewRequest(http.MethodGet, "/dev/errors/groups?project="+project+"&export=1&cursor="+cursor, nil)
		w := httptest.NewRecorder()
		s.handleErrorGroups(w, r)
		var result struct {
			Groups     []*errorGroup `json:"groups"`
			NextCursor string        `json:"nextCursor"`
		}
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil {
			t.Fatalf("export failed: %s", w.Body.String())
		}
		want := 500
		if page == 1 {
			want = 3
		}
		if len(result.Groups) != want {
			t.Fatalf("page %d has %d groups, want %d", page, len(result.Groups), want)
		}
		for _, g := range result.Groups {
			if seen[g.Fingerprint] {
				t.Fatal("duplicate exported group")
			}
			seen[g.Fingerprint] = true
		}
		cursor = result.NextCursor
		if page == 0 && cursor == "" {
			t.Fatal("missing continuation cursor")
		}
	}
	if len(seen) != 503 || cursor != "" {
		t.Fatalf("incomplete export: %d cursor %q", len(seen), cursor)
	}
}

func TestPostgresErrorExportAndResolution(t *testing.T) {
	url := os.Getenv("GONVEX_ERROR_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set GONVEX_ERROR_TEST_POSTGRES_URL to a disposable Postgres database")
	}
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := New(config.Config{})
	project := "test-error-export-" + uuid.NewString()
	s.telemetryDBs[project] = db
	for i := 0; i < 503; i++ {
		event := capturedError{Project: project, EventID: fmt.Sprint(i), Message: "failure", Culprit: fmt.Sprintf("function_%04x", i)}
		available, accepted, err := s.persistError(context.Background(), event)
		if !available || !accepted || err != nil {
			t.Fatalf("persist: %v %v %v", available, accepted, err)
		}
	}
	assertCompleteErrorExport(t, s, project)
	event := capturedError{Project: project, EventID: "0", Message: "failure", Culprit: "function_0000"}
	fp := fingerprint(event)
	if _, _, err := s.updatePersistentErrorGroup(context.Background(), project, fp, errorGroupUpdate{Status: "resolved"}); err != nil {
		t.Fatal(err)
	}
	if _, accepted, err := s.persistError(context.Background(), event); err != nil || accepted {
		t.Fatal("duplicate replay was accepted")
	}
	group, _, err := s.persistentErrorGroup(context.Background(), project, fp)
	if err != nil || group.Status != "resolved" {
		t.Fatal("duplicate reopened resolved group")
	}
	event.EventID = "new-occurrence"
	if _, accepted, err := s.persistError(context.Background(), event); err != nil || !accepted {
		t.Fatal("new occurrence was not accepted")
	}
	group, _, err = s.persistentErrorGroup(context.Background(), project, fp)
	if err != nil || group.Status != "unresolved" || !group.Regression {
		t.Fatal("durable unversioned recurrence remained hidden")
	}
}
