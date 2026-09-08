package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/gonvex/gonvex/server/internal/config"
	"github.com/google/uuid"
)

func TestErrorWindowCountsOccurrencesNotLifetime(t *testing.T) { testErrorWindow(t, nil) }
func TestPostgresErrorWindow(t *testing.T) {
	url := os.Getenv("GONVEX_ERROR_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("requires disposable Postgres")
	}
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	testErrorWindow(t, db)
}
func testErrorWindow(t *testing.T, db *sql.DB) {
	t.Helper()
	s := New(config.Config{})
	project := "window-" + uuid.NewString()
	if db != nil {
		s.telemetryDBs[project] = db
	}
	now := time.Now().UTC().Truncate(time.Second)
	for i, event := range []capturedError{
		{Message: "same failure", Timestamp: now.Add(-48 * time.Hour).Format(time.RFC3339), Tenant: "old", User: map[string]any{"id": "old-user"}, Release: "old"},
		{Message: "same failure", Timestamp: now.Add(-time.Hour).Format(time.RFC3339), Tenant: "current", User: map[string]any{"id": "current-user"}, Release: "new"},
		{Message: "same failure", Timestamp: now.Add(-30 * time.Minute).Format(time.RFC3339), Tenant: "current", User: map[string]any{"id": "current-user"}, Release: "new"},
		{Message: "historical only", Timestamp: now.Add(-48 * time.Hour).Format(time.RFC3339)},
		{Message: "other release", Timestamp: now.Add(-time.Hour).Format(time.RFC3339), Release: "other"},
	} {
		event.Project = project
		event.EventID = string(rune('a' + i))
		if db == nil {
			s.errorTracker.capture(event)
		} else if _, ok, err := s.persistError(context.Background(), event); err != nil || !ok {
			t.Fatalf("persist %v %v", ok, err)
		}
	}
	since := now.Add(-24 * time.Hour).Format(time.RFC3339)
	w := httptest.NewRecorder()
	s.handleErrorGroups(w, httptest.NewRequest("GET", "/dev/errors/groups?project="+project+"&since="+since+"&release=new&export=1", nil))
	var data struct {
		Groups []*errorGroup
		Since  string
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &data) != nil {
		t.Fatal(w.Body.String())
	}
	if data.Since != since || len(data.Groups) != 1 {
		t.Fatalf("window response: %s", w.Body.String())
	}
	g := data.Groups[0]
	if g.Count != 2 || len(g.Users) != 1 || g.Users["current-user"] != 2 || len(g.Tenants) != 1 || g.Tenants["current"] != 2 {
		t.Fatalf("lifetime data leaked: %+v", g)
	}
	w = httptest.NewRecorder()
	s.handleErrorGroups(w, httptest.NewRequest("GET", "/dev/errors/groups?project="+project+"&since="+since+"&export=1", nil))
	if json.Unmarshal(w.Body.Bytes(), &data) != nil || len(data.Groups) != 2 {
		t.Fatalf("historical-only group leaked: %s", w.Body.String())
	}
}
func TestErrorWindowRejectsInvalidSince(t *testing.T) {
	s := New(config.Config{})
	w := httptest.NewRecorder()
	s.handleErrorGroups(w, httptest.NewRequest("GET", "/dev/errors/groups?since=yesterday", nil))
	if w.Code != 400 {
		t.Fatalf("status %d", w.Code)
	}
}
