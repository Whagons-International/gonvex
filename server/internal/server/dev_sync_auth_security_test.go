package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gonvex/gonvex/server/internal/config"
)

func syncBody(t *testing.T, project string) *bytes.Reader {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"project": project, "functions": map[string]any{}})
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	return bytes.NewReader(raw)
}

// TestDevSyncRejectsBodyProjectWithoutHeader is the core regression guard.
//
// The key was validated against the project id in the *header* while the deploy
// acted on the project id in the *body*. Omitting the header authenticated the
// caller as "no project" -- which fell through to the open fallback -- and then
// deployed attacker Go into the project named in the body. /dev/sync compiles
// and plugin-loads that source into the runtime, so this was unauthenticated RCE.
func TestDevSyncRejectsBodyProjectWithoutHeader(t *testing.T) {
	server := New(config.Config{ProjectKeys: map[string]string{"victim-project": "victim-secret"}})

	req := httptest.NewRequest(http.MethodPost, "/dev/sync", syncBody(t, "victim-project"))
	req.Header.Set("Content-Type", "application/json")
	// No x-gonvex-project-id header, and no key.
	rec := httptest.NewRecorder()
	server.handleDevSync(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for headerless sync targeting a keyed project, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestDevSyncRejectsWrongKeyForBodyProject confirms the key is checked against
// the project the deploy acts on, not merely that *some* key was presented.
func TestDevSyncRejectsWrongKeyForBodyProject(t *testing.T) {
	server := New(config.Config{ProjectKeys: map[string]string{
		"victim-project":   "victim-secret",
		"attacker-project": "attacker-secret",
	}})

	req := httptest.NewRequest(http.MethodPost, "/dev/sync", syncBody(t, "victim-project"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-gonvex-key", "attacker-secret")
	rec := httptest.NewRecorder()
	server.handleDevSync(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 when presenting another project's key, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestDevSyncRejectsUnkeyedRuntimeFromRemoteAddress covers the fail-open default:
// a runtime deployed without any key must not accept sync from off-box.
func TestDevSyncRejectsUnkeyedRuntimeFromRemoteAddress(t *testing.T) {
	server := New(config.Config{})

	req := httptest.NewRequest(http.MethodPost, "/dev/sync", syncBody(t, "some-project"))
	req.Header.Set("Content-Type", "application/json")
	req.RemoteAddr = "203.0.113.7:44321" // off-box
	rec := httptest.NewRecorder()
	server.handleDevSync(rec, req)

	if rec.Code == http.StatusOK {
		t.Fatalf("unkeyed runtime accepted a remote sync: %s", rec.Body.String())
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestAcceptsSyncKeyFallbackRequiresLoopback pins the fallback rule directly.
func TestAcceptsSyncKeyFallbackRequiresLoopback(t *testing.T) {
	server := New(config.Config{})

	remote := httptest.NewRequest(http.MethodPost, "/dev/sync", nil)
	remote.RemoteAddr = "198.51.100.9:1234"
	if server.acceptsSyncKey("p", "", remote) {
		t.Fatal("unkeyed sync accepted from a non-loopback address")
	}

	local := httptest.NewRequest(http.MethodPost, "/dev/sync", nil)
	local.RemoteAddr = "127.0.0.1:5555"
	if !server.acceptsSyncKey("p", "", local) {
		t.Fatal("unkeyed sync rejected on loopback; local dev would break")
	}

	// Any registered project key means this is a real deployment, so the
	// loopback fallback must switch off entirely -- this is what protects a
	// runtime behind a reverse proxy that shares its network namespace.
	deployed := New(config.Config{ProjectKeys: map[string]string{"other": "k"}})
	if deployed.acceptsSyncKey("p", "", local) {
		t.Fatal("unkeyed sync accepted on a runtime that has registered project keys")
	}
}

// TestDevSyncStillRejectsHeaderBodyMismatch guards the pre-existing check.
func TestDevSyncStillRejectsHeaderBodyMismatch(t *testing.T) {
	server := New(config.Config{AllowUnauthenticatedSync: true})

	req := httptest.NewRequest(http.MethodPost, "/dev/sync", syncBody(t, "project-b"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-gonvex-project-id", "project-a")
	rec := httptest.NewRecorder()
	server.handleDevSync(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on header/body project mismatch, got %d: %s", rec.Code, rec.Body.String())
	}
}
