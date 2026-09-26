package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/pkg/projectbundle"
	"github.com/gonvex/gonvex/server/internal/config"
)

var bundleTestSequence atomic.Uint64

func TestDevSyncLoadsProjectBundle(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Go plugin bundles are not supported on Windows; use Linux/macOS runtime for plugin-backed sync tests")
	}

	moduleRoot, err := gonvexModuleRoot()
	if err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile(filepath.Join(moduleRoot, "pkg", "projectbundle", "testdata", "app", "register.go"))
	if err != nil {
		t.Fatal(err)
	}

	projectID := fmt.Sprintf("sync-test-%d", bundleTestSequence.Add(1))
	bundle := manifest.SourceBundle{
		ModulePath:  "gonvexapp/" + projectID,
		PackageName: "app",
		Files: map[string]string{
			"app/register.go": projectbundle.EncodeFile(source),
		},
	}
	bundle.Hash = projectbundle.HashFiles(bundle.Files)

	payload, err := json.Marshal(map[string]any{
		"project":     projectID,
		"generatedAt": "now",
		"functions": map[string]any{
			"sample.echo": map[string]any{"kind": "query", "handler": "SampleEcho", "file": "app/register.go"},
		},
		"schema": map[string]any{"tables": map[string]any{}},
		"bundle": bundle,
	})
	if err != nil {
		t.Fatal(err)
	}

	server := New(config.Config{
		AllowUnauthenticatedSync: true,
		GonvexModuleRoot:         moduleRoot,
		PluginCacheDir:           t.TempDir(),
	})
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/dev/sync", bytes.NewReader(payload)))
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	app := server.runtime.AppForProject(projectID)
	if app == nil {
		t.Fatal("expected synced project app")
	}
	result, err := server.executeQuery(context.Background(), projectID, "sample.echo", json.RawMessage(`{"name":"Ada"}`))
	if err != nil {
		t.Fatalf("execute synced query: %v", err)
	}
	payloadMap, ok := result.(map[string]any)
	if !ok || payloadMap["name"] != "Ada" {
		t.Fatalf("unexpected result: %#v", result)
	}
}

func gonvexModuleRoot() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", os.ErrInvalid
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..")), nil
}

// TestDevSyncRecycleHeaderOnlyForModuleReplacement pins the recycle contract:
// a first bundle load leaves this worker current and an unchanged bundle loads
// nothing, so neither may request a recycle; only a sync that replaces an
// already-loaded module (which Go cannot unload) sets the header.
func TestDevSyncRecycleHeaderOnlyForModuleReplacement(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Go plugin bundles are not supported on Windows; use Linux/macOS runtime for plugin-backed sync tests")
	}

	moduleRoot, err := gonvexModuleRoot()
	if err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile(filepath.Join(moduleRoot, "pkg", "projectbundle", "testdata", "app", "register.go"))
	if err != nil {
		t.Fatal(err)
	}

	projectID := fmt.Sprintf("sync-test-%d", bundleTestSequence.Add(1))
	payloadFor := func(sourceBytes []byte) []byte {
		bundle := manifest.SourceBundle{
			ModulePath:  "gonvexapp/" + projectID,
			PackageName: "app",
			Files: map[string]string{
				"app/register.go": projectbundle.EncodeFile(sourceBytes),
			},
		}
		bundle.Hash = projectbundle.HashFiles(bundle.Files)
		payload, err := json.Marshal(map[string]any{
			"project":     projectID,
			"generatedAt": "now",
			"functions": map[string]any{
				"sample.echo": map[string]any{"kind": "query", "handler": "SampleEcho", "file": "app/register.go"},
			},
			"schema": map[string]any{"tables": map[string]any{}},
			"bundle": bundle,
		})
		if err != nil {
			t.Fatal(err)
		}
		return payload
	}

	server := New(config.Config{
		GonvexModuleRoot:         moduleRoot,
		PluginCacheDir:           t.TempDir(),
		AllowUnauthenticatedSync: true,
	})
	sync := func(payload []byte) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/dev/sync", bytes.NewReader(payload)))
		if recorder.Code != http.StatusOK {
			t.Fatalf("expected status %d, got %d: %s", http.StatusOK, recorder.Code, recorder.Body.String())
		}
		return recorder
	}

	first := sync(payloadFor(source))
	if got := first.Header().Get("X-Gonvex-Recycle-Worker"); got != "" {
		t.Fatalf("first bundle load requested a recycle (header %q), want none", got)
	}
	unchanged := sync(payloadFor(source))
	if got := unchanged.Header().Get("X-Gonvex-Recycle-Worker"); got != "" {
		t.Fatalf("unchanged bundle sync requested a recycle (header %q), want none", got)
	}
	changed := sync(payloadFor(append(append([]byte{}, source...), []byte("\n// generation 2\n")...)))
	if got := changed.Header().Get("X-Gonvex-Recycle-Worker"); got != "1" {
		t.Fatalf("replacement bundle sync recycle header = %q, want \"1\"", got)
	}
}
