package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/config"
	"github.com/gorilla/websocket"
)

type recordingDB struct {
	exec func(query string, args ...any)
}

func (db *recordingDB) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	if db.exec != nil {
		db.exec(query, args...)
	}
	return nil, nil
}

type registeredQueryArgs struct {
	Name string `json:"name"`
}

type registeredMutationArgs struct {
	Title string `json:"title"`
}

type registeredMutationResult struct {
	Title     string `json:"title"`
	ProjectID string `json:"projectId"`
}

func TestHealth(t *testing.T) {
	t.Setenv("SOURCE_COMMIT", "")
	t.Setenv("GONVEX_RUNTIME_VERSION", "0123456789abcdef0123456789abcdef01234567")
	server := New(config.Config{PostgresURL: "postgres://example", S3Endpoint: "http://localhost:9000", S3Bucket: "gonvex-dev"})
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}
	var payload struct {
		Version string `json:"version"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Version != "0123456789abcdef0123456789abcdef01234567" {
		t.Fatalf("health version = %q", payload.Version)
	}
}

func TestHealthPrefersCoolifySourceCommit(t *testing.T) {
	const sourceCommit = "89abcdef0123456789abcdef0123456789abcdef"
	t.Setenv("SOURCE_COMMIT", sourceCommit)
	t.Setenv("GONVEX_RUNTIME_VERSION", "0123456789abcdef0123456789abcdef01234567")
	server := New(config.Config{})
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	var payload struct {
		Version string `json:"version"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Version != sourceCommit {
		t.Fatalf("health version = %q, want Coolify source commit %q", payload.Version, sourceCommit)
	}
}

func TestHealthIgnoresUnresolvedCoolifySourceCommit(t *testing.T) {
	t.Setenv("SOURCE_COMMIT", "HEAD")
	t.Setenv("GONVEX_RUNTIME_VERSION", "0123456789abcdef0123456789abcdef01234567")

	if version := runtimeBuildVersion(); version != "0123456789abcdef0123456789abcdef01234567" {
		t.Fatalf("runtime version = %q", version)
	}
}

func TestHealthWaitsForRuntimeManifestHydration(t *testing.T) {
	server := newServer(config.Config{}, nil, nil, nil)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d before manifest hydration, got %d", http.StatusServiceUnavailable, recorder.Code)
	}
}

func TestHealthFailsWhenPersistedRuntimeManifestCannotHydrate(t *testing.T) {
	server := New(config.Config{})
	server.markRuntimeHydrationFailure("project-with-broken-manifest")
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d", http.StatusServiceUnavailable, recorder.Code)
	}
	var payload struct {
		OK               bool `json:"ok"`
		RuntimeManifests struct {
			Ready          bool `json:"ready"`
			FailedProjects int  `json:"failedProjects"`
		} `json:"runtimeManifests"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.OK || payload.RuntimeManifests.Ready || payload.RuntimeManifests.FailedProjects != 1 {
		t.Fatalf("unexpected health payload: %+v", payload)
	}
}

func TestDataTablesWithoutDatabase(t *testing.T) {
	server := New(config.Config{})
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dev/data/tables", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var payload struct {
		Tables []any `json:"tables"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(payload.Tables) != 0 {
		t.Fatalf("expected no tables, got %d", len(payload.Tables))
	}
}

func TestDataRowsWithoutDatabase(t *testing.T) {
	server := New(config.Config{})
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dev/data/tables/tasks/rows", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var payload struct {
		Table   string `json:"table"`
		Columns []any  `json:"columns"`
		Rows    []any  `json:"rows"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Table != "tasks" || len(payload.Columns) != 0 || len(payload.Rows) != 0 {
		t.Fatalf("unexpected rows payload: %+v", payload)
	}
}

func TestDataRowsRejectsInvalidTableName(t *testing.T) {
	server := New(config.Config{})
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dev/data/tables/bad-name/rows", nil))

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
	}
}

func TestInternalDataTablesAreHidden(t *testing.T) {
	tests := []struct {
		name  string
		table string
		want  bool
	}{
		{name: "files metadata", table: "_gonvex_files", want: true},
		{name: "dashboard users", table: "gonvex_dashboard_users", want: true},
		{name: "account access tokens", table: "gonvex_account_access_tokens", want: true},
		{name: "project members", table: "gonvex_project_members", want: true},
		{name: "project invitations", table: "gonvex_project_invitations", want: true},
		{name: "project env", table: "gonvex_project_env", want: true},
		{name: "runtime projects", table: "gonvex_runtime_projects", want: true},
		{name: "runtime manifests", table: "gonvex_runtime_manifests", want: true},
		{name: "scheduled jobs", table: "gonvex_scheduled_jobs", want: true},
		{name: "telemetry", table: "telemetry_events", want: true},
		{name: "app users", table: "users", want: false},
		{name: "app tenants", table: "tenants", want: false},
		{name: "app join table", table: "userTenantMap", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := internalDataTable(test.table); got != test.want {
				t.Fatalf("expected %q internal=%v, got %v", test.table, test.want, got)
			}
		})
	}
}

func TestDataRowsRejectsInternalTable(t *testing.T) {
	server := New(config.Config{})
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dev/data/tables/_gonvex_files/rows", nil))

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d", http.StatusNotFound, recorder.Code)
	}
}

func TestInsertDataRowsWithoutDatabaseFails(t *testing.T) {
	server := New(config.Config{})
	recorder := httptest.NewRecorder()
	body := bytes.NewBufferString(`{"id":"task_1","title":"Test"}`)

	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/dev/data/tables/tasks/rows", body))

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
	}
}

func TestInsertDataRowsRejectsInternalTable(t *testing.T) {
	server := New(config.Config{})
	recorder := httptest.NewRecorder()
	body := bytes.NewBufferString(`{"id":"secret"}`)

	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/dev/data/tables/gonvex_project_members/rows", body))

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d", http.StatusNotFound, recorder.Code)
	}
}

func TestInsertDataRowsRejectsInvalidTableName(t *testing.T) {
	server := New(config.Config{})
	recorder := httptest.NewRecorder()
	body := bytes.NewBufferString(`{"id":"task_1"}`)

	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/dev/data/tables/bad-name/rows", body))

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
	}
}

func TestReplaceDataReferencesRejectsEmptyReplacementMap(t *testing.T) {
	server := New(config.Config{AdminKey: "admin-secret"})
	recorder := httptest.NewRecorder()
	body := bytes.NewBufferString(`{"replacements":{}}`)
	request := httptest.NewRequest(http.MethodPost, "/dev/data/references/replace", body)
	request.Header.Set("x-gonvex-key", "admin-secret")
	request.Header.Set("x-gonvex-project-id", "project")

	server.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d: %s", http.StatusBadRequest, recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "at least one replacement is required") {
		t.Fatalf("expected replacement validation error, got %s", recorder.Body.String())
	}
}

func TestUpdateDataRowRejectsEmptyPatch(t *testing.T) {
	server := New(config.Config{AdminKey: "admin-secret"})
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPatch, "/dev/data/tables/tasks/rows/task-1", bytes.NewBufferString(`{}`))
	request.Header.Set("x-gonvex-key", "admin-secret")
	request.Header.Set("x-gonvex-project-id", "project")

	server.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d: %s", http.StatusBadRequest, recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "at least one value is required") {
		t.Fatalf("expected empty patch validation error, got %s", recorder.Body.String())
	}
}

func TestUpdateDataRowAcceptsLocalDashboardOwner(t *testing.T) {
	server := New(config.Config{})
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPatch, "/dev/data/tables/tasks/rows/task-1", bytes.NewBufferString(`{"name":"Updated"}`))
	request.Header.Set("x-gonvex-project-id", "project")

	server.Handler().ServeHTTP(recorder, request)

	if recorder.Code == http.StatusForbidden || recorder.Code == http.StatusUnauthorized {
		t.Fatalf("expected local dashboard owner to pass data-write authorization, got %d: %s", recorder.Code, recorder.Body.String())
	}
}

func TestDeleteDataRowRequiresRuntimeAdminKey(t *testing.T) {
	server := New(config.Config{AdminKey: "admin-secret"})
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodDelete, "/dev/data/tables/tasks/rows/task-1", nil)
	request.Header.Set("x-gonvex-project-id", "project")

	server.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusUnauthorized && recorder.Code != http.StatusForbidden {
		t.Fatalf("expected admin auth rejection, got %d: %s", recorder.Code, recorder.Body.String())
	}
}

func TestDevSyncStoresManifest(t *testing.T) {
	server := New(config.Config{AllowUnauthenticatedSync: true})
	body := bytes.NewBufferString(`{"project":"test","generatedAt":"now","functions":{"tasks.list":{"kind":"query","handler":"List","file":"gonvex/tasks.go"}},"schema":{}}`)

	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/dev/sync", body))

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	manifest := server.runtime.Manifest()
	if len(manifest.Functions) != 1 {
		t.Fatalf("expected 1 function, got %d", len(manifest.Functions))
	}
}

func TestManifestForUnknownProjectIsEmpty(t *testing.T) {
	server := New(config.Config{})
	body := bytes.NewBufferString(`{"project":"app","generatedAt":"now","functions":{"tasks.list":{"kind":"query","handler":"List","file":"gonvex/tasks.go"}},"schema":{}}`)

	server.Handler().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/dev/sync", body))

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/dev/manifest", nil)
	request.Header.Set("x-gonvex-project-id", "testing")
	server.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var payload struct {
		Project   string         `json:"project"`
		Functions map[string]any `json:"functions"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Project != "testing" {
		t.Fatalf("expected testing project, got %q", payload.Project)
	}
	if len(payload.Functions) != 0 {
		t.Fatalf("expected no functions for unknown project, got %d", len(payload.Functions))
	}
}

func TestExecuteRegisteredQuery(t *testing.T) {
	app := gonvex.NewApp()
	app.Query("custom.echo", func(ctx *gonvex.QueryCtx, args registeredQueryArgs) (map[string]any, error) {
		return map[string]any{
			"name":      args.Name,
			"projectId": ctx.ProjectID,
			"tenantId":  ctx.TenantID,
			"hasDB":     ctx.DB != nil,
		}, nil
	})
	server := NewWithApp(config.Config{}, app)

	result, err := server.executeQuery(context.Background(), "project-a", "custom.echo", json.RawMessage(`{"name":"Ada"}`))
	if err != nil {
		t.Fatalf("execute registered query: %v", err)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map result, got %T", result)
	}
	if payload["name"] != "Ada" || payload["projectId"] != "project-a" || payload["tenantId"] != "project-a" || payload["hasDB"] != false {
		t.Fatalf("unexpected payload: %#v", payload)
	}
}

func TestExecuteRegisteredMutation(t *testing.T) {
	app := gonvex.NewApp()
	app.Mutation("custom.create", func(ctx *gonvex.MutationCtx, args registeredMutationArgs) (registeredMutationResult, error) {
		if ctx.Tx != nil {
			t.Fatal("expected nil transaction without configured database")
		}
		return registeredMutationResult{Title: args.Title, ProjectID: ctx.ProjectID}, nil
	})
	server := NewWithApp(config.Config{}, app)

	result, err := server.executeMutation(context.Background(), "project-a", "custom.create", json.RawMessage(`{"title":"Ship"}`))
	if err != nil {
		t.Fatalf("execute registered mutation: %v", err)
	}
	payload, ok := result.(registeredMutationResult)
	if !ok {
		t.Fatalf("expected registeredMutationResult, got %T", result)
	}
	if payload.Title != "Ship" || payload.ProjectID != "project-a" {
		t.Fatalf("unexpected payload: %#v", payload)
	}
}

func TestMetricsTracksDataCacheAndFunctionCalls(t *testing.T) {
	server := New(config.Config{})

	server.Handler().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/dev/data/tables/tasks/rows", nil))
	if _, err := server.executeQuery(context.Background(), "", "tasks.grid", nil); err != nil {
		t.Fatalf("execute query: %v", err)
	}
	if _, err := server.executeMutation(context.Background(), "", "missing.mutation", nil); err == nil {
		t.Fatal("expected missing mutation to fail")
	}

	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dev/metrics", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var payload struct {
		Functions map[string]struct {
			Calls  int64 `json:"calls"`
			Errors int64 `json:"errors"`
		} `json:"functions"`
		Cache struct {
			Bypasses int64 `json:"bypasses"`
		} `json:"cache"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Functions["tasks.grid"].Calls != 1 {
		t.Fatalf("expected tasks.grid call to be tracked, got %+v", payload.Functions["tasks.grid"])
	}
	if payload.Functions["missing.mutation"].Errors != 1 {
		t.Fatalf("expected missing mutation error to be tracked, got %+v", payload.Functions["missing.mutation"])
	}
	if payload.Cache.Bypasses != 1 {
		t.Fatalf("expected cache bypass to be tracked, got %d", payload.Cache.Bypasses)
	}
}

func TestMetricsLogsAreProjectScoped(t *testing.T) {
	server := New(config.Config{})
	if err := server.runtime.SyncManifest(manifest.Manifest{
		Project: "project-a",
		Functions: map[string]manifest.FunctionEntry{
			"tasks.list": {Kind: manifest.FunctionKindQuery, Handler: "ListTasks", File: "gonvex/tasks.go"},
		},
		Schema: manifest.EmptySchema(),
	}); err != nil {
		t.Fatalf("sync project-a manifest: %v", err)
	}
	if err := server.runtime.SyncManifest(manifest.Manifest{
		Project: "project-b",
		Functions: map[string]manifest.FunctionEntry{
			"messages.list": {Kind: manifest.FunctionKindQuery, Handler: "ListMessages", File: "gonvex/messages.go"},
		},
		Schema: manifest.EmptySchema(),
	}); err != nil {
		t.Fatalf("sync project-b manifest: %v", err)
	}
	server.metrics.recordFunction("project-a", "tasks.list", "query", 25*time.Millisecond, nil)
	server.metrics.recordFunction("project-a", "tasks.grid", "query", 25*time.Millisecond, fmt.Errorf("dashboard probe"))
	server.metrics.recordFunction("project-b", "messages.list", "query", 25*time.Millisecond, fmt.Errorf("boom"))
	dataRequest := httptest.NewRequest(http.MethodGet, "/dev/data/tables", nil)
	dataRequest.Header.Set("x-gonvex-project-id", "project-a")
	server.Handler().ServeHTTP(httptest.NewRecorder(), dataRequest)

	request := httptest.NewRequest(http.MethodGet, "/dev/metrics", nil)
	request.Header.Set("x-gonvex-project-id", "project-a")
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var payload struct {
		Logs []runtimeLogEntry `json:"logs"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(payload.Logs) != 3 {
		t.Fatalf("expected only project-a logs, got %+v", payload.Logs)
	}
	seen := map[string]bool{}
	for _, entry := range payload.Logs {
		if entry.Project != "project-a" {
			t.Fatalf("expected project-a scoped log, got %+v", entry)
		}
		seen[entry.Path] = true
	}
	for _, path := range []string{"tasks.list", "tasks.grid", "dev.data.tables"} {
		if !seen[path] {
			t.Fatalf("expected project-a log path %q in %+v", path, payload.Logs)
		}
	}

	clearRequest := httptest.NewRequest(http.MethodDelete, "/dev/logs", nil)
	clearRequest.Header.Set("x-gonvex-project-id", "project-a")
	clearRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(clearRecorder, clearRequest)
	if clearRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, clearRecorder.Code)
	}
	var clearPayload struct {
		Cleared int `json:"cleared"`
	}
	if err := json.NewDecoder(clearRecorder.Body).Decode(&clearPayload); err != nil {
		t.Fatalf("decode clear response: %v", err)
	}
	if clearPayload.Cleared != 3 {
		t.Fatalf("expected three cleared project-a logs, got %d", clearPayload.Cleared)
	}

	projectBRequest := httptest.NewRequest(http.MethodGet, "/dev/metrics", nil)
	projectBRequest.Header.Set("x-gonvex-project-id", "project-b")
	projectBRecorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(projectBRecorder, projectBRequest)
	if err := json.NewDecoder(projectBRecorder.Body).Decode(&payload); err != nil {
		t.Fatalf("decode project-b response: %v", err)
	}
	if len(payload.Logs) != 1 || payload.Logs[0].Project != "project-b" || payload.Logs[0].Path != "messages.list" {
		t.Fatalf("expected project-b log to remain, got %+v", payload.Logs)
	}
}

func TestMetricsStreamsProjectLogs(t *testing.T) {
	metrics := newRuntimeMetrics()
	id, logs, recent := metrics.subscribeLogs("project-a")
	defer metrics.unsubscribeLogs(id)
	if len(recent) != 0 {
		t.Fatalf("expected no recent logs, got %+v", recent)
	}

	metrics.recordFunction("project-b", "messages.list", "query", 10*time.Millisecond, nil)
	select {
	case entry := <-logs:
		t.Fatalf("did not expect project-b log on project-a stream: %+v", entry)
	default:
	}

	metrics.recordFunction("project-a", "tasks.list", "query", 10*time.Millisecond, nil)
	select {
	case entry := <-logs:
		if entry.Project != "project-a" || entry.Path != "tasks.list" {
			t.Fatalf("unexpected streamed log: %+v", entry)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for project log")
	}

	_, _, recent = metrics.subscribeLogs("project-a")
	if len(recent) != 1 || recent[0].Project != "project-a" {
		t.Fatalf("expected one retained project-a log, got %+v", recent)
	}
}

func TestMetricsWebSocketOmitsTelemetryLogPath(t *testing.T) {
	runtime := httptest.NewServer(New(config.Config{TelemetryLogPath: filepath.Join(t.TempDir(), "runtime.jsonl")}).Handler())
	defer runtime.Close()

	connection, _, err := websocket.DefaultDialer.Dial(
		"ws"+strings.TrimPrefix(runtime.URL, "http")+"/dev/metrics/stream?project=project-a",
		nil,
	)
	if err != nil {
		t.Fatalf("dial metrics stream: %v", err)
	}
	defer connection.Close()

	var message struct {
		Type    string                     `json:"type"`
		Metrics map[string]json.RawMessage `json:"metrics"`
	}
	if err := connection.ReadJSON(&message); err != nil {
		t.Fatalf("read metrics stream: %v", err)
	}
	if message.Type != "metrics" {
		t.Fatalf("stream message type = %q, want metrics", message.Type)
	}
	if _, exposed := message.Metrics["telemetryLogPath"]; exposed {
		t.Fatal("metrics stream exposed the server telemetry filesystem path")
	}
}

func TestLogStreamReplayFlag(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/dev/logs/stream", nil)
	if !logStreamReplay(request) {
		t.Fatal("expected log stream to replay retained logs by default")
	}

	for _, value := range []string{"0", "false", "no", "off"} {
		request := httptest.NewRequest(http.MethodGet, "/dev/logs/stream?replay="+value, nil)
		if logStreamReplay(request) {
			t.Fatalf("expected replay=%s to disable retained log replay", value)
		}
	}
}

func TestMetricsExposesRunningAndScheduler(t *testing.T) {
	server := New(config.Config{})

	if _, err := server.executeQuery(context.Background(), "", "tasks.grid", nil); err != nil {
		t.Fatalf("execute query: %v", err)
	}

	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dev/metrics", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var payload struct {
		Running struct {
			Current map[string]int64     `json:"current"`
			Series  []runningMetricPoint `json:"series"`
		} `json:"running"`
		Scheduler *schedulerSnapshot `json:"scheduler"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Scheduler == nil {
		t.Fatal("expected scheduler block in metrics snapshot")
	}
	if len(payload.Scheduler.Series) != metricsBucketCount {
		t.Fatalf("expected %d scheduler buckets, got %d", metricsBucketCount, len(payload.Scheduler.Series))
	}
	if len(payload.Running.Series) != metricsBucketCount {
		t.Fatalf("expected %d running buckets, got %d", metricsBucketCount, len(payload.Running.Series))
	}
	// Start/end are balanced, so the live gauge settles back to zero.
	if payload.Running.Current["query"] != 0 {
		t.Fatalf("expected running query gauge to settle at 0, got %d", payload.Running.Current["query"])
	}
}

func TestMetricsTracksTransactionTelemetry(t *testing.T) {
	telemetryDir := filepath.Join(t.TempDir(), "telemetry")
	telemetryPath := filepath.Join(telemetryDir, "runtime.jsonl")
	server := New(config.Config{TelemetryLogPath: telemetryPath})

	server.metrics.recordTransaction(transactionTelemetryEntry{
		Time:             "2026-06-20T00:00:00Z",
		Project:          "project-a",
		Tenant:           "tenant-a",
		OperationID:      "op-1",
		Kind:             "mutation",
		Path:             "tasks.create",
		Phase:            "server",
		Outcome:          "ok",
		ServerDurationMS: 25,
		ServerCommitMS:   20,
		ClientToCommitMS: 35,
	})
	server.metrics.recordTransaction(transactionTelemetryEntry{
		Time:              "2026-06-20T00:00:01Z",
		Project:           "project-a",
		Tenant:            "tenant-a",
		OperationID:       "op-2",
		Kind:              "query",
		Path:              "tasks.grid",
		Phase:             "browser",
		Reason:            "invalidate",
		Outcome:           "ok",
		ClientRoundTripMS: 45,
		ServerToBrowserMS: 7,
		ChangeToBrowserMS: 52,
	})

	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dev/metrics", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var payload struct {
		Transactions map[string]struct {
			ServerEvents             int64   `json:"serverEvents"`
			BrowserEvents            int64   `json:"browserEvents"`
			AverageServerCommitMS    float64 `json:"averageServerCommitMs"`
			AverageClientToCommitMS  float64 `json:"averageClientToCommitMs"`
			AverageServerToBrowserMS float64 `json:"averageServerToBrowserMs"`
			AverageChangeToBrowserMS float64 `json:"averageChangeToBrowserMs"`
		} `json:"transactions"`
		TelemetryLogs    []transactionTelemetryEntry `json:"telemetryLogs"`
		TelemetryLogPath *json.RawMessage            `json:"telemetryLogPath"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Transactions["mutation:tasks.create"].ServerEvents != 1 {
		t.Fatalf("expected mutation server event, got %+v", payload.Transactions["mutation:tasks.create"])
	}
	if payload.Transactions["mutation:tasks.create"].AverageServerCommitMS != 20 {
		t.Fatalf("expected commit average to be recorded, got %+v", payload.Transactions["mutation:tasks.create"])
	}
	if payload.Transactions["mutation:tasks.create"].AverageClientToCommitMS != 35 {
		t.Fatalf("expected client-to-commit average to be recorded, got %+v", payload.Transactions["mutation:tasks.create"])
	}
	if payload.Transactions["query:tasks.grid"].BrowserEvents != 1 {
		t.Fatalf("expected query browser event, got %+v", payload.Transactions["query:tasks.grid"])
	}
	if payload.Transactions["query:tasks.grid"].AverageServerToBrowserMS != 7 || payload.Transactions["query:tasks.grid"].AverageChangeToBrowserMS != 52 {
		t.Fatalf("expected browser delivery averages to be recorded, got %+v", payload.Transactions["query:tasks.grid"])
	}
	if len(payload.TelemetryLogs) != 2 {
		t.Fatalf("expected telemetry logs in snapshot, got %d", len(payload.TelemetryLogs))
	}
	if payload.TelemetryLogPath != nil {
		t.Fatal("metrics response exposed the server telemetry filesystem path")
	}
	ledger, err := os.ReadFile(telemetryPath)
	if err != nil {
		t.Fatalf("read telemetry ledger: %v", err)
	}
	if !strings.Contains(string(ledger), `"path":"tasks.create"`) || !strings.Contains(string(ledger), `"path":"tasks.grid"`) {
		t.Fatalf("expected telemetry ledger to contain both events, got %s", string(ledger))
	}
	directoryInfo, err := os.Stat(telemetryDir)
	if err != nil {
		t.Fatalf("stat telemetry directory: %v", err)
	}
	if got := directoryInfo.Mode().Perm(); got != 0o700 {
		t.Fatalf("telemetry directory mode = %o, want 700", got)
	}
	fileInfo, err := os.Stat(telemetryPath)
	if err != nil {
		t.Fatalf("stat telemetry file: %v", err)
	}
	if got := fileInfo.Mode().Perm(); got != 0o600 {
		t.Fatalf("telemetry file mode = %o, want 600", got)
	}
}

func TestTelemetryPermissionsHardenedDuringInitialization(t *testing.T) {
	telemetryDir := filepath.Join(t.TempDir(), "telemetry")
	if err := os.Mkdir(telemetryDir, 0o755); err != nil {
		t.Fatalf("create telemetry directory: %v", err)
	}
	telemetryPath := filepath.Join(telemetryDir, "runtime.jsonl")
	if err := os.WriteFile(telemetryPath, []byte("existing\n"), 0o644); err != nil {
		t.Fatalf("create telemetry file: %v", err)
	}

	_ = New(config.Config{TelemetryLogPath: telemetryPath})

	directoryInfo, err := os.Stat(telemetryDir)
	if err != nil {
		t.Fatalf("stat telemetry directory: %v", err)
	}
	if got := directoryInfo.Mode().Perm(); got != 0o700 {
		t.Fatalf("telemetry directory mode after initialization = %o, want 700", got)
	}
	fileInfo, err := os.Stat(telemetryPath)
	if err != nil {
		t.Fatalf("stat telemetry file: %v", err)
	}
	if got := fileInfo.Mode().Perm(); got != 0o600 {
		t.Fatalf("telemetry file mode after initialization = %o, want 600", got)
	}
}

func TestTransactionTelemetryCanBeDisabled(t *testing.T) {
	telemetryPath := filepath.Join(t.TempDir(), "telemetry.jsonl")
	server := New(config.Config{TelemetryEnabled: false, TelemetryLogPath: telemetryPath})

	server.recordTransactionTelemetry(transactionTelemetryEntry{
		Time:        "2026-06-20T00:00:00Z",
		Project:     "project-a",
		Tenant:      "tenant-a",
		OperationID: "op-1",
		Kind:        "query",
		Path:        "bulk.allReferenceData",
		Phase:       "browser",
		Outcome:     "ok",
	})

	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dev/metrics", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var payload struct {
		TelemetryLogs []transactionTelemetryEntry `json:"telemetryLogs"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(payload.TelemetryLogs) != 0 {
		t.Fatalf("expected no telemetry logs when disabled, got %d", len(payload.TelemetryLogs))
	}
	if _, err := os.Stat(telemetryPath); !os.IsNotExist(err) {
		t.Fatalf("expected telemetry file to be absent when disabled, got err=%v", err)
	}
}

func TestSuccessfulServerSubscriptionTelemetryIsNotDurable(t *testing.T) {
	for _, kind := range []string{"query", "sync"} {
		if transactionTelemetryIsDurable(transactionTelemetryEntry{Kind: kind, Phase: "server", Outcome: "ok"}) {
			t.Fatalf("successful server %s telemetry must stay aggregate-only", kind)
		}
		if !transactionTelemetryIsDurable(transactionTelemetryEntry{Kind: kind, Phase: "server", Outcome: "error"}) {
			t.Fatalf("server %s errors must remain durable", kind)
		}
	}
	if !transactionTelemetryIsDurable(transactionTelemetryEntry{Kind: "mutation", Phase: "server", Outcome: "ok"}) {
		t.Fatal("successful mutations must remain durable")
	}
	if !transactionTelemetryIsDurable(transactionTelemetryEntry{Kind: "query", Phase: "browser", Outcome: "ok"}) {
		t.Fatal("browser telemetry must remain durable")
	}
}

func TestClientTelemetryEntryIncludesBrowserDeviceInfo(t *testing.T) {
	entry := transactionEntryFromClientTelemetry("project-a", "tenant-a", clientMessage{
		Type:               "telemetry.event",
		ID:                 "op-1",
		Kind:               "query",
		Path:               "tasks.grid",
		Reason:             "invalidate",
		Outcome:            "ok",
		ClientReceivedAtMS: 123.456,
		Device:             json.RawMessage(`{"browserName":"Chrome","browserVersion":"126.0.0.0","deviceType":"desktop","platform":"Win32","userAgent":"Mozilla/5.0 Chrome/126.0.0.0","language":"en-US","timezone":"America/Los_Angeles","viewportWidth":1440,"viewportHeight":900}`),
		Trace:              &messageTrace{ServerChangeCommittedAtMS: 100.125, ServerSubscriptionSentAtMS: 110.25},
	})

	if entry.Project != "project-a" || entry.Tenant != "tenant-a" || entry.Kind != "query" || entry.Path != "tasks.grid" {
		t.Fatalf("unexpected telemetry identity: %+v", entry)
	}
	if entry.BrowserName != "Chrome" || entry.DeviceType != "desktop" || entry.Platform != "Win32" {
		t.Fatalf("expected browser/device info to be parsed, got %+v", entry)
	}
	if entry.ViewportWidth != 1440 || entry.ViewportHeight != 900 {
		t.Fatalf("expected viewport info to be parsed, got %+v", entry)
	}
	if entry.ChangeToBrowserMS <= 23 || entry.ChangeToBrowserMS >= 24 {
		t.Fatalf("expected fractional change-to-browser timing, got %+v", entry.ChangeToBrowserMS)
	}
	if !strings.Contains(entry.DeviceJSON, `"browserName":"Chrome"`) {
		t.Fatalf("expected raw device json to be preserved, got %q", entry.DeviceJSON)
	}
}

func TestTelemetrySchemaUsesDedicatedEventsTable(t *testing.T) {
	statements := []string{}
	db := &recordingDB{exec: func(query string, args ...any) {
		statements = append(statements, query)
	}}
	if err := ensureTelemetrySchema(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(statements, "\n")
	if !strings.Contains(joined, "CREATE TABLE IF NOT EXISTS telemetry_events") {
		t.Fatalf("expected dedicated telemetry_events table in:\n%s", joined)
	}
	if strings.Contains(joined, "gonvex_runtime_telemetry_events") {
		t.Fatalf("did not expect main registry telemetry table in:\n%s", joined)
	}
}

func TestDevSyncRequiresConfiguredKey(t *testing.T) {
	server := New(config.Config{DevSyncKey: "secret"})
	body := bytes.NewBufferString(`{"project":"app","generatedAt":"now","functions":{},"schema":{}}`)

	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/dev/sync", body))

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, recorder.Code)
	}
}

func TestDevSyncAcceptsBearerSyncKey(t *testing.T) {
	server := New(config.Config{DevSyncKey: "secret"})
	body := bytes.NewBufferString(`{"project":"app","generatedAt":"now","functions":{},"schema":{}}`)
	request := httptest.NewRequest(http.MethodPost, "/dev/sync", body)
	request.Header.Set("authorization", "Bearer secret")

	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
}

func TestDevSyncRejectsHeaderProjectMismatch(t *testing.T) {
	server := New(config.Config{})
	body := bytes.NewBufferString(`{"project":"app","generatedAt":"now","functions":{},"schema":{}}`)
	request := httptest.NewRequest(http.MethodPost, "/dev/sync", body)
	request.Header.Set("x-gonvex-project-id", "other")

	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
	}
}

func TestDevSyncRejectsUnregisteredProjectInsteadOfUsingControlDatabase(t *testing.T) {
	server := New(config.Config{
		AllowUnauthenticatedSync: true,
		PostgresURL:              "postgres://control.example/gonvex_control",
		ProjectDatabases: map[string]string{
			"whagons-5": "postgres://app.example/whagons_5",
		},
	})
	body := bytes.NewBufferString(`{"project":"whagons5-dev","generatedAt":"now","functions":{},"schema":{}}`)
	request := httptest.NewRequest(http.MethodPost, "/dev/sync", body)
	request.Header.Set("x-gonvex-project-id", "whagons5-dev")

	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("expected status %d, got %d: %s", http.StatusConflict, recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "same Gonvex project id") {
		t.Fatalf("expected an actionable project mismatch, got %s", recorder.Body.String())
	}
	if manifest := server.runtime.ManifestForProject("whagons5-dev"); len(manifest.Functions) != 0 {
		t.Fatalf("unregistered project was synced into runtime: %+v", manifest)
	}
}

func TestDevSyncUsesHeaderProjectWhenManifestProjectIsEmpty(t *testing.T) {
	server := New(config.Config{AllowUnauthenticatedSync: true})
	body := bytes.NewBufferString(`{"generatedAt":"now","functions":{},"schema":{}}`)
	request := httptest.NewRequest(http.MethodPost, "/dev/sync", body)
	request.Header.Set("x-gonvex-project-id", "header-project")

	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}
	if got := server.runtime.ManifestForProject("header-project").Project; got != "header-project" {
		t.Fatalf("expected header project manifest, got %q", got)
	}
}

func TestDevSyncKeepsProjectManifestAvailableAfterSync(t *testing.T) {
	server := New(config.Config{AllowUnauthenticatedSync: true})
	body := bytes.NewBufferString(`{"project":"persisted-project","generatedAt":"now","functions":{"messages.list":{"kind":"query","handler":"ListMessages","file":"gonvex/messages.go"}},"schema":{"tables":{}}}`)

	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/dev/sync", body))

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	manifest := server.runtime.ManifestForProject("persisted-project")
	if manifest.Project != "persisted-project" {
		t.Fatalf("expected synced project manifest, got %q", manifest.Project)
	}
	if _, ok := manifest.Functions["messages.list"]; !ok {
		t.Fatalf("expected synced function manifest, got %+v", manifest.Functions)
	}
}

func TestDevSyncAlwaysStampsRuntimeDatabaseArtifactVersion(t *testing.T) {
	server := New(config.Config{})
	body := bytes.NewBufferString(`{"project":"artifact-version-project","generatedAt":"now","functions":{},"schema":{"tables":{}},"notifySchemaVersion":"1"}`)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/dev/sync", body))

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	if got := server.runtime.ManifestForProject("artifact-version-project").NotifySchemaVersion; got != manifest.NotifySchemaVersion {
		t.Fatalf("runtime stored database artifact version %q, want %q", got, manifest.NotifySchemaVersion)
	}
}

func TestDevSyncSkipsSchemaLoadedFromPersistedManifest(t *testing.T) {
	server := New(config.Config{AllowUnauthenticatedSync: true})
	persisted := manifest.Manifest{
		Project:             "persisted-project",
		Functions:           map[string]manifest.FunctionEntry{},
		Schema:              manifest.EmptySchema(),
		NotifySchemaVersion: manifest.NotifySchemaVersion,
	}
	if err := server.runtime.SyncManifest(persisted); err != nil {
		t.Fatal(err)
	}

	body := bytes.NewBufferString(`{"project":"persisted-project","generatedAt":"now","functions":{},"schema":{"tables":{}}}`)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/dev/sync", body))

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response struct {
		SchemaSkipped bool `json:"schemaSkipped"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if !response.SchemaSkipped {
		t.Fatal("expected schema loaded from persisted manifest to skip DDL reapply")
	}
}

func TestDataRowsRejectsMalformedFilters(t *testing.T) {
	server := New(config.Config{})
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dev/data/tables/tasks/rows?filters=not-json", nil))

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, recorder.Code)
	}
}

func TestParseColumnsTrimsAndKeepsEmptySlots(t *testing.T) {
	got := parseColumns(" id, title ,,status ")
	want := []string{"id", "title", "", "status"}
	if len(got) != len(want) {
		t.Fatalf("expected %#v, got %#v", want, got)
	}
	for index := range got {
		if got[index] != want[index] {
			t.Fatalf("expected %#v, got %#v", want, got)
		}
	}
}

func TestSyncKeySources(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/dev/sync", nil)
	request.Header.Set("x-gonvex-key", "header-secret")
	request.Header.Set("authorization", "Bearer bearer-secret")
	if got := syncKey(request); got != "header-secret" {
		t.Fatalf("expected x-gonvex-key to win, got %q", got)
	}

	request = httptest.NewRequest(http.MethodPost, "/dev/sync", nil)
	request.Header.Set("authorization", "Bearer bearer-secret")
	if got := syncKey(request); got != "bearer-secret" {
		t.Fatalf("expected bearer secret, got %q", got)
	}

	request = httptest.NewRequest(http.MethodPost, "/dev/sync", nil)
	request.Header.Set("authorization", "Basic nope")
	if got := syncKey(request); got != "" {
		t.Fatalf("expected empty sync key, got %q", got)
	}
}

func TestProjectIDPrefersHeaderThenQuery(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/dev/manifest?project=query-project", nil)
	request.Header.Set("x-gonvex-project-id", "header-project")
	if got := projectID(request); got != "header-project" {
		t.Fatalf("expected header project, got %q", got)
	}

	request = httptest.NewRequest(http.MethodGet, "/dev/manifest?project=query-project", nil)
	if got := projectID(request); got != "query-project" {
		t.Fatalf("expected query project, got %q", got)
	}

	request = httptest.NewRequest(http.MethodGet, "/dev/manifest", nil)
	if got := projectID(request); got != "" {
		t.Fatalf("expected empty project, got %q", got)
	}
}

func TestSubscriptionTableUsesPathPrefix(t *testing.T) {
	tests := map[string]string{
		"tasks.grid":    "tasks",
		"messages.list": "messages",
		"users.profile": "users",
		"badpath":       "",
		".missing":      "",
	}
	for path, want := range tests {
		if got := subscriptionTable(path); got != want {
			t.Fatalf("subscriptionTable(%q) = %q, want %q", path, got, want)
		}
	}
}

func TestMutationInvalidationTableUsesFocusedTable(t *testing.T) {
	tests := map[string]string{
		"tasks.create":                "tasks",
		"roles.update":                "roles",
		"techSupport.recordHeartbeat": "supportSessions",
		"chat.sendWorkspaceChat":      "workspaceChat",
		"chat.sendMessage":            "directMessages",
		"chat.addReaction":            "messageReactions",
		"calls.sendSignal":            "callSignals",
		"badpath":                     "",
	}
	for path, want := range tests {
		if got := mutationInvalidationTable(path); got != want {
			t.Fatalf("mutationInvalidationTable(%q) = %q, want %q", path, got, want)
		}
	}
}

func TestAppRealtimeFunctionTables(t *testing.T) {
	tests := []struct {
		path string
		want []string
	}{
		{path: "chat.listWorkspaceChat", want: []string{"workspaceChat"}},
		{path: "chat.listAllMessages", want: []string{"directMessages"}},
		{path: "chat.listAllParticipants", want: []string{"conversationParticipants"}},
		{path: "chat.listAllReactions", want: []string{"messageReactions"}},
		{path: "chat.workspaceChatUnreadSummary", want: []string{"notifications"}},
		{path: "calls.listForUser", want: []string{"callSignals"}},
		{path: "taskFindings.listNotes", want: []string{"taskFindings", "findingNotes", "taskLogs", "taskWorkspaceContexts", "tasks"}},
		{path: "users.myPermissions", want: []string{"roles", "rolePermissions", "permissions", "userTeams", "users"}},
		{path: "settings.getByKey", want: []string{"globalSettings"}},
		{path: "settings.listNotifications", want: []string{"notifications"}},
		{path: "settings.listPlugins", want: []string{"plugins"}},
		{path: "taskResources.listTaskNotes", want: []string{"taskNotes", "tasks", "users"}},
		{path: "taskResources.listTaskViewsByTaskId", want: []string{"taskViews", "tasks", "users"}},
		{path: "taskResources.listTaskSignatures", want: []string{"taskSignatures", "tasks", "users"}},
		{path: "taskResources.listTaskShares", want: []string{"taskShares", "taskAcks", "tasks", "users"}},
	}
	for _, test := range tests {
		got := subscriptionTables(test.path)
		if strings.Join(got, ",") != strings.Join(test.want, ",") {
			t.Fatalf("subscriptionTables(%q) = %v, want %v", test.path, got, test.want)
		}
	}
}

func TestAppRealtimeMutationInvalidationTables(t *testing.T) {
	tests := []struct {
		path string
		want []string
	}{
		{path: "chat.createConversation", want: []string{"conversations", "conversationParticipants"}},
		{path: "chat.sendMessage", want: []string{"directMessages", "conversations", "notifications", "linkPreviews"}},
		{path: "chat.markAsRead", want: []string{"conversationParticipants", "directMessages"}},
		{path: "chat.sendWorkspaceChat", want: []string{"workspaceChat", "notifications", "linkPreviews"}},
		{path: "chat.addReaction", want: []string{"messageReactions"}},
		{path: "calls.sendSignal", want: []string{"callSignals"}},
		{path: "taskFindings.createNote", want: []string{"taskFindings", "findingNotes", "taskLogs", "taskWorkspaceContexts", "tasks"}},
		{path: "taskResources.assignUser", want: []string{"taskUsers", "tasks", "taskLogs", "notifications"}},
		{path: "taskResources.createNote", want: []string{"taskNotes", "tasks", "taskLogs"}},
		{path: "taskResources.createSignature", want: []string{"taskSignatures", "tasks", "taskLogs"}},
		{path: "taskResources.recordTaskViewByTaskId", want: []string{"taskViews", "tasks"}},
		{path: "taskResources.addTagByTaskId", want: []string{"taskTags", "tasks", "taskLogs"}},
		{path: "scheduling.processWorkspaceChatMessage", want: []string{"scheduleAvailabilitySubmissions", "scheduleAvailabilityItems", "scheduleRosterEntries"}},
		{path: "settings.set", want: []string{"globalSettings"}},
		{path: "settings.togglePlugin", want: []string{"plugins"}},
		{path: "settings.markAllRead", want: []string{"notifications"}},
		{path: "tasks.update", want: []string{"tasks", "taskUsers", "taskTags", "taskLogs", "taskCustomFieldValues", "taskApprovalInstances"}},
	}
	for _, test := range tests {
		got := mutationInvalidationTables(test.path)
		if strings.Join(got, ",") != strings.Join(test.want, ",") {
			t.Fatalf("mutationInvalidationTables(%q) = %v, want %v", test.path, got, test.want)
		}
	}
}

func TestTableChangeMatchesSubscription(t *testing.T) {
	sub := querySubscription{path: "bulk.allReferenceData"}
	if tableChangeMatchesSubscription(sub, tableChange{table: "tasks"}) {
		t.Fatal("expected task changes not to invalidate reference data")
	}
	if !tableChangeMatchesSubscription(sub, tableChange{table: "users"}) {
		t.Fatal("expected user changes to invalidate reference data")
	}
	if !tableChangeMatchesSubscription(sub, tableChange{table: ""}) {
		t.Fatal("expected broad table change to invalidate subscription")
	}
	if !tableChangeMatchesSubscription(sub, tableChange{tables: map[string]bool{"tasks": true, "users": true}}) {
		t.Fatal("expected any matching table in a coalesced mutation change to invalidate subscription")
	}
	if tableChangeMatchesSubscription(sub, tableChange{tables: map[string]bool{"tasks": true, "taskTags": true}}) {
		t.Fatal("expected unrelated coalesced mutation tables not to invalidate reference data")
	}
}

func TestTableChangeTablesAreStableAndDeduplicated(t *testing.T) {
	got := tableChangeTables(tableChange{tables: map[string]bool{"users": true, "tasks": true}})
	if strings.Join(got, ",") != "tasks,users" {
		t.Fatalf("tableChangeTables() = %v, want [tasks users]", got)
	}
}

func TestMutationRuntimeContextSuppressesPreCommitInvalidations(t *testing.T) {
	called := 0
	runtimeCtx := mutationRuntimeContext(gonvex.RuntimeContext{
		NotifyTableChange: func(string) { called++ },
	})
	runtimeCtx.NotifyTableChanged("tasks", "taskUsers", "taskTags")
	if called != 0 {
		t.Fatalf("mutation handler scheduled %d pre-commit invalidations, want 0", called)
	}
}

func TestTableChangeMatchesDerivedTaskSubscriptions(t *testing.T) {
	tests := []struct {
		path  string
		table string
		want  bool
	}{
		{path: "bulk.tasksByWorkspace", table: "tasks", want: true},
		{path: "bulk.tasksByWorkspace", table: "taskTags", want: true},
		{path: "bulk.taskPivotData", table: "taskCustomFieldValues", want: true},
		{path: "bulk.tasksByWorkspace", table: "users", want: false},
		{path: "tasks.tasksPage", table: "tasks", want: true},
		{path: "tasks.tasksPage", table: "users", want: false},
		{path: "chat.listWorkspaceChat", table: "workspaceChat", want: true},
		{path: "chat.listWorkspaceChat", table: "directMessages", want: false},
		{path: "chat.listAllMessages", table: "directMessages", want: true},
		{path: "calls.listForUser", table: "callSignals", want: true},
		{path: "taskFindings.listNotes", table: "findingNotes", want: true},
		{path: "bulk.taskPivotData", table: "taskUsers", want: true},
		{path: "taskResources.listTaskNotes", table: "taskNotes", want: true},
		{path: "taskResources.listTaskNotes", table: "taskUsers", want: false},
	}
	for _, test := range tests {
		got := tableChangeMatchesSubscription(querySubscription{path: test.path}, tableChange{table: test.table})
		if got != test.want {
			t.Fatalf("tableChangeMatchesSubscription(%q, %q) = %v, want %v", test.path, test.table, got, test.want)
		}
	}
}

func TestProjectSubscriptionsIncludeEveryQueryForOnlyThatProject(t *testing.T) {
	server := New(config.Config{})
	projectA := &wsConn{subs: map[string]querySubscription{
		"tasks":      {id: "tasks", project: "project-a", path: "bulk.tasksByWorkspace"},
		"references": {id: "references", project: "project-a", path: "bulk.allReferenceData"},
	}}
	projectB := &wsConn{subs: map[string]querySubscription{
		"other": {id: "other", project: "project-b", path: "bulk.allReferenceData"},
	}}
	server.wsConns[projectA] = true
	server.wsConns[projectB] = true

	ids := map[string]bool{}
	for _, sub := range server.projectSubscriptions("project-a") {
		ids[sub.id] = true
	}
	if len(ids) != 2 || !ids["tasks"] || !ids["references"] || ids["other"] {
		t.Fatalf("unexpected project subscription set: %v", ids)
	}
}
