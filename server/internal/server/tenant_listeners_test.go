package server

import (
	"context"
	"database/sql"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gonvex/gonvex/server/internal/config"
	"github.com/gonvex/gonvex/server/internal/schema"
)

func TestParseSyncNotifyPayloadUsesTablesOnlyWithRevisionIdentity(t *testing.T) {
	parsed := parseSyncNotifyPayload(`{"revision":42,"epoch":"epoch-a","tables":["tasks","statuses","tasks"]}`)
	if parsed.Epoch != "epoch-a" || parsed.Revision != 42 || !reflect.DeepEqual(parsed.Tables, []string{"statuses", "tasks"}) {
		t.Fatalf("unexpected parsed sync notification: %#v", parsed)
	}

	for name, raw := range map[string]string{
		"legacy":      `{"revision":42,"epoch":"epoch-a"}`,
		"empty":       `{"revision":42,"epoch":"epoch-a","tables":[]}`,
		"no epoch":    `{"revision":42,"tables":["tasks"]}`,
		"no revision": `{"epoch":"epoch-a","tables":["tasks"]}`,
		"malformed":   `{`,
	} {
		t.Run(name, func(t *testing.T) {
			if got := parseSyncNotifyPayload(raw); len(got.Tables) != 0 {
				t.Fatalf("fallback payload retained filterable tables: %#v", got)
			}
		})
	}
}

func TestTenantListenerReconnectsWhenDatabaseRouteChanges(t *testing.T) {
	runtime := New(config.Config{
		TenantListenerLimit:       8,
		TenantListenerIdleTimeout: time.Minute,
		TenantDatabases: map[string]string{
			"project-a:tenant-a": "postgres://listener.test/old-database",
		},
	})
	manager := runtime.subscriptions.listeners
	key := tenantListenerKey{project: "project-a", tenant: "tenant-a"}
	oldContext, oldCancel := context.WithCancel(context.Background())
	oldListener := &tenantListener{
		key: key, databaseURL: "postgres://listener.test/old-database",
		refs: 2, cancel: oldCancel, ready: make(chan struct{}), connected: true,
	}
	close(oldListener.ready)
	manager.active[key] = oldListener

	runtime.projectMu.Lock()
	runtime.config.TenantDatabases["project-a:tenant-a"] = "postgres://listener.test/new-database"
	runtime.projectMu.Unlock()

	ready := manager.acquire(key.project, key.tenant)
	manager.mu.Lock()
	current := manager.active[key]
	manager.mu.Unlock()
	if current == oldListener {
		t.Fatal("database route change reused a listener connected to the old tenant database")
	}
	select {
	case <-oldContext.Done():
	default:
		t.Fatal("database route change did not stop the old PostgreSQL listener")
	}
	if ready == nil || current == nil || ready != current.ready {
		t.Fatal("database route change did not publish the replacement readiness barrier")
	}
	current.cancel()
}

func TestTenantListenerReadinessResetsAcrossDisconnect(t *testing.T) {
	runtime := New(config.Config{
		TenantListenerLimit: 8,
		TenantDatabases: map[string]string{
			"tenant-a": "postgres://listener.test/database",
		},
	})
	manager := runtime.subscriptions.listeners
	key := tenantListenerKey{project: "project-a", tenant: "tenant-a"}
	listener := &tenantListener{
		key: key, databaseURL: "postgres://listener.test/database", ready: make(chan struct{}),
	}
	manager.active[key] = listener

	first := manager.acquire(key.project, key.tenant)
	if first == nil {
		t.Fatal("existing tenant listener must expose a readiness barrier")
	}
	select {
	case <-first:
		t.Fatal("a listener must not be ready before PostgreSQL LISTEN succeeds")
	default:
	}
	manager.markReady(listener)
	select {
	case <-first:
	default:
		t.Fatal("the readiness barrier did not open after LISTEN succeeded")
	}

	manager.markDisconnected(listener)
	second := manager.acquire(key.project, key.tenant)
	if second == nil || second == first {
		t.Fatal("a disconnected listener must publish a new readiness barrier")
	}
	select {
	case <-second:
		t.Fatal("new syncs must wait while the tenant listener reconnects")
	default:
	}
	manager.markReady(listener)
	select {
	case <-second:
	default:
		t.Fatal("reconnected listener did not release waiting syncs")
	}
}

func TestFreshnessActionsAreSerializedWithListenerDisconnect(t *testing.T) {
	runtime := New(config.Config{
		TenantListenerLimit: 8,
		TenantDatabases: map[string]string{
			"tenant-a": "postgres://listener.test/database",
		},
	})
	manager := runtime.subscriptions.listeners
	key := tenantListenerKey{project: "project-a", tenant: "tenant-a"}
	ready := make(chan struct{})
	close(ready)
	listener := &tenantListener{key: key, ready: ready, connected: true}
	manager.active[key] = listener

	actions := 0
	if _, ok := manager.whileConnected(key.project, key.tenant, func() { actions++ }); !ok {
		t.Fatal("a connected listener must permit a freshness-sensitive action")
	}
	manager.markDisconnected(listener)
	next, ok := manager.whileConnected(key.project, key.tenant, func() { actions++ })
	if ok || next == nil || next == ready {
		t.Fatal("a stale readiness barrier must not permit a late authoritative action")
	}
	if actions != 1 {
		t.Fatalf("freshness action ran %d times, want exactly once before disconnect", actions)
	}
}

func TestSyncAttachFailsClosedWithoutLiveListener(t *testing.T) {
	runtime := New(config.Config{TenantListenerLimit: 0})
	connection := &wsConn{
		server:  runtime,
		project: "project-a",
		tenant:  "tenant-a",
		syncs:   map[string]*syncSubscription{},
	}
	subscription := &syncSubscription{
		conn: connection, id: "sync-a", path: "tasks.sync",
		project: connection.project, tenant: connection.tenant,
	}

	if err := connection.attachSync(context.Background(), subscription); err == nil {
		t.Fatal("sync must remain non-authoritative when no live listener can close the handoff race")
	}
	if len(connection.syncs) != 0 || subscription.listenerAcquired {
		t.Fatal("failed listener acquisition must not attach an apparently live subscription")
	}
}

func TestTenantListenersShareOneConnectionPerDatabase(t *testing.T) {
	runtime := New(config.Config{
		TenantListenerLimit:       8,
		TenantListenerIdleTimeout: time.Hour,
		TenantDatabases: map[string]string{
			"project-a:tenant-a": "postgres://listener.test/shared",
			"project-b:tenant-a": "postgres://listener.test/shared",
		},
	})
	manager := runtime.subscriptions.listeners
	primaryReady := manager.acquire("project-a", "tenant-a")
	aliasReady := manager.acquire("project-b", "tenant-a")
	primary := tenantListenerKey{project: "project-a", tenant: "tenant-a"}
	alias := tenantListenerKey{project: "project-b", tenant: "tenant-a"}

	manager.mu.Lock()
	connections := len(manager.connections)
	connection := manager.active[primary].connection
	shared := connection != nil && manager.active[alias].connection == connection
	manager.mu.Unlock()
	if connections != 1 || !shared {
		t.Fatalf("projects routed to one tenant database opened %d LISTEN connections, want 1 shared", connections)
	}

	manager.markConnectionReady(connection, false)
	for name, ready := range map[string]<-chan struct{}{"primary": primaryReady, "alias": aliasReady} {
		select {
		case <-ready:
		default:
			t.Fatalf("%s listener did not become ready with the shared LISTEN connection", name)
		}
	}
	manager.markConnectionDisconnected(connection)
	if manager.healthy(primary.project, primary.tenant) || manager.healthy(alias.project, alias.tenant) {
		t.Fatal("a lost shared LISTEN connection must make every member unhealthy")
	}
	recoveries := manager.markConnectionReady(connection, true)
	if len(recoveries) != 2 {
		t.Fatalf("reconnect recovered %d members, want both", len(recoveries))
	}

	for _, key := range []tenantListenerKey{alias, primary} {
		manager.mu.Lock()
		listener := manager.active[key]
		manager.mu.Unlock()
		manager.release(key.project, key.tenant)
		manager.expire(key, listener)
		manager.mu.Lock()
		remaining := len(manager.connections)
		manager.mu.Unlock()
		if key == alias && remaining != 1 {
			t.Fatal("releasing the alias project closed the primary project's LISTEN connection")
		}
		if key == primary && remaining != 0 {
			t.Fatal("the LISTEN connection outlived its last project/tenant")
		}
	}
}

func TestSharedTenantListenerDeliversNotificationsToEveryProject(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("GONVEX_ERROR_TEST_POSTGRES_URL"))
	if databaseURL == "" {
		t.Skip("GONVEX_ERROR_TEST_POSTGRES_URL is not set")
	}
	separator := "?"
	if strings.Contains(databaseURL, "?") {
		separator = "&"
	}
	databaseURL += separator + "application_name=gonvex_shared_listener_test"
	runtime := New(config.Config{
		TenantListenerLimit:       8,
		TenantListenerIdleTimeout: time.Hour,
		TenantDatabases: map[string]string{
			"project-a:tenant-a": databaseURL,
			"project-b:tenant-a": databaseURL,
		},
	})
	manager := runtime.subscriptions.listeners
	readies := []<-chan struct{}{manager.acquire("project-a", "tenant-a"), manager.acquire("project-b", "tenant-a")}
	t.Cleanup(func() {
		manager.mu.Lock()
		for _, listener := range manager.active {
			manager.detachLocked(listener)
		}
		manager.mu.Unlock()
	})
	for _, ready := range readies {
		select {
		case <-ready:
		case <-time.After(10 * time.Second):
			t.Fatal("tenant listener did not connect")
		}
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var sessions int
	if err := db.QueryRow(
		`SELECT count(*) FROM pg_stat_activity WHERE application_name = 'gonvex_shared_listener_test' AND pid <> pg_backend_pid()`,
	).Scan(&sessions); err != nil {
		t.Fatal(err)
	}
	if sessions != 1 {
		t.Fatalf("two projects on one tenant database held %d LISTEN sessions, want 1", sessions)
	}

	if _, err := db.Exec(`SELECT pg_notify($1, $2)`, schema.NotifyChannel, `{"table":"tasks","ids":["task-1"],"operation":"update","changedColumns":["title"]}`); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		runtime.metrics.mu.Lock()
		batches := runtime.metrics.reactive.ChangeBatchesReceived
		runtime.metrics.mu.Unlock()
		if batches >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("shared listener delivered %d change batches, want one per project", batches)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestDetachedTenantListenerCancelIsIdempotent(t *testing.T) {
	runtime := New(config.Config{
		TenantListenerLimit:       8,
		TenantListenerIdleTimeout: time.Hour,
		TenantDatabases:           map[string]string{"project-a:tenant-a": "postgres://listener.test/idempotent"},
	})
	manager := runtime.subscriptions.listeners
	manager.acquire("project-a", "tenant-a")
	manager.mu.Lock()
	listener := manager.active[tenantListenerKey{project: "project-a", tenant: "tenant-a"}]
	cancel := listener.cancel
	manager.mu.Unlock()
	done := make(chan struct{})
	go func() {
		cancel()
		cancel()
		manager.mu.Lock()
		manager.detachLocked(listener)
		manager.mu.Unlock()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cancelling a detached listener deadlocked")
	}
	manager.mu.Lock()
	connections := len(manager.connections)
	manager.mu.Unlock()
	if connections != 0 {
		t.Fatalf("detached listener left %d LISTEN connections open", connections)
	}
}
