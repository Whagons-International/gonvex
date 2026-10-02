package server

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/config"
)

const tenantPerformanceDriverName = "gonvex-tenant-performance-test"

var registerTenantPerformanceDriver sync.Once

type tenantPerformanceDriver struct{}

type tenantPerformanceConn struct{}

type tenantPerformanceRows struct{}

func (tenantPerformanceDriver) Open(string) (driver.Conn, error) {
	return tenantPerformanceConn{}, nil
}

func (tenantPerformanceConn) Prepare(string) (driver.Stmt, error) {
	return nil, driver.ErrSkip
}

func (tenantPerformanceConn) Close() error { return nil }

func (tenantPerformanceConn) Begin() (driver.Tx, error) { return nil, driver.ErrSkip }

func (tenantPerformanceConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return tenantPerformanceRows{}, nil
}

func (tenantPerformanceRows) Columns() []string {
	return []string{"id", "name", "database", "domain"}
}

func (tenantPerformanceRows) Close() error { return nil }

func (tenantPerformanceRows) Next([]driver.Value) error { return io.EOF }

func openTenantPerformanceDB() (*sql.DB, error) {
	registerTenantPerformanceDriver.Do(func() {
		sql.Register(tenantPerformanceDriverName, tenantPerformanceDriver{})
	})
	return sql.Open(tenantPerformanceDriverName, "")
}

func TestTenantStoreResolverSharesConcurrentInitialization(t *testing.T) {
	resolver := newTenantStoreResolver(&config.Config{})
	release := make(chan struct{})
	var opens atomic.Int32
	resolver.openDatabase = func(_ context.Context, databaseURL string) (*sql.DB, error) {
		opens.Add(1)
		<-release
		return sql.Open("pgx", databaseURL)
	}

	const callers = 24
	stores := make(chan *tenantStore, callers)
	errors := make(chan error, callers)
	var ready sync.WaitGroup
	ready.Add(callers)
	start := make(chan struct{})
	for range callers {
		go func() {
			ready.Done()
			<-start
			store, err := resolver.Store(context.Background(), "project-a:tenant-a", "postgres://localhost/tenant-a")
			stores <- store
			errors <- err
		}()
	}
	ready.Wait()
	close(start)

	deadline := time.Now().Add(time.Second)
	for opens.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := opens.Load(); got != 1 {
		t.Fatalf("expected one database initialization while callers wait, got %d", got)
	}
	close(release)

	var first *tenantStore
	for range callers {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
		store := <-stores
		if first == nil {
			first = store
		} else if store != first {
			t.Fatal("concurrent callers received different tenant stores")
		}
	}
	if got := opens.Load(); got != 1 {
		t.Fatalf("expected one database initialization, got %d", got)
	}
	resolver.Close()
}

func TestProjectTenantHydrationIsCachedAndShared(t *testing.T) {
	server := &Server{tenantHydrationAt: map[string]time.Time{}}
	release := make(chan struct{})
	var hydrations atomic.Int32
	hydrate := func(context.Context, string) {
		hydrations.Add(1)
		<-release
	}

	const callers = 24
	var ready sync.WaitGroup
	ready.Add(callers)
	var completed sync.WaitGroup
	completed.Add(callers)
	start := make(chan struct{})
	for range callers {
		go func() {
			defer completed.Done()
			ready.Done()
			<-start
			server.hydrateProjectTenantDatabasesWith(context.Background(), "project-a", hydrate)
		}()
	}
	ready.Wait()
	close(start)

	deadline := time.Now().Add(time.Second)
	for hydrations.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := hydrations.Load(); got != 1 {
		t.Fatalf("expected one tenant hydration while callers wait, got %d", got)
	}
	close(release)
	completed.Wait()

	server.hydrateProjectTenantDatabasesWith(context.Background(), "project-a", func(context.Context, string) {
		hydrations.Add(1)
	})
	if got := hydrations.Load(); got != 1 {
		t.Fatalf("expected hydration result to remain cached, got %d runs", got)
	}
}

func TestProjectTenantHydrationErrorIsReturnedAndNotCached(t *testing.T) {
	server := &Server{tenantHydrationAt: map[string]time.Time{}}
	var hydrations atomic.Int32
	wantErr := errors.New("tenant discovery failed")
	hydrate := func(context.Context, string) error {
		hydrations.Add(1)
		return wantErr
	}

	for attempt := 0; attempt < 2; attempt++ {
		err := server.hydrateProjectTenantDatabasesWithError(context.Background(), "project-a", hydrate)
		if !errors.Is(err, wantErr) {
			t.Fatalf("hydration error = %v, want %v", err, wantErr)
		}
	}
	if got := hydrations.Load(); got != 2 {
		t.Fatalf("failed hydration was cached; got %d runs, want 2", got)
	}
}

func TestProjectSyncStorageCheckReturnsTenantDiscoveryFailure(t *testing.T) {
	server := newServer(config.Config{PostgresURL: "://invalid"}, nil, nil, nil)
	t.Cleanup(func() {
		server.cancel()
		server.tenantStores.Close()
	})
	desired := manifest.Schema{TenantTables: map[string]manifest.Table{
		"tasks": {Columns: map[string]manifest.Column{
			"id": {Type: "string", PrimaryKey: true},
		}},
	}}
	definitions := map[string]manifest.SyncDefinition{
		"tasks": {Table: "tasks", Key: "id", Columns: []string{"id"}},
	}

	installed, err := server.projectSchemaArtifactsInstalled(context.Background(), "project-a", desired, definitions)
	if err == nil {
		t.Fatal("tenant discovery failure was treated as installed sync storage")
	}
	if installed {
		t.Fatal("tenant discovery failure returned installed=true")
	}
}

func TestLandlordHydrationReusesLandlordAndMaintenancePools(t *testing.T) {
	resolver := newTenantStoreResolver(&config.Config{})
	var opens atomic.Int32
	resolver.openDatabase = func(context.Context, string) (*sql.DB, error) {
		opens.Add(1)
		return openTenantPerformanceDB()
	}
	server := &Server{
		config: config.Config{
			PostgresURL: "postgres://localhost/control",
			ProjectDatabases: map[string]string{
				"project-a": "postgres://localhost/project-a",
			},
		},
		tenantStores: resolver,
	}

	server.hydrateLandlordTenants(context.Background(), "project-a")
	server.hydrateLandlordTenants(context.Background(), "project-a")

	if got := opens.Load(); got != 2 {
		t.Fatalf("expected one landlord pool and one maintenance pool, got %d database opens", got)
	}
	resolver.Close()
}
