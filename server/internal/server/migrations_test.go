package server

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/gonvex/gonvex/server/internal/sqlmigration"
)

func TestTenantSQLMigrationsContinueAfterFailure(t *testing.T) {
	tenants := []tenantTarget{{ID: "a", databaseURL: "db-a"}, {ID: "b", databaseURL: "db-b"}, {ID: "c", databaseURL: "db-c"}}
	called := map[string]bool{}
	var calledMu sync.Mutex
	result, err := applyTenantSQLMigrations(context.Background(), tenants, []sqlmigration.Migration{{Name: "0001_x.sql"}}, false,
		func(_ context.Context, databaseURL string, _ []sqlmigration.Migration, _ bool) (sqlmigration.Result, error) {
			calledMu.Lock()
			called[databaseURL] = true
			calledMu.Unlock()
			if databaseURL == "db-b" {
				return sqlmigration.Result{}, errors.New("broken")
			}
			return sqlmigration.Result{Applied: []string{"0001_x.sql"}}, nil
		})
	if err == nil || !strings.Contains(err.Error(), "tenant b") {
		t.Fatalf("expected labeled aggregate error, got %v", err)
	}
	if len(called) != 3 {
		t.Fatalf("failure stopped fleet: %#v", called)
	}
	if len(result.Applied) != 2 {
		t.Fatalf("successful tenants not reported: %#v", result)
	}
}

func TestIntersectColumnSetsRequiresEveryTenant(t *testing.T) {
	got := intersectColumnSets([]map[string]bool{{"items.absent_elsewhere": true, "items.blocked": true, "items.all": true}, {"items.blocked": false, "items.all": true}})
	if !got["items.absent_elsewhere"] || got["items.blocked"] || !got["items.all"] {
		t.Fatalf("unexpected intersection: %#v", got)
	}
}

func TestMigrationTargetsMatchSchemaTargets(t *testing.T) {
	// A legacy registration shares the live tenant's database alias but points
	// at an older database copy. The schema apply skips it, so migrations must
	// too, or they index tables that were never created there.
	registered := map[string]tenantTarget{
		"live":      {ID: "yx7d1n0f", ProjectID: "p", Database: "arenal-paraiso", databaseURL: "db-live", Description: "Persisted tenant from landlord database."},
		"shadow":    {ID: "arenal-paraiso", ProjectID: "p", Database: "arenal-paraiso", databaseURL: "db-shadow"},
		"other":     {ID: "el-rey", ProjectID: "p", Database: "el-rey", databaseURL: "db-el-rey", Description: "Persisted tenant from landlord database."},
		"elsewhere": {ID: "x", ProjectID: "q", Database: "x", databaseURL: "db-x"},
	}
	var schemaURLs []string
	for _, tenant := range schemaTenantTargets([]tenantTarget{registered["live"], registered["shadow"], registered["other"]}) {
		schemaURLs = append(schemaURLs, tenant.databaseURL)
	}
	var migrationURLs []string
	for _, tenant := range tenantTargetsForProject(registered, "p") {
		migrationURLs = append(migrationURLs, tenant.databaseURL)
	}
	sort.Strings(schemaURLs)
	sort.Strings(migrationURLs)
	if strings.Join(migrationURLs, ",") != "db-el-rey,db-live" || strings.Join(schemaURLs, ",") != strings.Join(migrationURLs, ",") {
		t.Fatalf("migration targets %v must equal schema targets %v", migrationURLs, schemaURLs)
	}
}

func TestTenantSQLMigrationsSkipMissingDatabases(t *testing.T) {
	tenants := []tenantTarget{{ID: "gone", databaseURL: "db-gone"}, {ID: "ok", databaseURL: "db-ok"}}
	result, err := applyTenantSQLMigrations(context.Background(), tenants, []sqlmigration.Migration{{Name: "0001_x.sql"}}, false,
		func(_ context.Context, databaseURL string, _ []sqlmigration.Migration, _ bool) (sqlmigration.Result, error) {
			if databaseURL == "db-gone" {
				return sqlmigration.Result{}, errors.New(`pq: database "gone" does not exist`)
			}
			return sqlmigration.Result{Applied: []string{"0001_x.sql"}}, nil
		})
	if err != nil || len(result.Applied) != 1 {
		t.Fatalf("missing database must be skipped, got %v %#v", err, result)
	}
}
