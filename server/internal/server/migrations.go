package server

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/server/internal/schema"
	"github.com/gonvex/gonvex/server/internal/sqlmigration"
)

type projectMigrationResult struct {
	Landlord sqlmigration.Result `json:"landlord"`
	Tenants  sqlmigration.Result `json:"tenants"`
}

func (s *Server) emptyColumnDropOptions(ctx context.Context, project string, desired manifest.Schema) (schema.ApplyOptions, schema.ApplyOptions, error) {
	if !s.config.DropEmptyUndeclaredColumns {
		return schema.ApplyOptions{}, schema.ApplyOptions{}, nil
	}
	landlord, err := schema.EmptyUndeclaredColumns(ctx, s.databaseURLForProject(project), desired.LandlordSchema())
	if err != nil {
		return schema.ApplyOptions{}, schema.ApplyOptions{}, fmt.Errorf("inspect landlord empty columns: %w", err)
	}
	targets := s.projectTenantTargets(ctx, project)
	sets := make([]map[string]bool, 0, len(targets))
	for _, tenant := range targets {
		candidates, inspectErr := schema.EmptyUndeclaredColumns(ctx, tenant.databaseURL, desired.TenantSchema())
		if inspectErr != nil {
			return schema.ApplyOptions{}, schema.ApplyOptions{}, fmt.Errorf("inspect tenant %s empty columns: %w", tenant.ID, inspectErr)
		}
		sets = append(sets, candidates)
	}
	return schema.ApplyOptions{DropEmptyUndeclaredColumns: landlord}, schema.ApplyOptions{DropEmptyUndeclaredColumns: intersectColumnSets(sets)}, nil
}

func intersectColumnSets(sets []map[string]bool) map[string]bool {
	result := map[string]bool{}
	for _, set := range sets {
		for key, empty := range set {
			if empty {
				result[key] = true
			}
		}
	}
	for key := range result {
		for _, set := range sets {
			if empty, exists := set[key]; exists && !empty {
				delete(result, key)
				break
			}
		}
	}
	return result
}

func migrationsFromBundle(bundle *manifest.SourceBundle) ([]sqlmigration.Migration, error) {
	if bundle == nil {
		return nil, nil
	}
	files := map[string][]byte{}
	for file, encoded := range bundle.Files {
		if path.Dir(file) != "migrations" || !strings.HasSuffix(file, ".sql") {
			continue
		}
		contents, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, fmt.Errorf("decode migration %s: %w", file, err)
		}
		files[path.Base(file)] = contents
	}
	return sqlmigration.Parse(files)
}

// projectTenantTargets returns exactly the databases a deploy's schema apply
// reaches. Migrations and column inspection must never run on a database the
// schema step skipped: a lower-priority duplicate registration of the same
// tenant database alias (e.g. a legacy tenant id pointing at an older copy)
// never receives new tables, so a migration indexing one fails the deploy.
func (s *Server) projectTenantTargets(ctx context.Context, project string) []tenantTarget {
	s.hydrateProjectTenantDatabases(ctx, project)
	s.projectMu.RLock()
	defer s.projectMu.RUnlock()
	return tenantTargetsForProject(s.tenants, project)
}

func tenantTargetsForProject(registered map[string]tenantTarget, project string) []tenantTarget {
	tenants := make([]tenantTarget, 0, len(registered))
	for _, tenant := range registered {
		if tenant.ProjectID == project {
			tenants = append(tenants, tenant)
		}
	}
	result := schemaTenantTargets(tenants)
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

// schemaTenantTargets is the one selection shared by the schema apply and SQL
// migrations: one target per tenant database alias (highest priority wins),
// then one per database URL.
func schemaTenantTargets(tenants []tenantTarget) []tenantTarget {
	result := make([]tenantTarget, 0, len(tenants))
	seen := map[string]bool{}
	for _, tenant := range dedupeTenantTargets(tenants) {
		if tenant.databaseURL == "" || seen[tenant.databaseURL] {
			continue
		}
		seen[tenant.databaseURL] = true
		result = append(result, tenant)
	}
	return result
}

type migrationApplyFunc func(context.Context, string, []sqlmigration.Migration, bool) (sqlmigration.Result, error)

func (s *Server) applyProjectSQLMigrations(ctx context.Context, project string, migrations []sqlmigration.Migration, dryRun bool) (projectMigrationResult, error) {
	result := projectMigrationResult{}
	landlord := sqlmigration.Filter(migrations, sqlmigration.ScopeLandlord)
	var err error
	result.Landlord, err = sqlmigration.Apply(ctx, s.databaseURLForProject(project), landlord, dryRun)
	if err != nil {
		return result, fmt.Errorf("landlord database migration failed: %w", err)
	}
	for _, name := range result.Landlord.Applied {
		slog.Info("applied SQL migration", "project", project, "scope", "landlord", "migration", name)
	}
	tenantResult, err := applyTenantSQLMigrations(ctx, s.projectTenantTargets(ctx, project), sqlmigration.Filter(migrations, sqlmigration.ScopeTenant), dryRun, sqlmigration.Apply)
	result.Tenants = tenantResult
	for _, name := range tenantResult.Applied {
		slog.Info("applied SQL migration", "project", project, "scope", "tenant", "migration", name)
	}
	return result, err
}

func applyTenantSQLMigrations(ctx context.Context, tenants []tenantTarget, migrations []sqlmigration.Migration, dryRun bool, apply migrationApplyFunc) (sqlmigration.Result, error) {
	if len(migrations) == 0 || len(tenants) == 0 {
		return sqlmigration.Result{}, nil
	}
	type outcome struct {
		result sqlmigration.Result
		err    error
	}
	outcomes := make([]outcome, len(tenants))
	jobs := make(chan int)
	var workers sync.WaitGroup
	workers.Add(min(tenantSchemaApplyConcurrency, len(tenants)))
	for range min(tenantSchemaApplyConcurrency, len(tenants)) {
		go func() {
			defer workers.Done()
			for index := range jobs {
				outcomes[index].result, outcomes[index].err = apply(ctx, tenants[index].databaseURL, migrations, dryRun)
			}
		}()
	}
	for index := range tenants {
		jobs <- index
	}
	close(jobs)
	workers.Wait()
	result := sqlmigration.Result{}
	var failures []error
	for index, tenant := range tenants {
		if outcomes[index].err != nil {
			// Same rule as the schema apply: a registration whose database is gone
			// is skipped rather than failing the whole deploy.
			if isMissingTenantDatabaseError(outcomes[index].err) {
				slog.Warn("skipped SQL migrations for missing tenant database", "tenant", tenant.ID)
				continue
			}
			databaseName := databaseNameFromURL(tenant.databaseURL, tenant.databaseName)
			failures = append(failures, fmt.Errorf("tenant %s database %s migration failed: %w", tenant.ID, databaseName, outcomes[index].err))
			continue
		}
		for _, name := range outcomes[index].result.Applied {
			result.Applied = append(result.Applied, tenant.ID+": "+name)
		}
		for _, name := range outcomes[index].result.Pending {
			result.Pending = append(result.Pending, tenant.ID+": "+name)
		}
	}
	return result, errors.Join(failures...)
}
