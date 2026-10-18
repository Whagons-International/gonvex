package server

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/server/internal/config"
)

func TestHostMutationMiddlewareIncludesFailuresAndRequestIdentity(t *testing.T) {
	app := gonvex.NewApp()
	var calls int
	var observed error
	app.UseMutationMiddleware(func(ctx *gonvex.MutationCtx, path string, raw json.RawMessage, next gonvex.MutationNext) (any, error) {
		calls++
		if ctx.RequestID != "req-1" || ctx.TenantID != "tenant-a" || ctx.User.ID != "user-1" {
			t.Fatalf("lost identity: %+v", ctx.RuntimeContext)
		}
		result, err := next(ctx, raw)
		observed = err
		return result, err
	})
	app.Mutation("test.write", func(*gonvex.MutationCtx, map[string]any) (any, error) { return nil, errors.New("permission denied") })
	server := NewWithApp(config.Config{}, app)
	ctx := withMutationID(context.Background(), "req-1")
	caller := callerContext{user: &gonvex.User{ID: "user-1"}}
	if _, err := server.executeTenantMutationForCaller(ctx, "project-a", "tenant-a", caller, "test.write", nil); err == nil || observed == nil || calls != 1 {
		t.Fatalf("calls=%d err=%v observed=%v", calls, err, observed)
	}
	if _, err := server.executeTenantMutationForCaller(ctx, "project-a", "tenant-a", caller, "test.missing", nil); err == nil || observed == nil || calls != 2 {
		t.Fatalf("missing mutation not observed: calls=%d err=%v", calls, err)
	}
}

func TestHostMutationMiddlewareObservesFailedCommit(t *testing.T) {
	_, fixture := mutationIdempotencyTestContext(t)
	if _, err := fixture.DB.Exec(`CREATE TABLE parent (id text PRIMARY KEY); CREATE TABLE child (id text REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)`); err != nil {
		t.Fatal(err)
	}
	app := gonvex.NewApp()
	var observed error
	app.UseMutationMiddleware(func(ctx *gonvex.MutationCtx, _ string, raw json.RawMessage, next gonvex.MutationNext) (any, error) {
		result, err := next(ctx, raw)
		observed = err
		return result, err
	})
	app.Mutation("test.write", func(ctx *gonvex.MutationCtx, _ map[string]any) (any, error) {
		_, err := ctx.Tx.Exec(`INSERT INTO child VALUES ('missing-parent')`)
		if err != nil {
			t.Fatalf("expected deferred constraint, got %v", err)
		}
		return "handler succeeded", nil
	})
	server := NewWithApp(config.Config{PostgresURL: fixture.DatabaseURL}, app)
	t.Cleanup(server.Close)
	if _, err := server.executeMutation(context.Background(), "project-a", "test.write", nil); err == nil || observed == nil {
		t.Fatalf("commit error not observed: err=%v observed=%v", err, observed)
	}
	var count int
	if err := fixture.DB.QueryRow(`SELECT count(*) FROM child`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed commit persisted row: count=%d err=%v", count, err)
	}
}
