package gonvex

import (
	"context"
	"encoding/json"
)

// MutationNext executes a mutation with sanitized arguments and the supplied context.
type MutationNext func(*MutationCtx, json.RawMessage) (any, error)

// MutationMiddleware encloses argument decoding and execution. Hosts use
// RunMutation around their transaction so middleware sees the final commit outcome.
type MutationMiddleware func(*MutationCtx, string, json.RawMessage, MutationNext) (any, error)

// UseMutationMiddleware installs app-wide request middleware during registration.
func (a *App) UseMutationMiddleware(middleware MutationMiddleware) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.mutationMiddleware = middleware
}

type mutationMiddlewareKey struct{ app *App }

// RunMutation runs middleware once around a host operation, including failures
// before dispatch or after handler execution. Nested ExecuteMutation calls in
// that operation reuse the middleware context instead of logging a second outcome.
func (a *App) RunMutation(ctx *MutationCtx, path string, raw json.RawMessage, next MutationNext) (any, error) {
	if ctx == nil {
		ctx = &MutationCtx{}
	}
	ctx.normalize()
	a.mu.RLock()
	middleware := a.mutationMiddleware
	a.mu.RUnlock()
	key := mutationMiddlewareKey{app: a}
	if middleware == nil || ctx.Context.Value(key) != nil {
		return next(ctx, raw)
	}
	copy := *ctx
	copy.Context = context.WithValue(ctx.Context, key, true)
	return middleware(&copy, path, raw, next)
}
