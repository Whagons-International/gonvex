package gonvex

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"
)

type FunctionKind string

const (
	FunctionKindQuery            FunctionKind = "query"
	FunctionKindMutation         FunctionKind = "mutation"
	FunctionKindAction           FunctionKind = "action"
	FunctionKindHTTP             FunctionKind = "http"
	FunctionKindInternalMutation FunctionKind = "internalMutation"
	FunctionKindLiveGrid         FunctionKind = "liveGrid"
	FunctionKindSync             FunctionKind = "sync"
)

type App struct {
	mu        sync.RWMutex
	functions map[string]Function
	crons     []CronSpec
}

type Function struct {
	Path         string
	Kind         FunctionKind
	Public       bool
	Dependencies FunctionDependencies
	Handler      any
	ArgType      reflect.Type
	ResultType   reflect.Type
	Sync         *SyncDefinition

	handlerValue reflect.Value
}

// SyncDefinition describes a durable entity collection. Sync handlers return
// the initial authorized snapshot; subsequent inserts, updates, and deletes are
// delivered from Gonvex's transactional change log.
type SyncDefinition struct {
	Table            string
	Key              string
	Columns          []string
	EqualFilters     map[string]string
	ExcludeWhenSet   []string
	VisibilityTables []string
	OrderBy          string
	OrderDirection   string
	Mode             string
	MaxRows          int
	MaxBytes         int64
	Retention        time.Duration
}

type SyncTableOption struct {
	definition SyncDefinition
}

// SyncTable begins a single-table sync definition. Key defaults to "id".
func SyncTable(table string) *SyncTableOption {
	return &SyncTableOption{definition: SyncDefinition{
		Table:        strings.TrimSpace(table),
		Key:          "id",
		Mode:         "eager",
		EqualFilters: map[string]string{},
	}}
}

func (o *SyncTableOption) Key(column string) *SyncTableOption {
	o.definition.Key = strings.TrimSpace(column)
	return o
}

func (o *SyncTableOption) Columns(columns ...string) *SyncTableOption {
	o.definition.Columns = cleanDependencyNames(columns)
	return o
}

// EqualArg binds a table column to a same-named or explicit sync argument.
func (o *SyncTableOption) EqualArg(column string, argument ...string) *SyncTableOption {
	if o.definition.EqualFilters == nil {
		o.definition.EqualFilters = map[string]string{}
	}
	column = strings.TrimSpace(column)
	arg := column
	if len(argument) > 0 && strings.TrimSpace(argument[0]) != "" {
		arg = strings.TrimSpace(argument[0])
	}
	if column != "" && arg != "" {
		o.definition.EqualFilters[column] = arg
	}
	return o
}

// ExcludeWhenSet removes rows whose named column is non-null. It is intended
// for soft-delete and archive markers that the snapshot handler excludes too.
func (o *SyncTableOption) ExcludeWhenSet(columns ...string) *SyncTableOption {
	o.definition.ExcludeWhenSet = cleanDependencyNames(columns)
	return o
}

func (o *SyncTableOption) VisibilityDependsOn(tables ...string) *SyncTableOption {
	o.definition.VisibilityTables = cleanDependencyNames(tables)
	return o
}

func (o *SyncTableOption) OrderBy(column, direction string) *SyncTableOption {
	o.definition.OrderBy = strings.TrimSpace(column)
	direction = strings.ToLower(strings.TrimSpace(direction))
	if direction != "asc" {
		direction = "desc"
	}
	o.definition.OrderDirection = direction
	return o
}

func (o *SyncTableOption) Eager() *SyncTableOption {
	o.definition.Mode = "eager"
	return o
}

func (o *SyncTableOption) Progressive() *SyncTableOption {
	o.definition.Mode = "progressive"
	return o
}

func (o *SyncTableOption) Budget(maxRows int, maxBytes int64) *SyncTableOption {
	o.definition.MaxRows = maxRows
	o.definition.MaxBytes = maxBytes
	return o
}

func (o *SyncTableOption) RetainFor(duration time.Duration) *SyncTableOption {
	o.definition.Retention = duration
	return o
}

// FunctionDependencies are explicit invalidation and sharing declarations for
// a registered function. ShareByPermissions is opt-in: without it the runtime
// includes the user identity in a shared-subscription key, which is the safe
// default for handlers that inspect ctx.User.
type FunctionDependencies struct {
	Reads                []ReadDependency
	Writes               []WriteDependency
	ReadsEphemeral       bool
	WritesEphemeral      bool
	ShareByPermissions   bool
	ShareByVisibility    string
	ShareResultFrom      string
	ShareResultField     string
	OptimisticMutation   *OptimisticMutationDefinition
	OptimisticProjection *OptimisticProjectionDefinition
}

// OptimisticMutationDefinition tells generated clients how mutation arguments
// identify and patch one entity. It is client metadata only; authorization and
// authoritative writes remain entirely server-side.
type OptimisticMutationDefinition struct {
	Entity     string
	RowIDPath  []string
	FieldsPath []string
}

// OptimisticProjectionDefinition identifies an entity collection nested in a
// query result so the client can materialize the same pending entity mutations
// into live subscriptions and durable syncs.
type OptimisticProjectionDefinition struct {
	Entity     string
	Key        string
	ResultPath []string
}

type ReadDependency struct {
	Table     string
	Columns   []string
	Filters   []string
	OrdersBy  []string
	Windowed  bool
	Predicate string
}

type WriteDependency struct {
	Table   string
	Columns []string
}

type FunctionOption interface {
	applyFunctionOption(*FunctionDependencies)
}

type optimisticMutationOption struct{ definition OptimisticMutationDefinition }

// OptimisticMutation declares the entity patched by a mutation. RowIDArg and
// FieldsArg accept dotted paths for nested argument objects.
func OptimisticMutation(entity string) *optimisticMutationOption {
	return &optimisticMutationOption{definition: OptimisticMutationDefinition{Entity: strings.TrimSpace(entity)}}
}

func (o *optimisticMutationOption) RowIDArg(path string) *optimisticMutationOption {
	o.definition.RowIDPath = cleanOptimisticPath(path)
	return o
}

func (o *optimisticMutationOption) FieldsArg(path string) *optimisticMutationOption {
	o.definition.FieldsPath = cleanOptimisticPath(path)
	return o
}

func (o *optimisticMutationOption) applyFunctionOption(target *FunctionDependencies) {
	definition := o.definition
	definition.RowIDPath = append([]string(nil), definition.RowIDPath...)
	definition.FieldsPath = append([]string(nil), definition.FieldsPath...)
	target.OptimisticMutation = &definition
}

type optimisticProjectionOption struct {
	definition OptimisticProjectionDefinition
}

// OptimisticProjection declares where entity rows live inside a query result.
// ResultPath accepts a dotted path; omit it for a top-level row array.
func OptimisticProjection(entity string) *optimisticProjectionOption {
	return &optimisticProjectionOption{definition: OptimisticProjectionDefinition{
		Entity: strings.TrimSpace(entity),
		Key:    "id",
	}}
}

func (o *optimisticProjectionOption) Key(key string) *optimisticProjectionOption {
	o.definition.Key = strings.TrimSpace(key)
	return o
}

func (o *optimisticProjectionOption) ResultPath(path string) *optimisticProjectionOption {
	o.definition.ResultPath = cleanOptimisticPath(path)
	return o
}

func (o *optimisticProjectionOption) applyFunctionOption(target *FunctionDependencies) {
	definition := o.definition
	definition.ResultPath = append([]string(nil), definition.ResultPath...)
	target.OptimisticProjection = &definition
}

func cleanOptimisticPath(path string) []string {
	parts := strings.Split(path, ".")
	return cleanDependencyNames(parts)
}

type readOption struct{ dependencies []ReadDependency }

// Reads declares one or more tenant tables read by a query.
func Reads(tables ...string) *readOption {
	option := &readOption{}
	for _, table := range tables {
		if table = strings.TrimSpace(table); table != "" {
			option.dependencies = append(option.dependencies, ReadDependency{Table: table})
		}
	}
	return option
}

func (o *readOption) Columns(columns ...string) *readOption {
	for index := range o.dependencies {
		o.dependencies[index].Columns = cleanDependencyNames(columns)
	}
	return o
}

func (o *readOption) Filters(columns ...string) *readOption {
	for index := range o.dependencies {
		o.dependencies[index].Filters = cleanDependencyNames(columns)
	}
	return o
}

func (o *readOption) OrdersBy(columns ...string) *readOption {
	for index := range o.dependencies {
		o.dependencies[index].OrdersBy = cleanDependencyNames(columns)
	}
	return o
}

func (o *readOption) Windowed() *readOption {
	for index := range o.dependencies {
		o.dependencies[index].Windowed = true
	}
	return o
}

// Predicate narrows reactive invalidation using runtime-supported metadata.
// resultTaskIds matches task-linked changes whose taskId is in the previous
// result. resultTaskIdsOrColumnArg:workspaceId also matches changes addressed
// to the workspaceId argument, preserving insert and routing membership.
func (o *readOption) Predicate(name string) *readOption {
	for index := range o.dependencies {
		o.dependencies[index].Predicate = strings.TrimSpace(name)
	}
	return o
}

func (o *readOption) applyFunctionOption(target *FunctionDependencies) {
	target.Reads = append(target.Reads, o.dependencies...)
}

type writeOption struct{ dependencies []WriteDependency }

// Writes declares one or more tenant tables changed by a mutation or action.
func Writes(tables ...string) *writeOption {
	option := &writeOption{}
	for _, table := range tables {
		if table = strings.TrimSpace(table); table != "" {
			option.dependencies = append(option.dependencies, WriteDependency{Table: table})
		}
	}
	return option
}

func (o *writeOption) Columns(columns ...string) *writeOption {
	for index := range o.dependencies {
		o.dependencies[index].Columns = cleanDependencyNames(columns)
	}
	return o
}

func (o *writeOption) applyFunctionOption(target *FunctionDependencies) {
	target.Writes = append(target.Writes, o.dependencies...)
}

type shareByPermissionsOption struct{}

type readsEphemeralOption struct{}

type writesEphemeralOption struct{}

// ReadsEphemeral declares that a query result observes ctx.Ephemeral. The
// runtime uses this to bypass its durable query-result cache: ephemeral values
// expire independently of Postgres table generations.
func ReadsEphemeral() FunctionOption { return readsEphemeralOption{} }

func (readsEphemeralOption) applyFunctionOption(target *FunctionDependencies) {
	target.ReadsEphemeral = true
}

// WritesEphemeral declares that a mutation or action writes only
// ctx.Ephemeral and performs no durable database writes. The runtime therefore
// does not open a Postgres transaction or invalidate reactive query caches
// after the function returns.
func WritesEphemeral() FunctionOption { return writesEphemeralOption{} }

func (writesEphemeralOption) applyFunctionOption(target *FunctionDependencies) {
	target.WritesEphemeral = true
}

// ShareByPermissions allows callers with the same permission fingerprint to
// share one server-side subscription execution and result. Only use it when a
// handler's result does not otherwise depend on ctx.User.
func ShareByPermissions() FunctionOption { return shareByPermissionsOption{} }

func (shareByPermissionsOption) applyFunctionOption(target *FunctionDependencies) {
	target.ShareByPermissions = true
}

type shareByVisibilityOption struct{ resolver string }

// ShareByVisibility shares invalidation execution only when resolver returns
// the same non-empty result for both callers. The resolver contract is strict:
// equal keys must imply equal query results for the committed state.
func ShareByVisibility(resolver string) FunctionOption {
	return shareByVisibilityOption{resolver: strings.TrimSpace(resolver)}
}

func (option shareByVisibilityOption) applyFunctionOption(target *FunctionDependencies) {
	target.ShareByVisibility = option.resolver
}

type shareResultFromOption struct{ source, field string }

// ShareResultFrom executes one canonical query and returns the named field of
// its object result. Functions using it must accept args compatible with source.
func ShareResultFrom(source, field string) FunctionOption {
	return shareResultFromOption{source: strings.TrimSpace(source), field: strings.TrimSpace(field)}
}

func (option shareResultFromOption) applyFunctionOption(target *FunctionDependencies) {
	target.ShareResultFrom, target.ShareResultField = option.source, option.field
}

func cleanDependencyNames(values []string) []string {
	clean := make([]string, 0, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			clean = append(clean, value)
		}
	}
	return clean
}

type User struct {
	ID    string
	Email string
}

type RuntimeContext struct {
	context.Context

	ProjectID   string
	TenantID    string
	User        *User
	Permissions map[string]any
	DatabaseURL string
	DB          *sql.DB
	LandlordDB  *sql.DB
	TenantDB    *sql.DB
	Tx          *sql.Tx
	Storage     StorageAPI
	Sandbox     SandboxAPI
	Data        DataAPI
	Ephemeral   EphemeralAPI
	// ProjectEphemeral is shared by every tenant in this project. Use it only
	// for deliberately cross-tenant transient state such as an operator-facing
	// live-session registry; ordinary app presence belongs in Ephemeral.
	ProjectEphemeral EphemeralAPI
	Scheduler        Scheduler
	Logger           *slog.Logger

	// NotifyTableChange, when set by the host server, broadcasts a table
	// invalidation to live query subscribers. Long-running actions call
	// NotifyTableChanged after committing writes so subscribed clients refresh
	// mid-run instead of only when the function returns.
	NotifyTableChange func(table string)

	// Env holds the project-scoped environment variables (the dashboard's
	// per-project env store), wired by the host server. Read through EnvValue
	// so process env remains the fallback.
	Env map[string]string
}

type queryChangeContextKey struct{}

type QueryChangeInfo struct {
	Reason      string
	ChangedAtMS float64
	Details     *QueryChangeDetails
}

type QueryChangeDetails struct {
	Tables        []string
	TaskIDs       []string
	VisibilityKey string
}

// WithQueryChange attaches the table-change revision that caused a reactive
// query execution. Runtime hosts use it to let application handlers safely
// coalesce common base reads across identity-scoped subscription groups.
func WithQueryChange(ctx context.Context, reason string, changedAtMS float64) context.Context {
	return WithQueryChangeDetails(ctx, reason, changedAtMS, nil, nil)
}

// WithQueryChangeDetails attaches the committed table/task keys that selected
// a reactive query. Applications may use these only as an optimization hint;
// an empty set means precision was unavailable and requires a full refresh.
func WithQueryChangeDetails(ctx context.Context, reason string, changedAtMS float64, tables, taskIDs []string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	var details *QueryChangeDetails
	if len(tables) > 0 || len(taskIDs) > 0 {
		details = &QueryChangeDetails{Tables: append([]string(nil), tables...), TaskIDs: append([]string(nil), taskIDs...)}
	}
	return context.WithValue(ctx, queryChangeContextKey{}, QueryChangeInfo{Reason: reason, ChangedAtMS: changedAtMS, Details: details})
}

func WithQueryVisibilityKey(ctx context.Context, key string) context.Context {
	info := QueryChange(ctx)
	if info.Details == nil {
		info.Details = &QueryChangeDetails{}
	} else {
		copy := *info.Details
		info.Details = &copy
	}
	info.Details.VisibilityKey = key
	return context.WithValue(ctx, queryChangeContextKey{}, info)
}

// QueryChange returns the reactive invalidation revision attached by the host.
// Initial/direct queries normally return an empty reason and a zero timestamp.
func QueryChange(ctx context.Context) QueryChangeInfo {
	if ctx == nil {
		return QueryChangeInfo{}
	}
	info, _ := ctx.Value(queryChangeContextKey{}).(QueryChangeInfo)
	return info
}

// EnvValue resolves an environment variable for the executing function:
// project-scoped values (dashboard env store) win, the process environment is
// the fallback. Nil-safe for tests and offline tools.
func (rc *RuntimeContext) EnvValue(name string) string {
	if rc != nil {
		if v, ok := rc.Env[name]; ok && strings.TrimSpace(v) != "" {
			return v
		}
	}
	return os.Getenv(name)
}

// NotifyTableChanged pushes a live-query invalidation for each table. It is a
// no-op when the host has not wired NotifyTableChange (tests, offline tools).
func (rc *RuntimeContext) NotifyTableChanged(tables ...string) {
	if rc == nil || rc.NotifyTableChange == nil {
		return
	}
	for _, table := range tables {
		rc.NotifyTableChange(table)
	}
}

type QueryCtx struct {
	RuntimeContext
}

type MutationCtx struct {
	RuntimeContext
}

type ActionCtx struct {
	RuntimeContext
}

type HTTPContext struct {
	RuntimeContext
}

type HTTPRequest struct {
	Method     string              `json:"method"`
	Path       string              `json:"path"`
	RawQuery   string              `json:"rawQuery,omitempty"`
	Query      map[string][]string `json:"query,omitempty"`
	Headers    map[string][]string `json:"headers,omitempty"`
	Body       []byte              `json:"body,omitempty"`
	RemoteAddr string              `json:"remoteAddr,omitempty"`
}

type HTTPResponse struct {
	Status  int                 `json:"status,omitempty"`
	Headers map[string][]string `json:"headers,omitempty"`
	Body    []byte              `json:"body,omitempty"`
}

func TextResponse(status int, body string, contentType string) HTTPResponse {
	if contentType == "" {
		contentType = "text/plain; charset=utf-8"
	}
	return HTTPResponse{
		Status: status,
		Headers: map[string][]string{
			"content-type": []string{contentType},
		},
		Body: []byte(body),
	}
}

func JSONResponse(status int, value any) HTTPResponse {
	body, err := json.Marshal(value)
	if err != nil {
		body = []byte(`{"error":"failed to encode response"}`)
		status = 500
	}
	return HTTPResponse{
		Status: status,
		Headers: map[string][]string{
			"content-type": []string{"application/json"},
		},
		Body: body,
	}
}

type DispatchError struct {
	Code    string
	Path    string
	Message string
	Err     error
}

func (e *DispatchError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return "gonvex dispatch error"
}

func (e *DispatchError) Unwrap() error {
	return e.Err
}

func NewApp() *App {
	return &App{functions: map[string]Function{}}
}

func (a *App) Query(path string, handler any, options ...FunctionOption) {
	a.register(FunctionKindQuery, path, handler, options...)
}

func (a *App) Mutation(path string, handler any, options ...FunctionOption) {
	a.register(FunctionKindMutation, path, handler, options...)
}

func (a *App) Action(path string, handler any, options ...FunctionOption) {
	a.register(FunctionKindAction, path, handler, options...)
}

func (a *App) HTTP(path string, handler any, options ...FunctionOption) {
	a.register(FunctionKindHTTP, path, handler, options...)
}

// PublicHTTP registers an HTTP handler that may execute without a Gonvex user
// session even when native application authentication is enabled. It is
// intended for provider-signed callbacks such as payment webhooks. The handler
// must authenticate the request itself before changing state.
func (a *App) PublicHTTP(path string, handler any, options ...FunctionOption) {
	a.registerWithVisibility(FunctionKindHTTP, path, handler, true, options...)
}

func (a *App) InternalMutation(path string, handler any, options ...FunctionOption) {
	a.register(FunctionKindInternalMutation, path, handler, options...)
}

func (a *App) LiveGrid(path string, handler any, options ...FunctionOption) {
	a.register(FunctionKindLiveGrid, path, handler, options...)
}

func (a *App) Sync(path string, handler any, sync *SyncTableOption, options ...FunctionOption) {
	if sync == nil || strings.TrimSpace(sync.definition.Table) == "" {
		panic("gonvex: sync table is required")
	}
	if strings.TrimSpace(sync.definition.Key) == "" {
		panic("gonvex: sync key is required")
	}
	if len(sync.definition.Columns) == 0 {
		panic("gonvex: sync columns are required")
	}
	a.register(FunctionKindSync, path, handler, options...)
	a.mu.Lock()
	function := a.functions[path]
	definition := sync.definition
	definition.Columns = cleanDependencyNames(append(definition.Columns, definition.Key))
	function.Sync = &definition
	a.functions[path] = function
	a.mu.Unlock()
}

func (a *App) Functions() map[string]Function {
	a.mu.RLock()
	defer a.mu.RUnlock()
	functions := make(map[string]Function, len(a.functions))
	for path, function := range a.functions {
		functions[path] = function
	}
	return functions
}

func (a *App) Lookup(path string) (Function, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	function, ok := a.functions[path]
	return function, ok
}

func (a *App) ExecuteQuery(ctx *QueryCtx, path string, rawArgs json.RawMessage) (any, error) {
	if ctx == nil {
		ctx = &QueryCtx{}
	}
	return a.execute(path, rawArgs, ctx, FunctionKindQuery, FunctionKindLiveGrid, FunctionKindSync)
}

func (a *App) ExecuteSync(ctx *QueryCtx, path string, rawArgs json.RawMessage) (any, error) {
	if ctx == nil {
		ctx = &QueryCtx{}
	}
	return a.execute(path, rawArgs, ctx, FunctionKindSync)
}

func (a *App) ExecuteMutation(ctx *MutationCtx, path string, rawArgs json.RawMessage) (any, error) {
	if ctx == nil {
		ctx = &MutationCtx{}
	}
	return a.execute(path, rawArgs, ctx, FunctionKindMutation)
}

func (a *App) ExecuteInternalMutation(ctx *MutationCtx, path string, rawArgs json.RawMessage) (any, error) {
	if ctx == nil {
		ctx = &MutationCtx{}
	}
	return a.execute(path, rawArgs, ctx, FunctionKindInternalMutation)
}

func (a *App) ExecuteAction(ctx *ActionCtx, path string, rawArgs json.RawMessage) (any, error) {
	if ctx == nil {
		ctx = &ActionCtx{}
	}
	return a.execute(path, rawArgs, ctx, FunctionKindAction)
}

func (a *App) ExecuteHTTP(ctx *HTTPContext, path string, request HTTPRequest) (HTTPResponse, error) {
	if ctx == nil {
		ctx = &HTTPContext{}
	}
	rawArgs, err := json.Marshal(request)
	if err != nil {
		return HTTPResponse{}, err
	}
	result, err := a.execute(path, rawArgs, ctx, FunctionKindHTTP)
	if err != nil {
		return HTTPResponse{}, err
	}
	response, ok := result.(HTTPResponse)
	if !ok {
		return HTTPResponse{}, &DispatchError{Code: "invalid_response", Path: path, Message: fmt.Sprintf("HTTP function %q returned %T, want gonvex.HTTPResponse", path, result)}
	}
	return response, nil
}

func (a *App) register(kind FunctionKind, path string, handler any, options ...FunctionOption) {
	a.registerWithVisibility(kind, path, handler, false, options...)
}

func (a *App) registerWithVisibility(kind FunctionKind, path string, handler any, public bool, options ...FunctionOption) {
	path = strings.TrimSpace(path)
	if path == "" {
		panic("gonvex: function path is required")
	}

	function, err := newFunction(kind, path, handler)
	if err != nil {
		panic(err)
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if a.functions == nil {
		a.functions = map[string]Function{}
	}
	if existing, ok := a.functions[path]; ok {
		panic(fmt.Sprintf("gonvex: function %q already registered as %s", path, existing.Kind))
	}
	function.Public = public
	for _, option := range options {
		if option != nil {
			option.applyFunctionOption(&function.Dependencies)
		}
	}
	a.functions[path] = function
}

func (a *App) execute(path string, rawArgs json.RawMessage, ctx any, allowedKinds ...FunctionKind) (any, error) {
	function, ok := a.Lookup(path)
	if !ok {
		return nil, &DispatchError{Code: "not_found", Path: path, Message: fmt.Sprintf("function %q is not registered", path)}
	}
	if !kindAllowed(function.Kind, allowedKinds) {
		return nil, &DispatchError{Code: "wrong_kind", Path: path, Message: fmt.Sprintf("function %q is %s, not %s", path, function.Kind, joinKinds(allowedKinds))}
	}

	if normalizer, ok := ctx.(interface{ normalize() }); ok {
		normalizer.normalize()
	}

	arg, err := decodeArg(function.ArgType, rawArgs)
	if err != nil {
		return nil, &DispatchError{Code: "invalid_args", Path: path, Message: fmt.Sprintf("invalid args for %q: %v", path, err), Err: err}
	}

	results := function.handlerValue.Call([]reflect.Value{reflect.ValueOf(ctx), arg})
	if errValue := results[1]; !errValue.IsNil() {
		return nil, errValue.Interface().(error)
	}
	return results[0].Interface(), nil
}

func newFunction(kind FunctionKind, path string, handler any) (Function, error) {
	if handler == nil {
		return Function{}, fmt.Errorf("gonvex: handler for %q is nil", path)
	}
	value := reflect.ValueOf(handler)
	typ := value.Type()
	if typ.Kind() != reflect.Func {
		return Function{}, fmt.Errorf("gonvex: handler for %q must be a function", path)
	}
	if typ.NumIn() != 2 || typ.NumOut() != 2 {
		return Function{}, fmt.Errorf("gonvex: handler for %q must be func(ctx, args) (result, error)", path)
	}
	expectedCtx := ctxTypeForKind(kind)
	if !typ.In(0).AssignableTo(expectedCtx) {
		return Function{}, fmt.Errorf("gonvex: handler for %q must accept %s as its first argument", path, expectedCtx)
	}
	errorType := reflect.TypeOf((*error)(nil)).Elem()
	if !typ.Out(1).Implements(errorType) {
		return Function{}, fmt.Errorf("gonvex: handler for %q must return error as its second result", path)
	}
	return Function{
		Path:         path,
		Kind:         kind,
		Handler:      handler,
		ArgType:      typ.In(1),
		ResultType:   typ.Out(0),
		handlerValue: value,
	}, nil
}

func ctxTypeForKind(kind FunctionKind) reflect.Type {
	switch kind {
	case FunctionKindMutation, FunctionKindInternalMutation:
		return reflect.TypeOf((*MutationCtx)(nil))
	case FunctionKindAction:
		return reflect.TypeOf((*ActionCtx)(nil))
	case FunctionKindHTTP:
		return reflect.TypeOf((*HTTPContext)(nil))
	default:
		return reflect.TypeOf((*QueryCtx)(nil))
	}
}

func decodeArg(argType reflect.Type, raw json.RawMessage) (reflect.Value, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return reflect.Zero(argType), nil
	}
	if argType.Kind() == reflect.Interface {
		var value any
		if err := decodeJSON(raw, &value, false); err != nil {
			return reflect.Value{}, err
		}
		if value == nil {
			return reflect.Zero(argType), nil
		}
		return reflect.ValueOf(value), nil
	}

	target := reflect.New(argType)
	value := target
	disallowUnknown := argType.Kind() == reflect.Struct
	if argType.Kind() == reflect.Ptr {
		value = reflect.New(argType.Elem())
		disallowUnknown = argType.Elem().Kind() == reflect.Struct
	}
	if err := decodeJSON(raw, value.Interface(), disallowUnknown); err != nil {
		return reflect.Value{}, err
	}
	if argType.Kind() == reflect.Ptr {
		return value, nil
	}
	return target.Elem(), nil
}

func decodeJSON(raw json.RawMessage, target any, disallowUnknown bool) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if disallowUnknown {
		decoder.DisallowUnknownFields()
	}
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("unexpected trailing JSON")
	}
	return nil
}

func kindAllowed(kind FunctionKind, allowed []FunctionKind) bool {
	for _, allowedKind := range allowed {
		if kind == allowedKind {
			return true
		}
	}
	return false
}

func joinKinds(kinds []FunctionKind) string {
	parts := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		parts = append(parts, string(kind))
	}
	return strings.Join(parts, " or ")
}

func (c *RuntimeContext) normalize() {
	if c.Context == nil {
		c.Context = context.Background()
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.TenantID == "" {
		c.TenantID = c.ProjectID
	}
	if c.Storage == nil {
		c.Storage = storageUnavailable{}
	}
	if c.Scheduler == nil {
		c.Scheduler = schedulerUnavailable{}
	}
	if c.Ephemeral == nil {
		c.Ephemeral = ephemeralUnavailable{}
	}
	if c.ProjectEphemeral == nil {
		c.ProjectEphemeral = ephemeralUnavailable{}
	}
}

func (c *QueryCtx) normalize() {
	c.RuntimeContext.normalize()
}

func (c *MutationCtx) normalize() {
	c.RuntimeContext.normalize()
}

func (c *ActionCtx) normalize() {
	c.RuntimeContext.normalize()
}

func (c *HTTPContext) normalize() {
	c.RuntimeContext.normalize()
}

type Schema struct{}
type Table struct{}

type ColumnOption func(*columnOptions)

type columnOptions struct {
	nullable bool
}

func Nullable(options *columnOptions) {
	options.nullable = true
}

func (s *Schema) Table(name string, define func(*Table)) {}
func (s *Schema) LandlordTable(name string, define func(*Table)) {
	s.Table(name, define)
}
func (s *Schema) TenantTable(name string, define func(*Table)) {
	s.Table(name, define)
}

func (t *Table) ID(name string)                               {}
func (t *Table) String(name string, options ...ColumnOption)  {}
func (t *Table) Text(name string, options ...ColumnOption)    {}
func (t *Table) Int(name string, options ...ColumnOption)     {}
func (t *Table) Int64(name string, options ...ColumnOption)   {}
func (t *Table) Float64(name string, options ...ColumnOption) {}
func (t *Table) Bool(name string, options ...ColumnOption)    {}
func (t *Table) Time(name string, options ...ColumnOption)    {}
func (t *Table) JSON(name string, options ...ColumnOption)    {}

func (t *Table) Index(name string, columns ...string)        {}
func (t *Table) UniqueIndex(name string, columns ...string)  {}
func (t *Table) TrigramIndex(name string, columns ...string) {}
