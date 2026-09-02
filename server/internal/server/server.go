package server

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/pkg/projectbundle"
	"github.com/gonvex/gonvex/pkg/storage"
	"github.com/gonvex/gonvex/server/internal/config"
	"github.com/gonvex/gonvex/server/internal/data"
	"github.com/gonvex/gonvex/server/internal/datafiles"
	"github.com/gonvex/gonvex/server/internal/runtime"
	"github.com/gonvex/gonvex/server/internal/schema"
	"golang.org/x/sync/singleflight"
)

type Server struct {
	ctx             context.Context
	cancel          context.CancelFunc
	config          config.Config
	runtime         *runtime.Runtime
	app             *gonvex.App
	storage         *storage.Factory
	dataFiles       *datafiles.Manager
	tenantStores    *tenantStoreResolver
	ephemeral       ephemeralBackend
	cache           *rowsCache
	admission       *queryAdmission
	metrics         *runtimeMetrics
	scheduler       *scheduler
	telemetryWrites chan struct{}
	telemetryDBMu   sync.Mutex
	telemetryDBs    map[string]*sql.DB
	// Lazily initialized under mutationIdempotencyMu; both maps key on the
	// tenant database URL.
	mutationIdempotencyMu       sync.Mutex
	mutationIdempotencyReady    map[string]bool
	mutationIdempotencySweptAt  map[string]time.Time
	mutationIdempotencyInstalls singleflight.Group
	subscriptionTelemetry       chan []transactionTelemetryEntry
	projectMu                   sync.RWMutex
	projects                    map[string]projectTarget
	tenants                     map[string]tenantTarget
	// explicitTenantDatabases is the immutable deployment-level routing map.
	// Registry hydration may enrich tenant metadata, but must not replace an
	// operator-provided database endpoint for the same project/tenant key.
	explicitTenantDatabases map[string]string
	registryMu              sync.Mutex
	registryReady           bool
	authRegistryMu          sync.Mutex
	authRegistryDB          *sql.DB
	tenantHydrationMu       sync.Mutex
	tenantHydrationAt       map[string]time.Time
	tenantHydrations        singleflight.Group
	wsMu                    sync.RWMutex
	wsConns                 map[*wsConn]bool
	wsConnectionSeq         atomic.Uint64
	queryBatchMu            sync.Mutex
	queryBatchTimer         *time.Timer
	queryBatchConns         map[*wsConn]struct{}
	resourceMu              sync.Mutex
	resourceSampleAt        time.Time
	resourceCPUSeconds      float64
	resourceCPUPercent      float64
	subscriptions           *subscriptionManager
	tableChangeMu           sync.Mutex
	tableChangeWait         map[string]*time.Timer
	tableChanges            map[string]pendingTableChange
	projectEnvMu            sync.Mutex
	projectEnvCache         map[string]projectEnvCacheEntry
	projectEnvLoads         singleflight.Group
	tenantProvisions        singleflight.Group
	provisionTenant         func(context.Context, string, manifest.Schema) error
	// syncLocks serializes /dev/sync work per project so overlapping syncs
	// (e.g. a failed-then-retried push, or a client that fires twice) can't run
	// catalog DDL concurrently and trip "tuple concurrently updated".
	syncLockMu sync.Mutex
	syncLocks  map[string]*sync.Mutex
	// schemaHash records the fingerprint of the schema last applied to each
	// project's database, so an unchanged sync skips the trigger/DDL reapply.
	schemaHashMu          sync.Mutex
	schemaHash            map[string]string
	queryCacheStartedAtMS int64
	queryCacheSequence    atomic.Uint64
	errorTracker          *errorTracker
	runtimeErrors         chan runtimeLogEntry
	googleKeys            googleKeyCache
	firebaseKeys          googleKeyCache
	authRateLimiter       appAuthRateLimiter
	appAuthConfigMu       sync.Mutex
	appAuthRequirements   map[string]appAuthRequirementCacheEntry
	appAuthLookups        map[string]*appAuthRequirementLookup
	appAuthVersions       map[string]uint64
	syncPruneMu           sync.Mutex
	syncPrunedAt          map[string]time.Time
	runtimeHydrationMu    sync.RWMutex
	runtimeHydrationFails map[string]struct{}
	runtimeHydrationReady atomic.Bool
	telegramAlerts        *telegramAlertManager
}

func New(cfg config.Config) *Server {
	return NewWithApp(cfg, nil)
}

// NewRequired constructs a production runtime and verifies its mandatory
// Valkey dependency before any server background work starts.
func NewRequired(cfg config.Config) (*Server, error) {
	return NewRequiredWithApp(cfg, nil)
}

// NewRequiredWithApp is the app-aware production constructor. New and
// NewWithApp remain lightweight constructors for isolated package tests; all
// executable/runtime entry points use this fail-fast constructor.
func NewRequiredWithApp(cfg config.Config, app *gonvex.App) (*Server, error) {
	client, err := openRequiredValkey(cfg.ValkeyURL)
	if err != nil {
		return nil, err
	}
	ephemeral := &valkeyEphemeralBackend{client: client}
	cache := newRowsCacheWithClient(client, cfg.RowsCacheTTL)
	server := newServer(cfg, app, ephemeral, cache)
	legacyScheduler := newValkeyScheduledJobStore(client)
	postgresScheduler := server.postgresScheduledJobStore()
	if postgresScheduler != nil {
		migrationContext, cancelMigration := context.WithTimeout(context.Background(), 30*time.Second)
		migrationErr := legacyScheduler.refreshCompletionMarkers(migrationContext)
		cancelMigration()
		if migrationErr != nil {
			server.Close()
			_ = client.Close()
			return nil, fmt.Errorf("prepare scheduled job migration: %w", migrationErr)
		}
	}
	server.scheduler.store = newMigratingScheduledJobStore(
		postgresScheduler,
		legacyScheduler,
	)
	server.scheduler.start(server.ctx)
	go server.hydrateRuntimeState(server.ctx)
	return server, nil
}

func NewWithApp(cfg config.Config, app *gonvex.App) *Server {
	var cache *rowsCache
	var ephemeral ephemeralBackend
	var legacyScheduler legacyScheduledJobStore
	if strings.TrimSpace(cfg.ValkeyURL) != "" {
		client, err := newValkeyClient(cfg.ValkeyURL)
		if err != nil {
			slog.Warn("ephemeral store unavailable in lightweight constructor", "error", err)
		} else {
			cache = newRowsCacheWithClient(client, cfg.RowsCacheTTL)
			ephemeral = &valkeyEphemeralBackend{client: client}
			legacyScheduler = newValkeyScheduledJobStore(client)
		}
	}
	server := newServer(cfg, app, ephemeral, cache)
	// Lightweight constructors are used by isolated tests and local embedding.
	// They do not require the production readiness barrier or distributed
	// scheduler lease.
	server.runtimeHydrationReady.Store(true)
	if legacyScheduler != nil {
		postgresScheduler := server.postgresScheduledJobStore()
		if postgresScheduler != nil {
			migrationContext, cancelMigration := context.WithTimeout(context.Background(), 5*time.Second)
			if err := legacyScheduler.refreshCompletionMarkers(migrationContext); err != nil {
				slog.Warn("prepare scheduled job migration in lightweight runtime", "error", err)
			}
			cancelMigration()
		}
		server.scheduler.store = newMigratingScheduledJobStore(postgresScheduler, legacyScheduler)
	}
	server.scheduler.start(server.ctx)
	go server.hydrateRuntimeState(server.ctx)
	return server
}

func (s *Server) postgresScheduledJobStore() *postgresScheduledJobStore {
	if strings.TrimSpace(s.projectRegistryURL()) == "" {
		return nil
	}
	return newPostgresScheduledJobStore(func(ctx context.Context) (*sql.DB, error) {
		return s.pooledProjectRegistry(ctx)
	})
}

func newServer(cfg config.Config, app *gonvex.App, ephemeral ephemeralBackend, cache *rowsCache) *Server {
	if app == nil {
		app = gonvex.NewApp()
	}
	serverContext, cancel := context.WithCancel(context.Background())
	server := &Server{
		ctx:     serverContext,
		cancel:  cancel,
		config:  cfg,
		runtime: runtime.NewWithLoader(projectbundle.NewLoader(cfg.PluginCacheDir, cfg.GonvexModuleRoot)),
		app:     app,
		storage: storage.NewFactory(storage.Config{
			Endpoint:        cfg.S3Endpoint,
			Region:          cfg.S3Region,
			Bucket:          cfg.S3Bucket,
			AccessKeyID:     cfg.S3AccessKeyID,
			SecretAccessKey: cfg.S3SecretAccessKey,
			ForcePathStyle:  cfg.S3ForcePathStyle,
			PublicBaseURL:   cfg.StoragePublicURL,
			URLSigningKey:   cfg.S3SecretAccessKey,
		}),
		ephemeral:             ephemeral,
		cache:                 cache,
		metrics:               newRuntimeMetrics(cfg.TelemetryLogPath),
		telemetryWrites:       make(chan struct{}, 4),
		telemetryDBs:          map[string]*sql.DB{},
		subscriptionTelemetry: make(chan []transactionTelemetryEntry, 8192),
		projects:              map[string]projectTarget{},
		tenants:               map[string]tenantTarget{},
		tenantHydrationAt:     map[string]time.Time{},
		wsConns:               map[*wsConn]bool{},
		tableChangeWait:       map[string]*time.Timer{},
		tableChanges:          map[string]pendingTableChange{},
		syncLocks:             map[string]*sync.Mutex{},
		schemaHash:            map[string]string{},
		queryCacheStartedAtMS: time.Now().UTC().UnixMilli(),
		errorTracker:          newErrorTracker(10000),
		appAuthRequirements:   map[string]appAuthRequirementCacheEntry{},
		appAuthLookups:        map[string]*appAuthRequirementLookup{},
		appAuthVersions:       map[string]uint64{},
		syncPrunedAt:          map[string]time.Time{},
		runtimeHydrationFails: map[string]struct{}{},
		provisionTenant:       provisionTenantDatabase,
	}
	server.dataFiles = datafiles.NewManager(os.Getenv("GONVEX_DATA_DIR"))
	server.admission = newQueryAdmission(cfg.SubscriptionRerunConcurrency, cfg.QueryBootstrapConcurrency)
	server.metrics.admissionSource = server.admission.snapshot
	server.subscriptions = newSubscriptionManager(server)
	server.scheduler = newScheduler(server.runScheduledJob)
	server.tenantStores = newTenantStoreResolver(&server.config)
	server.startRuntimeErrorCapture()
	server.telegramAlerts = newTelegramAlertManager(cfg)
	go server.telegramAlerts.run(server.ctx)
	go server.runSubscriptionTelemetry()
	server.metrics.onFunctionError = server.queueRuntimeFunctionError
	if strings.TrimSpace(server.projectRegistryURL()) != "" {
		server.metrics.startMutationLogPersistence(postgresRuntimeMutationLogStore{server: server})
	}
	server.loadConfiguredTenantDatabases()
	server.startLandlordMigrations()
	server.startLoadSampler(server.ctx)
	return server
}

// runScheduledJob is the scheduler's executor: it dispatches a due job through
// the same mutation/action execution path as client-triggered calls, so
// scheduled work shows up in the function and concurrency metrics too.
func (s *Server) runScheduledJob(ctx context.Context, job scheduledJob) error {
	ctx = withMutationID(ctx, job.ID)
	app := s.appForProject(ctx, job.ProjectID)
	function, ok := app.Lookup(job.FunctionPath)
	if !ok {
		return fmt.Errorf("scheduled function %q is not registered", job.FunctionPath)
	}
	switch function.Kind {
	case gonvex.FunctionKindAction:
		_, err := s.executeTenantAction(ctx, job.ProjectID, job.TenantID, job.FunctionPath, job.Args)
		// Scheduled work commits outside a client call, so nothing else tells
		// subscribers about its writes — broadcast like ws.go does for
		// client-initiated mutation.call/action.call.
		if err == nil {
			s.broadcastMutationInvalidationsForCommitAt(job.ProjectID, job.TenantID, job.FunctionPath, job.ID, time.Now().UTC())
		}
		return err
	case gonvex.FunctionKindMutation:
		_, err := s.executeTenantMutation(ctx, job.ProjectID, job.TenantID, job.FunctionPath, job.Args)
		if err == nil {
			s.broadcastMutationInvalidationsForCommitAt(job.ProjectID, job.TenantID, job.FunctionPath, job.ID, time.Now().UTC())
		}
		return err
	case gonvex.FunctionKindInternalMutation:
		err := s.executeScheduledInternalMutation(ctx, job)
		if err == nil {
			s.broadcastMutationInvalidationsForCommitAt(job.ProjectID, job.TenantID, job.FunctionPath, job.ID, time.Now().UTC())
		}
		return err
	default:
		return fmt.Errorf("scheduled function %q must be a mutation or action, got %s", job.FunctionPath, function.Kind)
	}
}

// executeScheduledInternalMutation runs an internal mutation from the scheduler.
// Internal mutations aren't reachable from clients, so they're dispatched here
// rather than through executeTenantMutation, but still get metrics and a
// surrounding transaction.
func (s *Server) executeScheduledInternalMutation(ctx context.Context, job scheduledJob) (err error) {
	const kind = "internalMutation"
	s.metrics.recordFunctionStart(kind)
	started := time.Now()
	defer func() {
		s.metrics.recordFunctionEnd(kind)
		s.metrics.recordFunction(job.ProjectID, job.FunctionPath, kind, time.Since(started), err)
	}()

	app := s.appForProject(ctx, job.ProjectID)
	mutationCtx, ctxErr := s.mutationContext(ctx, job.ProjectID, job.TenantID, callerContext{})
	if ctxErr != nil {
		return ctxErr
	}
	_, err = s.runMutationInTx(mutationCtx, job.FunctionPath, job.Args, app.ExecuteInternalMutation)
	return err
}

// registerProjectCrons mirrors a project's declared crons into the scheduler.
// Safe to call repeatedly; unchanged crons keep their run history.
func (s *Server) registerProjectCrons(projectID string) {
	if s.scheduler == nil {
		return
	}
	app := s.runtime.AppForProject(projectID)
	if app == nil {
		return
	}
	s.hydrateProjectTenantDatabases(context.Background(), projectID)
	s.projectMu.RLock()
	tenantIDs := make([]string, 0)
	for _, tenant := range s.tenants {
		if tenant.ProjectID == projectID &&
			(tenant.Provisioned || strings.TrimSpace(tenant.databaseURL) != "") &&
			strings.TrimSpace(tenant.ID) != "" {
			tenantIDs = append(tenantIDs, tenant.ID)
		}
	}
	s.projectMu.RUnlock()
	s.scheduler.syncCrons(projectID, app.Crons(), tenantIDs...)
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /storage/{key...}", s.handleStorageProxy)
	mux.HandleFunc("POST /storage/{key...}", s.handleStorageUpload)
	mux.HandleFunc("PUT /storage/{key...}", s.handleStorageUpload)
	mux.HandleFunc("GET /dev/manifest", s.handleManifest)
	mux.HandleFunc("GET /dev/metrics", s.handleMetrics)
	mux.HandleFunc("GET /dev/metrics/stream", s.handleMetricsStream)
	mux.HandleFunc("DELETE /dev/cache", s.handleClearCache)
	mux.HandleFunc("DELETE /dev/logs", s.handleClearLogs)
	mux.HandleFunc("GET /dev/logs/stream", s.handleLogStream)
	mux.HandleFunc("POST /dev/auth/login", s.handleDashboardLogin)
	mux.HandleFunc("GET /dev/auth/me", s.handleAccountIdentity)
	mux.HandleFunc("GET /dev/auth/tokens", s.handleAccountTokens)
	mux.HandleFunc("POST /dev/auth/tokens", s.handleAccountTokens)
	mux.HandleFunc("DELETE /dev/auth/tokens/{token}", s.handleRevokeAccountToken)
	mux.HandleFunc("GET /dev/auth/users", s.handleDashboardUsers)
	mux.HandleFunc("POST /dev/auth/users", s.handleDashboardUsers)
	mux.HandleFunc("GET /dev/auth/notifications", s.handleListNotifications)
	mux.HandleFunc("POST /dev/auth/notifications/read", s.handleReadNotifications)
	mux.HandleFunc("GET /dev/projects", s.handleProjects)
	mux.HandleFunc("POST /dev/projects", s.handleCreateProject)
	mux.HandleFunc("PATCH /dev/projects/{project}", s.handleUpdateProject)
	mux.HandleFunc("GET /dev/projects/{project}/members", s.handleProjectMembers)
	mux.HandleFunc("POST /dev/projects/{project}/invitations", s.handleCreateProjectInvitation)
	mux.HandleFunc("POST /dev/projects/{project}/key", s.handleProjectKey)
	mux.HandleFunc("POST /dev/projects/{project}/key/rotate", s.handleRotateProjectKey)
	mux.HandleFunc("GET /dev/projects/{project}/env", s.handleProjectEnv)
	mux.HandleFunc("POST /dev/projects/{project}/env", s.handleSetProjectEnv)
	mux.HandleFunc("PUT /dev/projects/{project}/env", s.handleBulkProjectEnv)
	mux.HandleFunc("DELETE /dev/projects/{project}/env", s.handleDeleteProjectEnv)
	mux.HandleFunc("GET /dev/projects/{project}/auth/google", s.handleProjectGoogleAuth)
	mux.HandleFunc("PUT /dev/projects/{project}/auth/google", s.handleProjectGoogleAuth)
	mux.HandleFunc("DELETE /dev/projects/{project}/auth/google", s.handleProjectGoogleAuth)
	mux.HandleFunc("GET /dev/projects/{project}/auth/users", s.handleProjectAuthUsers)
	mux.HandleFunc("PATCH /dev/projects/{project}/auth/users/{user}", s.handleProjectAuthUser)
	mux.HandleFunc("DELETE /dev/projects/{project}/auth/users/{user}", s.handleProjectAuthUser)
	mux.HandleFunc("GET /dev/projects/{project}/auth/memberships", s.handleProjectAuthMemberships)
	mux.HandleFunc("PUT /dev/projects/{project}/auth/memberships", s.handleProjectAuthMemberships)
	mux.HandleFunc("DELETE /dev/projects/{project}/auth/memberships", s.handleProjectAuthMemberships)
	mux.HandleFunc("GET /dev/projects/{project}/auth/tenants", s.handleProjectAuthTenants)
	mux.HandleFunc("POST /dev/projects/{project}/auth/tenants", s.handleProjectAuthTenants)
	mux.HandleFunc("DELETE /dev/projects/{project}", s.handleDeleteProject)
	mux.HandleFunc("GET /dev/tenants", s.handleTenants)
	mux.HandleFunc("POST /dev/tenants", s.handleCreateTenant)
	mux.HandleFunc("DELETE /dev/tenants/{tenant}", s.handleDeleteTenant)
	mux.HandleFunc("GET /dev/storage/files", s.handleStorageFiles)
	mux.HandleFunc("GET /dev/data/tables", s.handleDataTables)
	mux.HandleFunc("GET /dev/data/tables/{table}/rows", s.handleDataRows)
	mux.HandleFunc("POST /dev/data/tables/{table}/rows", s.handleInsertDataRow)
	mux.HandleFunc("PATCH /dev/data/tables/{table}/rows/{row}", s.handleUpdateDataRow)
	mux.HandleFunc("DELETE /dev/data/tables/{table}/rows/{row}", s.handleDeleteDataRow)
	mux.HandleFunc("POST /dev/data/references/replace", s.handleReplaceDataReferences)
	mux.HandleFunc("POST /dev/sync", s.handleDevSync)
	mux.HandleFunc("GET /auth/config", s.handleAuthConfig)
	mux.HandleFunc("GET /auth/google/authorize", s.handleGoogleAuthorize)
	mux.HandleFunc("GET /auth/google/callback", s.handleGoogleCallback)
	mux.HandleFunc("POST /auth/token", s.handleAppAuthToken)
	mux.HandleFunc("POST /auth/logout", s.handleAppAuthLogout)
	mux.HandleFunc("GET /auth/me", s.handleAppAuthMe)
	mux.HandleFunc("GET /auth/tenants", s.handleAppAuthTenants)
	mux.HandleFunc("POST /auth/tenants", s.handleAppAuthTenants)
	mux.HandleFunc("GET /auth/tenants/{tenant}/members", s.handleAppAuthTenantMembers)
	mux.HandleFunc("POST /auth/tenants/{tenant}/members", s.handleAppAuthTenantMembers)
	mux.HandleFunc("DELETE /auth/tenants/{tenant}/members/{member}", s.handleDeleteAppAuthTenantMember)
	mux.HandleFunc("DELETE /auth/tenants/{tenant}/invitations/{email}", s.handleDeleteAppAuthTenantInvitation)
	mux.HandleFunc("POST /errors/register", s.handleErrorRegistration)
	mux.HandleFunc("POST /errors/envelope", s.handleErrorEnvelope)
	mux.HandleFunc("GET /dev/errors/status", s.handleErrorStatus)
	mux.HandleFunc("GET /dev/errors/groups", s.handleErrorGroups)
	mux.HandleFunc("GET /dev/errors/groups/{fingerprint}", s.handleErrorGroup)
	mux.HandleFunc("PATCH /dev/errors/groups/{fingerprint}", s.handleUpdateErrorGroup)
	mux.HandleFunc("GET /dev/errors/groups/{fingerprint}/bug-report", s.handleErrorBugReport)
	mux.HandleFunc("GET /ws", s.handleWebSocket)
	mux.HandleFunc("/", s.handleRegisteredHTTP)
	return withGzip(withJSON(s.withDashboardProjectAuth(mux)))
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	failedManifests := s.runtimeHydrationFailureCount()
	hydrated := s.runtimeHydrationReady.Load()
	status := http.StatusOK
	if !hydrated || failedManifests > 0 {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, map[string]any{
		"ok":          hydrated && failedManifests == 0,
		"version":     runtimeBuildVersion(),
		"time":        time.Now().UTC().Format(time.RFC3339Nano),
		"postgresSet": s.config.PostgresURL != "",
		"valkeySet":   s.config.ValkeyURL != "",
		"rowsCache":   s.cache.enabled(),
		"s3Set":       s.storage != nil,
		"googleAuth": map[string]any{
			"ready": s.googleAuthBrokerReady(), "callbackUrl": s.configuredGoogleCallbackURL(),
			"issues": s.googleAuthReadinessIssues(),
		},
		"runtimeManifests": map[string]any{
			"ready":          hydrated && failedManifests == 0,
			"failedProjects": failedManifests,
		},
	})
}

func (s *Server) handleManifest(w http.ResponseWriter, r *http.Request) {
	project := projectID(r)
	if err := s.requireProjectDatabase(project); err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	s.hydrateRuntimeStateForProject(r.Context(), project)
	writeJSON(w, http.StatusOK, s.runtime.ManifestForProject(project))
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.metricsSnapshot(r.Context(), projectID(r)))
}

func (s *Server) metricsSnapshot(ctx context.Context, project string) runtimeMetricsSnapshot {
	websocket := s.websocketSnapshot(project)
	s.hydrateRuntimeStateForProject(ctx, project)
	s.metrics.recordDatabase(project, s.tenantStores.DatabaseStats(project))
	snapshot := s.metrics.snapshot(s.runtime.ManifestForProject(project), websocket.Connections, websocket.Subscriptions, project)
	snapshot.WebSocket = websocket
	snapshot.Resources = s.resourceSnapshot(websocket)
	if s.scheduler != nil {
		schedulerSnapshot := s.scheduler.snapshot()
		snapshot.Scheduler = &schedulerSnapshot
	}
	return snapshot
}

func (s *Server) handleClearLogs(w http.ResponseWriter, r *http.Request) {
	cleared := s.metrics.clearLogs(projectID(r))
	writeJSON(w, http.StatusOK, map[string]int{"cleared": cleared})
}

func (s *Server) handleClearCache(w http.ResponseWriter, r *http.Request) {
	project := projectID(r)
	cleared := s.cache.clearProject(r.Context(), project)
	writeJSON(w, http.StatusOK, map[string]any{"cleared": cleared, "project": project})
}

// internalDataTable reports runtime-owned tables that should not be browsed as
// project data. The prefixes are reserved for Gonvex registry/auth tables and
// internal metadata such as _gonvex_files.
func internalDataTable(name string) bool {
	return name == "telemetry_events" || strings.HasPrefix(name, "gonvex_") || strings.HasPrefix(name, "_gonvex_")
}

func (s *Server) handleDataTables(w http.ResponseWriter, r *http.Request) {
	project := projectID(r)
	started := time.Now()
	var opErr error
	defer func() {
		s.metrics.recordRuntimeOperation(project, "dev.data.tables", "runtime", time.Since(started), opErr, "")
	}()

	databaseURL, err := s.dataRequestDatabaseURL(r)
	if err != nil {
		opErr = err
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	tables, err := data.ListTables(r.Context(), databaseURL)
	if err != nil {
		opErr = err
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	visible := tables[:0]
	for _, table := range tables {
		if internalDataTable(table.Name) {
			continue
		}
		visible = append(visible, table)
	}
	writeJSON(w, http.StatusOK, map[string]any{"tables": visible})
}

func (s *Server) handleDataRows(w http.ResponseWriter, r *http.Request) {
	project := projectID(r)
	started := time.Now()
	cacheOutcome := ""
	var opErr error
	defer func() {
		s.metrics.recordRuntimeOperation(project, "dev.data.rows", "runtime", time.Since(started), opErr, cacheOutcome)
	}()

	databaseURL, err := s.dataRequestDatabaseURL(r)
	if err != nil {
		opErr = err
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	table := r.PathValue("table")
	if internalDataTable(table) {
		opErr = fmt.Errorf("table not found")
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "table not found"})
		return
	}
	tenant := tenantIDFromRequest(project, tenantID(r))
	if s.cache.enabled() {
		key := s.cache.rowsKey(r.Context(), project, tenant, table, r.URL.Query())
		if payload, ok := s.cache.get(r.Context(), key); ok {
			cacheOutcome = "hit"
			s.metrics.recordCache(project, "hit")
			w.Header().Set("content-type", "application/json")
			w.Header().Set("x-gonvex-cache", "hit")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(payload)
			return
		}
		cacheOutcome = "miss"
		s.metrics.recordCache(project, "miss")
		w.Header().Set("x-gonvex-cache", "miss")
	} else {
		cacheOutcome = "bypass"
		s.metrics.recordCache(project, "bypass")
		w.Header().Set("x-gonvex-cache", "bypass")
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	filters, err := parseRowsFilters(r.URL.Query().Get("filters"))
	if err != nil {
		opErr = err
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	countMode := r.URL.Query().Get("count")
	result, err := data.ReadRows(r.Context(), databaseURL, table, data.RowsOptions{
		Limit:           limit,
		Offset:          offset,
		Search:          r.URL.Query().Get("search"),
		SortColumn:      r.URL.Query().Get("sort"),
		SortDirection:   r.URL.Query().Get("direction"),
		Filters:         filters,
		Columns:         parseColumns(r.URL.Query().Get("columns")),
		ExactTotal:      countMode != "false" && countMode != "estimate",
		EstimateTotal:   countMode == "estimate",
		CursorCreatedAt: r.URL.Query().Get("cursorCreatedAt"),
		CursorID:        r.URL.Query().Get("cursorId"),
	})
	if err != nil {
		opErr = err
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	payload, err := json.Marshal(result)
	if err != nil {
		opErr = err
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if s.cache.enabled() {
		s.cache.set(r.Context(), s.cache.rowsKey(r.Context(), project, tenant, table, r.URL.Query()), payload)
	}
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(payload)
}

func parseColumns(raw string) []string {
	if raw == "" {
		return nil
	}
	columns := strings.Split(raw, ",")
	for index, column := range columns {
		columns[index] = strings.TrimSpace(column)
	}
	return columns
}

func parseRowsFilters(raw string) ([]data.RowsFilter, error) {
	if raw == "" {
		return nil, nil
	}
	var filters []data.RowsFilter
	if err := json.Unmarshal([]byte(raw), &filters); err != nil {
		return nil, err
	}
	return filters, nil
}

func (s *Server) handleInsertDataRow(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if internalDataTable(r.PathValue("table")) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "table not found"})
		return
	}
	databaseURL, err := s.dataRequestDatabaseURL(r)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}

	var payload map[string]any
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	result, err := data.InsertRow(r.Context(), databaseURL, r.PathValue("table"), payload)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	s.broadcastTenantTableChange(projectID(r), tenantIDFromRequest(projectID(r), tenantID(r)), r.PathValue("table"))
	writeJSON(w, http.StatusCreated, result)
}

func (s *Server) handleUpdateDataRow(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if !s.authorizeDashboardDataWrite(w, r) {
		return
	}
	table := r.PathValue("table")
	if internalDataTable(table) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "table not found"})
		return
	}
	var payload map[string]any
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if len(payload) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "at least one value is required"})
		return
	}
	databaseURL, err := s.dataRequestDatabaseURL(r)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	result, err := data.UpdateRow(r.Context(), databaseURL, table, r.PathValue("row"), payload)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	project := projectID(r)
	s.broadcastTenantTableChange(project, tenantIDFromRequest(project, tenantID(r)), table)
	writeJSON(w, http.StatusOK, result)
}

// authorizeDashboardDataWrite accepts the runtime admin key used by trusted
// automation and the project owner/admin dashboard sessions used by the Data
// editor. Read-only project members must not be able to change application
// rows just because they can inspect the project dashboard.
func (s *Server) authorizeDashboardDataWrite(w http.ResponseWriter, r *http.Request) bool {
	if s.acceptsAdminKey(syncKey(r)) {
		return true
	}
	actor, ok := s.dashboardActorFromRequest(r)
	if !ok {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "project owner or admin access is required"})
		return false
	}
	project := projectID(r)
	if project == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "project id is required"})
		return false
	}
	if !s.canManageProject(r.Context(), actor, project) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "project owner or admin access is required"})
		return false
	}
	return true
}

func (s *Server) handleDeleteDataRow(w http.ResponseWriter, r *http.Request) {
	if !s.acceptsAdminKey(syncKey(r)) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "runtime admin key is required"})
		return
	}
	table := r.PathValue("table")
	if internalDataTable(table) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "table not found"})
		return
	}
	databaseURL, err := s.dataRequestDatabaseURL(r)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	result, err := data.DeleteRow(r.Context(), databaseURL, table, r.PathValue("row"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	project := projectID(r)
	s.broadcastTenantTableChange(project, tenantIDFromRequest(project, tenantID(r)), table)
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleReplaceDataReferences(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if !s.acceptsAdminKey(syncKey(r)) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "runtime admin key is required"})
		return
	}
	var payload struct {
		Replacements map[string]string `json:"replacements"`
		DryRun       bool              `json:"dryRun"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if len(payload.Replacements) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "at least one replacement is required"})
		return
	}
	if len(payload.Replacements) > 10_000 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "too many replacements"})
		return
	}
	databaseURL, err := s.dataRequestDatabaseURL(r)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	result, err := data.ReplaceReferences(
		r.Context(),
		databaseURL,
		payload.Replacements,
		payload.DryRun,
	)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if !payload.DryRun {
		project := projectID(r)
		tenant := tenantIDFromRequest(project, tenantID(r))
		changedTables := map[string]bool{}
		for _, column := range result.Columns {
			if changedTables[column.Table] {
				continue
			}
			changedTables[column.Table] = true
			s.broadcastTenantTableChange(project, tenant, column.Table)
		}
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleDevSync(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	// Surface every sync attempt (success and failure) in the project Logs tab,
	// so a failing `gonvex dev` sync is visible in the dashboard and not only on
	// the developer's terminal.
	started := time.Now()
	logProject := ""
	var syncErr error
	defer func() {
		if logProject != "" {
			s.metrics.recordRuntimeOperation(logProject, "dev.sync", "runtime", time.Since(started), syncErr, "")
		}
	}()

	// Per-project auth: the sync uploads source the runtime compiles and runs,
	// so it must present the target project's own key. Hydrate the project first
	// so its key is loaded, then require it. Falls back to the global
	// GONVEX_DEV_SYNC_KEY only for projects that have no key yet.
	syncProjectID := strings.TrimSpace(r.Header.Get("x-gonvex-project-id"))
	logProject = syncProjectID
	if syncProjectID != "" {
		s.hydrateRuntimeStateForProject(r.Context(), syncProjectID)
	}
	if !s.acceptsSyncKey(syncProjectID, syncKey(r)) {
		syncErr = fmt.Errorf("invalid Gonvex sync key")
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid Gonvex sync key"})
		return
	}

	var next manifest.Manifest
	if err := json.NewDecoder(r.Body).Decode(&next); err != nil {
		syncErr = err
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	if next.Functions == nil {
		next.Functions = map[string]manifest.FunctionEntry{}
	}
	if next.Project == "" {
		next.Project = r.Header.Get("x-gonvex-project-id")
	}
	if next.Project != "" {
		logProject = next.Project
	}
	if headerProject := r.Header.Get("x-gonvex-project-id"); headerProject != "" && next.Project != "" && headerProject != next.Project {
		syncErr = fmt.Errorf("manifest project does not match x-gonvex-project-id")
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "manifest project does not match x-gonvex-project-id"})
		return
	}
	if err := s.requireProjectDatabase(next.Project); err != nil {
		syncErr = err
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	if next.Schema.Tables == nil {
		next.Schema = manifest.EmptySchema()
	}
	// Database artifact compatibility is owned by this runtime, not by the CLI
	// that happened to generate the incoming manifest. Always stamp the current
	// version so an older client cannot accidentally suppress a required trigger
	// upgrade.
	next.NotifySchemaVersion = manifest.NotifySchemaVersion

	// Serialize per project: schema.Apply reinstalls NOTIFY triggers via
	// DROP/CREATE TRIGGER + CREATE OR REPLACE FUNCTION, which update pg_catalog
	// rows. Two overlapping syncs (or a sync racing live query traffic) trip
	// Postgres' "tuple concurrently updated". One sync at a time per project.
	lock := s.projectSyncLock(next.Project)
	lock.Lock()
	defer lock.Unlock()

	sqlMigrations, err := migrationsFromBundle(next.Bundle)
	if err != nil {
		syncErr = err
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
		return
	}
	dryRun := r.URL.Query().Get("dryRun") == "true"
	var sqlMigrationResult projectMigrationResult
	if dryRun {
		sqlMigrationResult, err = s.applyProjectSQLMigrations(r.Context(), next.Project, sqlMigrations, true)
		if err != nil {
			syncErr = err
			writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"error": err.Error(), "migrations": sqlMigrationResult})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "dryRun": true, "project": next.Project, "migrations": sqlMigrationResult})
		return
	}

	var (
		migrationResult       schema.Result
		tenantMigrationResult schema.Result
		schemaSkipped         bool
	)
	// Skip the DDL reapply when the schema is byte-identical to what we last
	// applied. This is the common dev case (editing a handler, not the schema)
	// and avoids reinstalling every table's trigger against live traffic.
	fingerprint := schemaFingerprint(next.Schema, next.Functions)
	loadedManifest := s.runtime.ManifestForProject(next.Project)
	loadedFingerprint := schemaFingerprint(loadedManifest.Schema, loadedManifest.Functions)
	syncDefinitions := manifestSyncDefinitions(next)
	unchangedSchema := !s.config.DropEmptyUndeclaredColumns && fingerprint != "" && (s.schemaFingerprintApplied(next.Project, fingerprint) || (loadedFingerprint == fingerprint && loadedManifest.NotifySchemaVersion == next.NotifySchemaVersion))
	if unchangedSchema {
		storageInstalled, storageErr := s.projectSyncStorageInstalled(r.Context(), next.Project, next.Schema, syncDefinitions)
		if storageErr != nil {
			syncErr = storageErr
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": storageErr.Error()})
			return
		}
		schemaSkipped = storageInstalled
	}
	if !schemaSkipped {
		landlordApplyOptions, tenantApplyOptions, optionErr := s.emptyColumnDropOptions(r.Context(), next.Project, next.Schema)
		if optionErr != nil {
			syncErr = optionErr
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": optionErr.Error()})
			return
		}
		landlordSyncDefinitions, definitionErr := syncDefinitionsForSchema(syncDefinitions, next.Schema.LandlordSchema())
		if definitionErr != nil {
			syncErr = definitionErr
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": definitionErr.Error()})
			return
		}
		migrationResult, err = schema.ApplyWithOptions(
			r.Context(),
			s.databaseURLForProject(next.Project),
			next.Schema.LandlordSchema(),
			landlordSyncDefinitions,
			landlordApplyOptions,
		)
		if err != nil {
			syncErr = err
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
			return
		}
		tenantMigrationResult, err = s.applyTenantSchemasForProject(r.Context(), next.Project, next.Schema, syncDefinitions, tenantApplyOptions)
		if err != nil {
			syncErr = err
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
			return
		}
		s.markSchemaFingerprint(next.Project, fingerprint)
	}
	sqlMigrationResult, err = s.applyProjectSQLMigrations(r.Context(), next.Project, sqlMigrations, false)
	if err != nil {
		syncErr = err
		writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"error": err.Error(), "migrations": sqlMigrationResult})
		return
	}

	bundleHash := ""
	if next.Bundle != nil {
		bundleHash = next.Bundle.Hash
	}
	slog.Info("dev sync applying manifest", "project", next.Project, "functions", len(next.Functions), "bundleHash", bundleHash)
	// Only a sync that replaces an already-loaded plugin module needs a worker
	// recycle: Go cannot unload the previous module. An unchanged bundle hash
	// loads nothing, and a first load leaves this worker current, so recycling
	// for those would only manufacture a reconnect wave. A failed load attempt
	// still recycles because plugin.Open can poison the process (see
	// projectbundle). Unsupervised/local embedding safely ignores the header.
	previousHash := ""
	if previous := s.runtime.ManifestForProject(next.Project); previous.Bundle != nil {
		previousHash = previous.Bundle.Hash
	}
	alreadyLoaded := s.runtime.AppForProject(next.Project) != nil
	bundleChanged := next.Bundle != nil && next.Bundle.Hash != previousHash
	loadAttempted := next.Bundle != nil && (bundleChanged || !alreadyLoaded)
	if err := s.syncRuntimeManifest(next); err != nil {
		if loadAttempted {
			w.Header().Set("X-Gonvex-Recycle-Worker", "1")
		}
		syncErr = err
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
		return
	}
	if alreadyLoaded && bundleChanged {
		w.Header().Set("X-Gonvex-Recycle-Worker", "1")
	}
	s.registerProjectCrons(next.Project)
	if err := s.saveRuntimeManifest(r.Context(), next); err != nil {
		syncErr = err
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	// Surface the synced project in the dashboard chooser / env UI. Dev sync
	// historically only loaded the in-memory manifest; without a registry row
	// GET /dev/projects stayed empty even though the app was healthy.
	s.ensureSyncedProjectListed(r.Context(), next.Project, syncKey(r))
	s.cache.invalidateRows(r.Context(), next.Project, tenantIDFromRequest(next.Project, ""), "")
	s.rerunProjectSubscriptions(next.Project)
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":              true,
		"project":         next.Project,
		"functionCount":   len(next.Functions),
		"schema":          migrationResult,
		"tenantSchema":    tenantMigrationResult,
		"migrations":      sqlMigrationResult,
		"schemaSkipped":   schemaSkipped,
		"runtimeReloaded": true,
	})
}

// projectSyncLock returns the mutex that serializes /dev/sync work for a project.
func (s *Server) projectSyncLock(projectID string) *sync.Mutex {
	s.syncLockMu.Lock()
	defer s.syncLockMu.Unlock()
	mu, ok := s.syncLocks[projectID]
	if !ok {
		mu = &sync.Mutex{}
		s.syncLocks[projectID] = mu
	}
	return mu
}

// schemaFingerprint hashes the desired schema so an unchanged sync can skip the
// DDL reapply. json.Marshal sorts map keys, so the output is deterministic.
func schemaFingerprint(sc manifest.Schema, functions map[string]manifest.FunctionEntry) string {
	data, err := json.Marshal(struct {
		Schema              manifest.Schema                    `json:"schema"`
		Sync                map[string]manifest.SyncDefinition `json:"sync,omitempty"`
		NotifySchemaVersion string                             `json:"notifySchemaVersion"`
	}{Schema: sc.Normalize(), Sync: manifestSyncDefinitions(manifest.Manifest{Functions: functions}), NotifySchemaVersion: schema.NotifySchemaVersion})
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// schemaFingerprintApplied reports whether fingerprint matches the schema last
// applied to this project's database.
func (s *Server) schemaFingerprintApplied(projectID, fingerprint string) bool {
	s.schemaHashMu.Lock()
	defer s.schemaHashMu.Unlock()
	return s.schemaHash[projectID] == fingerprint
}

// markSchemaFingerprint records the schema fingerprint applied to a project.
func (s *Server) markSchemaFingerprint(projectID, fingerprint string) {
	if fingerprint == "" {
		return
	}
	s.schemaHashMu.Lock()
	defer s.schemaHashMu.Unlock()
	s.schemaHash[projectID] = fingerprint
}

func syncKey(r *http.Request) string {
	if value := r.Header.Get("x-gonvex-key"); value != "" {
		return value
	}
	value := r.Header.Get("authorization")
	if strings.HasPrefix(strings.ToLower(value), "bearer ") {
		return strings.TrimSpace(value[len("Bearer "):])
	}
	return ""
}

func (s *Server) acceptsAdminKey(key string) bool {
	key = strings.TrimSpace(key)
	if key == "" {
		return false
	}
	if s.config.AdminKey != "" && key == s.config.AdminKey {
		return true
	}
	return s.config.AdminKey == "" && s.config.DevSyncKey != "" && key == s.config.DevSyncKey
}

// acceptsSyncKey gates POST /dev/sync. If the target project has a registered
// key, exactly that key is required (per-project). Otherwise it falls back to
// the global GONVEX_DEV_SYNC_KEY, and if neither is configured the endpoint is
// open (local dev only).
func (s *Server) acceptsSyncKey(projectID, provided string) bool {
	provided = strings.TrimSpace(provided)
	s.projectMu.RLock()
	registered := ""
	if projectID != "" {
		registered = strings.TrimSpace(s.config.ProjectKeys[projectID])
	}
	s.projectMu.RUnlock()
	if registered != "" {
		return provided != "" && constantTimeString(provided, registered)
	}
	if s.config.DevSyncKey != "" {
		return provided != "" && constantTimeString(provided, s.config.DevSyncKey)
	}
	return true
}

func projectID(r *http.Request) string {
	if value := strings.TrimSpace(r.Header.Get("x-gonvex-project-id")); value != "" {
		return value
	}
	if value := strings.TrimSpace(r.URL.Query().Get("project")); value != "" {
		return value
	}
	return ""
}

func tenantID(r *http.Request) string {
	if value := strings.TrimSpace(r.Header.Get("x-gonvex-tenant-id")); value != "" {
		return value
	}
	if value := strings.TrimSpace(r.URL.Query().Get("tenant")); value != "" {
		return value
	}
	return ""
}

func (s *Server) dataRequestDatabaseURL(r *http.Request) (string, error) {
	project := projectID(r)
	if err := s.requireProjectDatabase(project); err != nil {
		return "", err
	}
	s.hydrateProjectTenantDatabases(r.Context(), project)
	databaseURL := s.databaseURLForTenant(project, tenantID(r))
	if tenant := tenantID(r); tenant != "" && databaseURL == "" {
		return "", fmt.Errorf("tenant %q is not related to project %q", tenant, project)
	}
	return databaseURL, nil
}

func (s *Server) databaseURLForProject(projectID string) string {
	s.projectMu.RLock()
	defer s.projectMu.RUnlock()
	return s.config.DatabaseURL(projectID)
}

// requireProjectDatabase preserves the zero-configuration, single-database
// local runtime while preventing a multi-project runtime from routing a typo
// or stale project id into its control database. That fallback can make a
// function appear healthy while it reads an empty set of landlord tables.
func (s *Server) requireProjectDatabase(projectID string) error {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return nil
	}

	configured := func() (string, bool, int) {
		s.projectMu.RLock()
		defer s.projectMu.RUnlock()
		databaseURL, exists := s.config.ProjectDatabases[projectID]
		return strings.TrimSpace(databaseURL), exists, len(s.config.ProjectDatabases)
	}

	databaseURL, exists, count := configured()
	if exists && databaseURL != "" {
		return nil
	}
	// Always re-read the project registry on a miss. Projects can be created in the
	// dashboard (or seed SQL) while this runtime is already up with other projects
	// loaded — without this, /dev/sync keeps 409'ing until a full restart.
	if !exists {
		s.hydrateProjects()
		databaseURL, exists, count = configured()
		if exists && databaseURL != "" {
			return nil
		}
	}
	if count == 0 {
		return nil
	}
	if exists {
		return fmt.Errorf("project %q is registered without a database", projectID)
	}
	return fmt.Errorf("project %q is not registered with a database; use the same Gonvex project id for the client and deploy", projectID)
}

func (s *Server) databaseURLForTenant(projectID string, tenantID string) string {
	s.projectMu.RLock()
	defer s.projectMu.RUnlock()
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" || tenantID == projectID {
		return s.config.DatabaseURL(projectID)
	}
	if tenant, ok := tenantForDatabaseRouting(s.tenants, projectID, tenantID); ok {
		if value := s.configuredTenantDatabaseURLLocked(projectID, tenant); value != "" {
			return value
		}
		if tenant.databaseURL != "" {
			return tenant.databaseURL
		}
	}
	if !isUUIDProjectID(projectID) {
		if value := s.configuredTenantDatabaseURLLocked(projectID, tenantTarget{ID: tenantID}); value != "" {
			return value
		}
		// Preserve the historical single-database fallback for existing project
		// IDs. UUID projects require an explicit tenant relationship and never
		// route an unknown tenant to the landlord database.
		return s.config.DatabaseURL(projectID)
	}
	return ""
}

func tenantForDatabaseRouting(tenants map[string]tenantTarget, projectID string, tenantID string) (tenantTarget, bool) {
	exact, foundExact := tenants[tenantStoreKey(projectID, tenantID)]
	if registered, foundRegistered := registeredTenantForAlias(tenants, projectID, tenantID); foundRegistered {
		return registered, true
	}
	if foundExact && exact.registered {
		return exact, true
	}
	return exact, foundExact
}

func registeredTenantForAlias(tenants map[string]tenantTarget, projectID string, alias string) (tenantTarget, bool) {
	alias = strings.TrimSpace(alias)
	if alias == "" {
		return tenantTarget{}, false
	}
	matches := []tenantTarget{}
	for _, tenant := range tenants {
		if tenant.ProjectID != projectID || !tenant.registered {
			continue
		}
		aliasMatches := strings.EqualFold(strings.TrimSpace(tenant.Database), alias) ||
			strings.EqualFold(strings.TrimSpace(tenant.domain), alias)
		if !aliasMatches {
			continue
		}
		matches = append(matches, tenant)
	}
	if len(matches) == 1 {
		return matches[0], true
	}
	var legacy tenantTarget
	legacyCount := 0
	for _, tenant := range matches {
		if isLegacyConvexDocumentID(tenant.ID) {
			legacy = tenant
			legacyCount++
		}
	}
	if legacyCount == 1 {
		return legacy, true
	}
	return tenantTarget{}, false
}

func (s *Server) hydrateRuntimeState(ctx context.Context) {
	defer s.runtimeHydrationReady.Store(true)
	// Resolve every project's database + key from the control plane so
	// databaseURLForProject works right after a restart, without waiting for
	// something to list projects first.
	s.hydrateProjects()
	manifests, err := s.loadRuntimeManifests(ctx)
	if err != nil {
		s.markRuntimeHydrationFailure("__catalog__")
		slog.Debug("load persisted Gonvex runtime manifests", "error", err)
		return
	}
	s.clearRuntimeHydrationFailure("__catalog__")
	for _, next := range manifests {
		if err := s.syncRuntimeManifest(next); err != nil {
			slog.Warn("load persisted Gonvex runtime manifest", "project", next.Project, "error", err)
			continue
		}
		// Only seed the skip cache when the persisted manifest proves the current
		// database-artifact version was installed. A runtime upgrade may need to
		// redefine sync infrastructure even when schema and functions are unchanged.
		if next.NotifySchemaVersion == manifest.NotifySchemaVersion {
			s.markSchemaFingerprint(next.Project, schemaFingerprint(next.Schema, next.Functions))
		}
		s.registerProjectCrons(next.Project)
	}
}

func (s *Server) hydrateRuntimeStateForProject(ctx context.Context, projectID string) {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return
	}
	// Projects are created dynamically, so resolve this project's database from
	// the control plane (gonvex_runtime_projects) on demand if we haven't yet.
	// Without this, databaseURLForProject falls back to POSTGRES_URL and the
	// runtime reads landlord tables from the wrong (control) database. This must
	// run even when the app/manifest is already loaded, since the DB mapping is
	// independent of the compiled bundle.
	s.projectMu.RLock()
	_, haveDB := s.config.ProjectDatabases[projectID]
	s.projectMu.RUnlock()
	if !haveDB {
		s.hydrateProjects()
	}
	if s.runtime.AppForProject(projectID) != nil {
		return
	}
	next, ok, err := s.loadRuntimeManifest(ctx, projectID)
	if err != nil {
		s.markRuntimeHydrationFailure(projectID)
		slog.Debug("load persisted Gonvex project runtime manifest", "project", projectID, "error", err)
		return
	}
	if !ok {
		s.clearRuntimeHydrationFailure(projectID)
		return
	}
	if err := s.syncRuntimeManifest(next); err != nil {
		slog.Warn("load persisted Gonvex project runtime manifest", "project", projectID, "error", err)
		return
	}
	if next.NotifySchemaVersion == manifest.NotifySchemaVersion {
		s.markSchemaFingerprint(projectID, schemaFingerprint(next.Schema, next.Functions))
	}
	s.registerProjectCrons(projectID)
}

func (s *Server) syncRuntimeManifest(next manifest.Manifest) error {
	if err := s.runtime.SyncManifest(next); err != nil {
		s.markRuntimeHydrationFailure(next.Project)
		return err
	}
	s.clearRuntimeHydrationFailure(next.Project)
	return nil
}

func (s *Server) markRuntimeHydrationFailure(projectID string) {
	s.runtimeHydrationMu.Lock()
	s.runtimeHydrationFails[projectID] = struct{}{}
	s.runtimeHydrationMu.Unlock()
}

func (s *Server) clearRuntimeHydrationFailure(projectID string) {
	s.runtimeHydrationMu.Lock()
	delete(s.runtimeHydrationFails, projectID)
	s.runtimeHydrationMu.Unlock()
}

func (s *Server) runtimeHydrationFailureCount() int {
	s.runtimeHydrationMu.RLock()
	defer s.runtimeHydrationMu.RUnlock()
	return len(s.runtimeHydrationFails)
}

func (s *Server) appForProject(ctx context.Context, projectID string) *gonvex.App {
	s.hydrateRuntimeStateForProject(ctx, projectID)
	if app := s.runtime.AppForProject(projectID); app != nil {
		return app
	}
	return s.app
}

func (s *Server) configuredTenantDatabaseURLLocked(projectID string, tenant tenantTarget) string {
	if s.config.TenantDatabases == nil {
		return ""
	}
	for _, candidate := range tenantLookupCandidates(tenant) {
		if candidate == "" {
			continue
		}
		if value := s.config.TenantDatabases[tenantStoreKey(projectID, candidate)]; value != "" {
			return value
		}
		if !isUUIDProjectID(projectID) {
			if value := s.config.TenantDatabases[candidate]; value != "" {
				return value
			}
		}
	}
	if !isUUIDProjectID(projectID) {
		needles := tenantDatabaseNeedles(tenant)
		for key, value := range s.config.TenantDatabases {
			configuredProject, _ := splitTenantDatabaseKey(key)
			if configuredProject != "" && configuredProject != projectID {
				continue
			}
			normalizedDatabase := normalizeDatabaseAlias(databaseNameFromURL(value, ""))
			for _, needle := range needles {
				if needle != "" && strings.Contains(normalizedDatabase, needle) {
					return value
				}
			}
		}
	}
	return ""
}

func tenantLookupCandidates(tenant tenantTarget) []string {
	return uniqueStrings([]string{
		tenant.ID,
		tenant.Database,
		tenant.domain,
		slug(tenant.Name),
		strings.ReplaceAll(slug(tenant.Name), "_", "-"),
	})
}

func tenantDatabaseNeedles(tenant tenantTarget) []string {
	values := []string{tenant.ID, tenant.Database, tenant.domain, tenant.Name}
	needles := make([]string, 0, len(values))
	for _, value := range values {
		needle := normalizeDatabaseAlias(value)
		if len(needle) >= 4 {
			needles = append(needles, needle)
		}
	}
	return uniqueStrings(needles)
}

func normalizeDatabaseAlias(value string) string {
	return strings.ToLower(strings.NewReplacer("-", "", "_", "", " ", "").Replace(strings.TrimSpace(value)))
}

func uniqueStrings(values []string) []string {
	seen := map[string]bool{}
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		result = append(result, value)
	}
	return result
}

func withJSON(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("access-control-allow-origin", "*")
		w.Header().Set("access-control-allow-headers", "content-type, authorization, x-api-key, x-gonvex-key, x-gonvex-project-id, x-gonvex-tenant-id")
		w.Header().Set("access-control-allow-methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		if strings.HasPrefix(r.URL.Path, "/auth/") {
			w.Header().Set("cache-control", "no-store")
			w.Header().Set("pragma", "no-cache")
			w.Header().Set("referrer-policy", "no-referrer")
			w.Header().Set("x-content-type-options", "nosniff")
			w.Header().Set("x-frame-options", "DENY")
			w.Header().Set("content-security-policy", "default-src 'none'; frame-ancestors 'none'")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
