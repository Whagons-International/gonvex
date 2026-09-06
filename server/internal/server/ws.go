package server

import (
	"compress/flate"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
	"github.com/gonvex/gonvex/server/internal/data"
	"github.com/gonvex/gonvex/server/internal/sandbox"
	"github.com/gorilla/websocket"
)

type clientMessage struct {
	Type               string                  `json:"type"`
	ID                 string                  `json:"id"`
	Path               string                  `json:"path,omitempty"`
	Args               json.RawMessage         `json:"args,omitempty"`
	Token              string                  `json:"token,omitempty"`
	Project            string                  `json:"project,omitempty"`
	Tenant             string                  `json:"tenant,omitempty"`
	Trace              *messageTrace           `json:"trace,omitempty"`
	IdempotencyKey     string                  `json:"idempotencyKey,omitempty"`
	Kind               string                  `json:"kind,omitempty"`
	Reason             string                  `json:"reason,omitempty"`
	Outcome            string                  `json:"outcome,omitempty"`
	Error              string                  `json:"error,omitempty"`
	ClientSentAtMS     float64                 `json:"clientSentAtMs,omitempty"`
	ClientReceivedAtMS float64                 `json:"clientReceivedAtMs,omitempty"`
	ClientDurationMS   float64                 `json:"clientDurationMs,omitempty"`
	Device             json.RawMessage         `json:"device,omitempty"`
	Cursor             *syncCursor             `json:"cursor,omitempty"`
	Keys               []string                `json:"keys,omitempty"`
	Hashes             map[string]string       `json:"hashes,omitempty"`
	Digest             string                  `json:"digest,omitempty"`
	FullIntegrity      bool                    `json:"fullIntegrity,omitempty"`
	Opens              []syncOpenRequest       `json:"opens,omitempty"`
	CacheRevision      string                  `json:"cacheRevision,omitempty"`
	Subscribes         []querySubscribeRequest `json:"subscribes,omitempty"`
	Calls              []mutationCallRequest   `json:"calls,omitempty"`
	Capabilities       *clientCapabilities     `json:"capabilities,omitempty"`
}

// maxBatchedClientRequests bounds every batched client frame (sync.openMany,
// query.subscribeMany, mutation.callMany).
const maxBatchedClientRequests = 256

type querySubscribeRequest struct {
	ID            string          `json:"id"`
	Path          string          `json:"path"`
	Args          json.RawMessage `json:"args,omitempty"`
	CacheRevision string          `json:"cacheRevision,omitempty"`
}

type mutationCallRequest struct {
	ID    string          `json:"id"`
	Path  string          `json:"path"`
	Args  json.RawMessage `json:"args,omitempty"`
	Trace *messageTrace   `json:"trace,omitempty"`
	// IdempotencyKey marks a replayable write from the client outbox. Replays
	// reuse the key, so the runtime executes the mutation once and serves the
	// stored result to every duplicate delivery.
	IdempotencyKey string `json:"idempotencyKey,omitempty"`
}

type syncOpenRequest struct {
	ID            string            `json:"id"`
	Path          string            `json:"path"`
	Args          json.RawMessage   `json:"args,omitempty"`
	Cursor        *syncCursor       `json:"cursor,omitempty"`
	Keys          []string          `json:"keys,omitempty"`
	Hashes        map[string]string `json:"hashes,omitempty"`
	Digest        string            `json:"digest,omitempty"`
	FullIntegrity bool              `json:"fullIntegrity,omitempty"`
}

type serverCapabilities struct {
	ProtocolVersion int    `json:"protocolVersion,omitempty"`
	RuntimeVersion  string `json:"runtimeVersion,omitempty"`
	SyncBatch       int    `json:"syncBatch,omitempty"`
	SyncIntegrity   int    `json:"syncIntegrity,omitempty"`
	QueryBatch      int    `json:"queryBatch,omitempty"`
	MutationBatch   int    `json:"mutationBatch,omitempty"`
	SyncWatermark   int    `json:"syncWatermark,omitempty"`
}

type clientCapabilities struct {
	SyncReadyMany    int `json:"syncReadyMany,omitempty"`
	SyncWatermark    int `json:"syncWatermark,omitempty"`
	QueryPagePatch   int `json:"queryPagePatch,omitempty"`
	QueryObjectPatch int `json:"queryObjectPatch,omitempty"`
	QueryOrderDelta  int `json:"queryOrderDelta,omitempty"`
	QueryFanout      int `json:"queryFanout,omitempty"`
	QueryResultBatch int `json:"queryResultBatch,omitempty"`
}

type keyedCollectionPatch struct {
	Inserted []json.RawMessage `json:"inserted,omitempty"`
	Updated  []json.RawMessage `json:"updated,omitempty"`
	Deleted  []string          `json:"deleted,omitempty"`
	Order    []string          `json:"order,omitempty"`
	Prepend  []string          `json:"prepend,omitempty"`
	Append   []string          `json:"append,omitempty"`
}

type syncReadyMessage struct {
	ID        string      `json:"id"`
	Path      string      `json:"path,omitempty"`
	Cursor    *syncCursor `json:"cursor"`
	Mode      string      `json:"mode,omitempty"`
	Digest    string      `json:"digest,omitempty"`
	Truncated bool        `json:"truncated"`
}

type serverMessage struct {
	Type                 string                          `json:"type"`
	ID                   string                          `json:"id,omitempty"`
	IDs                  []string                        `json:"ids,omitempty"`
	QueryType            string                          `json:"queryType,omitempty"`
	Path                 string                          `json:"path,omitempty"`
	Project              string                          `json:"project,omitempty"`
	Tenant               string                          `json:"tenant,omitempty"`
	Result               any                             `json:"result,omitempty"`
	Error                string                          `json:"error,omitempty"`
	Reason               string                          `json:"reason,omitempty"`
	Trace                any                             `json:"trace,omitempty"`
	QueryPerf            json.RawMessage                 `json:"-"`
	QueryCache           *queryCacheDirective            `json:"queryCache,omitempty"`
	Capabilities         *serverCapabilities             `json:"capabilities,omitempty"`
	CacheScope           string                          `json:"cacheScope,omitempty"`
	CacheRevision        string                          `json:"cacheRevision,omitempty"`
	SubscriptionRevision *subscriptionRevision           `json:"subscriptionRevision,omitempty"`
	BaseRevision         *subscriptionRevision           `json:"baseRevision,omitempty"`
	ThroughRevision      *subscriptionRevision           `json:"throughRevision,omitempty"`
	Inserted             []json.RawMessage               `json:"inserted,omitempty"`
	Updated              []json.RawMessage               `json:"updated,omitempty"`
	Deleted              []string                        `json:"deleted,omitempty"`
	Order                []string                        `json:"order,omitempty"`
	Prepend              []string                        `json:"prepend,omitempty"`
	Append               []string                        `json:"append,omitempty"`
	Collections          map[string]keyedCollectionPatch `json:"collections,omitempty"`
	FullResult           json.RawMessage                 `json:"-"`
	Cursor               *syncCursor                     `json:"cursor,omitempty"`
	Key                  string                          `json:"key,omitempty"`
	OrderBy              string                          `json:"orderBy,omitempty"`
	OrderDirection       string                          `json:"orderDirection,omitempty"`
	Mode                 string                          `json:"mode,omitempty"`
	MaxRows              int                             `json:"maxRows,omitempty"`
	MaxBytes             int64                           `json:"maxBytes,omitempty"`
	Upserts              []json.RawMessage               `json:"upserts,omitempty"`
	MutationIDs          []string                        `json:"mutationIds,omitempty"`
	Hashes               map[string]string               `json:"hashes,omitempty"`
	Digest               string                          `json:"digest,omitempty"`
	Truncated            *bool                           `json:"truncated,omitempty"`
	Ready                []syncReadyMessage              `json:"ready,omitempty"`
	Messages             []serverMessage                 `json:"messages,omitempty"`
	Revision             uint64                          `json:"revision,omitempty"`
}

// explicitNull makes a nil handler result serialize as an explicit JSON null
// on *.result messages. Convex resolves null-returning functions to null;
// omitting the field (omitempty) would leave clients reading `undefined`,
// which useQuery treats as "still loading".
func explicitNull(result any) any {
	if result == nil {
		return json.RawMessage("null")
	}
	return result
}

type messageTrace struct {
	ServerSocketWriteStartedAtMS  float64         `json:"serverSocketWriteStartedAtMs,omitempty"`
	ClientSentAtMS                float64         `json:"clientSentAtMs,omitempty"`
	ServerReceivedAtMS            float64         `json:"serverReceivedAtMs,omitempty"`
	ServerMutationStartedAtMS     float64         `json:"serverMutationStartedAtMs,omitempty"`
	ServerMutationCommittedAtMS   float64         `json:"serverMutationCommittedAtMs,omitempty"`
	ServerCompletedAtMS           float64         `json:"serverCompletedAtMs,omitempty"`
	ServerBroadcastScheduledAtMS  float64         `json:"serverBroadcastScheduledAtMs,omitempty"`
	ServerChangeCommittedAtMS     float64         `json:"serverChangeCommittedAtMs,omitempty"`
	ServerSubscriptionStartedAtMS float64         `json:"serverSubscriptionStartedAtMs,omitempty"`
	ServerSubscriptionSentAtMS    float64         `json:"serverSubscriptionSentAtMs,omitempty"`
	ServerDurationMS              float64         `json:"serverDurationMs,omitempty"`
	QueryPerf                     json.RawMessage `json:"queryPerf,omitempty"`
}

type clientDeviceInfo struct {
	UserAgent               string  `json:"userAgent,omitempty"`
	BrowserName             string  `json:"browserName,omitempty"`
	BrowserVersion          string  `json:"browserVersion,omitempty"`
	DeviceType              string  `json:"deviceType,omitempty"`
	Platform                string  `json:"platform,omitempty"`
	Language                string  `json:"language,omitempty"`
	Timezone                string  `json:"timezone,omitempty"`
	ViewportWidth           int     `json:"viewportWidth,omitempty"`
	ViewportHeight          int     `json:"viewportHeight,omitempty"`
	HardwareConcurrency     int     `json:"hardwareConcurrency,omitempty"`
	DeviceMemory            float64 `json:"deviceMemory,omitempty"`
	TouchPoints             int     `json:"touchPoints,omitempty"`
	ConnectionType          string  `json:"connectionType,omitempty"`
	EffectiveConnectionType string  `json:"effectiveConnectionType,omitempty"`
}

type taskGridArgs struct {
	Offset          int               `json:"offset"`
	Limit           int               `json:"limit"`
	Columns         []string          `json:"columns"`
	Search          string            `json:"search,omitempty"`
	Sort            string            `json:"sort,omitempty"`
	Direction       string            `json:"direction,omitempty"`
	Count           string            `json:"count,omitempty"`
	Filters         []data.RowsFilter `json:"filters,omitempty"`
	CursorCreatedAt string            `json:"cursorCreatedAt,omitempty"`
	CursorID        string            `json:"cursorId,omitempty"`
}

type randomizeStatusPriorityArgs struct {
	Count int `json:"count"`
}

const (
	tableChangeDebounce       = 75 * time.Millisecond
	tableChangeTriggerBatch   = time.Millisecond
	websocketWriteTimeout     = 10 * time.Second
	websocketProtocolVersion  = 2
	developmentRuntimeVersion = "development"
)

// Only requests that arrive while a query is already running pay this small
// trailing-edge window. An idle subscription reruns immediately.
var subscriptionRerunCooldown time.Duration

func runtimeBuildVersion() string {
	// Coolify resolves SOURCE_COMMIT to the exact checkout for webhook and API
	// deployments. Prefer it over the legacy manually-maintained value so a
	// branch-following development app cannot advertise a stale SHA.
	if version := strings.TrimSpace(os.Getenv("SOURCE_COMMIT")); isFullGitSHA(version) {
		return version
	}
	if version := strings.TrimSpace(os.Getenv("GONVEX_RUNTIME_VERSION")); version != "" {
		return version
	}
	return developmentRuntimeVersion
}

func isFullGitSHA(value string) bool {
	if len(value) != 40 {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

// subscriptionToken is deliberately non-zero-sized. Go may give separate
// zero-sized allocations the same address, which would collapse distinct
// listeners when pointers are used as map keys.
type subscriptionToken struct {
	marker        byte
	active        atomic.Bool
	cacheRevision atomic.Value
}

func newSubscriptionToken(cacheRevisions ...string) *subscriptionToken {
	cacheRevision := ""
	if len(cacheRevisions) > 0 {
		cacheRevision = cacheRevisions[0]
	}
	token := &subscriptionToken{marker: 1}
	token.active.Store(true)
	token.cacheRevision.Store(cacheRevision)
	return token
}

type querySubscription struct {
	conn          *wsConn
	id            string
	project       string
	tenant        string
	path          string
	args          json.RawMessage
	rowIDs        map[string]bool
	caller        callerContext
	ctx           context.Context
	cancel        context.CancelFunc
	token         *subscriptionToken
	cacheScope    string
	cacheRevision string
	visibilityKey string
}

type tableChange struct {
	project        string
	tenant         string
	table          string
	tables         map[string]bool
	broad          bool
	rowIDs         map[string]bool
	taskIDs        map[string]bool
	userIDs        map[string]bool
	workspaceIDs   map[string]bool
	operation      string
	changedColumns []string
	changedAtMS    float64
	// details retains filtering precision independently for every physical
	// table in a merged commit batch. Legacy producers may leave it nil and use
	// the singular fields above.
	details map[string]tableChangeDetail
	// declaredTables is the mutation manifest's conservative write superset.
	// It is retained for candidate/metric accounting even when a healthy
	// listener makes details authoritative for subscription delivery.
	declaredTables map[string]bool
	// triggerObserved identifies a table event received from PostgreSQL LISTEN.
	// It is only meaningful while accumulating a pending commit.
	triggerObserved bool
	// commitID joins the broad declared-write invalidation emitted by the
	// mutation caller with the precise PostgreSQL notifications emitted by the
	// same transaction. It is the mutation ID stored in gonvex.mutation_id.
	commitID string
}

type tableChangeDetail struct {
	operation      string
	changedColumns []string
	rowIDs         map[string]bool
	taskIDs        map[string]bool
	userIDs        map[string]bool
	workspaceIDs   map[string]bool
	// precise means the table was observed by the trigger (or another precise
	// source). broad only disables column/row pruning for this table.
	precise bool
	broad   bool
}

type pendingTableChange struct {
	project                string
	tenant                 string
	commitID               string
	declaredTables         map[string]bool
	observedDetails        map[string]tableChangeDetail
	cacheInvalidatedTables map[string]bool
	otherDetails           map[string]tableChangeDetail
	changedAtMS            float64
}

type wsConn struct {
	server  *Server
	conn    *websocket.Conn
	id      string
	project string
	tenant  string
	// tenantPinned records that the tenant came from the connect request rather
	// than from defaulting. An unpinned tenant must not survive an auth message
	// that finally names the project: it was derived before the project was
	// known, so it is "default" rather than the project's own tenant.
	tenantPinned      bool
	user              *gonvex.User
	perms             map[string]any
	auth              bool
	authToken         string
	authCheckedAt     time.Time
	cacheScope        string
	syncScope         string
	connectedAt       time.Time
	lastActiveAt      time.Time
	lastActivity      string
	lastPath          string
	device            clientDeviceInfo
	mu                sync.Mutex
	subs              map[string]querySubscription
	syncs             map[string]*syncSubscription
	syncReadyMany     bool
	syncWatermark     bool
	queryPagePatch    bool
	queryObjectPatch  bool
	queryOrderDelta   bool
	queryFanout       bool
	queryResultBatch  bool
	pendingQueries    []serverMessage
	queryBatchStarted time.Time
	pendingReady      []serverMessage
	pendingWatermarks []pendingSyncWatermark
	readyTimer        *time.Timer
	bytesReceived     atomic.Uint64
	bytesSent         atomic.Uint64
	// writesInFlight counts mutation/action frames currently executing on this
	// connection's reader goroutine, so a worker drain can close idle sockets
	// first and avoid interrupting an acknowledged-but-uncommitted write.
	writesInFlight atomic.Int32
}

type pendingSyncWatermark struct {
	revision uint64
	waiting  map[string]struct{}
}

const syncReadyFlushDelay = 15 * time.Millisecond
const queryResultFlushDelay = 2 * time.Millisecond
const queryResultMaxBatchDelay = 50 * time.Millisecond

type callerContext struct {
	user        *gonvex.User
	permissions map[string]any
}

// subject is the authenticated identity that idempotency claims are scoped
// to, so one user's stored mutation result is never replayable by another.
func (caller callerContext) subject() string {
	if caller.user == nil {
		return ""
	}
	return caller.user.ID
}

var wsUpgrader = websocket.Upgrader{
	EnableCompression: true,
	CheckOrigin:       func(_ *http.Request) bool { return true },
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	conn.EnableWriteCompression(true)
	_ = conn.SetCompressionLevel(flate.BestSpeed)
	project := projectID(r)
	requestedTenant := tenantID(r)
	connectedAt := time.Now().UTC()
	client := &wsConn{
		server:       s,
		conn:         conn,
		id:           fmt.Sprintf("conn-%06d", s.wsConnectionSeq.Add(1)),
		project:      project,
		tenant:       tenantIDFromRequest(project, requestedTenant),
		tenantPinned: strings.TrimSpace(requestedTenant) != "",
		connectedAt:  connectedAt,
		lastActiveAt: connectedAt,
		lastActivity: "connected",
		subs:         map[string]querySubscription{},
		syncs:        map[string]*syncSubscription{},
	}
	pingHandler := conn.PingHandler()
	conn.SetPingHandler(func(message string) error {
		client.flushPendingReadies()
		return pingHandler(message)
	})
	closeHandler := conn.CloseHandler()
	conn.SetCloseHandler(func(code int, text string) error {
		client.flushPendingReadies()
		return closeHandler(code, text)
	})
	s.addWSConn(client)
	defer func() {
		client.cancelSubscriptions()
		s.removeWSConn(client)
		client.close()
	}()
	var initialCache *queryCacheDirective
	if !s.projectRequiresAuthentication(r.Context(), client.project) {
		initialCache = s.queryCacheDirective(client.project, client.tenant, callerContext{})
		if initialCache != nil {
			client.cacheScope = initialCache.Scope
			client.syncScope = initialCache.SyncScope
		}
	}
	client.write(serverMessage{
		Type:       "session.ready",
		Project:    client.project,
		Tenant:     client.tenant,
		QueryCache: initialCache,
		Capabilities: &serverCapabilities{
			ProtocolVersion: websocketProtocolVersion,
			RuntimeVersion:  runtimeBuildVersion(),
			SyncBatch:       1,
			SyncIntegrity:   1,
			QueryBatch:      1,
			MutationBatch:   1,
			SyncWatermark:   1,
		},
	})

	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}
		client.bytesReceived.Add(uint64(len(payload)))
		var message clientMessage
		if err := json.Unmarshal(payload, &message); err != nil {
			return
		}
		client.handle(r.Context(), message)
	}
}

func (c *wsConn) handle(ctx context.Context, message clientMessage) {
	receivedAt := time.Now()
	c.observeActivity(message, receivedAt)
	switch message.Type {
	case "mutation.call", "mutation.callMany", "action.call":
		c.writesInFlight.Add(1)
		defer c.writesInFlight.Add(-1)
	}
	switch message.Type {
	case "auth":
		requestedProject := strings.TrimSpace(message.Project)
		if requestedProject == "" {
			requestedProject = c.project
		}
		// Only a tenant the client actually asked for may act as the fallback.
		// A defaulted one was computed at connect time, before this message
		// named the project, so reusing it would pin the socket to "default"
		// and fail every read with "tenant is not related to project".
		currentTenant := ""
		if c.tenantPinned {
			currentTenant = c.tenant
		}
		user, permissions, project, tenant, err := c.server.authenticateSocket(ctx, requestedProject, currentTenant, message.Token, message.Tenant)
		if err != nil {
			c.clearAuthentication()
			c.write(serverMessage{Type: "auth.error", ID: message.ID, Error: err.Error()})
			return
		}
		caller := callerContext{user: user, permissions: permissions}
		directive := c.server.queryCacheDirective(project, tenant, caller)
		cacheScope := ""
		connSyncScope := ""
		if directive != nil {
			cacheScope = directive.Scope
			connSyncScope = directive.SyncScope
		}
		c.mu.Lock()
		oldProject := c.project
		oldTenant := c.tenant
		oldSyncScope := c.syncScope
		oldSubs := make([]querySubscription, 0, len(c.subs))
		c.user = user
		c.perms = permissions
		c.project = project
		c.tenant = tenant
		c.auth = true
		c.authToken = message.Token
		c.authCheckedAt = time.Now()
		c.cacheScope = cacheScope
		c.syncScope = connSyncScope
		c.syncReadyMany = message.Capabilities != nil && message.Capabilities.SyncReadyMany == 1
		c.syncWatermark = message.Capabilities != nil && message.Capabilities.SyncWatermark == 1
		c.queryPagePatch = message.Capabilities != nil && message.Capabilities.QueryPagePatch == 1
		c.queryObjectPatch = message.Capabilities != nil && message.Capabilities.QueryObjectPatch == 1
		c.queryOrderDelta = message.Capabilities != nil && message.Capabilities.QueryOrderDelta == 1
		c.queryFanout = message.Capabilities != nil && message.Capabilities.QueryFanout == 1
		// Immediate writes have lower time-to-last-user at large fanout. Keep the
		// advertised capability wire-compatible, but do not introduce a batching
		// delay until a coordinator can beat direct delivery under load.
		c.queryResultBatch = false
		subs := make([]querySubscription, 0, len(c.subs))
		for id, sub := range c.subs {
			oldSubs = append(oldSubs, sub)
			if sub.cancel != nil {
				sub.cancel()
			}
			subCtx, cancel := context.WithCancel(ctx)
			sub.ctx = subCtx
			sub.cancel = cancel
			sub.project = project
			sub.tenant = tenant
			sub.caller = caller
			sub.token.active.Store(false)
			sub.token = newSubscriptionToken("")
			sub.cacheScope = cacheScope
			c.subs[id] = sub
			subs = append(subs, sub)
		}
		c.mu.Unlock()
		for _, sub := range oldSubs {
			c.server.subscriptions.detach(sub)
		}
		authResult := map[string]any{"userId": user.ID, "projectId": project, "tenantId": tenant}
		if directive != nil {
			authResult["queryCache"] = directive
		}
		c.write(serverMessage{Type: "auth.result", ID: message.ID, Result: authResult})
		for _, sub := range subs {
			c.server.subscriptions.attach(sub)
		}
		// Sync subscriptions are keyed by visibility (project/tenant/user/
		// permissions), not by the code-bundle cache scope. A re-auth after a
		// deploy keeps the same visibility and must not force every collection
		// back through a full snapshot.
		if oldProject != project || oldTenant != tenant || (oldSyncScope != "" && oldSyncScope != connSyncScope) {
			c.resetSyncSubscriptions("visibility-changed")
		}
	case "query.subscribe":
		if !c.requireAuth(ctx, "query.error", message.ID) {
			return
		}
		c.subscribeQuery(ctx, querySubscribeRequest{
			ID: message.ID, Path: message.Path, Args: message.Args, CacheRevision: message.CacheRevision,
		})
	case "query.subscribeMany":
		if !c.requireAuth(ctx, "query.error", "") {
			return
		}
		if len(message.Subscribes) > maxBatchedClientRequests {
			c.write(serverMessage{Type: "query.error", Error: "query batch cannot contain more than 256 subscribes"})
			return
		}
		for _, subscribe := range message.Subscribes {
			c.subscribeQuery(ctx, subscribe)
		}
	case "query.unsubscribe":
		c.mu.Lock()
		sub, ok := c.subs[message.ID]
		if ok {
			delete(c.subs, message.ID)
			sub.token.active.Store(false)
		}
		c.mu.Unlock()
		if ok && sub.cancel != nil {
			c.server.subscriptions.detach(sub)
			sub.cancel()
		}
	case "sync.open":
		if !c.requireAuth(ctx, "sync.error", message.ID) {
			return
		}
		c.openSync(ctx, message)
	case "sync.openMany":
		if !c.requireAuth(ctx, "sync.error", "") {
			return
		}
		c.openSyncMany(ctx, message.Opens)
	case "sync.close":
		c.closeSync(message.ID)
	case "mutation.call":
		if !c.requireAuth(ctx, "mutation.error", message.ID) {
			return
		}
		c.callMutation(ctx, receivedAt, mutationCallRequest{
			ID: message.ID, Path: message.Path, Args: message.Args, Trace: message.Trace,
			IdempotencyKey: message.IdempotencyKey,
		})
	case "mutation.callMany":
		// Offline queues flush their backlog in one frame on reconnect. Calls
		// execute sequentially in queue order; each gets its own result/error
		// frame so the client settles them individually, and one failure does
		// not abandon the writes queued after it.
		if !c.requireAuth(ctx, "mutation.error", "") {
			return
		}
		if len(message.Calls) > maxBatchedClientRequests {
			c.write(serverMessage{Type: "mutation.error", Error: "mutation batch cannot contain more than 256 calls"})
			return
		}
		for _, call := range message.Calls {
			c.callMutation(ctx, receivedAt, call)
		}
	case "action.call":
		if !c.requireAuth(ctx, "action.error", message.ID) {
			return
		}
		trace := traceFromClient(message.Trace)
		trace.ServerReceivedAtMS = epochMillis(receivedAt)
		result, err := c.server.executeTenantActionForCaller(ctx, c.project, c.tenant, c.caller(), message.Path, message.Args)
		completedAt := time.Now().UTC()
		trace.ServerCompletedAtMS = epochMillis(completedAt)
		trace.ServerDurationMS = float64(completedAt.Sub(receivedAt).Microseconds()) / 1000
		if err != nil {
			c.write(serverMessage{Type: "action.error", ID: message.ID, Path: message.Path, Error: err.Error(), Trace: trace})
			c.server.recordTransactionTelemetry(transactionEntryFromTrace(c.project, c.tenant, message.ID, "action", message.Path, "server", "", "error", err.Error(), trace))
			return
		}
		c.write(serverMessage{Type: "action.result", ID: message.ID, Path: message.Path, Result: explicitNull(result), Trace: trace})
		c.server.recordTransactionTelemetry(transactionEntryFromTrace(c.project, c.tenant, message.ID, "action", message.Path, "server", "", "ok", "", trace))
		// Actions write rows too (assistant.processThread appends the reply,
		// tasks.bulkDelete soft-deletes, ...). Without a broadcast their writes
		// never invalidate live queries, so clients sit on stale results until a
		// reload. Mirror the mutation path's completion broadcast.
		c.server.broadcastMutationInvalidationsForCommitAt(c.project, c.tenant, message.Path, message.ID, completedAt)
	case "telemetry.event":
		c.server.recordTransactionTelemetry(transactionEntryFromClientTelemetry(c.project, c.tenant, message))
	default:
		c.write(serverMessage{Type: "query.error", ID: message.ID, Error: "unknown websocket message type"})
	}
}

func (c *wsConn) observeActivity(message clientMessage, observedAt time.Time) {
	activity := strings.TrimSpace(message.Type)
	if activity == "telemetry.event" && strings.TrimSpace(message.Kind) != "" {
		activity = strings.TrimSpace(message.Kind)
	}
	c.mu.Lock()
	c.lastActiveAt = observedAt.UTC()
	c.lastActivity = activity
	lastPath := strings.TrimSpace(message.Path)
	if message.Type == "sync.openMany" && len(message.Opens) > 0 {
		paths := make([]string, 0, min(len(message.Opens), 3))
		for _, open := range message.Opens {
			if path := strings.TrimSpace(open.Path); path != "" && len(paths) < 3 {
				paths = append(paths, path)
			}
		}
		lastPath = strings.Join(paths, ", ")
	}
	c.lastPath = lastPath
	if len(message.Device) > 0 {
		var device clientDeviceInfo
		if json.Unmarshal(message.Device, &device) == nil {
			c.device = device
		}
	}
	c.mu.Unlock()
}

func traceFromClient(in *messageTrace) *messageTrace {
	if in == nil {
		return &messageTrace{}
	}
	copy := *in
	return &copy
}

func epochMillis(t time.Time) float64 {
	return float64(t.UTC().UnixNano()) / float64(time.Millisecond)
}

func transactionEntryFromTrace(project string, tenant string, operationID string, kind string, path string, phase string, reason string, outcome string, errorMessage string, trace *messageTrace) transactionTelemetryEntry {
	now := time.Now().UTC()
	entry := transactionTelemetryEntry{
		Time:        now.Format(time.RFC3339Nano),
		Project:     project,
		Tenant:      tenant,
		OperationID: operationID,
		Kind:        kind,
		Path:        path,
		Phase:       phase,
		Reason:      reason,
		Outcome:     outcome,
		Error:       errorMessage,
	}
	if trace == nil {
		return entry
	}
	entry.ClientSentAtMS = trace.ClientSentAtMS
	entry.ServerReceivedAtMS = trace.ServerReceivedAtMS
	entry.ServerCommittedAtMS = trace.ServerMutationCommittedAtMS
	entry.ServerCompletedAtMS = trace.ServerCompletedAtMS
	entry.ServerSentAtMS = trace.ServerSubscriptionSentAtMS
	entry.ChangeCommittedAtMS = trace.ServerChangeCommittedAtMS
	entry.ServerSocketWriteStartedAtMS = trace.ServerSocketWriteStartedAtMS
	if trace.ServerSocketWriteStartedAtMS > 0 && trace.ServerSubscriptionSentAtMS > 0 {
		entry.ServerSocketQueueMS = max(0, trace.ServerSocketWriteStartedAtMS-trace.ServerSubscriptionSentAtMS)
	}
	entry.ServerDurationMS = trace.ServerDurationMS
	if trace.ServerMutationStartedAtMS > 0 && trace.ServerMutationCommittedAtMS > 0 {
		entry.ServerCommitMS = float64(trace.ServerMutationCommittedAtMS - trace.ServerMutationStartedAtMS)
	} else if trace.ServerReceivedAtMS > 0 && trace.ServerMutationCommittedAtMS > 0 {
		entry.ServerCommitMS = float64(trace.ServerMutationCommittedAtMS - trace.ServerReceivedAtMS)
	}
	if trace.ClientSentAtMS > 0 && trace.ServerMutationCommittedAtMS > 0 {
		entry.ClientToCommitMS = float64(trace.ServerMutationCommittedAtMS - trace.ClientSentAtMS)
	}
	if trace.ServerSubscriptionStartedAtMS > 0 && trace.ServerSubscriptionSentAtMS > 0 {
		entry.SubscriptionDurationMS = float64(trace.ServerSubscriptionSentAtMS - trace.ServerSubscriptionStartedAtMS)
	}
	return entry
}

func transactionEntryFromClientTelemetry(project string, tenant string, message clientMessage) transactionTelemetryEntry {
	trace := traceFromClient(message.Trace)
	entry := transactionEntryFromTrace(project, tenant, message.ID, message.Kind, message.Path, "browser", message.Reason, message.Outcome, message.Error, trace)
	entry.ClientReceivedAtMS = message.ClientReceivedAtMS
	entry.ClientDurationMS = message.ClientDurationMS
	if len(message.Device) > 0 {
		entry.DeviceJSON = string(message.Device)
		var device clientDeviceInfo
		if err := json.Unmarshal(message.Device, &device); err == nil {
			entry.UserAgent = device.UserAgent
			entry.BrowserName = device.BrowserName
			entry.BrowserVersion = device.BrowserVersion
			entry.DeviceType = device.DeviceType
			entry.Platform = device.Platform
			entry.Language = device.Language
			entry.Timezone = device.Timezone
			entry.ViewportWidth = device.ViewportWidth
			entry.ViewportHeight = device.ViewportHeight
		}
	}
	if entry.Time == "" {
		entry.Time = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if message.ClientReceivedAtMS > 0 {
		if message.ClientSentAtMS > 0 {
			entry.ClientSentAtMS = message.ClientSentAtMS
		}
		if message.ClientDurationMS > 0 {
			entry.ClientRoundTripMS = message.ClientDurationMS
		} else if entry.ClientSentAtMS > 0 {
			entry.ClientRoundTripMS = float64(message.ClientReceivedAtMS - entry.ClientSentAtMS)
		}
		if trace.ServerCompletedAtMS > 0 {
			entry.ServerToBrowserMS = float64(message.ClientReceivedAtMS - trace.ServerCompletedAtMS)
		} else if trace.ServerSubscriptionSentAtMS > 0 {
			entry.ServerToBrowserMS = float64(message.ClientReceivedAtMS - trace.ServerSubscriptionSentAtMS)
		}
		if trace.ServerChangeCommittedAtMS > 0 {
			entry.ChangeToBrowserMS = float64(message.ClientReceivedAtMS - trace.ServerChangeCommittedAtMS)
		}
	}
	// Commit → this telemetry ack arriving back at the server, measured
	// entirely on the server clock. The client reports synchronously after
	// applying an update, so this is a skew-free upper bound on when the
	// user's GUI reflected the change (it adds only the upstream network
	// hop). ChangeToBrowserMS above mixes server and browser clocks and is
	// kept as the informational point estimate.
	if trace.ServerChangeCommittedAtMS > 0 {
		ackAtMS := float64(time.Now().UTC().UnixMilli())
		if ackAtMS > trace.ServerChangeCommittedAtMS {
			entry.ChangeToAckMS = ackAtMS - trace.ServerChangeCommittedAtMS
		}
	}
	if entry.Outcome == "" {
		entry.Outcome = "ok"
	}
	return entry
}

func (c *wsConn) requireAuth(ctx context.Context, errorType string, id string) bool {
	if !c.server.projectRequiresAuthentication(ctx, c.project) {
		return true
	}
	c.mu.Lock()
	authenticated := c.auth
	c.mu.Unlock()
	if authenticated && c.revalidateAppAuth(ctx) == nil {
		return true
	}
	if authenticated {
		c.clearAuthentication()
	}
	c.write(serverMessage{Type: errorType, ID: id, Error: "authentication is required"})
	return false
}

func (c *wsConn) revalidateAppAuth(ctx context.Context) error {
	c.mu.Lock()
	token := c.authToken
	project := c.project
	tenant := c.tenant
	checkedAt := c.authCheckedAt
	authenticated := c.auth
	c.mu.Unlock()
	nativeEnabled := false
	if strings.TrimSpace(c.server.projectRegistryURL()) != "" {
		var err error
		nativeEnabled, err = c.server.nativeAppAuthEnabled(ctx, project)
		if err != nil {
			return fmt.Errorf("project authentication configuration is unavailable")
		}
	}
	if !c.server.config.RequireAuth && !nativeEnabled {
		return nil
	}
	if !authenticated {
		return fmt.Errorf("authentication is required")
	}
	nativeToken := strings.HasPrefix(strings.TrimSpace(token), "gvx_session_")
	if nativeEnabled && !nativeToken {
		return fmt.Errorf("a Gonvex app session is required")
	}
	if !nativeToken || time.Since(checkedAt) < 5*time.Second {
		return nil
	}
	session, _, err := c.server.validateAppSession(ctx, project, token, tenant)
	if err != nil {
		return err
	}
	c.mu.Lock()
	if c.authToken == token {
		c.user = &gonvex.User{ID: session.User.ID, Email: session.User.Email}
		c.perms = session.Permissions
		c.authCheckedAt = time.Now()
	}
	c.mu.Unlock()
	return nil
}

func (c *wsConn) caller() callerContext {
	c.mu.Lock()
	defer c.mu.Unlock()
	return callerContext{user: c.user, permissions: c.perms}
}

func (c *wsConn) clearAuthentication() {
	c.mu.Lock()
	oldSubs := make([]querySubscription, 0, len(c.subs))
	c.user = nil
	c.perms = nil
	c.auth = false
	c.authToken = ""
	c.authCheckedAt = time.Time{}
	c.cacheScope = ""
	c.syncScope = ""
	for id, sub := range c.subs {
		oldSubs = append(oldSubs, sub)
		if sub.cancel != nil {
			sub.cancel()
		}
		sub.caller = callerContext{}
		sub.cacheScope = ""
		sub.token.active.Store(false)
		sub.token = newSubscriptionToken("")
		c.subs[id] = sub
	}
	c.mu.Unlock()
	for _, sub := range oldSubs {
		c.server.subscriptions.detach(sub)
	}
	c.resetSyncSubscriptions("visibility-changed")
}

func (c *wsConn) subscribeQuery(ctx context.Context, request querySubscribeRequest) {
	if request.ID == "" || request.Path == "" {
		c.write(serverMessage{Type: "query.error", ID: request.ID, Error: "query id and path are required"})
		return
	}
	subCtx, cancel := context.WithCancel(ctx)
	sub := querySubscription{conn: c, id: request.ID, project: c.project, tenant: c.tenant, path: request.Path, args: request.Args, caller: c.caller(), ctx: subCtx, cancel: cancel, token: newSubscriptionToken(request.CacheRevision), cacheScope: c.currentCacheScope(), cacheRevision: request.CacheRevision}
	c.mu.Lock()
	previous, hadPrevious := c.subs[request.ID]
	c.subs[request.ID] = sub
	if hadPrevious {
		previous.token.active.Store(false)
	}
	c.mu.Unlock()
	if hadPrevious && previous.cancel != nil {
		previous.cancel()
	}
	if hadPrevious {
		c.server.subscriptions.detach(previous)
	}
	c.server.subscriptions.attach(sub)
}

func (c *wsConn) callMutation(ctx context.Context, receivedAt time.Time, request mutationCallRequest) {
	trace := traceFromClient(request.Trace)
	trace.ServerReceivedAtMS = epochMillis(receivedAt)
	trace.ServerMutationStartedAtMS = epochMillis(time.Now())
	caller := c.caller()
	mutationCtx := withMutationID(ctx, request.ID)
	if key := strings.TrimSpace(request.IdempotencyKey); key != "" {
		mutationCtx = withMutationIdempotency(mutationCtx, key, caller.subject())
	}
	result, err := c.server.executeTenantMutationForCaller(mutationCtx, c.project, c.tenant, caller, request.Path, request.Args)
	committedAt := time.Now().UTC()
	trace.ServerMutationCommittedAtMS = epochMillis(committedAt)
	trace.ServerCompletedAtMS = epochMillis(committedAt)
	trace.ServerDurationMS = float64(committedAt.Sub(receivedAt).Microseconds()) / 1000
	if err != nil {
		c.write(serverMessage{Type: "mutation.error", ID: request.ID, Path: request.Path, Error: err.Error(), Trace: trace})
		c.server.recordTransactionTelemetry(transactionEntryFromTrace(c.project, c.tenant, request.ID, "mutation", request.Path, "server", "", "error", err.Error(), trace))
		return
	}
	trace.ServerBroadcastScheduledAtMS = epochMillis(time.Now())
	c.write(serverMessage{Type: "mutation.result", ID: request.ID, Path: request.Path, Result: explicitNull(result), Trace: trace})
	c.server.recordTransactionTelemetry(transactionEntryFromTrace(c.project, c.tenant, request.ID, "mutation", request.Path, "server", "", "ok", "", trace))
	c.server.broadcastMutationInvalidationsForCommitAt(c.project, c.tenant, request.Path, request.ID, committedAt)
}

func (c *wsConn) currentCacheScope() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cacheScope
}

func (c *wsConn) currentSyncScope() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.syncScope
}

func (c *wsConn) cancelSubscriptions() {
	c.mu.Lock()
	subs := make([]querySubscription, 0, len(c.subs))
	for _, sub := range c.subs {
		sub.token.active.Store(false)
		subs = append(subs, sub)
	}
	c.subs = map[string]querySubscription{}
	c.mu.Unlock()
	for _, sub := range subs {
		c.server.subscriptions.detach(sub)
		if sub.cancel != nil {
			sub.cancel()
		}
	}
	c.closeAllSyncs()
}

func (c *wsConn) write(message serverMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.flushPendingReadiesLocked()
	if c.queryResultBatch && isBatchableQueryMessage(message.Type) && c.conn != nil {
		now := time.Now()
		if len(c.pendingQueries) == 0 {
			c.queryBatchStarted = now
		}
		c.pendingQueries = append(c.pendingQueries, message)
		c.server.scheduleQueryBatch(c)
		return
	}
	c.flushPendingQueriesLocked()
	c.writeLocked(message)
	if message.Type == "sync.reset" || message.Type == "sync.error" {
		c.resolvePendingWatermarksLocked(message.ID, ^uint64(0))
	}
}

func isBatchableQueryMessage(messageType string) bool {
	switch messageType {
	case "query.result", "query.progress", "query.patch", "query.pagePatch", "query.objectPatch", "query.fanout":
		return true
	default:
		return false
	}
}

func (c *wsConn) flushPendingQueries() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.flushPendingQueriesLocked()
}

func (c *wsConn) flushPendingQueriesLocked() {
	if len(c.pendingQueries) == 0 {
		return
	}
	pending := c.pendingQueries
	c.pendingQueries = nil
	c.queryBatchStarted = time.Time{}
	if len(pending) == 1 {
		c.writeLocked(pending[0])
		return
	}
	c.writeLocked(serverMessage{Type: "query.batch", Messages: pending})
}

// scheduleQueryBatch uses one short leading-edge timer for the entire runtime
// rather than one trailing-edge timer per socket. Independent query paths from
// the same commit still coalesce on each connection, while a steady mutation
// stream cannot postpone a flush beyond this fixed window.
func (s *Server) scheduleQueryBatch(connection *wsConn) {
	if s == nil || connection == nil {
		return
	}
	s.queryBatchMu.Lock()
	if s.queryBatchConns == nil {
		s.queryBatchConns = map[*wsConn]struct{}{}
	}
	s.queryBatchConns[connection] = struct{}{}
	if s.queryBatchTimer == nil {
		s.queryBatchTimer = time.AfterFunc(queryResultFlushDelay, s.flushQueryBatches)
	}
	s.queryBatchMu.Unlock()
}

func (s *Server) flushQueryBatches() {
	s.queryBatchMu.Lock()
	connections := make([]*wsConn, 0, len(s.queryBatchConns))
	for connection := range s.queryBatchConns {
		connections = append(connections, connection)
	}
	s.queryBatchConns = nil
	s.queryBatchTimer = nil
	s.queryBatchMu.Unlock()
	workerCount := min(32, len(connections))
	var workers sync.WaitGroup
	for worker := range workerCount {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := worker; index < len(connections); index += workerCount {
				connections[index].flushPendingQueries()
			}
		}()
	}
	workers.Wait()
}

func (c *wsConn) writeSyncReady(message serverMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.syncReadyMany {
		c.writeLocked(message)
	} else if c.conn != nil {
		c.pendingReady = append(c.pendingReady, message)
		c.armReadyTimerLocked()
	}
	if message.Cursor != nil {
		c.resolvePendingWatermarksLocked(message.ID, message.Cursor.Revision)
	}
}

func (c *wsConn) writeSyncWatermark(revision uint64, waiting []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.syncWatermark || c.conn == nil || revision == 0 {
		return
	}
	pending := pendingSyncWatermark{revision: revision, waiting: make(map[string]struct{}, len(waiting))}
	for _, id := range waiting {
		if _, current := c.syncs[id]; id != "" && current {
			pending.waiting[id] = struct{}{}
		}
	}
	c.pendingWatermarks = append(c.pendingWatermarks, pending)
	c.releasePendingWatermarksLocked()
}

func (c *wsConn) armReadyTimerLocked() {
	if c.readyTimer == nil {
		c.readyTimer = time.AfterFunc(syncReadyFlushDelay, c.flushPendingReadies)
	}
}

func (c *wsConn) resolvePendingWatermarksLocked(id string, throughRevision uint64) {
	if id == "" {
		return
	}
	for index := range c.pendingWatermarks {
		pending := &c.pendingWatermarks[index]
		if pending.revision <= throughRevision {
			delete(pending.waiting, id)
		}
	}
	c.releasePendingWatermarksLocked()
}

func (c *wsConn) releasePendingWatermarksLocked() {
	for len(c.pendingWatermarks) > 0 && len(c.pendingWatermarks[0].waiting) == 0 {
		pending := c.pendingWatermarks[0]
		c.pendingWatermarks = c.pendingWatermarks[1:]
		c.pendingReady = append(c.pendingReady, serverMessage{Type: "sync.watermark", Revision: pending.revision})
		c.armReadyTimerLocked()
	}
}

func (c *wsConn) flushPendingReadies() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.flushPendingReadiesLocked()
}

func (c *wsConn) flushPendingReadiesLocked() {
	if c.readyTimer != nil {
		c.readyTimer.Stop()
		c.readyTimer = nil
	}
	if len(c.pendingReady) == 0 {
		return
	}
	pending := c.pendingReady
	c.pendingReady = nil
	for len(pending) > 0 {
		if pending[0].Type != "sync.ready" {
			c.writeLocked(pending[0])
			pending = pending[1:]
			continue
		}
		end := 1
		for end < len(pending) && pending[end].Type == "sync.ready" {
			end++
		}
		c.writePendingReadyGroupLocked(pending[:end])
		pending = pending[end:]
	}
}

func (c *wsConn) writePendingReadyGroupLocked(pending []serverMessage) {
	if len(pending) == 1 {
		c.writeLocked(pending[0])
		return
	}
	ready := make([]syncReadyMessage, 0, len(pending))
	for _, message := range pending {
		truncated := false
		if message.Truncated != nil {
			truncated = *message.Truncated
		}
		ready = append(ready, syncReadyMessage{
			ID: message.ID, Path: message.Path, Cursor: message.Cursor, Mode: message.Mode,
			Digest: message.Digest, Truncated: truncated,
		})
	}
	c.writeLocked(serverMessage{Type: "sync.readyMany", Ready: ready})
}

func (c *wsConn) writeLocked(message serverMessage) {
	if c.conn == nil {
		return
	}
	writeStarted := time.Now()
	message, queueMS := stampSocketWrite(message, writeStarted)
	_ = c.conn.SetWriteDeadline(writeStarted.Add(websocketWriteTimeout))
	payload, err := json.Marshal(message)
	if err != nil {
		return
	}
	writeErr := c.conn.WriteMessage(websocket.TextMessage, payload)
	writeMS := float64(time.Since(writeStarted).Microseconds()) / 1000
	if queueMS >= 200 || writeMS >= 200 {
		slog.Warn("slow websocket delivery", "connection", c.id, "project", c.project, "tenant", c.tenant,
			"type", message.Type, "path", message.Path, "operation", message.ID,
			"queueMs", queueMS, "writeMs", writeMS, "bytes", len(payload),
			"queries", deliveryQueryIDs(message), "failed", writeErr != nil)
	}
	if err := writeErr; err != nil {
		slog.Warn("websocket write failed", "connection", c.id, "project", c.project, "tenant", c.tenant, "type", message.Type, "path", message.Path, "error", err)
		_ = c.conn.Close()
		c.conn = nil
	} else {
		c.bytesSent.Add(uint64(len(payload)))
	}
}

func (c *wsConn) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.flushPendingReadiesLocked()
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}

func (s *Server) addWSConn(conn *wsConn) {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	s.wsConns[conn] = true
}

func (s *Server) removeWSConn(conn *wsConn) {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	delete(s.wsConns, conn)
}

func (s *Server) revokeAppAuthConnections(projectID string, userID string) {
	s.wsMu.RLock()
	connections := make([]*wsConn, 0, len(s.wsConns))
	for connection := range s.wsConns {
		connections = append(connections, connection)
	}
	s.wsMu.RUnlock()
	for _, connection := range connections {
		connection.mu.Lock()
		matches := connection.project == projectID && connection.user != nil && connection.user.ID == userID && strings.HasPrefix(connection.authToken, "gvx_session_")
		connection.mu.Unlock()
		if matches {
			connection.clearAuthentication()
			connection.write(serverMessage{Type: "auth.error", ID: "session-revoked", Error: "authentication session was revoked"})
		}
	}
}

func (s *Server) revokeAppAuthTokenConnection(token string) {
	if token == "" {
		return
	}
	s.wsMu.RLock()
	connections := make([]*wsConn, 0, len(s.wsConns))
	for connection := range s.wsConns {
		connections = append(connections, connection)
	}
	s.wsMu.RUnlock()
	for _, connection := range connections {
		connection.mu.Lock()
		matches := constantTimeString(connection.authToken, token)
		connection.mu.Unlock()
		if matches {
			connection.clearAuthentication()
			connection.write(serverMessage{Type: "auth.error", ID: "session-revoked", Error: "authentication session was revoked"})
		}
	}
}

// enforceNativeAppAuthConnections immediately cancels anonymous and legacy
// subscriptions when a project transitions from public/legacy auth to native
// app auth. Existing native sessions can remain and are revalidated normally.
func (s *Server) enforceNativeAppAuthConnections(projectID string) {
	s.wsMu.RLock()
	connections := make([]*wsConn, 0, len(s.wsConns))
	for connection := range s.wsConns {
		connections = append(connections, connection)
	}
	s.wsMu.RUnlock()
	for _, connection := range connections {
		connection.mu.Lock()
		matches := connection.project == projectID && !strings.HasPrefix(strings.TrimSpace(connection.authToken), "gvx_session_")
		connection.mu.Unlock()
		if matches {
			connection.clearAuthentication()
			connection.write(serverMessage{Type: "auth.error", ID: "auth-required", Error: "this project now requires a Gonvex app session"})
		}
	}
}

type websocketConnectionSnapshot struct {
	ID             string   `json:"id"`
	Project        string   `json:"project"`
	Tenant         string   `json:"tenant"`
	UserID         string   `json:"userId,omitempty"`
	UserEmail      string   `json:"userEmail,omitempty"`
	Authenticated  bool     `json:"authenticated"`
	ConnectedAt    string   `json:"connectedAt"`
	LastActiveAt   string   `json:"lastActiveAt"`
	LastActivity   string   `json:"lastActivity"`
	LastPath       string   `json:"lastPath,omitempty"`
	Browser        string   `json:"browser,omitempty"`
	DeviceType     string   `json:"deviceType,omitempty"`
	Platform       string   `json:"platform,omitempty"`
	ConnectionType string   `json:"connectionType,omitempty"`
	Subscriptions  []string `json:"subscriptions"`
}

// websocketCounts is the cheap sibling of websocketSnapshot for the background
// load sampler: connection/user/subscription totals across all projects,
// without building per-connection detail records.
func (s *Server) websocketCounts() (connections int, users int, subscriptions int) {
	s.wsMu.RLock()
	conns := make([]*wsConn, 0, len(s.wsConns))
	for conn := range s.wsConns {
		conns = append(conns, conn)
	}
	s.wsMu.RUnlock()

	seen := map[string]bool{}
	for _, conn := range conns {
		conn.mu.Lock()
		subscriptions += len(conn.subs) + len(conn.syncs)
		identity := "anonymous"
		if conn.user != nil && conn.user.ID != "" {
			identity = conn.user.ID
		}
		conn.mu.Unlock()
		connections++
		seen[identity] = true
	}
	return connections, len(seen), subscriptions
}

func (s *Server) websocketSnapshot(projectFilter string) websocketMetricSnapshot {
	const detailLimit = 500
	s.wsMu.RLock()
	connections := make([]*wsConn, 0, len(s.wsConns))
	for conn := range s.wsConns {
		connections = append(connections, conn)
	}
	s.wsMu.RUnlock()

	snapshot := websocketMetricSnapshot{Details: []websocketConnectionSnapshot{}}
	users := map[string]bool{}
	totalConnections := 0
	for _, conn := range connections {
		conn.mu.Lock()
		if projectFilter != "" && conn.project != projectFilter {
			conn.mu.Unlock()
			continue
		}
		detail := websocketConnectionSnapshot{
			ID:             conn.id,
			Project:        conn.project,
			Tenant:         conn.tenant,
			Authenticated:  conn.auth,
			ConnectedAt:    conn.connectedAt.Format(time.RFC3339Nano),
			LastActiveAt:   conn.lastActiveAt.Format(time.RFC3339Nano),
			LastActivity:   conn.lastActivity,
			LastPath:       conn.lastPath,
			Browser:        strings.TrimSpace(strings.Join([]string{conn.device.BrowserName, conn.device.BrowserVersion}, " ")),
			DeviceType:     conn.device.DeviceType,
			Platform:       conn.device.Platform,
			ConnectionType: conn.device.EffectiveConnectionType,
			Subscriptions:  make([]string, 0, len(conn.subs)+len(conn.syncs)),
		}
		if detail.ID == "" {
			detail.ID = fmt.Sprintf("conn-%06d", len(snapshot.Details)+1)
		}
		if conn.user != nil {
			detail.UserID = conn.user.ID
			detail.UserEmail = conn.user.Email
		}
		for _, sub := range conn.subs {
			detail.Subscriptions = append(detail.Subscriptions, sub.path)
		}
		for _, syncSubscription := range conn.syncs {
			detail.Subscriptions = append(detail.Subscriptions, syncSubscription.path)
		}
		conn.mu.Unlock()
		sort.Strings(detail.Subscriptions)
		totalConnections++
		snapshot.BytesReceived += conn.bytesReceived.Load()
		snapshot.BytesSent += conn.bytesSent.Load()
		snapshot.Subscriptions += len(detail.Subscriptions)
		if len(snapshot.Details) < detailLimit {
			snapshot.Details = append(snapshot.Details, detail)
		} else {
			snapshot.DetailsTruncated = true
		}
		identity := detail.UserID
		if identity == "" {
			identity = "anonymous"
		}
		users[identity] = true
	}
	sort.Slice(snapshot.Details, func(left, right int) bool {
		if snapshot.Details[left].LastActiveAt == snapshot.Details[right].LastActiveAt {
			return snapshot.Details[left].ID < snapshot.Details[right].ID
		}
		return snapshot.Details[left].LastActiveAt > snapshot.Details[right].LastActiveAt
	})
	snapshot.Connections = totalConnections
	snapshot.Users = len(users)
	return snapshot
}

// rerunProjectSubscriptions refreshes every live query after a project bundle
// is installed. A client can connect while /dev/sync is still compiling the
// bundle and receive an initial "not implemented" error; table-specific
// invalidation is insufficient because reference-data queries do not depend on
// the tasks table. The new bundle can also change any query's implementation,
// so all subscriptions for that project must be evaluated again.
func (s *Server) projectSubscriptions(projectID string) []querySubscription {
	s.wsMu.RLock()
	connections := make([]*wsConn, 0, len(s.wsConns))
	for conn := range s.wsConns {
		connections = append(connections, conn)
	}
	s.wsMu.RUnlock()

	subs := make([]querySubscription, 0)
	for _, conn := range connections {
		conn.mu.Lock()
		for _, sub := range conn.subs {
			if sub.project == projectID {
				subs = append(subs, sub)
			}
		}
		conn.mu.Unlock()
	}
	return subs
}

func (s *Server) rerunProjectSubscriptions(projectID string) {
	s.subscriptions.rebindProject(s.projectSubscriptions(projectID))
}

func (s *Server) broadcastTableChange(projectID string, table string) {
	s.broadcastTenantTableChange(projectID, tenantIDFromRequest(projectID, ""), table)
}

func (s *Server) broadcastTenantTableChange(projectID string, tenantID string, table string) {
	s.broadcastTenantTableChangeAt(projectID, tenantID, table, time.Now().UTC())
}

func (s *Server) broadcastTenantTableChangeAt(projectID string, tenantID string, table string, changedAt time.Time) {
	s.scheduleTableChange(tableChange{project: projectID, tenant: tenantIDFromRequest(projectID, tenantID), table: table, broad: true, changedAtMS: epochMillis(changedAt)})
}

// broadcastMutationInvalidationsAt translates a public function path into the
// physical tables its handler can change. App modules do not always share a
// name with their tables (for example chat.sendWorkspaceChat writes
// workspaceChat), so invalidating only the path prefix leaves both the row
// cache and live queries stale after a successful mutation.
func (s *Server) broadcastMutationInvalidationsAt(projectID string, tenantID string, path string, changedAt time.Time) {
	s.broadcastMutationInvalidationsForCommitAt(projectID, tenantID, path, "", changedAt)
}

func (s *Server) broadcastMutationInvalidationsForCommitAt(projectID string, tenantID string, path string, commitID string, changedAt time.Time) {
	if s.functionWritesEphemeral(projectID, path) {
		return
	}
	tables := s.declaredWriteTables(projectID, path)
	if len(tables) == 0 {
		tables = mutationInvalidationTables(path)
	}
	if len(tables) == 0 {
		s.cache.invalidateQueries(context.Background(), projectID, tenantIDFromRequest(projectID, tenantID), nil)
		return
	}
	changedTables := make(map[string]bool, len(tables))
	for _, table := range tables {
		changedTables[table] = true
	}
	s.scheduleTableChange(tableChange{
		project:     projectID,
		tenant:      tenantIDFromRequest(projectID, tenantID),
		tables:      changedTables,
		broad:       true,
		changedAtMS: epochMillis(changedAt),
		commitID:    strings.TrimSpace(commitID),
	})
}

func (s *Server) broadcastMutationInvalidations(projectID string, tenantID string, path string) {
	s.broadcastMutationInvalidationsAt(projectID, tenantID, path, time.Now().UTC())
}

func (s *Server) broadcastRowIDChange(projectID string, table string, rowIDs []string) {
	s.broadcastTenantRowIDChange(projectID, tenantIDFromRequest(projectID, ""), table, rowIDs)
}

func (s *Server) broadcastTenantRowIDChange(projectID string, tenantID string, table string, rowIDs []string) {
	ids := map[string]bool{}
	for _, id := range rowIDs {
		ids[id] = true
	}
	s.scheduleTableChange(tableChange{project: projectID, tenant: tenantIDFromRequest(projectID, tenantID), table: table, rowIDs: ids, changedAtMS: epochMillis(time.Now())})
}

func (s *Server) scheduleTableChange(change tableChange) {
	changedTables := tableChangeTables(change)
	if change.triggerObserved {
		// PostgreSQL emits trigger notifications only after commit. Invalidate
		// their authoritative physical tables before this function returns so a
		// query cannot reuse pre-commit cache entries during the delivery batch.
		s.invalidateTableCaches(change.project, change.tenant, changedTables)
	}
	s.tableChangeMu.Lock()
	tableKey := strings.Join(changedTables, "\x1f")
	if commitID := strings.TrimSpace(change.commitID); commitID != "" {
		tableKey = "commit\x1f" + commitID
	}
	key := strings.Join([]string{change.project, change.tenant, tableKey}, ":")
	pending := s.tableChanges[key]
	pending.project = change.project
	pending.tenant = change.tenant
	pending.commitID = strings.TrimSpace(change.commitID)
	if pending.commitID != "" {
		// The declared mutation event carries the actual commit timestamp; the
		// LISTEN event is observed slightly later. Keep the earliest positive
		// timestamp so client TTLU frames correlate with the mutation result and
		// measure from commit, regardless of which event reaches this merger first.
		if pending.changedAtMS == 0 || (change.changedAtMS > 0 && change.changedAtMS < pending.changedAtMS) {
			pending.changedAtMS = change.changedAtMS
		}
	} else if change.changedAtMS > pending.changedAtMS {
		pending.changedAtMS = change.changedAtMS
	}
	for _, table := range changedTables {
		table = strings.TrimSpace(table)
		if table == "" {
			continue
		}
		if change.triggerObserved {
			if pending.observedDetails == nil {
				pending.observedDetails = map[string]tableChangeDetail{}
			}
			pending.observedDetails[table] = mergeTableChangeDetail(pending.observedDetails[table], detailForTable(change, table, true))
			if pending.cacheInvalidatedTables == nil {
				pending.cacheInvalidatedTables = map[string]bool{}
			}
			pending.cacheInvalidatedTables[table] = true
			continue
		}
		if pending.commitID != "" && change.broad {
			if pending.declaredTables == nil {
				pending.declaredTables = map[string]bool{}
			}
			pending.declaredTables[table] = true
			continue
		}
		if pending.otherDetails == nil {
			pending.otherDetails = map[string]tableChangeDetail{}
		}
		pending.otherDetails[table] = mergeTableChangeDetail(pending.otherDetails[table], detailForTable(change, table, !change.broad))
	}
	s.tableChanges[key] = pending
	if timer := s.tableChangeWait[key]; timer != nil {
		timer.Stop()
	}
	delay := tableChangeDebounce
	// PostgreSQL notifications are emitted only after commit and contain the
	// authoritative physical table/row details. Deliver the first observed
	// change immediately; retain the debounce only while waiting for that
	// notification to refine a mutation's declared write superset.
	if change.triggerObserved || pending.commitID == "" {
		delay = tableChangeTriggerBatch
	}
	s.tableChangeWait[key] = time.AfterFunc(delay, func() {
		s.flushTableChange(key)
	})
	s.tableChangeMu.Unlock()
}

func (s *Server) flushTableChange(key string) {
	s.tableChangeMu.Lock()
	change, exists := s.tableChanges[key]
	delete(s.tableChangeWait, key)
	delete(s.tableChanges, key)
	s.tableChangeMu.Unlock()
	if !exists {
		// A stopped timer can already be waiting on tableChangeMu. The newer
		// timer owns the merged batch; never turn the stale callback into a
		// tenant-wide invalidation.
		return
	}

	listenerHealthy := s.subscriptions.listeners.healthy(change.project, change.tenant)
	if change.commitID != "" && listenerHealthy && len(change.observedDetails) == 0 && !s.changeTouchesLandlordTable(change) {
		// A successful handler may be a no-op. With a healthy LISTEN connection,
		// the absence of a trigger notification means there is no committed row
		// change to propagate. A delayed notification remains safe: it creates a
		// new immediate precise batch. Listener failure uses the declared broad
		// fallback below, and recovery refreshes every tenant subscription.
		return
	}
	delivery := pendingChangeForDelivery(change, listenerHealthy)
	if len(tableChangeTables(delivery)) == 0 || (len(delivery.tables) == 0 && strings.TrimSpace(delivery.table) == "") {
		// An empty table set is never authoritative. Preserve the legacy
		// tenant-wide correctness backstop for malformed/unknown changes.
		delivery.broad = true
	}
	changedTables := tableChangeTables(delivery)
	cacheTables := make([]string, 0, len(changedTables))
	for _, table := range changedTables {
		if !change.cacheInvalidatedTables[table] {
			cacheTables = append(cacheTables, table)
		}
	}
	if len(changedTables) == 0 {
		// Preserve the tenant-wide query-generation backstop for malformed or
		// unknown changes that do not identify any physical table.
		s.cache.invalidateQueries(context.Background(), delivery.project, delivery.tenant, nil)
	} else {
		s.invalidateTableCaches(delivery.project, delivery.tenant, cacheTables)
	}
	s.subscriptions.requestChange(delivery)
	s.resetSyncsForVisibilityChange(delivery)
}

func (s *Server) invalidateTableCaches(projectID string, tenantID string, tables []string) {
	if len(tables) == 0 {
		return
	}
	s.cache.invalidateQueries(context.Background(), projectID, tenantID, tables)
	for _, table := range tables {
		s.cache.invalidateRows(context.Background(), projectID, tenantID, table)
	}
}

// changeTouchesLandlordTable reports whether a declared mutation write can be
// committed outside the active tenant database. The tenant listener cannot
// observe landlord triggers, so absence of a tenant notification is not proof
// that such a mutation was a no-op.
func (s *Server) changeTouchesLandlordTable(change pendingTableChange) bool {
	landlordTables := s.runtime.ManifestForProject(change.project).Schema.LandlordTables
	for table := range change.declaredTables {
		if _, ok := landlordTables[table]; ok {
			return true
		}
	}
	return false
}

func detailForTable(change tableChange, table string, precise bool) tableChangeDetail {
	if detail, ok := change.details[table]; ok {
		detail.precise = detail.precise || precise
		return detail
	}
	return tableChangeDetail{
		operation: change.operation, changedColumns: append([]string(nil), change.changedColumns...),
		rowIDs: cloneBoolMap(change.rowIDs), taskIDs: cloneBoolMap(change.taskIDs), userIDs: cloneBoolMap(change.userIDs), workspaceIDs: cloneBoolMap(change.workspaceIDs), precise: precise, broad: change.broad,
	}
}

func mergeTableChangeDetail(current, next tableChangeDetail) tableChangeDetail {
	current.precise = current.precise || next.precise
	current.broad = current.broad || next.broad
	if current.operation == "" {
		current.operation = next.operation
	} else if next.operation != "" && current.operation != next.operation {
		current.operation = ""
		current.broad = true
	}
	current.changedColumns = appendUniqueStrings(current.changedColumns, next.changedColumns...)
	if current.rowIDs == nil && len(next.rowIDs) > 0 {
		current.rowIDs = map[string]bool{}
	}
	for id := range next.rowIDs {
		current.rowIDs[id] = true
	}
	if current.taskIDs == nil && len(next.taskIDs) > 0 {
		current.taskIDs = map[string]bool{}
	}
	for id := range next.taskIDs {
		current.taskIDs[id] = true
	}
	if current.userIDs == nil && len(next.userIDs) > 0 {
		current.userIDs = map[string]bool{}
	}
	for id := range next.userIDs {
		current.userIDs[id] = true
	}
	if current.workspaceIDs == nil && len(next.workspaceIDs) > 0 {
		current.workspaceIDs = map[string]bool{}
	}
	for id := range next.workspaceIDs {
		current.workspaceIDs[id] = true
	}
	return current
}

func pendingChangeForDelivery(pending pendingTableChange, listenerHealthy bool) tableChange {
	change := tableChange{
		project: pending.project, tenant: pending.tenant, commitID: pending.commitID,
		changedAtMS: pending.changedAtMS, declaredTables: cloneBoolMap(pending.declaredTables),
		tables: map[string]bool{}, details: map[string]tableChangeDetail{},
	}
	authoritative := len(pending.observedDetails) > 0 && (pending.commitID == "" || listenerHealthy)
	if authoritative {
		for table, detail := range pending.observedDetails {
			change.tables[table] = true
			change.details[table] = detail
		}
	} else {
		// If listener authority is unavailable, every table mentioned by either
		// source is delivered broadly. The declared manifest set remains the
		// primary superset; observed/other tables are unioned in case no manifest
		// declaration exists.
		for table := range pending.declaredTables {
			change.tables[table] = true
		}
		for table := range pending.observedDetails {
			change.tables[table] = true
		}
		for table := range pending.otherDetails {
			change.tables[table] = true
		}
		for table := range change.tables {
			change.details[table] = tableChangeDetail{broad: true}
		}
		change.broad = true
	}
	if pending.commitID == "" {
		for table, detail := range pending.otherDetails {
			change.tables[table] = true
			change.details[table] = detail
		}
	}
	if len(change.tables) == 1 {
		for table := range change.tables {
			change.table = table
			detail := change.details[table]
			change.operation = detail.operation
			change.changedColumns = append([]string(nil), detail.changedColumns...)
			change.rowIDs = cloneBoolMap(detail.rowIDs)
			change.taskIDs = cloneBoolMap(detail.taskIDs)
			change.userIDs = cloneBoolMap(detail.userIDs)
			change.workspaceIDs = cloneBoolMap(detail.workspaceIDs)
			change.broad = detail.broad
		}
	}
	return change
}

func cloneBoolMap(source map[string]bool) map[string]bool {
	if len(source) == 0 {
		return nil
	}
	copy := make(map[string]bool, len(source))
	for key, value := range source {
		copy[key] = value
	}
	return copy
}

func tableChangeMatchesSubscription(sub querySubscription, change tableChange) bool {
	if len(change.tables) == 0 {
		return change.table == "" || subscriptionDependsOnTable(sub.path, change.table)
	}
	for table := range change.tables {
		if table == "" || subscriptionDependsOnTable(sub.path, table) {
			return true
		}
	}
	return false
}

func tableChangeTables(change tableChange) []string {
	if len(change.tables) == 0 {
		return []string{change.table}
	}
	tables := make([]string, 0, len(change.tables))
	for table := range change.tables {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	return tables
}

func tableMapKeys(tables map[string]bool) []string {
	result := make([]string, 0, len(tables))
	for table := range tables {
		if strings.TrimSpace(table) != "" {
			result = append(result, table)
		}
	}
	sort.Strings(result)
	return result
}

func effectiveTableCount(change tableChange) int {
	if len(change.tables) > 0 {
		return len(change.tables)
	}
	if strings.TrimSpace(change.table) != "" {
		return 1
	}
	return 0
}

func appendUniqueStrings(existing []string, values ...string) []string {
	seen := make(map[string]struct{}, len(existing)+len(values))
	for _, value := range existing {
		seen[value] = struct{}{}
	}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		existing = append(existing, value)
	}
	sort.Strings(existing)
	return existing
}

func (s *Server) declaredWriteTables(projectID, path string) []string {
	entry, ok := s.runtime.ManifestForProject(projectID).Functions[path]
	if !ok || len(entry.Dependencies.Writes) == 0 {
		return nil
	}
	tables := make([]string, 0, len(entry.Dependencies.Writes))
	for _, write := range entry.Dependencies.Writes {
		tables = appendUniqueStrings(tables, write.Table)
	}
	return tables
}

func (s *Server) queryDependencyTables(projectID, path string) []string {
	entry, ok := s.runtime.ManifestForProject(projectID).Functions[path]
	if !ok || len(entry.Dependencies.Reads) == 0 {
		return subscriptionTables(path)
	}
	tables := make([]string, 0, len(entry.Dependencies.Reads))
	for _, read := range entry.Dependencies.Reads {
		tables = appendUniqueStrings(tables, read.Table)
	}
	return tables
}

func (s *Server) queryReadsEphemeral(projectID, path string) bool {
	if entry, ok := s.runtime.ManifestForProject(projectID).Functions[path]; ok {
		return entry.Dependencies.ReadsEphemeral
	}
	if function, ok := s.app.Lookup(path); ok {
		return function.Dependencies.ReadsEphemeral
	}
	return false
}

func (s *Server) functionWritesEphemeral(projectID, path string) bool {
	if entry, ok := s.runtime.ManifestForProject(projectID).Functions[path]; ok {
		return entry.Dependencies.WritesEphemeral
	}
	if function, ok := s.app.Lookup(path); ok {
		return function.Dependencies.WritesEphemeral
	}
	return false
}

func (s *Server) rerunSubscriptions(subs []querySubscription, reason string, changeCommittedAtMS float64) {
	for _, sub := range subs {
		s.subscriptions.request(sub, reason, changeCommittedAtMS)
	}
}

func subscriptionIntersectsChange(sub querySubscription, change tableChange) bool {
	if change.broad || subscriptionCanChangeMembership(sub) || len(change.rowIDs) == 0 || len(sub.rowIDs) == 0 {
		return true
	}
	for id := range change.rowIDs {
		if sub.rowIDs[id] {
			return true
		}
	}
	return false
}

func subscriptionCanChangeMembership(sub querySubscription) bool {
	if sub.path != "tasks.grid" {
		return true
	}
	var args taskGridArgs
	if len(sub.args) > 0 {
		if err := json.Unmarshal(sub.args, &args); err != nil {
			return true
		}
	}
	return strings.TrimSpace(args.Search) != "" || args.Sort != "" || len(args.Filters) > 0
}

func (s *Server) executeSubscription(ctx context.Context, sub querySubscription, reason string, changeCommittedAtMS float64) {
	// A subscription can be cancelled while a table-change fan-out is already
	// queued. That stale rerun belongs to the subscription, not the connection,
	// and must never clear authentication for every other live subscription.
	if ctx.Err() != nil {
		return
	}
	if err := sub.conn.revalidateAppAuth(ctx); err != nil {
		if ctx.Err() != nil {
			return
		}
		sub.conn.clearAuthentication()
		sub.conn.write(serverMessage{Type: "query.error", ID: sub.id, Error: "authentication is required"})
		return
	}
	startedAt := time.Now().UTC()
	result, err := s.executeTenantQueryForCallerCached(ctx, sub.project, sub.tenant, sub.caller, sub.path, sub.args, sub.cacheScope, reason)
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		sub.conn.write(serverMessage{Type: "query.error", ID: sub.id, Error: err.Error()})
		s.recordTransactionTelemetry(transactionTelemetryEntry{
			Time:        time.Now().UTC().Format(time.RFC3339Nano),
			Project:     sub.project,
			Tenant:      sub.tenant,
			OperationID: sub.id,
			Kind:        "query",
			Path:        sub.path,
			Phase:       "server",
			Reason:      reason,
			Outcome:     "error",
			Error:       err.Error(),
		})
		return
	}
	sub.rowIDs = resultRowIDs(result)
	sub.conn.mu.Lock()
	current, ok := sub.conn.subs[sub.id]
	if ok && current.token == sub.token {
		current.rowIDs = sub.rowIDs
		sub.conn.subs[sub.id] = current
	}
	sub.conn.mu.Unlock()
	if !ok || current.token != sub.token {
		return
	}
	sentAt := time.Now().UTC()
	trace := &messageTrace{
		ServerChangeCommittedAtMS:     changeCommittedAtMS,
		ServerSubscriptionStartedAtMS: epochMillis(startedAt),
		ServerSubscriptionSentAtMS:    epochMillis(sentAt),
		ServerDurationMS:              float64(sentAt.Sub(startedAt).Microseconds()) / 1000,
	}
	payload, marshalErr := json.Marshal(explicitNull(result))
	if marshalErr != nil {
		sub.conn.write(serverMessage{Type: "query.error", ID: sub.id, Path: sub.path, Error: marshalErr.Error()})
		return
	}
	hash, queryPerf := queryResultSemantics(payload)
	trace.QueryPerf = queryPerf
	cacheRevision := s.nextQueryCacheRevision(hash)
	if queryCacheRevisionMatchesHash(currentListenerCacheRevision(sub), hash) {
		sub.conn.write(serverMessage{
			Type: "query.progress", ID: sub.id, Path: sub.path, Reason: reason, Trace: trace,
			ThroughRevision: &subscriptionRevision{Epoch: s.subscriptions.epoch, Sequence: s.subscriptions.sequence.Add(1)},
		})
	} else {
		sub.conn.write(serverMessage{Type: "query.result", ID: sub.id, Path: sub.path, Result: json.RawMessage(payload), Reason: reason, Trace: trace, CacheScope: sub.cacheScope, CacheRevision: cacheRevision})
		// Later unchanged-payload checks must compare against what was
		// actually delivered, not the subscribe-time revision.
		storeListenerCacheRevision(sub, cacheRevision)
	}
	s.recordTransactionTelemetry(transactionEntryFromTrace(sub.project, sub.tenant, sub.id, "query", sub.path, "server", reason, "ok", "", trace))
}

func resultRowIDs(result any) map[string]bool {
	ids := map[string]bool{}
	collect := func(row map[string]any) {
		for _, key := range []string{"id", "_id"} {
			if value, ok := row[key].(string); ok && value != "" {
				ids[value] = true
				return
			}
		}
	}
	switch rows := result.(type) {
	case interface{ GonvexResultRowIDs() []string }:
		for _, id := range rows.GonvexResultRowIDs() {
			if id = strings.TrimSpace(id); id != "" {
				ids[id] = true
			}
		}
	case data.RowsResult:
		for _, row := range rows.Rows {
			collect(row)
		}
	case []map[string]any:
		for _, row := range rows {
			collect(row)
		}
	case []any:
		for _, value := range rows {
			if row, ok := value.(map[string]any); ok {
				collect(row)
			}
		}
	case map[string]any:
		for _, field := range []string{"rows", "items"} {
			switch pageRows := rows[field].(type) {
			case []map[string]any:
				for _, row := range pageRows {
					collect(row)
				}
			case []any:
				for _, value := range pageRows {
					if row, ok := value.(map[string]any); ok {
						collect(row)
					}
				}
			}
		}
	}
	if len(ids) == 0 {
		return nil
	}
	return ids
}

func (s *Server) executeQuery(ctx context.Context, projectID string, path string, rawArgs json.RawMessage) (result any, err error) {
	return s.executeTenantQuery(ctx, projectID, tenantIDFromRequest(projectID, ""), path, rawArgs)
}

func (s *Server) executeTenantQuery(ctx context.Context, projectID string, tenantID string, path string, rawArgs json.RawMessage) (result any, err error) {
	return s.executeTenantQueryForCaller(ctx, projectID, tenantID, callerContext{}, path, rawArgs)
}

func (s *Server) executeTenantQueryForCaller(ctx context.Context, projectID string, tenantID string, caller callerContext, path string, rawArgs json.RawMessage) (result any, err error) {
	release, admitted := s.acquireQueryAdmission(ctx, admissionForeground, projectID, tenantID)
	if !admitted {
		return nil, ctx.Err()
	}
	defer release()
	return s.executeTenantQueryForCallerCached(ctx, projectID, tenantID, caller, path, rawArgs, "", "internal")
}

func (s *Server) executeTenantQueryForCallerCached(ctx context.Context, projectID string, tenantID string, caller callerContext, path string, rawArgs json.RawMessage, cacheScope string, reason string) (result any, err error) {
	kind := s.functionKind(projectID, path, "query")
	s.metrics.recordFunctionStart(kind)
	execution := newRuntimeFunctionLog(projectID, tenantID, path, kind, caller, rawArgs)
	execution.entry.Cache = "bypass"
	execution.entry.Source = "database"
	execution.entry.Reason = reason
	defer func() {
		s.metrics.recordFunctionEnd(kind)
		s.metrics.recordFunctionExecution(execution, err)
	}()

	cacheKey := ""
	cacheGeneration := ""
	queryTables := s.queryDependencyTables(projectID, path)
	if strings.TrimSpace(cacheScope) != "" && s.cache.enabled() && !s.queryReadsEphemeral(projectID, path) {
		if generation, ok := s.cache.queryGeneration(ctx, projectID, tenantID, queryTables); ok {
			cacheGeneration = generation
			cacheKey = s.cache.queryKey(projectID, tenantID, generation, cacheScope, path, rawArgs)
		} else {
			execution.entry.Cache = "error"
		}
		// An invalidation-triggered rerun exists because a mutation just
		// changed one of this query's tables. Serving it from the cache can
		// replay the pre-mutation payload (observed: a delete's rerun answered
		// "unchanged" and the subscribed grid never dropped the row). Always
		// recompute for invalidations; the fresh result is still stored below.
		if cacheKey != "" && reason == "invalidate" {
			execution.entry.Cache = "bypass"
			s.metrics.recordCache(projectID, "bypass")
		} else if cacheKey != "" {
			payload, outcome := s.cache.read(ctx, cacheKey)
			if outcome == "hit" {
				if decodeErr := json.Unmarshal(payload, &result); decodeErr == nil {
					execution.entry.Cache = "hit"
					execution.entry.Source = "redis"
					s.metrics.recordCache(projectID, "hit")
					return result, nil
				}
				execution.entry.Cache = "error"
				s.metrics.recordCache(projectID, "bypass")
			} else {
				execution.entry.Cache = outcome
				if outcome == "miss" {
					s.metrics.recordCache(projectID, "miss")
				} else {
					s.metrics.recordCache(projectID, "bypass")
				}
			}
		} else {
			s.metrics.recordCache(projectID, "bypass")
		}
	} else if strings.TrimSpace(cacheScope) != "" {
		s.metrics.recordCache(projectID, "bypass")
	}

	databaseStartedAt := time.Now()
	result, err = s.executeTenantQueryForCallerUncached(ctx, projectID, tenantID, caller, path, rawArgs)
	s.metrics.recordReactive(func(metric *reactiveMetricState) {
		metric.DatabaseQueryCount++
		metric.DatabaseQueryDurationMS += float64(time.Since(databaseStartedAt).Microseconds()) / 1000
	})
	// Reactive invalidations already publish and retain the committed result in
	// the subscription manager. Encoding and synchronously writing the same large
	// value to Valkey delays every subscriber without improving correctness; a
	// later initial/one-shot query may refill the cache through the normal path.
	if err == nil && cacheKey != "" && reason != "invalidate" {
		currentGeneration, generationOK := s.cache.queryGeneration(ctx, projectID, tenantID, queryTables)
		if payload, encodeErr := json.Marshal(result); encodeErr == nil && generationOK && currentGeneration == cacheGeneration {
			if decision := queryCacheWriteDecision(path, result); decision.store {
				s.cache.setWithTTL(ctx, cacheKey, payload, decision.ttl)
			}
		}
	}
	return result, err
}

// queryCacheWriteDecision decides whether a successful query result should be
// stored in Valkey and for how long. The goal is "Valkey is never lastingly wrong":
//   - normal hits keep the configured TTL
//   - empty / near-empty payloads get a short TTL so a transient poison result
//     (schema-cache race returning empty statuses while workspaces load) cannot
//     stick for the full 10m window
//   - bulk.allReferenceData with empty statuses+priorities while other reference
//     data is present is refused entirely (known poison shape from this incident)
type queryCacheWriteChoice struct {
	store bool
	ttl   time.Duration
}

func queryCacheWriteDecision(path string, result any) queryCacheWriteChoice {
	// Default: store with the cache's configured TTL (caller passes 0 → rowsCache.ttl).
	defaultChoice := queryCacheWriteChoice{store: true, ttl: 0}

	if path == "bulk.allReferenceData" {
		if isPoisonedAllReferenceData(result) {
			return queryCacheWriteChoice{store: false}
		}
		if isEmptyAllReferenceData(result) {
			return queryCacheWriteChoice{store: true, ttl: emptyResultTTL}
		}
		return defaultChoice
	}

	// Never cache nil / missing-row results. Existence lookups like
	// tenants.getByDomain returning null during a schema/landlord blip would
	// otherwise stick for emptyResultTTL and bounce clients to missingTenant.
	if result == nil {
		return queryCacheWriteChoice{store: false}
	}
	if isEmptyQueryResult(result) {
		return queryCacheWriteChoice{store: true, ttl: emptyResultTTL}
	}
	return defaultChoice
}

func isPoisonedAllReferenceData(result any) bool {
	m, ok := result.(map[string]any)
	if !ok || m == nil {
		return false
	}
	// Poison: statuses and priorities both empty, but the tenant clearly has
	// other reference data (workspaces/teams). A blank new tenant can have empty
	// statuses — those still get a short TTL via isEmptyAllReferenceData.
	if !isEmptyList(m["statuses"]) || !isEmptyList(m["priorities"]) {
		return false
	}
	for _, key := range []string{"workspaces", "teams", "categories", "templates", "users"} {
		if !isEmptyList(m[key]) {
			return true
		}
	}
	return false
}

func isEmptyAllReferenceData(result any) bool {
	m, ok := result.(map[string]any)
	if !ok || m == nil {
		return true
	}
	// "Empty" for caching purposes: no statuses and no priorities.
	return isEmptyList(m["statuses"]) && isEmptyList(m["priorities"])
}

func isEmptyQueryResult(result any) bool {
	if result == nil {
		return true
	}
	switch v := result.(type) {
	case []any:
		return len(v) == 0
	case []map[string]any:
		return len(v) == 0
	case map[string]any:
		if page, ok := v["page"]; ok {
			return isEmptyList(page)
		}
		// Single-object payloads are never treated as empty just for being a map.
		return false
	default:
		return false
	}
}

func isEmptyList(value any) bool {
	if value == nil {
		return true
	}
	switch v := value.(type) {
	case []any:
		return len(v) == 0
	case []map[string]any:
		return len(v) == 0
	default:
		// Non-list values are not "empty lists".
		return false
	}
}

func (s *Server) executeTenantQueryForCallerUncached(ctx context.Context, projectID string, tenantID string, caller callerContext, path string, rawArgs json.RawMessage) (any, error) {
	if isLegacyTaskQuery(path) {
		return s.executeLegacyQuery(ctx, projectID, tenantID, path, rawArgs)
	}
	app := s.appForProject(ctx, projectID)
	if _, ok := app.Lookup(path); ok {
		queryCtx, err := s.queryContext(ctx, projectID, tenantID, caller)
		if err != nil {
			return nil, err
		}
		return app.ExecuteQuery(queryCtx, path, rawArgs)
	}
	return nil, fmt.Errorf("query %q is not implemented by the runtime", path)
}

func (s *Server) executeLegacyQuery(ctx context.Context, projectID string, tenantID string, path string, rawArgs json.RawMessage) (any, error) {
	// Resolve the project/tenant database before reading. The registered-function
	// path hydrates via appForProject + runtimeContext, but the legacy grid path
	// skips both, so without this the first query after a (re)start hits the
	// fallback control DB and fails with relation "tasks" does not exist.
	s.hydrateRuntimeStateForProject(ctx, projectID)
	s.hydrateProjectTenantDatabases(ctx, projectID)
	databaseURL := s.databaseURLForTenant(projectID, tenantID)
	var err error
	databaseURL, err = s.ensureRuntimeTenantDatabase(ctx, projectID, tenantIDFromRequest(projectID, tenantID), databaseURL)
	if err != nil {
		return nil, err
	}
	switch path {
	case "tasks.grid":
		var args taskGridArgs
		if len(rawArgs) > 0 {
			if err := json.Unmarshal(rawArgs, &args); err != nil {
				return nil, err
			}
		}
		return data.ReadTaskGrid(ctx, databaseURL, data.RowsOptions{
			Limit:           args.Limit,
			Offset:          args.Offset,
			Search:          args.Search,
			SortColumn:      args.Sort,
			SortDirection:   args.Direction,
			Columns:         args.Columns,
			Filters:         args.Filters,
			ExactTotal:      args.Count != "false" && args.Count != "estimate",
			EstimateTotal:   args.Count == "estimate",
			CursorCreatedAt: args.CursorCreatedAt,
			CursorID:        args.CursorID,
		})
	default:
		return nil, fmt.Errorf("query %q is not implemented by the runtime", path)
	}
}

func (s *Server) executeMutation(ctx context.Context, projectID string, path string, rawArgs json.RawMessage) (result any, err error) {
	return s.executeTenantMutation(ctx, projectID, tenantIDFromRequest(projectID, ""), path, rawArgs)
}

func (s *Server) executeTenantMutation(ctx context.Context, projectID string, tenantID string, path string, rawArgs json.RawMessage) (result any, err error) {
	return s.executeTenantMutationForCaller(ctx, projectID, tenantID, callerContext{}, path, rawArgs)
}

func (s *Server) executeTenantMutationForCaller(ctx context.Context, projectID string, tenantID string, caller callerContext, path string, rawArgs json.RawMessage) (result any, err error) {
	kind := s.functionKind(projectID, path, "mutation")
	s.metrics.recordFunctionStart(kind)
	execution := newRuntimeFunctionLog(projectID, tenantID, path, kind, caller, rawArgs)
	defer func() {
		s.metrics.recordFunctionEnd(kind)
		s.metrics.recordFunctionExecution(execution, err)
	}()

	if isLegacyTaskMutation(path) {
		return s.executeLegacyMutation(ctx, projectID, tenantID, path, rawArgs)
	}
	app := s.appForProject(ctx, projectID)
	if _, ok := app.Lookup(path); ok {
		mutationCtx, err := s.mutationContext(ctx, projectID, tenantID, caller)
		if err != nil {
			return nil, err
		}
		result, err := s.executeRegisteredMutation(app, mutationCtx, path, rawArgs)
		if err != nil {
			return nil, err
		}
		if path == "tenants.create" {
			if err := s.provisionCreatedTenant(ctx, projectID, result); err != nil {
				return nil, err
			}
			// Optional app hook: seed structural defaults into the newly
			// provisioned tenant database (roles, permissions, etc.). Runs with
			// the new tenant as active context so TenantTable writes land in the
			// right DB — not the landlord DB used during tenants.create itself.
			if err := s.runTenantsOnProvisioned(ctx, projectID, result, caller); err != nil {
				return nil, err
			}
		}
		return result, nil
	}
	return nil, fmt.Errorf("mutation %q is not implemented by the runtime", path)
}

// runTenantsOnProvisioned invokes the optional internal mutation
// "tenants.onProvisioned" against the newly created tenant database after
// provisionCreatedTenant succeeds. Apps that do not register the hook are
// skipped. Failures surface so create does not silently leave an empty shell.
func (s *Server) runTenantsOnProvisioned(ctx context.Context, projectID string, result any, caller callerContext) error {
	tenantID := tenantIDFromMutationResult(result)
	if tenantID == "" {
		return nil
	}
	app := s.appForProject(ctx, projectID)
	function, ok := app.Lookup("tenants.onProvisioned")
	if !ok || function.Kind != gonvex.FunctionKindInternalMutation {
		return nil
	}
	mutationCtx, err := s.mutationContext(ctx, projectID, tenantID, caller)
	if err != nil {
		return fmt.Errorf("tenants.onProvisioned: %w", err)
	}
	rawArgs, err := json.Marshal(map[string]any{"tenantId": tenantID})
	if err != nil {
		return fmt.Errorf("tenants.onProvisioned args: %w", err)
	}
	if _, err := s.runMutationInTx(mutationCtx, "tenants.onProvisioned", rawArgs, app.ExecuteInternalMutation); err != nil {
		return fmt.Errorf("tenants.onProvisioned: %w", err)
	}
	return nil
}

func (s *Server) executeRegisteredMutation(app *gonvex.App, mutationCtx *gonvex.MutationCtx, path string, rawArgs json.RawMessage) (any, error) {
	if function, ok := app.Lookup(path); ok && function.Dependencies.WritesEphemeral {
		return app.ExecuteMutation(mutationCtx, path, rawArgs)
	}
	return s.runMutationInTx(mutationCtx, path, rawArgs, app.ExecuteMutation)
}

// runMutationInTx runs a mutation-style handler inside a database transaction
// when a database is configured, committing on success and rolling back on
// error. It is shared by client-triggered mutations and scheduled internal
// mutations so both get the same transactional guarantees.
func (s *Server) runMutationInTx(mutationCtx *gonvex.MutationCtx, path string, rawArgs json.RawMessage, exec func(*gonvex.MutationCtx, string, json.RawMessage) (any, error)) (any, error) {
	if mutationCtx.DB == nil {
		return exec(mutationCtx, path, rawArgs)
	}
	if mutationCtx.Context == nil {
		mutationCtx.Context = context.Background()
	}
	claim, hasClaim := mutationIdempotencyFromContext(mutationCtx.Context)
	if hasClaim {
		if err := s.ensureMutationIdempotencyStorage(mutationCtx.Context, mutationCtx.DB, mutationCtx.DatabaseURL); err != nil {
			return nil, err
		}
	}
	tx, err := mutationCtx.DB.BeginTx(mutationCtx.Context, nil)
	if err != nil {
		return nil, err
	}
	mutationCtx.Tx = tx
	if hasClaim {
		claimed, err := claimMutationIdempotency(mutationCtx.Context, tx, claim, path)
		if err != nil {
			_ = tx.Rollback()
			mutationCtx.Tx = nil
			return nil, err
		}
		if !claimed {
			// A previous delivery of this write already committed. Serve its
			// stored result instead of executing the handler a second time.
			_ = tx.Rollback()
			mutationCtx.Tx = nil
			return replayMutationIdempotencyResult(mutationCtx.Context, mutationCtx.DB, claim, path)
		}
	}
	if mutationID := mutationIDFromContext(mutationCtx.Context); mutationID != "" {
		if _, err := tx.ExecContext(mutationCtx.Context, `SELECT set_config('gonvex.mutation_id', $1, true)`, mutationID); err != nil {
			_ = tx.Rollback()
			return nil, err
		}
	}
	originalScheduler := mutationCtx.Scheduler
	deferred := newDeferredScheduler(originalScheduler)
	mutationCtx.Scheduler = deferred
	defer func() {
		mutationCtx.Scheduler = originalScheduler
	}()
	result, err := exec(mutationCtx, path, rawArgs)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if hasClaim {
		if err := storeMutationIdempotencyResult(mutationCtx.Context, tx, claim, result); err != nil {
			_ = tx.Rollback()
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	mutationCtx.Tx = nil
	if hasClaim {
		s.maybeSweepMutationIdempotency(mutationCtx.DB, mutationCtx.DatabaseURL)
	}
	if err := deferred.flush(); err != nil {
		mutationCtx.Logger.Error("failed to publish committed scheduled work", "path", path, "error", err)
	}
	return result, nil
}

func (s *Server) executeLegacyMutation(ctx context.Context, projectID string, tenantID string, path string, rawArgs json.RawMessage) (any, error) {
	s.hydrateRuntimeStateForProject(ctx, projectID)
	s.hydrateProjectTenantDatabases(ctx, projectID)
	switch path {
	case "tasks.randomizeStatusPriority":
		var args randomizeStatusPriorityArgs
		if len(rawArgs) > 0 {
			if err := json.Unmarshal(rawArgs, &args); err != nil {
				return nil, err
			}
		}
		databaseURL := s.databaseURLForTenant(projectID, tenantID)
		databaseURL, err := s.ensureRuntimeTenantDatabase(ctx, projectID, tenantIDFromRequest(projectID, tenantID), databaseURL)
		if err != nil {
			return nil, err
		}
		result, err := data.RandomizeTaskStatusPriority(ctx, databaseURL, args.Count)
		if err != nil {
			return nil, err
		}
		go s.broadcastTenantTableChange(projectID, tenantID, "tasks")
		return result, nil
	default:
		return nil, fmt.Errorf("mutation %q is not implemented by the runtime", path)
	}
}

func (s *Server) executeAction(ctx context.Context, projectID string, path string, rawArgs json.RawMessage) (result any, err error) {
	return s.executeTenantAction(ctx, projectID, tenantIDFromRequest(projectID, ""), path, rawArgs)
}

func (s *Server) executeTenantAction(ctx context.Context, projectID string, tenantID string, path string, rawArgs json.RawMessage) (result any, err error) {
	return s.executeTenantActionForCaller(ctx, projectID, tenantID, callerContext{}, path, rawArgs)
}

func (s *Server) executeTenantActionForCaller(ctx context.Context, projectID string, tenantID string, caller callerContext, path string, rawArgs json.RawMessage) (result any, err error) {
	kind := s.functionKind(projectID, path, "action")
	s.metrics.recordFunctionStart(kind)
	execution := newRuntimeFunctionLog(projectID, tenantID, path, kind, caller, rawArgs)
	defer func() {
		s.metrics.recordFunctionEnd(kind)
		s.metrics.recordFunctionExecution(execution, err)
	}()

	app := s.appForProject(ctx, projectID)
	if _, ok := app.Lookup(path); ok {
		actionCtx, err := s.actionContext(ctx, projectID, tenantID, caller)
		if err != nil {
			return nil, err
		}
		return app.ExecuteAction(actionCtx, path, rawArgs)
	}
	return nil, fmt.Errorf("action %q is not implemented by the runtime", path)
}

func (s *Server) functionKind(projectID string, path string, fallback string) string {
	if function, ok := s.app.Lookup(path); ok && function.Kind != "" {
		return string(function.Kind)
	}
	if entry, ok := s.runtime.ManifestForProject(projectID).Functions[path]; ok && entry.Kind != "" {
		return string(entry.Kind)
	}
	return fallback
}

func (s *Server) queryContext(ctx context.Context, projectID string, tenantID string, caller callerContext) (*gonvex.QueryCtx, error) {
	runtimeCtx, err := s.runtimeContext(ctx, projectID, tenantID, caller)
	if err != nil {
		return nil, err
	}
	return &gonvex.QueryCtx{RuntimeContext: runtimeCtx}, nil
}

func (s *Server) mutationContext(ctx context.Context, projectID string, tenantID string, caller callerContext) (*gonvex.MutationCtx, error) {
	runtimeCtx, err := s.runtimeContext(ctx, projectID, tenantID, caller)
	if err != nil {
		return nil, err
	}
	runtimeCtx = mutationRuntimeContext(runtimeCtx)
	return &gonvex.MutationCtx{RuntimeContext: runtimeCtx}, nil
}

// mutationRuntimeContext prevents handlers from broadcasting table changes
// while their transaction is still open. Client and scheduled mutation paths
// publish one dependency-mapped invalidation batch after a successful commit;
// allowing handler notifications here causes stale pre-commit reruns and one
// duplicate rerun per notified table.
func mutationRuntimeContext(runtimeCtx gonvex.RuntimeContext) gonvex.RuntimeContext {
	runtimeCtx.NotifyTableChange = nil
	return runtimeCtx
}

func (s *Server) actionContext(ctx context.Context, projectID string, tenantID string, caller callerContext) (*gonvex.ActionCtx, error) {
	runtimeCtx, err := s.runtimeContext(ctx, projectID, tenantID, caller)
	if err != nil {
		return nil, err
	}
	return &gonvex.ActionCtx{RuntimeContext: runtimeCtx}, nil
}

func (s *Server) runtimeContext(ctx context.Context, projectID string, tenantID string, caller callerContext) (gonvex.RuntimeContext, error) {
	if err := s.requireProjectDatabase(projectID); err != nil {
		return gonvex.RuntimeContext{}, err
	}
	activeTenant := tenantIDFromRequest(projectID, tenantID)
	s.hydrateProjectTenantDatabases(ctx, projectID)
	databaseURL := s.databaseURLForTenant(projectID, activeTenant)
	var err error
	databaseURL, err = s.ensureRuntimeTenantDatabase(ctx, projectID, activeTenant, databaseURL)
	if err != nil {
		return gonvex.RuntimeContext{}, err
	}
	store, err := s.tenantStores.Store(ctx, tenantStoreKey(projectID, activeTenant), databaseURL)
	if err != nil {
		return gonvex.RuntimeContext{}, err
	}
	landlordURL := s.databaseURLForProject(projectID)
	landlordStore, err := s.tenantStores.Store(ctx, tenantStoreKey(projectID, "__landlord__"), landlordURL)
	if err != nil {
		return gonvex.RuntimeContext{}, err
	}
	logger := slog.Default().With("project", projectID, "tenant", activeTenant)
	storageAPI := s.storageForTenant(ctx, projectID, activeTenant, store.DB, caller, logger)
	dataAPI := s.dataForTenant(projectID, activeTenant, storageAPI)
	return gonvex.RuntimeContext{
		Context:          ctx,
		ProjectID:        projectID,
		TenantID:         activeTenant,
		DatabaseURL:      store.DatabaseURL,
		DB:               store.DB,
		LandlordDB:       landlordStore.DB,
		TenantDB:         store.DB,
		Storage:          storageAPI,
		Sandbox:          s.sandboxForCaller(projectID, activeTenant, caller, dataAPI),
		Data:             dataAPI,
		Ephemeral:        newScopedEphemeralAPI(ctx, s.ephemeral, projectID, activeTenant),
		ProjectEphemeral: newProjectScopedEphemeralAPI(ctx, s.ephemeral, projectID),
		Scheduler:        s.scheduler.For(projectID, activeTenant),
		User:             caller.user,
		Permissions:      caller.permissions,
		Logger:           logger,
		NotifyTableChange: func(table string) {
			s.broadcastTenantTableChange(projectID, activeTenant, table)
		},
		Env: s.projectEnvValues(ctx, projectID),
	}, nil
}

func (s *Server) sandboxForCaller(projectID string, tenantID string, caller callerContext, dataAPI gonvex.DataAPI) gonvex.SandboxAPI {
	if dataAPI == nil {
		dataAPI = gonvex.UnavailableData()
	}
	runner := sandbox.NewRunner("")
	// Prefer identity injected into the RunGo context (e.g. assistant loop
	// rebinding the thread owner after a scheduled empty-caller start). Fall
	// back to the closed-over caller from RuntimeContext construction (browser
	// WS sessions already have a real user here).
	effectiveCaller := func(ctx context.Context) callerContext {
		if user, permissions, ok := gonvex.SandboxIdentityFromContext(ctx); ok {
			return callerContext{user: user, permissions: permissions}
		}
		return caller
	}
	// Same for tenant: scheduled loops / consent re-entry bind the thread
	// tenant onto the RunGo context. Prefer that over the closed-over value so
	// host RPCs open the right DB and inject the right tenantId.
	effectiveTenant := func(ctx context.Context) string {
		if tid := strings.TrimSpace(gonvex.SandboxTenantFromContext(ctx)); tid != "" {
			return tid
		}
		return tenantID
	}
	runner.Host = sandbox.HostFunc(func(ctx context.Context, req sandbox.HostCallRequest) (any, error) {
		hostCaller := effectiveCaller(ctx)
		hostTenant := effectiveTenant(ctx)
		args := injectSandboxHostTenantArgs(req.Args, hostTenant)
		switch strings.TrimSpace(req.Kind) {
		case "query":
			path, resolvedArgs, err := s.resolveSandboxFunction(ctx, projectID, hostTenant, hostCaller, "query", strings.TrimSpace(req.Path), args)
			if err != nil {
				return nil, err
			}
			return s.executeTenantQueryForCaller(ctx, projectID, hostTenant, hostCaller, path, resolvedArgs)
		case "action":
			path := strings.TrimSpace(req.Path)
			// Browser parity: api.whagons.action always goes through the curated
			// SandboxClient surface. Prefer assistant.sandboxAction whenever it
			// is registered so names like tasks.bulkDelete (which also exist as
			// raw runtime Actions) get confirm gates + friendly args, not the
			// bare runtime path that required a manual tenantId.
			app := s.appForProject(ctx, projectID)
			if path != "assistant.sandboxAction" {
				if _, ok := app.Lookup("assistant.sandboxAction"); ok {
					wrapped, wrapErr := json.Marshal(map[string]any{
						"name":     path,
						"args":     json.RawMessage(args),
						"tenantId": hostTenant,
					})
					if wrapErr != nil {
						return nil, wrapErr
					}
					result, err := s.executeTenantActionForCaller(ctx, projectID, hostTenant, hostCaller, "assistant.sandboxAction", wrapped)
					if err == nil {
						s.broadcastMutationInvalidations(projectID, hostTenant, path)
						return result, nil
					}
					if !sandboxHostUnknownCuratedAction(err) {
						return nil, err
					}
					// Unknown on the curated surface — fall through to a
					// registered runtime Action with tenant-injected args.
				}
			}
			result, err := s.executeTenantActionForCaller(ctx, projectID, hostTenant, hostCaller, path, args)
			if err == nil {
				s.broadcastMutationInvalidations(projectID, hostTenant, req.Path)
			}
			return result, err
		case "mutation":
			path, resolvedArgs, err := s.resolveSandboxFunction(ctx, projectID, hostTenant, hostCaller, "mutation", strings.TrimSpace(req.Path), args)
			if err != nil {
				return nil, err
			}
			result, err := s.executeTenantMutationForCaller(ctx, projectID, hostTenant, hostCaller, path, resolvedArgs)
			if err == nil {
				s.broadcastMutationInvalidations(projectID, hostTenant, path)
			}
			return result, err
		case "data.inspect":
			var inspectReq gonvex.DataInspectRequest
			if err := json.Unmarshal(args, &inspectReq); err != nil {
				return nil, fmt.Errorf("invalid data.inspect args: %w", err)
			}
			return dataAPI.Inspect(ctx, inspectReq)
		case "data.query":
			var queryReq gonvex.DataQueryRequest
			if err := json.Unmarshal(args, &queryReq); err != nil {
				return nil, fmt.Errorf("invalid data.query args: %w", err)
			}
			return dataAPI.Query(ctx, queryReq)
		case "data.profile":
			var profileReq gonvex.DataProfileRequest
			if err := json.Unmarshal(args, &profileReq); err != nil {
				return nil, fmt.Errorf("invalid data.profile args: %w", err)
			}
			return dataAPI.Profile(ctx, profileReq)
		default:
			return nil, fmt.Errorf("unsupported sandbox host call kind %q", req.Kind)
		}
	})
	return runner
}

// resolveSandboxFunction gates sandbox-originated query/mutation calls through
// the app's assistant.sandboxResolve query when one is registered. The app
// owns the policy (blocked modules, name aliases like tasks.list); the runtime
// just executes whatever {name, args} the resolver returns. Apps without a
// resolver keep the raw path — same behavior as before this hook existed.
func (s *Server) resolveSandboxFunction(ctx context.Context, projectID string, tenantID string, caller callerContext, kind string, path string, args json.RawMessage) (string, json.RawMessage, error) {
	app := s.appForProject(ctx, projectID)
	if _, ok := app.Lookup("assistant.sandboxResolve"); !ok {
		return path, args, nil
	}
	wrapped, err := json.Marshal(map[string]any{"kind": kind, "name": path, "args": args, "tenantId": tenantID})
	if err != nil {
		return "", nil, err
	}
	resolved, err := s.executeTenantQueryForCaller(ctx, projectID, tenantID, caller, "assistant.sandboxResolve", wrapped)
	if err != nil {
		return "", nil, err
	}
	resolvedMap, ok := resolved.(map[string]any)
	if !ok {
		return "", nil, fmt.Errorf("assistant.sandboxResolve returned an unexpected result for %q", path)
	}
	name, _ := resolvedMap["name"].(string)
	if strings.TrimSpace(name) == "" {
		return "", nil, fmt.Errorf("assistant.sandboxResolve returned no function name for %q", path)
	}
	resolvedArgs, err := json.Marshal(resolvedMap["args"])
	if err != nil {
		return "", nil, err
	}
	return name, resolvedArgs, nil
}

// storageForTenant builds the per-request storage handle bound to the active
// tenant database. It returns nil (leaving the not-configured fallback in
// place) when storage is unconfigured or the metadata table cannot be ensured,
// so storage problems never break functions that don't use storage.
func (s *Server) storageForTenant(ctx context.Context, projectID, tenantID string, db *sql.DB, caller callerContext, logger *slog.Logger) gonvex.StorageAPI {
	if s.storage == nil || db == nil {
		return nil
	}
	ownerID := ""
	if caller.user != nil {
		ownerID = caller.user.ID
	}
	tenant, err := s.storage.Tenant(ctx, db, projectID, tenantID, ownerID)
	if err != nil {
		logger.Warn("storage unavailable for tenant", "error", err)
		return nil
	}
	return tenant
}

func isLegacyTaskQuery(path string) bool {
	return path == "tasks.grid"
}

func isLegacyTaskMutation(path string) bool {
	return path == "tasks.randomizeStatusPriority"
}

func subscriptionTable(path string) string {
	prefix, _, ok := strings.Cut(path, ".")
	if !ok || prefix == "" {
		return ""
	}
	return prefix
}

func subscriptionDependsOnTable(path string, table string) bool {
	table = strings.TrimSpace(table)
	if table == "" {
		return true
	}
	for _, dep := range subscriptionTables(path) {
		if dep == table {
			return true
		}
	}
	return false
}

func subscriptionTables(path string) []string {
	switch path {
	case "calls.listForUser":
		return []string{"callSignals"}
	case "chat.listConversations":
		return []string{"conversations"}
	case "chat.listAllParticipants":
		return []string{"conversationParticipants"}
	case "chat.listAllMessages":
		return []string{"directMessages"}
	case "chat.listAllReactions":
		return []string{"messageReactions"}
	case "chat.listAllLinkPreviews":
		return []string{"linkPreviews"}
	case "chat.listWorkspaceChat":
		return []string{"workspaceChat"}
	case "chat.workspaceChatUnreadSummary":
		return []string{"notifications"}
	case "bulk.allReferenceData":
		return []string{
			"approvalApprovers",
			"approvals",
			"audienceTeams",
			"audienceUserTeams",
			"categories",
			"categoryCustomFields",
			"categoryPriorities",
			"cleaningStatuses",
			"customFields",
			"employeeProfiles",
			"fieldUpdatePermissions",
			"formFields",
			"formVersions",
			"forms",
			"invitations",
			"jobPositions",
			"notificationSoundRules",
			"priorities",
			"properties",
			"roles",
			"slaPolicies",
			"slas",
			"spotTypes",
			"spots",
			"statuses",
			"statusTransitionGroups",
			"statusTransitions",
			"tags",
			"taskApprovalInstances",
			"taskForms",
			"teamSettingsWorkspaces",
			"teams",
			"templates",
			"userTeams",
			"users",
			"workspaceGroups",
			"workspaces",
		}
	case "bulk.tasksByWorkspace", "bulk.taskSummaryCounts", "bulk.workspaceTaskCounts", "bulk.cachedWorkspaceTaskCounts":
		return []string{"tasks", "taskUsers", "taskTags", "taskCustomFieldValues", "taskApprovalInstances"}
	case "bulk.taskPivotData":
		return []string{"taskUsers", "taskTags", "taskCustomFieldValues", "taskApprovalInstances", "tasks"}
	case "roles.effectivePermissions":
		return []string{"roles", "rolePermissions", "userTeams", "users"}
	case "users.myPermissions":
		return []string{"roles", "rolePermissions", "permissions", "userTeams", "users"}
	case "users.myTenants":
		return []string{"tenants", "userTenantMap", "users"}
	case "settings.getByKey", "settings.getPublicAuthSettings":
		return []string{"globalSettings"}
	case "settings.listNotifications":
		return []string{"notifications"}
	case "settings.listPlugins", "settings.getPlugin":
		return []string{"plugins"}
	case "taskResources.listTaskNotes", "taskResources.listTaskNotesPaged", "taskResources.noteSummariesByTenant", "taskResources.noteCountsByTenant", "taskResources.attachmentCountsByTenant", "taskResources.listTaskIdsWithAttachments", "taskResources.listWorkspaceAttachments":
		return []string{"taskNotes", "tasks", "users"}
	case "taskResources.listTaskViewsByTaskId", "taskResources.listTaskViewsByTaskPgId":
		return []string{"taskViews", "tasks", "users"}
	case "taskResources.listTaskHistory", "taskResources.listTaskLogs":
		return []string{"taskLogs", "tasks", "users"}
	case "taskResources.listTaskSignatures", "taskResources.listTaskIdsWithSignatures":
		return []string{"taskSignatures", "tasks", "users"}
	case "taskResources.listSharedToMe", "taskResources.listTaskShares", "taskResources.sharedWithMeCount":
		return []string{"taskShares", "taskAcks", "tasks", "users"}
	case "taskResources.getTaskAcknowledgmentProgressForTask":
		return []string{"taskAcks", "taskAckReads", "taskShares", "tasks"}
	}
	if strings.HasPrefix(path, "tasks.") {
		return []string{"tasks", "taskUsers", "taskTags", "taskCustomFieldValues", "taskApprovalInstances"}
	}
	if strings.HasPrefix(path, "taskFindings.") {
		return []string{"taskFindings", "findingNotes", "taskLogs", "taskWorkspaceContexts", "tasks"}
	}
	if strings.HasPrefix(path, "settings.") {
		return []string{"settings", "plugins", "envVars"}
	}
	if table := subscriptionTable(path); table != "" {
		return []string{table}
	}
	return nil
}

func mutationInvalidationTable(path string) string {
	tables := mutationInvalidationTables(path)
	if len(tables) > 0 {
		return tables[0]
	}
	return ""
}

func mutationInvalidationTables(path string) []string {
	switch path {
	case "calls.sendSignal", "calls.acknowledgeSignals", "calls.acknowledgeRoomSignals":
		return []string{"callSignals"}
	case "chat.createConversation":
		return []string{"conversations", "conversationParticipants"}
	case "chat.sendMessage":
		return []string{"directMessages", "conversations", "notifications", "linkPreviews"}
	case "chat.updateMessage", "chat.deleteMessage", "chat.markMessagesDelivered":
		return []string{"directMessages"}
	case "chat.markAsRead":
		return []string{"conversationParticipants", "directMessages"}
	case "chat.sendWorkspaceChat":
		return []string{"workspaceChat", "notifications", "linkPreviews"}
	case "chat.updateWorkspaceChatMessage", "chat.deleteWorkspaceChatMessage":
		return []string{"workspaceChat"}
	case "chat.addReaction", "chat.removeReaction":
		return []string{"messageReactions"}
	case "chat.markWorkspaceChatRead":
		return []string{"notifications"}
	case "chat.generateUploadUrl":
		return []string{"files"}
	case "taskResources.assignUser", "taskResources.unassignUser":
		return []string{"taskUsers", "tasks", "taskLogs", "notifications"}
	case "taskResources.createNote", "taskResources.updateNote", "taskResources.removeNote", "taskResources.markVoiceMemoListened", "taskResources.removeNoteAttachment":
		return []string{"taskNotes", "tasks", "taskLogs"}
	case "taskResources.createSignature":
		return []string{"taskSignatures", "tasks", "taskLogs"}
	case "taskResources.shareTask", "taskResources.revokeShare":
		return []string{"taskShares", "taskAcks", "tasks", "notifications"}
	case "taskResources.acknowledgeTaskShare":
		return []string{"taskShares", "taskAcks", "taskAckReads", "tasks", "notifications"}
	case "taskResources.recordTaskViewByTaskId", "taskResources.recordTaskViewByTaskPgId":
		return []string{"taskViews", "tasks"}
	case "taskResources.addTagByTaskId", "taskResources.addTagByPgId", "taskResources.addTagByTaskPgId", "taskResources.removeTagByTaskId", "taskResources.removeTagByPgId", "taskResources.removeTagByTaskPgId":
		return []string{"taskTags", "tasks", "taskLogs"}
	case "taskResources.generateUploadUrl":
		return []string{"files"}
	case "scheduling.processWorkspaceChatMessage":
		return []string{"scheduleAvailabilitySubmissions", "scheduleAvailabilityItems", "scheduleRosterEntries"}
	case "settings.set":
		return []string{"globalSettings"}
	case "settings.togglePlugin", "settings.upsertPlugin", "settings.ensureDefaultPlugins":
		return []string{"plugins"}
	case "settings.markRead", "settings.markAllRead", "settings.clearAllNotifications":
		return []string{"notifications"}
	}
	if strings.HasPrefix(path, "techSupport.") {
		return []string{"supportSessions"}
	}
	if strings.HasPrefix(path, "taskFindings.") {
		return []string{"taskFindings", "findingNotes", "taskLogs", "taskWorkspaceContexts", "tasks"}
	}
	if strings.HasPrefix(path, "tasks.") {
		return []string{"tasks", "taskUsers", "taskTags", "taskLogs", "taskCustomFieldValues", "taskApprovalInstances"}
	}
	if table := subscriptionTable(path); table != "" {
		return []string{table}
	}
	return []string{""}
}
