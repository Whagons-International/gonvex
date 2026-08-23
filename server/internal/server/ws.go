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
	"github.com/gonvex/gonvex/pkg/manifest"
	"github.com/gonvex/gonvex/pkg/moduleengine"
	"github.com/gonvex/gonvex/server/internal/dbpool"
	gonvexsandbox "github.com/gonvex/gonvex/server/internal/sandbox"
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
	Scope              string                  `json:"scope,omitempty"`
	ControlOnly        bool                    `json:"controlOnly,omitempty"`
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
	Cursor             *replicaCursor          `json:"cursor,omitempty"`
	Keys               []string                `json:"keys,omitempty"`
	Hashes             map[string]string       `json:"hashes,omitempty"`
	Digest             string                  `json:"digest,omitempty"`
	FullIntegrity      bool                    `json:"fullIntegrity,omitempty"`
	Opens              []replicaOpenRequest    `json:"opens,omitempty"`
	WindowRevision     string                  `json:"windowRevision,omitempty"`
	Subscribes         []querySubscribeRequest `json:"subscribes,omitempty"`
	Calls              []reducerCallRequest    `json:"calls,omitempty"`
	Capabilities       *clientCapabilities     `json:"capabilities,omitempty"`
	Events             []json.RawMessage       `json:"events,omitempty"`
	Release            string                  `json:"release,omitempty"`
	Environment        string                  `json:"environment,omitempty"`
}

// maxBatchedClientRequests bounds every batched client frame (replica.openMany,
// query.subscribeMany, reducer.callMany).
const maxBatchedClientRequests = 256

type querySubscribeRequest struct {
	ID             string          `json:"id"`
	Path           string          `json:"path"`
	Args           json.RawMessage `json:"args,omitempty"`
	Scope          string          `json:"scope,omitempty"`
	WindowRevision string          `json:"windowRevision,omitempty"`
}

type reducerCallRequest struct {
	ID    string          `json:"id"`
	Path  string          `json:"path"`
	Args  json.RawMessage `json:"args,omitempty"`
	Scope string          `json:"scope,omitempty"`
	Trace *messageTrace   `json:"trace,omitempty"`
	// IdempotencyKey marks a replayable command from the client outbox. Replays
	// reuse the key, so the runtime executes the reducer once and serves the
	// stored result to every duplicate delivery.
	IdempotencyKey string `json:"idempotencyKey,omitempty"`
}

type replicaOpenRequest struct {
	ID            string            `json:"id"`
	Path          string            `json:"path"`
	Args          json.RawMessage   `json:"args,omitempty"`
	Cursor        *replicaCursor    `json:"cursor,omitempty"`
	Keys          []string          `json:"keys,omitempty"`
	Hashes        map[string]string `json:"hashes,omitempty"`
	Digest        string            `json:"digest,omitempty"`
	FullIntegrity bool              `json:"fullIntegrity,omitempty"`
}

type serverCapabilities struct {
	ProtocolVersion  int    `json:"protocolVersion,omitempty"`
	RuntimeVersion   string `json:"runtimeVersion,omitempty"`
	ReplicaBatch     int    `json:"replicaBatch,omitempty"`
	ReplicaIntegrity int    `json:"replicaIntegrity,omitempty"`
	QueryBatch       int    `json:"queryBatch,omitempty"`
	ReducerBatch     int    `json:"reducerBatch,omitempty"`
	ReplicaWatermark int    `json:"replicaWatermark,omitempty"`
}

type clientCapabilities struct {
	ReplicaReadyMany int `json:"replicaReadyMany,omitempty"`
	ReplicaWatermark int `json:"replicaWatermark,omitempty"`
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

type replicaReadyMessage struct {
	ID        string         `json:"id"`
	Path      string         `json:"path,omitempty"`
	Cursor    *replicaCursor `json:"cursor"`
	Mode      string         `json:"mode,omitempty"`
	Digest    string         `json:"digest,omitempty"`
	Truncated bool           `json:"truncated"`
}

type replicaChangeMessage struct {
	Entity         string          `json:"entity"`
	ID             string          `json:"id"`
	Operation      string          `json:"operation"`
	OldValue       json.RawMessage `json:"oldValue,omitempty"`
	NewValue       json.RawMessage `json:"newValue,omitempty"`
	ChangedColumns []string        `json:"changedColumns,omitempty"`
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
	Replica              *replicaDirective               `json:"replica,omitempty"`
	Capabilities         *serverCapabilities             `json:"capabilities,omitempty"`
	ReplicaScope         string                          `json:"replicaScope,omitempty"`
	WindowRevision       string                          `json:"windowRevision,omitempty"`
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
	Cursor               *replicaCursor                  `json:"cursor,omitempty"`
	Key                  string                          `json:"key,omitempty"`
	OrderBy              string                          `json:"orderBy,omitempty"`
	OrderDirection       string                          `json:"orderDirection,omitempty"`
	Mode                 string                          `json:"mode,omitempty"`
	MaxRows              int                             `json:"maxRows,omitempty"`
	MaxBytes             int64                           `json:"maxBytes,omitempty"`
	Upserts              []json.RawMessage               `json:"upserts,omitempty"`
	OriginCommandIDs     []string                        `json:"originCommandIds,omitempty"`
	Hashes               map[string]string               `json:"hashes,omitempty"`
	Digest               string                          `json:"digest,omitempty"`
	Truncated            *bool                           `json:"truncated,omitempty"`
	Ready                []replicaReadyMessage           `json:"ready,omitempty"`
	Messages             []serverMessage                 `json:"messages,omitempty"`
	Revision             uint64                          `json:"revision,omitempty"`
	OriginCommandID      string                          `json:"originCommandId,omitempty"`
	CommittedRevision    uint64                          `json:"committedRevision,omitempty"`
	Changes              []replicaChangeMessage          `json:"changes,omitempty"`
	Accepted             int                             `json:"accepted,omitempty"`
	Fingerprints         []string                        `json:"fingerprints,omitempty"`
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
	ClientSentAtMS                float64         `json:"clientSentAtMs,omitempty"`
	ServerReceivedAtMS            float64         `json:"serverReceivedAtMs,omitempty"`
	ServerReducerStartedAtMS      float64         `json:"serverReducerStartedAtMs,omitempty"`
	ServerReducerCommittedAtMS    float64         `json:"serverReducerCommittedAtMs,omitempty"`
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
	marker         byte
	active         atomic.Bool
	windowRevision atomic.Value
}

func newSubscriptionToken(windowRevisions ...string) *subscriptionToken {
	windowRevision := ""
	if len(windowRevisions) > 0 {
		windowRevision = windowRevisions[0]
	}
	token := &subscriptionToken{marker: 1}
	token.active.Store(true)
	token.windowRevision.Store(windowRevision)
	return token
}

type querySubscription struct {
	conn           *wsConn
	id             string
	project        string
	tenant         string
	path           string
	args           json.RawMessage
	rowIDs         map[string]bool
	caller         callerContext
	ctx            context.Context
	cancel         context.CancelFunc
	token          *subscriptionToken
	replicaScope   string
	windowRevision string
	visibilityKey  string
}

type tableChange struct {
	project string
	tenant  string
	// requiredRevision is the authoritative revision assigned to the committed
	// Postgres transaction that produced this change.
	requiredRevision uint64
	table            string
	tables           map[string]bool
	rowIDs           map[string]bool
	oldValues        []json.RawMessage
	newValues        []json.RawMessage
	operation        string
	changedColumns   []string
	changedAtMS      float64
	// details retains filtering precision independently for every physical
	// table in a merged commit batch. Legacy producers may leave it nil and use
	// the singular fields above.
	details map[string]tableChangeDetail
	// triggerObserved identifies a table event received from the durable change
	// feed. It is only meaningful while accumulating a pending commit.
	triggerObserved bool
	// originCommandID is the originating reducer command ID stored in
	// gonvex.command_id for optimistic reconciliation.
	originCommandID string
}

type tableChangeDetail struct {
	operation      string
	changedColumns []string
	rowIDs         map[string]bool
	oldValues      []json.RawMessage
	newValues      []json.RawMessage
}

type pendingTableChange struct {
	project                string
	tenant                 string
	originCommandID        string
	observedDetails        map[string]tableChangeDetail
	cacheInvalidatedTables map[string]bool
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
	user              *gonvex.Account
	member            *gonvex.Member
	perms             map[string]any
	auth              bool
	controlOnly       bool
	remoteIP          string
	authToken         string
	impersonationID   string
	impersonatorID    string
	authCheckedAt     time.Time
	replicaScope      string
	visibilityScope   string
	connectedAt       time.Time
	lastActiveAt      time.Time
	lastActivity      string
	lastPath          string
	device            clientDeviceInfo
	mu                sync.Mutex
	subs              map[string]querySubscription
	replicas          map[string]*replicaSubscription
	replicaReadyMany  bool
	replicaWatermark  bool
	queryPagePatch    bool
	queryObjectPatch  bool
	queryOrderDelta   bool
	queryFanout       bool
	queryResultBatch  bool
	pendingQueries    []serverMessage
	queryBatchStarted time.Time
	pendingReady      []serverMessage
	pendingWatermarks []pendingReplicaWatermark
	readyTimer        *time.Timer
	bytesReceived     atomic.Uint64
	bytesSent         atomic.Uint64
	// writesInFlight counts reducer/action frames currently executing on this
	// connection's reader goroutine, so a worker drain can close idle sockets
	// first and avoid interrupting an acknowledged-but-uncommitted write.
	writesInFlight atomic.Int32
}

type pendingReplicaWatermark struct {
	revision uint64
	waiting  map[string]struct{}
}

const syncReadyFlushDelay = 15 * time.Millisecond
const queryResultFlushDelay = 2 * time.Millisecond
const queryResultMaxBatchDelay = 50 * time.Millisecond

type callerContext struct {
	user        *gonvex.Account
	member      *gonvex.Member
	permissions map[string]any
}

// subject is the authenticated identity that idempotency claims are scoped
// to, so one user's stored reducer result is never replayable by another.
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
		remoteIP:     s.requestRemoteIP(r),
		tenantPinned: strings.TrimSpace(requestedTenant) != "",
		connectedAt:  connectedAt,
		lastActiveAt: connectedAt,
		lastActivity: "connected",
		subs:         map[string]querySubscription{},
		replicas:     map[string]*replicaSubscription{},
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
	var initialReplica *replicaDirective
	if !s.projectRequiresAuthentication(r.Context(), client.project) {
		initialReplica = s.replicaDirective(client.project, client.tenant, callerContext{})
		if initialReplica != nil {
			client.replicaScope = initialReplica.Scope
			client.visibilityScope = initialReplica.VisibilityScope
		}
	}
	client.write(serverMessage{
		Type:    "session.ready",
		Project: client.project,
		Tenant:  client.tenant,
		Replica: initialReplica,
		Capabilities: &serverCapabilities{
			ProtocolVersion:  websocketProtocolVersion,
			RuntimeVersion:   runtimeBuildVersion(),
			ReplicaBatch:     1,
			ReplicaIntegrity: 1,
			QueryBatch:       1,
			ReducerBatch:     1,
			ReplicaWatermark: 1,
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
	case "reducer.call", "reducer.callMany", "action.call":
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
		var user *gonvex.Account
		var permissions map[string]any
		var project, tenant string
		var err error
		impersonationID, impersonatorID := "", ""
		if strings.HasPrefix(strings.TrimSpace(message.Token), "gvx_imp_") {
			user, permissions, project, tenant, impersonationID, impersonatorID, err = c.server.authenticateImpersonationSocket(ctx, requestedProject, message.Token, c.id)
		} else if message.ControlOnly && strings.TrimSpace(message.Token) == "" {
			// Bind an anonymous socket to one logical project so it can invoke only
			// explicitly public Control Plane functions. This exposes no database
			// selector or credential and avoids treating a normal signed-out browser
			// as an authentication failure.
			if requestedProject == "" {
				err = fmt.Errorf("project is required for public Control Plane calls")
			} else {
				err = c.server.requireControlProject(ctx, requestedProject)
				project = requestedProject
			}
			if err == nil {
				c.clearAuthentication()
				c.mu.Lock()
				c.project = project
				c.tenant = ""
				c.controlOnly = true
				c.authCheckedAt = time.Now()
				c.mu.Unlock()
				c.write(serverMessage{Type: "auth.result", ID: message.ID, Result: map[string]any{"projectId": project, "accountId": "", "tenantId": ""}})
				return
			}
		} else if message.ControlOnly {
			user, project, err = c.server.authenticateControlSocket(ctx, requestedProject, message.Token)
		} else {
			user, permissions, project, tenant, err = c.server.authenticateSocket(ctx, requestedProject, currentTenant, message.Token, message.Tenant)
		}
		if err != nil {
			c.clearAuthentication()
			// A rejected credential must not deadlock refresh-token and other
			// explicitly public Control Plane calls queued behind this auth frame.
			// Bind only the logical project after validating it exists. No Account,
			// Member, tenant, database credential, or non-public authority survives.
			if requestedProject != "" && c.server.requireControlProject(ctx, requestedProject) == nil {
				c.mu.Lock()
				c.project = requestedProject
				c.tenant = ""
				c.controlOnly = true
				c.mu.Unlock()
			}
			c.write(serverMessage{Type: "auth.error", ID: message.ID, Error: err.Error()})
			return
		}
		var member *gonvex.Member
		if user != nil && !message.ControlOnly {
			member, err = c.server.loadTenantMember(ctx, project, tenant, user.ID)
			if err != nil {
				c.clearAuthentication()
				c.write(serverMessage{Type: "auth.error", ID: message.ID, Error: err.Error()})
				return
			}
			permissions = member.Permissions
		}
		caller := callerContext{user: user, member: member, permissions: permissions}
		directive := c.server.replicaDirective(project, tenant, caller)
		replicaScope := ""
		connVisibilityScope := ""
		if directive != nil {
			replicaScope = directive.Scope
			connVisibilityScope = directive.VisibilityScope
		}
		c.mu.Lock()
		oldProject := c.project
		oldTenant := c.tenant
		oldVisibilityScope := c.visibilityScope
		oldSubs := make([]querySubscription, 0, len(c.subs))
		c.user = user
		c.member = member
		c.perms = permissions
		c.project = project
		c.tenant = tenant
		c.auth = true
		c.controlOnly = message.ControlOnly
		c.authToken = message.Token
		c.impersonationID = impersonationID
		c.impersonatorID = impersonatorID
		c.authCheckedAt = time.Now()
		c.replicaScope = replicaScope
		c.visibilityScope = connVisibilityScope
		c.replicaReadyMany = message.Capabilities != nil && message.Capabilities.ReplicaReadyMany == 1
		c.replicaWatermark = message.Capabilities != nil && message.Capabilities.ReplicaWatermark == 1
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
			sub.replicaScope = replicaScope
			c.subs[id] = sub
			subs = append(subs, sub)
		}
		c.mu.Unlock()
		for _, sub := range oldSubs {
			c.server.subscriptions.detach(sub)
		}
		accountID := ""
		if user != nil {
			accountID = user.ID
		}
		authResult := map[string]any{"accountId": accountID, "projectId": project, "tenantId": tenant}
		if directive != nil {
			authResult["replica"] = directive
		}
		c.write(serverMessage{Type: "auth.result", ID: message.ID, Result: authResult})
		for _, sub := range subs {
			c.server.subscriptions.attach(sub)
		}
		// Replica subscriptions are keyed by visibility (project/tenant/user/
		// permissions), not by the module-generation cache scope. A re-auth after a
		// deploy keeps the same visibility and must not force every collection
		// back through a full snapshot.
		if oldProject != project || oldTenant != tenant || (oldVisibilityScope != "" && oldVisibilityScope != connVisibilityScope) {
			c.resetReplicaSubscriptions("visibility-changed")
		}
	case "query.subscribe":
		if message.Scope == "control" {
			c.write(serverMessage{Type: "query.error", ID: message.ID, Path: message.Path, Error: "Control Plane queries are one-shot"})
			return
		}
		if !c.requireAuth(ctx, "query.error", message.ID) {
			return
		}
		c.subscribeQuery(ctx, querySubscribeRequest{
			ID: message.ID, Path: message.Path, Args: message.Args, Scope: message.Scope, WindowRevision: message.WindowRevision,
		})
	case "query.call":
		if message.Scope == "control" {
			c.callControlPlane(ctx, "query", message.ID, message.Path, message.Args, "")
			return
		}
		if !c.requireAuth(ctx, "query.error", message.ID) {
			return
		}
		result, err := c.server.executeTenantQueryForCaller(ctx, c.project, c.tenant, c.caller(), message.Path, message.Args)
		if err != nil {
			c.write(serverMessage{Type: "query.error", ID: message.ID, Path: message.Path, Error: err.Error()})
			return
		}
		c.write(serverMessage{Type: "query.result", ID: message.ID, Path: message.Path, Result: explicitNull(result), Reason: "initial"})
	case "query.subscribeMany":
		if !c.requireAuth(ctx, "query.error", "") {
			return
		}
		if len(message.Subscribes) > maxBatchedClientRequests {
			c.write(serverMessage{Type: "query.error", Error: "query batch cannot contain more than 256 subscribes"})
			return
		}
		for _, subscribe := range message.Subscribes {
			if subscribe.Scope == "control" {
				c.write(serverMessage{Type: "query.error", ID: subscribe.ID, Path: subscribe.Path, Error: "Control Plane queries are one-shot"})
				continue
			}
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
	case "replica.open":
		if !c.requireAuth(ctx, "replica.error", message.ID) {
			return
		}
		c.openReplica(ctx, message)
	case "replica.openMany":
		if !c.requireAuth(ctx, "replica.error", "") {
			return
		}
		c.openReplicaMany(ctx, message.Opens)
	case "replica.close":
		c.closeReplica(message.ID)
	case "reducer.call":
		if message.Scope == "control" {
			c.callControlPlane(ctx, "reducer", message.ID, message.Path, message.Args, message.IdempotencyKey)
			return
		}
		if !c.requireAuth(ctx, "reducer.error", message.ID) {
			return
		}
		c.callReducer(ctx, receivedAt, reducerCallRequest{
			ID: message.ID, Path: message.Path, Args: message.Args, Scope: message.Scope, Trace: message.Trace,
			IdempotencyKey: message.IdempotencyKey,
		})
	case "reducer.callMany":
		// Offline queues flush their backlog in one frame on reconnect. Calls
		// execute sequentially in queue order; each gets its own result/error
		// frame so the client settles them individually, and one failure does
		// not abandon the writes queued after it.
		if !c.requireAuth(ctx, "reducer.error", "") {
			return
		}
		if len(message.Calls) > maxBatchedClientRequests {
			c.write(serverMessage{Type: "reducer.error", Error: "reducer batch cannot contain more than 256 calls"})
			return
		}
		for _, call := range message.Calls {
			if call.Scope == "control" {
				c.callControlPlane(ctx, "reducer", call.ID, call.Path, call.Args, call.IdempotencyKey)
				continue
			}
			c.callReducer(ctx, receivedAt, call)
		}
	case "action.call":
		if message.Scope == "control" {
			idempotencyKey := message.IdempotencyKey
			if idempotencyKey == "" {
				idempotencyKey = message.ID
			}
			c.callControlPlane(ctx, "action", message.ID, message.Path, message.Args, idempotencyKey)
			return
		}
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
	case "telemetry.event":
		c.server.recordTransactionTelemetry(transactionEntryFromClientTelemetry(c.project, c.tenant, message))
	case "error.register", "error.heartbeat", "error.envelope":
		c.handleNativeErrorTelemetry(ctx, message)
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
	if message.Type == "replica.openMany" && len(message.Opens) > 0 {
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
	entry.ServerCommittedAtMS = trace.ServerReducerCommittedAtMS
	entry.ServerCompletedAtMS = trace.ServerCompletedAtMS
	entry.ServerSentAtMS = trace.ServerSubscriptionSentAtMS
	entry.ChangeCommittedAtMS = trace.ServerChangeCommittedAtMS
	entry.ServerDurationMS = trace.ServerDurationMS
	if trace.ServerReducerStartedAtMS > 0 && trace.ServerReducerCommittedAtMS > 0 {
		entry.ServerCommitMS = float64(trace.ServerReducerCommittedAtMS - trace.ServerReducerStartedAtMS)
	} else if trace.ServerReceivedAtMS > 0 && trace.ServerReducerCommittedAtMS > 0 {
		entry.ServerCommitMS = float64(trace.ServerReducerCommittedAtMS - trace.ServerReceivedAtMS)
	}
	if trace.ClientSentAtMS > 0 && trace.ServerReducerCommittedAtMS > 0 {
		entry.ClientToCommitMS = float64(trace.ServerReducerCommittedAtMS - trace.ClientSentAtMS)
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
	controlOnly := c.controlOnly
	impersonationID := c.impersonationID
	c.mu.Unlock()
	if !authenticated {
		return fmt.Errorf("authentication is required")
	}
	if impersonationID != "" {
		account, member, err := c.server.revalidateImpersonation(ctx, project, tenant, impersonationID, c.id)
		if err != nil {
			return err
		}
		c.mu.Lock()
		if c.impersonationID == impersonationID {
			c.user = account
			c.member = member
			c.perms = member.Permissions
			c.authCheckedAt = time.Now()
		}
		c.mu.Unlock()
		return nil
	}
	nativeToken := strings.HasPrefix(strings.TrimSpace(token), "gvx_session_")
	if !nativeToken {
		return fmt.Errorf("a Gonvex app session is required")
	}
	if time.Since(checkedAt) < 5*time.Second {
		return nil
	}
	if controlOnly {
		account, authenticatedProject, err := c.server.authenticateControlSocket(ctx, project, token)
		if err != nil {
			return err
		}
		c.mu.Lock()
		if c.authToken == token {
			c.user = account
			c.project = authenticatedProject
			c.authCheckedAt = time.Now()
		}
		c.mu.Unlock()
		return nil
	}
	session, _, err := c.server.validateAppSession(ctx, project, token, tenant)
	if err != nil {
		return err
	}
	member, err := c.server.loadTenantMember(ctx, project, tenant, session.Account.canonicalID())
	if err != nil {
		return err
	}
	c.mu.Lock()
	if c.authToken == token {
		c.user = &gonvex.Account{ID: session.Account.canonicalID(), Email: session.Account.Email, Name: session.Account.Name, AvatarURL: session.Account.Picture}
		c.member = member
		c.perms = member.Permissions
		c.authCheckedAt = time.Now()
	}
	c.mu.Unlock()
	return nil
}

func (c *wsConn) caller() callerContext {
	c.mu.Lock()
	defer c.mu.Unlock()
	return callerContext{user: c.user, member: c.member, permissions: c.perms}
}

func (c *wsConn) clearAuthentication() {
	c.mu.Lock()
	oldSubs := make([]querySubscription, 0, len(c.subs))
	c.user = nil
	c.member = nil
	c.perms = nil
	c.auth = false
	c.controlOnly = false
	c.authToken = ""
	c.impersonationID = ""
	c.impersonatorID = ""
	c.authCheckedAt = time.Time{}
	c.replicaScope = ""
	c.visibilityScope = ""
	for id, sub := range c.subs {
		oldSubs = append(oldSubs, sub)
		if sub.cancel != nil {
			sub.cancel()
		}
		sub.caller = callerContext{}
		sub.replicaScope = ""
		sub.token.active.Store(false)
		sub.token = newSubscriptionToken("")
		c.subs[id] = sub
	}
	c.mu.Unlock()
	for _, sub := range oldSubs {
		c.server.subscriptions.detach(sub)
	}
	c.resetReplicaSubscriptions("visibility-changed")
}

func (c *wsConn) subscribeQuery(ctx context.Context, request querySubscribeRequest) {
	if request.ID == "" || request.Path == "" {
		c.write(serverMessage{Type: "query.error", ID: request.ID, Error: "query id and path are required"})
		return
	}
	_, livePlan, ok := c.server.liveQueryDependencies(ctx, c.project, request.Path)
	if !ok || livePlan == nil {
		c.write(serverMessage{Type: "query.error", ID: request.ID, Path: request.Path, Error: "Live Query is not registered with a structured plan"})
		return
	}
	if _, err := c.server.requiredVisibilityPlan(c.project, livePlan.Table); err != nil {
		c.write(serverMessage{Type: "query.error", ID: request.ID, Path: request.Path, Error: err.Error()})
		return
	}
	subCtx, cancel := context.WithCancel(ctx)
	sub := querySubscription{conn: c, id: request.ID, project: c.project, tenant: c.tenant, path: request.Path, args: request.Args, caller: c.caller(), ctx: subCtx, cancel: cancel, token: newSubscriptionToken(request.WindowRevision), replicaScope: c.currentReplicaScope(), windowRevision: request.WindowRevision}
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

func (c *wsConn) callReducer(ctx context.Context, receivedAt time.Time, request reducerCallRequest) {
	trace := traceFromClient(request.Trace)
	trace.ServerReceivedAtMS = epochMillis(receivedAt)
	trace.ServerReducerStartedAtMS = epochMillis(time.Now())
	caller := c.caller()
	reducerCtx := withCommandID(ctx, request.ID)
	if key := strings.TrimSpace(request.IdempotencyKey); key != "" {
		reducerCtx = withReducerIdempotency(reducerCtx, key, caller.subject())
	}
	result, err := c.server.executeTenantReducerForCaller(reducerCtx, c.project, c.tenant, caller, request.Path, request.Args)
	committedAt := time.Now().UTC()
	trace.ServerReducerCommittedAtMS = epochMillis(committedAt)
	trace.ServerCompletedAtMS = epochMillis(committedAt)
	trace.ServerDurationMS = float64(committedAt.Sub(receivedAt).Microseconds()) / 1000
	if err != nil {
		c.write(serverMessage{Type: "reducer.error", ID: request.ID, Path: request.Path, Error: err.Error(), Trace: trace})
		c.server.recordTransactionTelemetry(transactionEntryFromTrace(c.project, c.tenant, request.ID, "reducer", request.Path, "server", "", "error", err.Error(), trace))
		return
	}
	trace.ServerBroadcastScheduledAtMS = epochMillis(time.Now())
	committedRevision := c.server.commandCommittedRevision(ctx, c.project, c.tenant, request.ID)
	c.write(serverMessage{Type: "reducer.result", ID: request.ID, Path: request.Path, Result: explicitNull(result), OriginCommandID: request.ID, CommittedRevision: committedRevision, Trace: trace})
	c.server.recordTransactionTelemetry(transactionEntryFromTrace(c.project, c.tenant, request.ID, "reducer", request.Path, "server", "", "ok", "", trace))
}

func (s *Server) commandCommittedRevision(ctx context.Context, projectID, tenantID, commandID string) uint64 {
	commandID = strings.TrimSpace(commandID)
	if commandID == "" {
		return 0
	}
	urls := appendUniqueStrings(nil,
		s.databaseURLForTenant(projectID, tenantIDFromRequest(projectID, tenantID)),
		s.databaseURLForProject(projectID),
	)
	var revision uint64
	for _, databaseURL := range urls {
		if strings.TrimSpace(databaseURL) == "" {
			continue
		}
		db, err := dbpool.Open(databaseURL)
		if err != nil {
			continue
		}
		var candidate uint64
		err = db.QueryRowContext(ctx, `
			SELECT COALESCE(MAX(revision), 0)
			FROM _gonvex_sync_changes
			WHERE command_id = $1
		`, commandID).Scan(&candidate)
		db.Close()
		if err == nil && candidate > revision {
			revision = candidate
		}
	}
	return revision
}

func (c *wsConn) currentReplicaScope() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.replicaScope
}

func (c *wsConn) currentVisibilityScope() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.visibilityScope
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
	c.closeAllReplicas()
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
	if message.Type == "replica.reset" || message.Type == "replica.error" {
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
// the same commit still coalesce on each connection, while steady reducer
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

func (c *wsConn) writeReplicaReady(message serverMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.replicaReadyMany {
		c.writeLocked(message)
	} else if c.conn != nil {
		c.pendingReady = append(c.pendingReady, message)
		c.armReadyTimerLocked()
	}
	if message.Cursor != nil {
		c.resolvePendingWatermarksLocked(message.ID, message.Cursor.Revision)
	}
}

func (c *wsConn) writeReplicaWatermark(revision uint64, waiting []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.replicaWatermark || c.conn == nil || revision == 0 {
		return
	}
	pending := pendingReplicaWatermark{revision: revision, waiting: make(map[string]struct{}, len(waiting))}
	for _, id := range waiting {
		if _, current := c.replicas[id]; id != "" && current {
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
		c.pendingReady = append(c.pendingReady, serverMessage{Type: "replica.watermark", Revision: pending.revision})
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
		if pending[0].Type != "replica.ready" {
			c.writeLocked(pending[0])
			pending = pending[1:]
			continue
		}
		end := 1
		for end < len(pending) && pending[end].Type == "replica.ready" {
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
	ready := make([]replicaReadyMessage, 0, len(pending))
	for _, message := range pending {
		truncated := false
		if message.Truncated != nil {
			truncated = *message.Truncated
		}
		ready = append(ready, replicaReadyMessage{
			ID: message.ID, Path: message.Path, Cursor: message.Cursor, Mode: message.Mode,
			Digest: message.Digest, Truncated: truncated,
		})
	}
	c.writeLocked(serverMessage{Type: "replica.readyMany", Ready: ready})
}

func (c *wsConn) writeLocked(message serverMessage) {
	if c.conn == nil {
		return
	}
	_ = c.conn.SetWriteDeadline(time.Now().Add(websocketWriteTimeout))
	payload, err := json.Marshal(message)
	if err != nil {
		return
	}
	if err := c.conn.WriteMessage(websocket.TextMessage, payload); err != nil {
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
		identityMatches := connection.user != nil && connection.user.ID == userID
		if connection.member != nil {
			identityMatches = identityMatches || connection.member.ID == userID || connection.member.AccountID == userID
		}
		matches := connection.project == projectID && identityMatches && strings.HasPrefix(connection.authToken, "gvx_session_")
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

// enforceNativeAppAuthConnections immediately cancels connections without a
// canonical Gonvex app session when project authentication is enabled.
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
	AccountID      string   `json:"accountId,omitempty"`
	AccountEmail   string   `json:"accountEmail,omitempty"`
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
func (s *Server) websocketCounts() (connections int, accounts int, subscriptions int) {
	s.wsMu.RLock()
	conns := make([]*wsConn, 0, len(s.wsConns))
	for conn := range s.wsConns {
		conns = append(conns, conn)
	}
	s.wsMu.RUnlock()

	seen := map[string]bool{}
	for _, conn := range conns {
		conn.mu.Lock()
		subscriptions += len(conn.subs) + len(conn.replicas)
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
	accounts := map[string]bool{}
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
			Subscriptions:  make([]string, 0, len(conn.subs)+len(conn.replicas)),
		}
		if detail.ID == "" {
			detail.ID = fmt.Sprintf("conn-%06d", len(snapshot.Details)+1)
		}
		if conn.user != nil {
			detail.AccountID = conn.user.ID
			detail.AccountEmail = conn.user.Email
		}
		for _, sub := range conn.subs {
			detail.Subscriptions = append(detail.Subscriptions, sub.path)
		}
		for _, replicaSubscription := range conn.replicas {
			detail.Subscriptions = append(detail.Subscriptions, replicaSubscription.path)
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
		identity := detail.AccountID
		if identity == "" {
			identity = "anonymous"
		}
		accounts[identity] = true
	}
	sort.Slice(snapshot.Details, func(left, right int) bool {
		if snapshot.Details[left].LastActiveAt == snapshot.Details[right].LastActiveAt {
			return snapshot.Details[left].ID < snapshot.Details[right].ID
		}
		return snapshot.Details[left].LastActiveAt > snapshot.Details[right].LastActiveAt
	})
	snapshot.Connections = totalConnections
	snapshot.Accounts = len(accounts)
	return snapshot
}

// rerunProjectSubscriptions refreshes every Live Query after a module generation
// is installed. A client can connect while /dev/sync is still loading the
// generation and receive an initial "not implemented" error; table-specific
// change routing is insufficient because reference-data queries do not depend on
// the tasks table. The new generation can also change any Query's implementation,
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
	if originCommandID := strings.TrimSpace(change.originCommandID); originCommandID != "" {
		tableKey = "commit\x1f" + originCommandID
	}
	key := strings.Join([]string{change.project, change.tenant, tableKey}, ":")
	pending := s.tableChanges[key]
	pending.project = change.project
	pending.tenant = change.tenant
	pending.originCommandID = strings.TrimSpace(change.originCommandID)
	if pending.originCommandID != "" {
		// The committed change-feed event carries the actual commit timestamp; the
		// LISTEN event is observed slightly later. Keep the earliest positive
		// timestamp so client TTLU frames correlate with the reducer result and
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
		if pending.observedDetails == nil {
			pending.observedDetails = map[string]tableChangeDetail{}
		}
		pending.observedDetails[table] = mergeTableChangeDetail(pending.observedDetails[table], detailForTable(change, table))
		if pending.cacheInvalidatedTables == nil {
			pending.cacheInvalidatedTables = map[string]bool{}
		}
		pending.cacheInvalidatedTables[table] = true
	}
	s.tableChanges[key] = pending
	if timer := s.tableChangeWait[key]; timer != nil {
		timer.Stop()
	}
	delay := tableChangeDebounce
	// Change-feed notifications arrive only after commit. The short batch window
	// merges tables from one revision; it has no correctness meaning.
	delay = tableChangeTriggerBatch
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

	delivery := pendingChangeForDelivery(change)
	if len(tableChangeTables(delivery)) == 0 || (len(delivery.tables) == 0 && strings.TrimSpace(delivery.table) == "") {
		// A malformed wake-up is not converted into a guessed invalidation.
		// The listener's revision recovery path supplies an authoritative refresh.
		return
	}
	changedTables := tableChangeTables(delivery)
	cacheTables := make([]string, 0, len(changedTables))
	for _, table := range changedTables {
		if !change.cacheInvalidatedTables[table] {
			cacheTables = append(cacheTables, table)
		}
	}
	s.invalidateTableCaches(delivery.project, delivery.tenant, cacheTables)
	if !delivery.triggerObserved {
		s.invalidateVisibilityContexts(delivery.project, delivery.tenant, changedTables)
		s.subscriptions.rebindVisibilityForChange(delivery)
		s.resetReplicasForVisibilityChange(delivery)
	}
	s.subscriptions.requestChange(delivery)
}

func (s *Server) invalidateTableCaches(projectID string, tenantID string, tables []string) {
	if len(tables) == 0 {
		return
	}
	for _, table := range tables {
		s.cache.invalidateRows(context.Background(), projectID, tenantID, table)
	}
}

func detailForTable(change tableChange, table string) tableChangeDetail {
	if detail, ok := change.details[table]; ok {
		return detail
	}
	return tableChangeDetail{
		operation: change.operation, changedColumns: append([]string(nil), change.changedColumns...),
		rowIDs:    cloneBoolMap(change.rowIDs),
		oldValues: cloneRawMessages(change.oldValues), newValues: cloneRawMessages(change.newValues),
	}
}

func mergeTableChangeDetail(current, next tableChangeDetail) tableChangeDetail {
	if current.operation == "" {
		current.operation = next.operation
	} else if next.operation != "" && current.operation != next.operation {
		current.operation = "mixed"
	}
	current.changedColumns = appendUniqueStrings(current.changedColumns, next.changedColumns...)
	if current.rowIDs == nil && len(next.rowIDs) > 0 {
		current.rowIDs = map[string]bool{}
	}
	for id := range next.rowIDs {
		current.rowIDs[id] = true
	}
	current.oldValues = append(current.oldValues, cloneRawMessages(next.oldValues)...)
	current.newValues = append(current.newValues, cloneRawMessages(next.newValues)...)
	return current
}

func pendingChangeForDelivery(pending pendingTableChange) tableChange {
	change := tableChange{
		project: pending.project, tenant: pending.tenant, originCommandID: pending.originCommandID,
		changedAtMS: pending.changedAtMS,
		tables:      map[string]bool{}, details: map[string]tableChangeDetail{},
	}
	for table, detail := range pending.observedDetails {
		change.tables[table] = true
		change.details[table] = detail
	}
	if len(change.tables) == 1 {
		for table := range change.tables {
			change.table = table
			detail := change.details[table]
			change.operation = detail.operation
			change.changedColumns = append([]string(nil), detail.changedColumns...)
			change.rowIDs = cloneBoolMap(detail.rowIDs)
			change.oldValues = cloneRawMessages(detail.oldValues)
			change.newValues = cloneRawMessages(detail.newValues)
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

func cloneRawMessages(source []json.RawMessage) []json.RawMessage {
	cloned := make([]json.RawMessage, 0, len(source))
	for _, value := range source {
		cloned = append(cloned, append(json.RawMessage(nil), value...))
	}
	return cloned
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
	return s.executeTenantQueryForCallerTracked(ctx, projectID, tenantID, caller, path, rawArgs, "internal")
}

func (s *Server) executeTenantQueryForCallerTracked(ctx context.Context, projectID string, tenantID string, caller callerContext, path string, rawArgs json.RawMessage, reason string) (result any, err error) {
	kind := s.functionKind(projectID, path, "query")
	s.metrics.recordFunctionStart(kind)
	execution := newRuntimeFunctionLog(projectID, tenantID, path, kind, caller, rawArgs)
	execution.entry.Source = "database"
	execution.entry.Reason = reason
	defer func() {
		s.metrics.recordFunctionEnd(kind)
		s.metrics.recordFunctionExecution(execution, err)
	}()

	databaseStartedAt := time.Now()
	result, err = s.executeTenantQueryForCallerUncached(ctx, projectID, tenantID, caller, path, rawArgs, false)
	s.metrics.recordReactive(func(metric *reactiveMetricState) {
		metric.DatabaseQueryCount++
		metric.DatabaseQueryDurationMS += float64(time.Since(databaseStartedAt).Microseconds()) / 1000
	})
	return result, err
}

func (s *Server) executeTenantQueryForCallerUncached(ctx context.Context, projectID string, tenantID string, caller callerContext, path string, rawArgs json.RawMessage, allowInternal bool) (any, error) {
	engine := s.engineForProject(ctx, projectID)
	if engine != nil {
		descriptor, ok := engine.Describe(path)
		if !ok {
			return nil, fmt.Errorf("query %q is not implemented by the runtime", path)
		}
		if descriptor.Kind != moduleengine.KindQuery {
			return nil, fmt.Errorf("function %q is not a query", path)
		}
		if descriptor.Internal != allowInternal {
			if descriptor.Internal {
				return nil, fmt.Errorf("query %q is internal", path)
			}
			return nil, fmt.Errorf("Action Query tool %q must target an internal Query", path)
		}
		if descriptor.Delivery == "" || descriptor.Delivery == gonvex.DeliveryOneShot {
			if descriptor.Dependencies.LiveQueryPlan == nil {
				return nil, fmt.Errorf("one-shot query %q requires a structured live query plan", path)
			}
			// One-shot Queries use the exact same structured SQL and centralized
			// visibility path as Live Queries. Never invoke arbitrary module SQL.
			payload, marshalErr := json.Marshal(descriptor.Dependencies.LiveQueryPlan)
			if marshalErr != nil {
				return nil, fmt.Errorf("query %q live query plan: %w", path, marshalErr)
			}
			var plan manifest.LiveQueryPlan
			if unmarshalErr := json.Unmarshal(payload, &plan); unmarshalErr != nil {
				return nil, fmt.Errorf("query %q live query plan: %w", path, unmarshalErr)
			}
			return s.executeStructuredLiveQuery(ctx, projectID, tenantID, caller, plan, rawArgs)
		}
		return nil, fmt.Errorf("query %q is delivered as %s and cannot be called as a one-shot query", path, descriptor.Delivery)
	}
	return nil, fmt.Errorf("query %q is not implemented by the runtime", path)
}

func (s *Server) executeReducer(ctx context.Context, projectID string, path string, rawArgs json.RawMessage) (result any, err error) {
	return s.executeTenantReducer(ctx, projectID, tenantIDFromRequest(projectID, ""), path, rawArgs)
}

func (s *Server) executeTenantReducer(ctx context.Context, projectID string, tenantID string, path string, rawArgs json.RawMessage) (result any, err error) {
	return s.executeTenantReducerForCaller(ctx, projectID, tenantID, callerContext{}, path, rawArgs)
}

func (s *Server) executeTenantReducerForCaller(ctx context.Context, projectID string, tenantID string, caller callerContext, path string, rawArgs json.RawMessage) (result any, err error) {
	kind := s.functionKind(projectID, path, "reducer")
	s.metrics.recordFunctionStart(kind)
	execution := newRuntimeFunctionLog(projectID, tenantID, path, kind, caller, rawArgs)
	defer func() {
		s.metrics.recordFunctionEnd(kind)
		s.metrics.recordFunctionExecution(execution, err)
	}()

	engine := s.engineForProject(ctx, projectID)
	if engine == nil {
		return nil, fmt.Errorf("project %q has no active TypeScript module", projectID)
	}
	if _, ok := engine.Describe(path); ok {
		reducerCtx, err := s.reducerContext(ctx, projectID, tenantID, caller)
		if err != nil {
			return nil, err
		}
		result, err := s.executeRegisteredReducer(engine, reducerCtx, path, rawArgs)
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
			// right DB — not the project control-plane DB used during tenants.create itself.
			if err := s.runTenantsOnProvisioned(ctx, projectID, result, caller); err != nil {
				return nil, err
			}
		}
		return result, nil
	}
	return nil, fmt.Errorf("reducer %q is not implemented by the runtime", path)
}

// runTenantsOnProvisioned invokes the optional internal reducer
// "tenants.onProvisioned" against the newly created tenant database after
// provisionCreatedTenant succeeds. Apps that do not register the hook are
// skipped. Failures surface so create does not silently leave an empty shell.
func (s *Server) runTenantsOnProvisioned(ctx context.Context, projectID string, result any, caller callerContext) error {
	tenantID := tenantIDFromReducerResult(result)
	if tenantID == "" {
		return nil
	}
	engine := s.engineForProject(ctx, projectID)
	if engine == nil {
		return nil
	}
	descriptor, ok := engine.Describe("tenants.onProvisioned")
	if !ok || descriptor.Kind != moduleengine.KindReducer || !descriptor.Internal {
		return nil
	}
	reducerCtx, err := s.reducerContext(ctx, projectID, tenantID, caller)
	if err != nil {
		return fmt.Errorf("tenants.onProvisioned: %w", err)
	}
	rawArgs, err := json.Marshal(map[string]any{"tenantId": tenantID})
	if err != nil {
		return fmt.Errorf("tenants.onProvisioned args: %w", err)
	}
	if _, err := s.runReducerInTx(reducerCtx, "tenants.onProvisioned", rawArgs, moduleengine.ReducerExec(engine.InvokeInternalReducer)); err != nil {
		return fmt.Errorf("tenants.onProvisioned: %w", err)
	}
	return nil
}

func (s *Server) executeRegisteredReducer(engine moduleengine.ModuleEngine, reducerCtx *gonvex.ReducerCtx, path string, rawArgs json.RawMessage) (any, error) {
	return s.runReducerInTx(reducerCtx, path, rawArgs, moduleengine.ReducerExec(engine.InvokeReducer))
}

// runReducerInTx runs a reducer handler inside a database transaction
// when a database is configured, committing on success and rolling back on
// error. It is shared by client-triggered reducers and scheduled internal
// reducers so both get the same transactional guarantees.
func (s *Server) runReducerInTx(reducerCtx *gonvex.ReducerCtx, path string, rawArgs json.RawMessage, exec func(*gonvex.ReducerCtx, string, json.RawMessage) (any, error)) (any, error) {
	// Reducer execution intentionally receives a capability-restricted context,
	// but do not mutate the caller's context while applying that restriction.
	// A context may be reused for a replay (and tests do so); clearing DB on the
	// first call would otherwise bypass the transaction and idempotency claim on
	// the next call.
	executionCtx := *reducerCtx
	if executionCtx.DB == nil {
		restrictReducerCapabilities(&executionCtx)
		return exec(&executionCtx, path, rawArgs)
	}
	database := executionCtx.DB
	if executionCtx.Context == nil {
		executionCtx.Context = context.Background()
	}
	claim, hasClaim := reducerIdempotencyFromContext(executionCtx.Context)
	if hasClaim {
		if err := s.ensureReducerIdempotencyStorage(executionCtx.Context, database, executionCtx.DatabaseURL); err != nil {
			return nil, err
		}
	}
	tx, err := database.BeginTx(executionCtx.Context, nil)
	if err != nil {
		return nil, err
	}
	executionCtx.Tx = tx
	if hasClaim {
		claimed, err := claimReducerIdempotency(executionCtx.Context, tx, claim, path)
		if err != nil {
			_ = tx.Rollback()
			return nil, err
		}
		if !claimed {
			// A previous delivery of this write already committed. Serve its
			// stored result instead of executing the handler a second time.
			_ = tx.Rollback()
			return replayReducerIdempotencyResult(executionCtx.Context, database, claim, path)
		}
	}
	if commandID := commandIDFromContext(executionCtx.Context); commandID != "" {
		if _, err := tx.ExecContext(executionCtx.Context, `SELECT set_config('gonvex.command_id', $1, true)`, commandID); err != nil {
			_ = tx.Rollback()
			return nil, err
		}
	}
	originalScheduler := executionCtx.Scheduler
	deferred := newDeferredScheduler(originalScheduler)
	executionCtx.Scheduler = deferred
	executionCtx.Outbox = postgresActionOutbox{tx: tx, user: executionCtx.Auth.Account}
	// Reducer code receives only the transaction handle. Raw pools would allow
	// an accidental write to commit outside the atomic business intent.
	restrictReducerCapabilities(&executionCtx)
	defer func() {
		executionCtx.Scheduler = originalScheduler
	}()
	result, err := exec(&executionCtx, path, rawArgs)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if hasClaim {
		if err := storeReducerIdempotencyResult(executionCtx.Context, tx, claim, result); err != nil {
			_ = tx.Rollback()
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	if hasClaim {
		s.maybeSweepReducerIdempotency(database, executionCtx.DatabaseURL)
	}
	if err := deferred.flush(); err != nil {
		executionCtx.Logger.Error("failed to publish committed scheduled work", "path", path, "error", err)
	}
	go s.drainActionOutbox(executionCtx.ProjectID, executionCtx.TenantID)
	return result, nil
}

func restrictReducerCapabilities(ctx *gonvex.ReducerCtx) {
	ctx.DB = nil
	ctx.TenantDB = nil
	ctx.Storage = gonvex.UnavailableStorage()
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

	engine := s.engineForProject(ctx, projectID)
	if engine == nil {
		return nil, fmt.Errorf("project %q has no active TypeScript module", projectID)
	}
	if descriptor, ok := engine.Describe(path); ok {
		if descriptor.ActionProfile == "agent" {
			select {
			case s.agentActionAdmission <- struct{}{}:
				defer func() { <-s.agentActionAdmission }()
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		actionCtx, err := s.actionContext(ctx, projectID, tenantID, caller)
		if err != nil {
			return nil, err
		}
		if descriptor.ActionProfile == "agent" {
			actionCtx.ExecutionTimeout = s.config.AgentActionTimeout
		}
		result, err := engine.InvokeAction(actionCtx, moduleengine.Invocation{Path: path, Args: rawArgs})
		if err != nil {
			return nil, err
		}
		return result.Value, nil
	}
	return nil, fmt.Errorf("action %q is not implemented by the runtime", path)
}

func (s *Server) functionKind(projectID string, path string, fallback string) string {
	if entry, ok := s.runtime.ManifestForProject(projectID).Functions[path]; ok && entry.Kind != "" {
		return string(entry.Kind)
	}
	if engine := s.runtime.EngineForProject(projectID); engine != nil {
		if descriptor, ok := engine.Describe(path); ok && descriptor.Kind != "" {
			return string(descriptor.Kind)
		}
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

func (s *Server) reducerContext(ctx context.Context, projectID string, tenantID string, caller callerContext) (*gonvex.ReducerCtx, error) {
	runtimeCtx, err := s.runtimeContext(ctx, projectID, tenantID, caller)
	if err != nil {
		return nil, err
	}
	return &gonvex.ReducerCtx{RuntimeContext: runtimeCtx}, nil
}

func (s *Server) actionContext(ctx context.Context, projectID string, tenantID string, caller callerContext) (*gonvex.ActionCtx, error) {
	runtimeCtx, err := s.runtimeContext(ctx, projectID, tenantID, caller)
	if err != nil {
		return nil, err
	}
	// Actions own external/non-transactional work. They cannot reach an
	// application database handle; durable state changes
	// must re-enter through ctx.Reducers.Call.
	runtimeCtx.DB = nil
	runtimeCtx.TenantDB = nil
	runtimeCtx.Tx = nil
	runtimeCtx.Reducers = &actionReducerCaller{
		server: s, project: projectID, tenant: tenantID, caller: caller,
		parent: commandIDFromContext(ctx),
	}
	runtimeCtx.Queries = &actionQueryCaller{server: s, project: projectID, tenant: tenantID, caller: caller}
	runtimeCtx.AgentActionsEnabled = s.config.AgentActionsEnabled
	runtimeCtx.ExecutionTimeout = s.config.ModuleHostExecutionTimeout
	if s.sandboxes != nil && s.sandboxes.Enabled() && caller.user != nil {
		runtimeCtx.Sandbox = &actionSandbox{
			manager: s.sandboxes, dataFiles: s.dataFiles, storage: runtimeCtx.Storage,
			scope: gonvexsandbox.Scope{ProjectID: projectID, TenantID: tenantIDFromRequest(projectID, tenantID), AccountID: caller.user.ID},
		}
	}
	return &gonvex.ActionCtx{RuntimeContext: runtimeCtx}, nil
}

func (s *Server) runtimeContext(ctx context.Context, projectID string, tenantID string, caller callerContext) (gonvex.RuntimeContext, error) {
	if err := s.requireProjectDatabase(projectID); err != nil {
		return gonvex.RuntimeContext{}, err
	}
	activeTenant := tenantIDFromRequest(projectID, tenantID)
	if caller.user != nil && caller.member == nil {
		member, err := s.loadTenantMember(ctx, projectID, activeTenant, caller.user.ID)
		if err != nil {
			return gonvex.RuntimeContext{}, err
		}
		caller.member = member
		caller.permissions = member.Permissions
	}
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
	logger := slog.Default().With("project", projectID, "tenant", activeTenant)
	storageAPI := s.storageForTenant(ctx, projectID, activeTenant, store.DB, caller, logger)
	return gonvex.RuntimeContext{
		Context:          ctx,
		ProjectID:        projectID,
		TenantID:         activeTenant,
		OperationID:      commandIDFromContext(ctx),
		Auth:             gonvex.AuthContext{Account: caller.user},
		Tenant:           &gonvex.TenantIdentity{ID: activeTenant, ProjectID: projectID},
		Member:           caller.member,
		DatabaseURL:      store.DatabaseURL,
		DB:               store.DB,
		TenantDB:         store.DB,
		Storage:          storageAPI,
		Scheduler:        s.scheduler.For(projectID, activeTenant),
		Logger:           logger,
		Env:              s.projectEnvValues(ctx, projectID),
		ExecutionTimeout: s.config.ModuleHostExecutionTimeout,
	}, nil
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
