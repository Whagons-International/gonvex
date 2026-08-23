package gonvex

import (
	"context"
	"database/sql"
	"log/slog"
	"time"
)

type DeliveryMode string

const (
	DeliveryOneShot DeliveryMode = "oneShot"
	DeliveryLive    DeliveryMode = "live"
	DeliveryReplica DeliveryMode = "replica"
)

// ReplicaCollectionDefinition describes a bounded, durable entity collection. Its handler returns
// the initial authorized snapshot; subsequent inserts, updates, and deletes are
// delivered from Gonvex's transactional change log.
type ReplicaCollectionDefinition struct {
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

// FunctionDependencies are generated, inspectable delivery contracts. Live
// Query reads are derived from LiveQueryPlan; application code never declares
// write sets or broad invalidations.
type FunctionDependencies struct {
	ShareByPermissions  bool
	ShareResultFrom     string
	ShareResultField    string
	LiveQueryPlan       *LiveQueryPlan
	NonOptimisticReason string
}

// Account is the global human identity authenticated by the Gonvex Control
// Plane. Tenant-specific roles, teams, permissions, and business profile data
// never belong here; they belong to Member.
type Account struct {
	ID        string `json:"id"`
	Email     string `json:"email,omitempty"`
	Name      string `json:"name,omitempty"`
	AvatarURL string `json:"avatarUrl,omitempty"`
}

type AuthContext struct {
	Account *Account `json:"account,omitempty"`
}

// TenantIdentity identifies the active tenant selected through the Control
// Plane. It intentionally contains no database URL.
type TenantIdentity struct {
	ID        string `json:"id"`
	ProjectID string `json:"projectId"`
	Name      string `json:"name,omitempty"`
}

// Member is the tenant-local identity and authorization subject. ID is the
// stable tenant member ID referenced by tasks, approvals, teams, and logs;
// AccountID links it to the one global Account.
type Member struct {
	ID          string         `json:"id"`
	AccountID   string         `json:"accountId"`
	Status      string         `json:"status"`
	DisplayName string         `json:"displayName,omitempty"`
	AvatarURL   string         `json:"avatarUrl,omitempty"`
	Role        string         `json:"role,omitempty"`
	Permissions map[string]any `json:"permissions,omitempty"`
}

type RuntimeContext struct {
	context.Context

	ProjectID   string
	TenantID    string
	OperationID string
	Auth        AuthContext
	Tenant      *TenantIdentity
	Member      *Member
	DatabaseURL string
	DB          *sql.DB
	TenantDB    *sql.DB
	Tx          *sql.Tx
	Storage     StorageAPI
	Scheduler   Scheduler
	Reducers    ReducerAPI
	Queries     QueryAPI
	Sandbox     SandboxAPI
	Outbox      ActionOutbox
	// AgentActionsEnabled is an operator gate. A module declaration can narrow
	// capabilities but cannot turn the agent runtime on by itself.
	AgentActionsEnabled bool
	// ExecutionTimeout is the host-approved budget for this invocation. Module
	// declarations cannot raise it.
	ExecutionTimeout time.Duration
	Logger           *slog.Logger

	// Env holds only project-scoped variables resolved by the host. Raw runtime
	// process environment is never exposed to application modules.
	Env map[string]string
}

type QueryCtx struct {
	RuntimeContext
}

type ReducerCtx struct {
	RuntimeContext
}

type ActionCtx struct {
	RuntimeContext
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
	if c.Tenant == nil {
		c.Tenant = &TenantIdentity{ID: c.TenantID, ProjectID: c.ProjectID}
	}
	if c.Storage == nil {
		c.Storage = storageUnavailable{}
	}
	if c.Scheduler == nil {
		c.Scheduler = schedulerUnavailable{}
	}
	if c.Reducers == nil {
		c.Reducers = UnavailableReducers()
	}
	if c.Queries == nil {
		c.Queries = UnavailableQueries()
	}
	if c.Outbox == nil {
		c.Outbox = UnavailableActionOutbox()
	}
}

func (c *QueryCtx) normalize() {
	c.RuntimeContext.normalize()
}

func (c *ReducerCtx) normalize() {
	c.RuntimeContext.normalize()
}

func (c *ActionCtx) normalize() {
	c.RuntimeContext.normalize()
}
