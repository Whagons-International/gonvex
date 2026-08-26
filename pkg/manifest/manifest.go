package manifest

import "encoding/json"

type FunctionKind string

const (
	FunctionKindQuery   FunctionKind = "query"
	FunctionKindReducer FunctionKind = "reducer"
	FunctionKindAction  FunctionKind = "action"
)

type DeliveryMode string

const (
	DeliveryOneShot DeliveryMode = "oneShot"
	DeliveryLive    DeliveryMode = "live"
	DeliveryReplica DeliveryMode = "replica"
)

const NotifySchemaVersion = "15"

type FunctionEntry struct {
	Kind               FunctionKind                 `json:"kind"`
	Handler            string                       `json:"handler"`
	File               string                       `json:"file"`
	Args               ModuleSchema                 `json:"args,omitempty"`
	Result             ModuleSchema                 `json:"result,omitempty"`
	Internal           bool                         `json:"internal,omitempty"`
	Delivery           DeliveryMode                 `json:"delivery,omitempty"`
	Dependencies       FunctionDependencies         `json:"dependencies,omitempty"`
	Replica            *ReplicaCollectionDefinition `json:"replica,omitempty"`
	Offline            any                          `json:"offline,omitempty"`
	Optimistic         any                          `json:"optimistic,omitempty"`
	ActionProfile      string                       `json:"actionProfile,omitempty"`
	ActionCapabilities *ActionCapabilities          `json:"actionCapabilities,omitempty"`
}

type ActionCapabilities struct {
	NetworkOrigins []string                     `json:"networkOrigins,omitempty"`
	Secrets        []string                     `json:"secrets,omitempty"`
	Tools          map[string]ActionToolBinding `json:"tools,omitempty"`
	Scheduler      bool                         `json:"scheduler,omitempty"`
	Storage        bool                         `json:"storage,omitempty"`
	Sandbox        *SandboxCapability           `json:"sandbox,omitempty"`
}

type SandboxCapability struct {
	DuckDB bool `json:"duckdb,omitempty"`
}

type ActionToolBinding struct {
	Kind     FunctionKind `json:"kind"`
	Function string       `json:"function"`
}

// ModuleSchema is the JSON representation of the recursive PortableSchema
// contract shared by the TypeScript SDK, generated manifests, and module
// hosts. It remains a map at this boundary so Go can transport every schema
// variant without inventing a second schema language.
type ModuleSchema map[string]any

// ReplicaCollectionDefinition describes an entity-shaped, locally materialized collection.
// V1 intentionally supports a single source table and equality filters. More
// complex joins and aggregates remain ordinary live queries.
type ReplicaCollectionDefinition struct {
	Table                 string            `json:"table"`
	Key                   string            `json:"key"`
	Columns               []string          `json:"columns"`
	EqualFilters          map[string]string `json:"equalFilters,omitempty"`
	ExcludeWhenSet        []string          `json:"excludeWhenSet,omitempty"`
	VisibilityTables      []string          `json:"visibilityTables,omitempty"`
	VisibilityPlanHash    string            `json:"visibilityPlanHash,omitempty"`
	OrderBy               string            `json:"orderBy,omitempty"`
	OrderDirection        string            `json:"orderDirection,omitempty"`
	Mode                  string            `json:"mode,omitempty"`
	MaxRows               int               `json:"maxRows,omitempty"`
	MaxBytes              int64             `json:"maxBytes,omitempty"`
	RetentionMilliseconds int64             `json:"retentionMs,omitempty"`
}

// FunctionDependencies contain generated, inspectable delivery contracts.
// Live Queries without a structured plan are rejected rather than broadly
// invalidated.
type FunctionDependencies struct {
	ShareByPermissions  bool           `json:"shareByPermissions,omitempty"`
	ShareResultFrom     string         `json:"shareResultFrom,omitempty"`
	ShareResultField    string         `json:"shareResultField,omitempty"`
	LiveQueryPlan       *LiveQueryPlan `json:"liveQueryPlan,omitempty"`
	NonOptimisticReason string         `json:"nonOptimisticReason,omitempty"`
}

type LiveQueryPlan struct {
	Table      string          `json:"table"`
	Key        string          `json:"key"`
	Columns    []string        `json:"columns,omitempty"`
	ResultPath []string        `json:"resultPath,omitempty"`
	Where      *LiveExpression `json:"where,omitempty"`
	Search     *LiveSearch     `json:"search,omitempty"`
	Filters    *LiveFilters    `json:"filters,omitempty"`
	Sort       *LiveSort       `json:"sort,omitempty"`
	Window     *LiveWindow     `json:"window,omitempty"`
	ServerOnly bool            `json:"serverOnly,omitempty"`
}

type FilterOperator string

type LiveFilters struct {
	Argument         string           `json:"argument"`
	AllowedColumns   []string         `json:"allowedColumns"`
	AllowedOperators []FilterOperator `json:"allowedOperators"`
}

type LiveExpression struct {
	Operator string            `json:"operator"`
	Column   string            `json:"column,omitempty"`
	Value    *LiveValue        `json:"value,omitempty"`
	ValueTo  *LiveValue        `json:"valueTo,omitempty"`
	Children []*LiveExpression `json:"children,omitempty"`
}

type LiveValue struct {
	Argument       string `json:"-"`
	Literal        any    `json:"-"`
	literalPresent bool
}

// MarshalJSON preserves an explicitly declared null literal. A plain
// `json:"literal,omitempty"` tag cannot distinguish {"literal":null} from an
// absent literal field, but that distinction is part of the signed TypeScript
// module artifact contract.
func (value LiveValue) MarshalJSON() ([]byte, error) {
	fields := map[string]any{}
	if value.Argument != "" {
		fields["argument"] = value.Argument
	}
	if value.literalPresent || value.Literal != nil {
		fields["literal"] = value.Literal
	}
	return json.Marshal(fields)
}

// UnmarshalJSON records whether literal appeared even when its value was
// null, so decoding and re-encoding an uploaded artifact is lossless.
func (value *LiveValue) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	*value = LiveValue{}
	if raw, exists := fields["argument"]; exists {
		if err := json.Unmarshal(raw, &value.Argument); err != nil {
			return err
		}
	}
	if raw, exists := fields["literal"]; exists {
		value.literalPresent = true
		if err := json.Unmarshal(raw, &value.Literal); err != nil {
			return err
		}
	}
	return nil
}

type LiveSearch struct {
	Argument string   `json:"argument"`
	Columns  []string `json:"columns"`
}
type LiveSort struct {
	ColumnArgument    string   `json:"columnArgument,omitempty"`
	DirectionArgument string   `json:"directionArgument,omitempty"`
	AllowedColumns    []string `json:"allowedColumns"`
	DefaultColumn     string   `json:"defaultColumn"`
	DefaultDirection  string   `json:"defaultDirection"`
}
type LiveWindow struct {
	OffsetArgument string `json:"offsetArgument"`
	LimitArgument  string `json:"limitArgument"`
	DefaultLimit   int    `json:"defaultLimit"`
	MaxLimit       int    `json:"maxLimit"`
	Count          string `json:"count,omitempty"`
}

type Schema struct {
	Tables             map[string]Table `json:"tables"`
	ControlPlaneTables map[string]Table `json:"controlPlaneTables,omitempty"`
	TenantTables       map[string]Table `json:"tenantTables,omitempty"`
}

type Table struct {
	Columns map[string]Column `json:"columns"`
	Indexes map[string]Index  `json:"indexes"`
}

type Column struct {
	Type       string `json:"type"`
	Nullable   bool   `json:"nullable"`
	PrimaryKey bool   `json:"primaryKey"`
}

type Index struct {
	Columns []string `json:"columns"`
	Unique  bool     `json:"unique"`
	Kind    string   `json:"kind,omitempty"`
}

// VisibilityPlan describes how rows from one source table are authorized.
// The manifest carries this language-neutral plan; runtime interpretation is
// deliberately separate from artifact declaration and normalization.
type VisibilityPlan struct {
	Table string                   `json:"table"`
	Key   string                   `json:"key"`
	Sets  map[string]VisibilitySet `json:"sets"`
	Where *VisibilityExpression    `json:"where"`
}

type VisibilitySet struct {
	Table      string                 `json:"table"`
	Alias      string                 `json:"alias,omitempty"`
	Select     string                 `json:"select"`
	SelectFrom string                 `json:"selectFrom,omitempty"`
	Joins      []VisibilityJoin       `json:"joins"`
	Where      []VisibilityConstraint `json:"where"`
}

type VisibilityJoin struct {
	Table       string `json:"table"`
	Alias       string `json:"alias,omitempty"`
	LeftAlias   string `json:"leftAlias,omitempty"`
	LeftColumn  string `json:"leftColumn"`
	RightColumn string `json:"rightColumn"`
}

type VisibilityConstraint struct {
	Table   string `json:"table"`
	Column  string `json:"column"`
	Context string `json:"context"`
}

type VisibilityExpression struct {
	Operator string                  `json:"operator"`
	Column   string                  `json:"column,omitempty"`
	Context  string                  `json:"context,omitempty"`
	Set      string                  `json:"set,omitempty"`
	Value    string                  `json:"value,omitempty"`
	Children []*VisibilityExpression `json:"children,omitempty"`
}

// ModuleArtifact is the language-neutral module payload emitted by the
// TypeScript CLI. The fields describe the artifact rather than a runtime
// implementation so other module languages can use the same wire shape.
type ModuleArtifact struct {
	Language                    string                    `json:"language"`
	Generation                  int                       `json:"generation"`
	Hash                        string                    `json:"hash,omitempty"`
	Entrypoint                  string                    `json:"entrypoint"`
	Functions                   map[string]ModuleFunction `json:"functions"`
	Crons                       []ModuleCron              `json:"crons,omitempty"`
	Files                       map[string]string         `json:"files"`
	JavaScript                  *ModuleJavaScript         `json:"javascript,omitempty"`
	Visibility                  map[string]VisibilityPlan `json:"visibility,omitempty"`
	InvitationAcceptanceReducer string                    `json:"invitationAcceptanceReducer,omitempty"`
}

// ModuleCron is a language-neutral recurring Reducer or Action declaration.
// Exactly one of IntervalMS and Expression must be set.
type ModuleCron struct {
	Name       string          `json:"name"`
	Function   string          `json:"function"`
	Args       json.RawMessage `json:"args,omitempty"`
	Scope      string          `json:"scope"`
	IntervalMS int64           `json:"intervalMs,omitempty"`
	Expression string          `json:"expression,omitempty"`
}

// ModuleFunction carries generated function metadata and portable schema and
// reducer-policy values. The runtime validates them before activation.
type ModuleFunction struct {
	Kind               FunctionKind                 `json:"kind"`
	Handler            string                       `json:"handler"`
	File               string                       `json:"file"`
	Export             string                       `json:"export,omitempty"`
	Args               ModuleSchema                 `json:"args,omitempty"`
	Result             ModuleSchema                 `json:"result,omitempty"`
	Dependencies       FunctionDependencies         `json:"dependencies,omitempty"`
	Internal           bool                         `json:"internal,omitempty"`
	Delivery           DeliveryMode                 `json:"delivery,omitempty"`
	Replica            *ReplicaCollectionDefinition `json:"replica,omitempty"`
	Offline            any                          `json:"offline,omitempty"`
	Optimistic         any                          `json:"optimistic,omitempty"`
	ActionProfile      string                       `json:"actionProfile,omitempty"`
	ActionCapabilities *ActionCapabilities          `json:"actionCapabilities,omitempty"`
	Interactive        bool                         `json:"interactive,omitempty"`
	Classification     string                       `json:"classification,omitempty"`
	Description        string                       `json:"description,omitempty"`
	Agent              *FunctionAgentMetadata       `json:"agent,omitempty"`
}

type FunctionAgentMetadata struct {
	Tags         []string `json:"tags,omitempty"`
	Confirmation string   `json:"confirmation,omitempty"`
}

type ModuleJavaScript struct {
	Path      string `json:"path"`
	Hash      string `json:"hash"`
	Code      string `json:"code"`
	SourceMap string `json:"sourceMap,omitempty"`
}

type Manifest struct {
	Project             string                    `json:"project"`
	GeneratedAt         string                    `json:"generatedAt"`
	Functions           map[string]FunctionEntry  `json:"functions"`
	Schema              Schema                    `json:"schema"`
	Module              *ModuleArtifact           `json:"module,omitempty"`
	Visibility          map[string]VisibilityPlan `json:"visibility,omitempty"`
	NotifySchemaVersion string                    `json:"notifySchemaVersion,omitempty"`
}

func EmptySchema() Schema {
	controlPlaneTables := map[string]Table{}
	tenantTables := map[string]Table{}
	return Schema{
		Tables:             tenantTables,
		ControlPlaneTables: controlPlaneTables,
		TenantTables:       tenantTables,
	}
}

func (s Schema) Normalize() Schema {
	if s.ControlPlaneTables == nil {
		s.ControlPlaneTables = map[string]Table{}
	}

	if s.TenantTables == nil {
		if s.Tables == nil {
			s.TenantTables = map[string]Table{}
		} else {
			s.TenantTables = s.Tables
		}
	}
	s.Tables = s.TenantTables
	return s
}

// Normalize initializes schema maps and mirrors the one visibility declaration
// into the module artifact used by the execution host.
func (m Manifest) Normalize() Manifest {
	m.Schema = m.Schema.Normalize()
	if m.Module != nil {
		normalized := *m.Module
		if m.Visibility == nil {
			m.Visibility = normalized.Visibility
		}
		if normalized.Visibility == nil {
			normalized.Visibility = m.Visibility
		}
		m.Module = &normalized
	}
	return m
}

// ControlPlaneSchema returns the control-plane portion of the schema.
func (s Schema) ControlPlaneSchema() Schema {
	s = s.Normalize()
	return Schema{Tables: s.ControlPlaneTables}
}

func (s Schema) TenantSchema() Schema {
	s = s.Normalize()
	if s.TenantTables == nil {
		return Schema{Tables: s.Tables}
	}
	return Schema{Tables: s.TenantTables}
}
