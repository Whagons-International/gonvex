package manifest

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

const NotifySchemaVersion = "15"

type FunctionEntry struct {
	Kind         FunctionKind         `json:"kind"`
	Handler      string               `json:"handler"`
	File         string               `json:"file"`
	Dependencies FunctionDependencies `json:"dependencies,omitempty"`
	Sync         *SyncDefinition      `json:"sync,omitempty"`
}

// SyncDefinition describes an entity-shaped, locally materialized collection.
// V1 intentionally supports a single source table and equality filters. More
// complex joins and aggregates remain ordinary live queries.
type SyncDefinition struct {
	Table                 string            `json:"table"`
	Key                   string            `json:"key"`
	Columns               []string          `json:"columns"`
	EqualFilters          map[string]string `json:"equalFilters,omitempty"`
	ExcludeWhenSet        []string          `json:"excludeWhenSet,omitempty"`
	VisibilityTables      []string          `json:"visibilityTables,omitempty"`
	OrderBy               string            `json:"orderBy,omitempty"`
	OrderDirection        string            `json:"orderDirection,omitempty"`
	Mode                  string            `json:"mode,omitempty"`
	MaxRows               int               `json:"maxRows,omitempty"`
	MaxBytes              int64             `json:"maxBytes,omitempty"`
	RetentionMilliseconds int64             `json:"retentionMs,omitempty"`
}

// FunctionDependencies describe the database surface a function observes or
// changes. Older manifests omit this field; the runtime treats that as unknown
// and falls back to conservative project/tenant-scoped invalidation.
type FunctionDependencies struct {
	Reads                []ReadDependency                `json:"reads,omitempty"`
	Writes               []WriteDependency               `json:"writes,omitempty"`
	ReadsEphemeral       bool                            `json:"readsEphemeral,omitempty"`
	WritesEphemeral      bool                            `json:"writesEphemeral,omitempty"`
	ShareByPermissions   bool                            `json:"shareByPermissions,omitempty"`
	ShareByVisibility    string                          `json:"shareByVisibility,omitempty"`
	ShareResultFrom      string                          `json:"shareResultFrom,omitempty"`
	ShareResultField     string                          `json:"shareResultField,omitempty"`
	OptimisticMutation   *OptimisticMutationDefinition   `json:"optimisticMutation,omitempty"`
	OptimisticProjection *OptimisticProjectionDefinition `json:"optimisticProjection,omitempty"`
}

type OptimisticMutationDefinition struct {
	Entity     string   `json:"entity"`
	RowIDPath  []string `json:"rowIdPath"`
	FieldsPath []string `json:"fieldsPath"`
}

type OptimisticProjectionDefinition struct {
	Entity     string   `json:"entity"`
	Key        string   `json:"key"`
	ResultPath []string `json:"resultPath"`
}

type ReadDependency struct {
	Table     string   `json:"table"`
	Columns   []string `json:"columns,omitempty"`
	Filters   []string `json:"filters,omitempty"`
	OrdersBy  []string `json:"ordersBy,omitempty"`
	Windowed  bool     `json:"windowed,omitempty"`
	Predicate string   `json:"predicate,omitempty"`
}

type WriteDependency struct {
	Table   string   `json:"table"`
	Columns []string `json:"columns,omitempty"`
}

type Schema struct {
	Tables         map[string]Table `json:"tables"`
	LandlordTables map[string]Table `json:"landlordTables,omitempty"`
	TenantTables   map[string]Table `json:"tenantTables,omitempty"`
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

type Manifest struct {
	Project             string                   `json:"project"`
	GeneratedAt         string                   `json:"generatedAt"`
	Functions           map[string]FunctionEntry `json:"functions"`
	Schema              Schema                   `json:"schema"`
	Bundle              *SourceBundle            `json:"bundle,omitempty"`
	NotifySchemaVersion string                   `json:"notifySchemaVersion,omitempty"`
}

func EmptySchema() Schema {
	return Schema{
		Tables:         map[string]Table{},
		LandlordTables: map[string]Table{},
		TenantTables:   map[string]Table{},
	}
}

func (s Schema) Normalize() Schema {
	if s.Tables == nil {
		s.Tables = map[string]Table{}
	}
	if s.LandlordTables == nil && s.TenantTables == nil {
		return s
	}
	if s.LandlordTables == nil {
		s.LandlordTables = map[string]Table{}
	}
	if s.TenantTables == nil {
		s.TenantTables = s.Tables
	}
	s.Tables = s.TenantTables
	return s
}

func (s Schema) LandlordSchema() Schema {
	s = s.Normalize()
	if s.LandlordTables == nil {
		return Schema{Tables: s.Tables}
	}
	return Schema{Tables: s.LandlordTables}
}

func (s Schema) TenantSchema() Schema {
	s = s.Normalize()
	if s.TenantTables == nil {
		return Schema{Tables: s.Tables}
	}
	return Schema{Tables: s.TenantTables}
}
