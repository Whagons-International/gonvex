package manifest

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"regexp"
	"strings"
)

const LanguageTypeScript = "typescript"
const ModuleArtifactGeneration = 8

// Language reports the artifact's normalized language, defaulting to
// TypeScript because that is the only language the artifact pipeline emits.
func (a ModuleArtifact) NormalizedLanguage() string {
	language := strings.ToLower(strings.TrimSpace(a.Language))
	if language == "" {
		return LanguageTypeScript
	}
	return language
}

// IsTypeScript reports whether this artifact can execute in the TypeScript
// module host.
func (a ModuleArtifact) IsTypeScript() bool {
	return a.NormalizedLanguage() == LanguageTypeScript
}

// DecodeJavaScript returns the artifact's bundled ESM source, verifying that it
// matches the hash the build recorded. The check happens here, before anything
// is handed to an engine, so a truncated or substituted bundle fails as a
// manifest error rather than as a mysterious module error later.
func (a ModuleArtifact) DecodeJavaScript() ([]byte, error) {
	if a.JavaScript == nil {
		return nil, fmt.Errorf("module artifact has no JavaScript bundle")
	}
	code, err := base64.StdEncoding.DecodeString(a.JavaScript.Code)
	if err != nil {
		return nil, fmt.Errorf("module JavaScript is not valid base64: %w", err)
	}
	if len(code) == 0 {
		return nil, fmt.Errorf("module JavaScript bundle is empty")
	}
	expected := strings.ToLower(strings.TrimSpace(a.JavaScript.Hash))
	if expected == "" {
		return nil, fmt.Errorf("module JavaScript has no hash to verify")
	}
	digest := sha256.Sum256(code)
	if actual := hex.EncodeToString(digest[:]); actual != expected {
		return nil, fmt.Errorf("module JavaScript hash %s does not match the manifest hash %s", actual, expected)
	}
	return code, nil
}

// Validate reports whether an artifact is executable: right language, verified
// bundle, and at least one well-formed function declaration.
func (a ModuleArtifact) Validate() error {
	if !a.IsTypeScript() {
		return fmt.Errorf("module language %q has no module host", a.NormalizedLanguage())
	}
	if _, err := a.DecodeJavaScript(); err != nil {
		return err
	}
	if a.Generation != ModuleArtifactGeneration {
		return fmt.Errorf("module artifact generation %d is unsupported; expected %d", a.Generation, ModuleArtifactGeneration)
	}
	expectedHash, err := a.ComputedHash()
	if err != nil {
		return err
	}
	if actual := strings.ToLower(strings.TrimSpace(a.Hash)); actual == "" || actual != expectedHash {
		return fmt.Errorf("module artifact hash %q does not match canonical contract hash %q", actual, expectedHash)
	}
	for path, function := range a.Functions {
		if strings.TrimSpace(path) == "" {
			return fmt.Errorf("module declares a function with an empty path")
		}
		if path == "control" || strings.HasPrefix(path, "control.") {
			return fmt.Errorf("module function %q uses the host-reserved Control Plane namespace", path)
		}
		switch function.Kind {
		case FunctionKindQuery, FunctionKindReducer, FunctionKindAction:
		default:
			return fmt.Errorf("module function %q has unknown kind %q", path, function.Kind)
		}
		if strings.TrimSpace(function.Handler) == "" || strings.TrimSpace(function.File) == "" {
			return fmt.Errorf("module function %q requires a handler and source file", path)
		}
		if err := validateModuleSchema(function.Args, fmt.Sprintf("module function %q args", path), false); err != nil {
			return err
		}
		if err := validateModuleSchema(function.Result, fmt.Sprintf("module function %q result", path), false); err != nil {
			return err
		}
		switch function.Delivery {
		case "", DeliveryOneShot, DeliveryLive, DeliveryReplica:
		default:
			return fmt.Errorf("module function %q has unknown delivery %q", path, function.Delivery)
		}
		if function.Kind == FunctionKindQuery && (function.Delivery == "" || function.Delivery == DeliveryOneShot) {
			plan := function.Dependencies.LiveQueryPlan
			if plan == nil {
				return fmt.Errorf("one-shot query %q requires a structured live query plan", path)
			}
			if err := validateStructuredQueryPlan(plan, path); err != nil {
				return err
			}
		}
		if function.Kind == FunctionKindQuery && function.Delivery == DeliveryLive && function.Dependencies.LiveQueryPlan == nil {
			return fmt.Errorf("live query %q requires a structured live query plan", path)
		}
		if function.Kind == FunctionKindQuery && function.Delivery == DeliveryReplica && function.Replica == nil {
			return fmt.Errorf("replica collection %q requires a replica definition", path)
		}
		if function.Kind == FunctionKindQuery && function.Internal && function.Delivery != "" && function.Delivery != DeliveryOneShot {
			return fmt.Errorf("internal Query %q must use one-shot delivery", path)
		}
		if function.Kind != FunctionKindQuery && function.Delivery != "" {
			return fmt.Errorf("module function %q uses query delivery on a %s", path, function.Kind)
		}
		if function.Kind == FunctionKindReducer {
			if err := validateOfflinePolicy(function.Offline, path); err != nil {
				return err
			}
			if function.Optimistic != nil {
				if err := validateOptimisticTransaction(function.Optimistic, path); err != nil {
					return err
				}
			}
			if !function.Internal && function.Optimistic == nil && strings.TrimSpace(function.Dependencies.NonOptimisticReason) == "" {
				return fmt.Errorf("interactive reducer %q requires an optimistic transaction or nonOptimisticReason", path)
			}
		} else if function.Offline != nil || function.Optimistic != nil {
			return fmt.Errorf("module function %q declares reducer policy on a %s", path, function.Kind)
		}
		if function.Kind == FunctionKindAction {
			if err := validateActionCapabilities(function.ActionProfile, function.ActionCapabilities, path); err != nil {
				return err
			}
		} else if function.ActionProfile != "" || function.ActionCapabilities != nil {
			return fmt.Errorf("module function %q declares Action capabilities on a %s", path, function.Kind)
		}
	}
	for path, function := range a.Functions {
		if function.Kind != FunctionKindAction || function.ActionCapabilities == nil {
			continue
		}
		for name, binding := range function.ActionCapabilities.Tools {
			targetPath := strings.TrimSpace(binding.Function)
			target, ok := a.Functions[targetPath]
			if !ok {
				return fmt.Errorf("action %q tool %q targets unknown function %q", path, name, targetPath)
			}
			if target.Kind != binding.Kind {
				return fmt.Errorf("action %q tool %q declares %s but %q is %s", path, name, binding.Kind, targetPath, target.Kind)
			}
			if binding.Kind == FunctionKindQuery && (!target.Internal || (target.Delivery != "" && target.Delivery != DeliveryOneShot)) {
				return fmt.Errorf("action %q tool %q must target an internal one-shot Query", path, name)
			}
			if binding.Kind == FunctionKindReducer && target.Internal {
				return fmt.Errorf("action %q tool %q must target a public business-intent Reducer", path, name)
			}
		}
	}
	if path := strings.TrimSpace(a.InvitationAcceptanceReducer); path != "" {
		target, ok := a.Functions[path]
		if !ok || target.Kind != FunctionKindReducer || !target.Internal {
			return fmt.Errorf("invitation acceptance function %q must be an internal reducer", path)
		}
	}
	cronNames := make(map[string]struct{}, len(a.Crons))
	for _, cron := range a.Crons {
		name := strings.TrimSpace(cron.Name)
		path := strings.TrimSpace(cron.Function)
		if name == "" {
			return fmt.Errorf("module declares a cron with an empty name")
		}
		if _, exists := cronNames[name]; exists {
			return fmt.Errorf("module declares duplicate cron %q", name)
		}
		cronNames[name] = struct{}{}
		if path == "" {
			return fmt.Errorf("module cron %q requires a function path", name)
		}
		hasInterval := cron.IntervalMS != 0
		hasExpression := strings.TrimSpace(cron.Expression) != ""
		if hasInterval == hasExpression {
			return fmt.Errorf("module cron %q requires exactly one intervalMs or expression", name)
		}
		const maxDurationMilliseconds = int64((1<<63 - 1) / 1_000_000)
		if cron.IntervalMS < 0 || cron.IntervalMS > maxDurationMilliseconds {
			return fmt.Errorf("module cron %q intervalMs is outside the supported duration", name)
		}
		if cron.Scope != "project" && cron.Scope != "tenant" {
			return fmt.Errorf("module cron %q has unknown scope %q", name, cron.Scope)
		}
		if len(cron.Args) > 0 && !json.Valid(cron.Args) {
			return fmt.Errorf("module cron %q args are not valid JSON", name)
		}
		target, exists := a.Functions[path]
		if !exists {
			return fmt.Errorf("module cron %q targets unknown function %q", name, path)
		}
		if target.Kind == FunctionKindQuery {
			return fmt.Errorf("module cron %q must target a reducer or action", name)
		}
	}
	return nil
}

// ComputedHash returns the canonical identity emitted by the TypeScript CLI.
// It covers source/migration files and every executable/routing declaration,
// while storing only the JavaScript bundle's verified digest rather than its
// duplicate base64 payload.
func (a ModuleArtifact) ComputedHash() (string, error) {
	if a.JavaScript == nil {
		return "", fmt.Errorf("module artifact has no JavaScript bundle")
	}
	files := a.Files
	if files == nil {
		files = map[string]string{}
	}
	functions := a.Functions
	if functions == nil {
		functions = map[string]ModuleFunction{}
	}
	hashFunctions := make(map[string]any, len(functions))
	for path, function := range functions {
		hashFunctions[path] = moduleFunctionHashContract(function)
	}
	visibility := a.Visibility
	if visibility == nil {
		visibility = map[string]VisibilityPlan{}
	}
	crons := a.Crons
	if crons == nil {
		crons = []ModuleCron{}
	}
	payload := map[string]any{
		"generation":                  a.Generation,
		"language":                    a.NormalizedLanguage(),
		"entrypoint":                  a.Entrypoint,
		"files":                       files,
		"functions":                   hashFunctions,
		"visibility":                  visibility,
		"crons":                       crons,
		"invitationAcceptanceReducer": strings.TrimSpace(a.InvitationAcceptanceReducer),
		"javascript": map[string]any{
			"path": a.JavaScript.Path,
			"hash": strings.ToLower(strings.TrimSpace(a.JavaScript.Hash)),
		},
	}
	raw, err := marshalCanonicalJSON(payload)
	if err != nil {
		return "", fmt.Errorf("encode module artifact hash contract: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var canonical any
	if err := decoder.Decode(&canonical); err != nil {
		return "", fmt.Errorf("normalize module artifact hash contract: %w", err)
	}
	normalized, err := marshalCanonicalJSON(canonical)
	if err != nil {
		return "", fmt.Errorf("encode canonical module artifact hash contract: %w", err)
	}
	digest := sha256.Sum256(normalized)
	return hex.EncodeToString(digest[:]), nil
}

// marshalCanonicalJSON matches the TypeScript CLI's canonicalJson contract:
// object keys sort lexicographically, arrays retain order, and strings are
// ordinary JSON strings without Go's optional HTML escaping.
func marshalCanonicalJSON(value any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buffer.Bytes(), []byte("\n")), nil
}

// moduleFunctionHashContract mirrors the JavaScript object's omitted optional
// properties. encoding/json does not apply omitempty to a zero-value struct,
// so hashing ModuleFunction directly would invent dependencies:{} that the
// TypeScript artifact never emitted.
func moduleFunctionHashContract(function ModuleFunction) map[string]any {
	contract := map[string]any{
		"kind":    function.Kind,
		"handler": function.Handler,
		"file":    function.File,
	}
	if function.Export != "" {
		contract["export"] = function.Export
	}
	if function.Args != nil {
		contract["args"] = function.Args
	}
	if function.Result != nil {
		contract["result"] = function.Result
	}
	if !reflect.DeepEqual(function.Dependencies, FunctionDependencies{}) {
		contract["dependencies"] = function.Dependencies
	}
	if function.Internal {
		contract["internal"] = true
	}
	if function.Delivery != "" {
		contract["delivery"] = function.Delivery
	}
	if function.Replica != nil {
		contract["replica"] = function.Replica
	}
	if function.Offline != nil {
		contract["offline"] = function.Offline
	}
	if function.Optimistic != nil {
		contract["optimistic"] = function.Optimistic
	}
	if function.ActionProfile != "" {
		contract["actionProfile"] = function.ActionProfile
	}
	if function.ActionCapabilities != nil {
		contract["actionCapabilities"] = function.ActionCapabilities
	}
	if function.Interactive {
		contract["interactive"] = true
	}
	if function.Classification != "" {
		contract["classification"] = function.Classification
	}
	if function.Description != "" {
		contract["description"] = function.Description
	}
	if function.Agent != nil {
		contract["agent"] = function.Agent
	}
	return contract
}

var actionToolNamePattern = regexp.MustCompile(`^[A-Za-z_$][A-Za-z0-9_$]*$`)
var secretNamePattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)

func validateActionCapabilities(profile string, capabilities *ActionCapabilities, path string) error {
	if profile == "" {
		profile = "standard"
	}
	if profile != "standard" && profile != "agent" {
		return fmt.Errorf("action %q profile must be standard or agent", path)
	}
	if capabilities == nil {
		return nil
	}
	seenOrigins := map[string]struct{}{}
	for _, origin := range capabilities.NetworkOrigins {
		parsed, err := url.Parse(origin)
		if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Host == "" || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Scheme+"://"+parsed.Host != origin {
			return fmt.Errorf("action %q network origin %q must be an exact HTTP(S) origin", path, origin)
		}
		if _, duplicate := seenOrigins[origin]; duplicate {
			return fmt.Errorf("action %q declares duplicate network origin %q", path, origin)
		}
		seenOrigins[origin] = struct{}{}
	}
	seenSecrets := map[string]struct{}{}
	for _, name := range capabilities.Secrets {
		if !secretNamePattern.MatchString(name) {
			return fmt.Errorf("action %q secret %q must be an uppercase environment name", path, name)
		}
		if _, duplicate := seenSecrets[name]; duplicate {
			return fmt.Errorf("action %q declares duplicate secret %q", path, name)
		}
		seenSecrets[name] = struct{}{}
	}
	if len(capabilities.Tools) > 0 && profile != "agent" {
		return fmt.Errorf("action %q tools require profile agent", path)
	}
	if capabilities.Sandbox != nil && profile != "agent" {
		return fmt.Errorf("action %q sandbox requires profile agent", path)
	}
	for name, binding := range capabilities.Tools {
		if !actionToolNamePattern.MatchString(name) || strings.TrimSpace(binding.Function) == "" || (binding.Kind != FunctionKindQuery && binding.Kind != FunctionKindReducer) {
			return fmt.Errorf("action %q has invalid tool binding %q", path, name)
		}
	}
	return nil
}

func validateStructuredQueryPlan(plan *LiveQueryPlan, path string) error {
	if strings.TrimSpace(plan.Table) == "" || strings.TrimSpace(plan.Key) == "" || len(plan.Columns) == 0 {
		return fmt.Errorf("one-shot query %q requires a structured live query plan with a table, key, and columns", path)
	}
	for _, column := range plan.Columns {
		if strings.TrimSpace(column) == "" {
			return fmt.Errorf("one-shot query %q live query plan contains an empty column", path)
		}
	}
	if plan.Window != nil && plan.Window.Count != "" && plan.Window.Count != "exact" {
		return fmt.Errorf("one-shot query %q live query plan window count must be exact", path)
	}
	for _, column := range plan.Columns {
		if column == plan.Key {
			return nil
		}
	}
	return fmt.Errorf("one-shot query %q live query plan columns must include its key", path)
}

// Identity is the artifact's stable content identity: the artifact hash when
// the build recorded one, the JavaScript hash otherwise. Callers use it to skip
// reloading an unchanged module.
func (a ModuleArtifact) Identity() string {
	if hash := strings.TrimSpace(a.Hash); hash != "" {
		return hash
	}
	if a.JavaScript != nil {
		return strings.TrimSpace(a.JavaScript.Hash)
	}
	return ""
}
