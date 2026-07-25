package sandbox

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/gonvex/gonvex/pkg/gonvex"
)

const (
	defaultTimeout       = 60 * time.Second
	defaultMaxOutputByte = 1 << 20
)

const (
	rpcPrefix    = "__GONVEX_RPC__"
	resultPrefix = "__GONVEX_RESULT__"
)

var blockedGoTokens = []*regexp.Regexp{
	regexp.MustCompile(`(?m)^\s*package\s+`),
	regexp.MustCompile("(?m)^\\s*import\\s*(?:\\(|\"|`)"),
	regexp.MustCompile(`\bunsafe\b`),
	regexp.MustCompile(`\bsyscall\b`),
	regexp.MustCompile(`\bos/exec\b`),
	regexp.MustCompile(`\bnet/http\b`),
	regexp.MustCompile(`\bnet\b`),
	regexp.MustCompile(`\bplugin\b`),
	regexp.MustCompile(`\bC\.`),
	regexp.MustCompile(`//\s*#cgo`),
}

type Runner struct {
	WorkDir        string
	DefaultTimeout time.Duration
	MaxOutputBytes int64
	Host           Host
}

type Host interface {
	Call(ctx context.Context, req HostCallRequest) (any, error)
}

type HostFunc func(ctx context.Context, req HostCallRequest) (any, error)

func (fn HostFunc) Call(ctx context.Context, req HostCallRequest) (any, error) {
	return fn(ctx, req)
}

type HostCallRequest struct {
	Kind string          `json:"kind"`
	Path string          `json:"path"`
	Args json.RawMessage `json:"args"`
}

func NewRunner(workDir string) *Runner {
	return &Runner{
		WorkDir:        strings.TrimSpace(workDir),
		DefaultTimeout: defaultTimeout,
		MaxOutputBytes: defaultMaxOutputByte,
	}
}

func (r *Runner) RunGo(ctx context.Context, req gonvex.GoSandboxRequest) (gonvex.GoSandboxResult, error) {
	started := time.Now()
	if strings.TrimSpace(req.Purpose) == "" {
		return gonvex.GoSandboxResult{OK: false, Error: "sandbox_run_go requires a purpose"}, nil
	}
	code := strings.TrimSpace(req.Code)
	if code == "" {
		return gonvex.GoSandboxResult{OK: false, Error: "sandbox_run_go requires code"}, nil
	}
	if err := validateGoBody(code); err != nil {
		return gonvex.GoSandboxResult{OK: false, Error: err.Error()}, nil
	}

	timeout := r.DefaultTimeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	if req.Limits.TimeoutMs > 0 {
		timeout = time.Duration(req.Limits.TimeoutMs) * time.Millisecond
	}
	maxOutput := r.MaxOutputBytes
	if maxOutput <= 0 {
		maxOutput = defaultMaxOutputByte
	}
	if req.Limits.MaxOutputBytes > 0 {
		maxOutput = req.Limits.MaxOutputBytes
	}

	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	tmpRoot := r.WorkDir
	if tmpRoot == "" {
		tmpRoot = os.TempDir()
	}
	workDir, err := os.MkdirTemp(tmpRoot, "gonvex-go-sandbox-*")
	if err != nil {
		return gonvex.GoSandboxResult{}, err
	}
	defer os.RemoveAll(workDir)

	if err := os.WriteFile(filepath.Join(workDir, "go.mod"), []byte("module sandbox\n\ngo 1.22\n"), 0o644); err != nil {
		return gonvex.GoSandboxResult{}, err
	}
	if err := writeSandboxSources(workDir, code); err != nil {
		return gonvex.GoSandboxResult{}, err
	}

	cmd := exec.CommandContext(runCtx, "go", "run", ".")
	cmd.Dir = workDir
	cmd.Env = minimalGoEnv()
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return gonvex.GoSandboxResult{}, err
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return gonvex.GoSandboxResult{}, err
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return gonvex.GoSandboxResult{}, err
	}
	var stderr limitedBuffer
	stderr.limit = 64 << 10

	if err := cmd.Start(); err != nil {
		return gonvex.GoSandboxResult{}, err
	}

	output := make(chan processOutput, 1)
	go r.handleStdout(runCtx, stdoutPipe, stdin, maxOutput, output)
	go func() {
		_, _ = io.Copy(&stderr, stderrPipe)
	}()

	err = cmd.Wait()
	duration := time.Since(started).Milliseconds()
	_ = stdin.Close()
	processed := <-output
	if runCtx.Err() == context.DeadlineExceeded {
		return gonvex.GoSandboxResult{OK: false, Error: fmt.Sprintf("sandbox_run_go timed out after %s", timeout), DurationMs: duration}, nil
	}
	if err != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = strings.TrimSpace(processed.logs.String())
		}
		if message == "" {
			message = err.Error()
		}
		return gonvex.GoSandboxResult{OK: false, Error: annotateSandboxError(message), DurationMs: duration}, nil
	}
	if processed.err != nil {
		return gonvex.GoSandboxResult{OK: false, Error: annotateSandboxError(processed.err.Error()), DurationMs: duration}, nil
	}
	if processed.truncated {
		return gonvex.GoSandboxResult{OK: false, Error: fmt.Sprintf("sandbox_run_go output exceeded %d bytes", maxOutput), DurationMs: duration}, nil
	}
	if len(processed.result) == 0 {
		return gonvex.GoSandboxResult{OK: false, Error: "sandbox_run_go did not return a result", DurationMs: duration}, nil
	}
	var decoded map[string]any
	if err := json.Unmarshal(processed.result, &decoded); err != nil {
		return gonvex.GoSandboxResult{OK: false, Error: "sandbox_run_go did not return valid JSON", DurationMs: duration}, nil
	}
	return normalizeResult(decoded, duration), nil
}

type processOutput struct {
	result    []byte
	logs      limitedBuffer
	truncated bool
	err       error
}

func (r *Runner) handleStdout(ctx context.Context, stdout io.Reader, stdin io.Writer, maxOutput int64, done chan<- processOutput) {
	out := processOutput{}
	out.logs.limit = maxOutput
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), int(maxOutput)+64*1024)
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, rpcPrefix):
			response := r.handleRPC(ctx, strings.TrimPrefix(line, rpcPrefix))
			_, _ = fmt.Fprintln(stdin, response)
		case strings.HasPrefix(line, resultPrefix):
			out.result = []byte(strings.TrimPrefix(line, resultPrefix))
		default:
			_, _ = out.logs.Write([]byte(line + "\n"))
			out.truncated = out.truncated || out.logs.truncated
		}
	}
	if err := scanner.Err(); err != nil {
		out.err = err
	}
	done <- out
}

func (r *Runner) handleRPC(ctx context.Context, raw string) string {
	var req HostCallRequest
	if err := json.Unmarshal([]byte(raw), &req); err != nil {
		return rpcError("invalid sandbox RPC request: " + err.Error())
	}
	if r.Host == nil {
		return rpcError("sandbox host API is not configured")
	}
	report, hasReport := gonvex.SandboxProgressFromContext(ctx)
	started := time.Now()
	if hasReport {
		report(gonvex.SandboxProgressEvent{
			At:    started.UTC().UnixMilli(),
			Phase: "start",
			Kind:  strings.TrimSpace(req.Kind),
			Path:  strings.TrimSpace(req.Path),
		})
	}
	result, err := r.Host.Call(ctx, req)
	if hasReport {
		durationMs := time.Since(started).Milliseconds()
		if err != nil {
			ok := false
			report(gonvex.SandboxProgressEvent{
				At:         time.Now().UTC().UnixMilli(),
				Phase:      "end",
				Kind:       strings.TrimSpace(req.Kind),
				Path:       strings.TrimSpace(req.Path),
				OK:         &ok,
				Error:      err.Error(),
				DurationMs: durationMs,
			})
		} else {
			ok := true
			report(gonvex.SandboxProgressEvent{
				At:         time.Now().UTC().UnixMilli(),
				Phase:      "end",
				Kind:       strings.TrimSpace(req.Kind),
				Path:       strings.TrimSpace(req.Path),
				OK:         &ok,
				DurationMs: durationMs,
			})
		}
	}
	if err != nil {
		return rpcError(err.Error())
	}
	return rpcResponse(map[string]any{"ok": true, "result": result})
}

func rpcError(message string) string {
	return rpcResponse(map[string]any{"ok": false, "error": message})
}

func rpcResponse(value map[string]any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return `{"ok":false,"error":"failed to encode sandbox RPC response"}`
	}
	return string(encoded)
}

func validateGoBody(code string) error {
	for _, pattern := range blockedGoTokens {
		if pattern.MatchString(code) {
			return fmt.Errorf("sandbox_run_go blocked unsafe Go code pattern: %s", pattern.String())
		}
	}
	return nil
}

const (
	// The host plumbing and the user body are deliberately emitted as two
	// separate files. Go resolves imports per file, so a package imported by
	// the host file is NOT in scope for the user file. That property -- not the
	// regex blocklist -- is what keeps `os`, and therefore the host filesystem,
	// environment and process table, out of reach of sandbox code.
	hostSourceFile = "main.go"
	userSourceFile = "user.go"
)

// writeSandboxSources emits the two-file program that `go run .` compiles.
func writeSandboxSources(dir, code string) error {
	if err := os.WriteFile(filepath.Join(dir, hostSourceFile), []byte(renderHost()), 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, userSourceFile), []byte(renderUser(code)), 0o644)
}

// renderUser emits the user body in a file whose only imports are a curated,
// side-effect-free subset of the standard library. Adding a package here grants
// it to every sandbox script, so anything that can touch the filesystem, the
// network, the process table, or memory unsafely must never be added.
func renderUser(code string) string {
	return `package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Referenced so the curated imports compile whether or not the user body uses
// them; Go rejects unused imports.
var (
	_ = json.Marshal
	_ = errors.New
	_ = fmt.Sprintf
	_ = math.Abs
	_ = sort.Ints
	_ = strconv.Itoa
	_ = strings.TrimSpace
	_ = time.Now
)

func Run() (any, error) {
` + code + `
}
`
}

func renderHost() string {
	return `package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

var sandboxRPCMu sync.Mutex
var sandboxRPCReader = bufio.NewReader(os.Stdin)

func main() {
	result, err := Run()
	if err != nil {
		body, _ := json.Marshal(map[string]any{
			"ok": false,
			"error": err.Error(),
		})
		fmt.Println("` + resultPrefix + `" + string(body))
		return
	}
	body, _ := json.Marshal(map[string]any{
		"ok": true,
		"result": result,
	})
	fmt.Println("` + resultPrefix + `" + string(body))
}

func fail(format string, args ...any) (any, error) {
	return nil, fmt.Errorf(format, args...)
}

func whagonsQuery(path string, args any) (map[string]any, error) {
	return whagonsCall("query", path, args)
}

func whagonsAction(path string, args any) (map[string]any, error) {
	return whagonsCall("action", path, args)
}

func whagonsMutation(path string, args any) (map[string]any, error) {
	return whagonsCall("mutation", path, args)
}

// whdataQuery runs read-only SELECT SQL against an uploaded data file's
// DuckDB artifact. limit <= 0 uses the server default.
// Return shape: map with keys "ok", "columns", "rowCount", "rows"
// where "rows" is []any of map[string]any. Prefer whdataRows when you need
// to iterate row maps — do not treat the whdataQuery result itself as a slice.
func whdataQuery(fileKey string, sql string, limit int) (map[string]any, error) {
	return whagonsCall("data.query", "", map[string]any{"fileKey": fileKey, "sql": sql, "limit": limit})
}

// whdataRows is the preferred helper for import/analysis loops. It unwraps
// whdataQuery's {"rows": [...]} envelope into []map[string]any so callers can
// range without type-probing.
func whdataRows(fileKey string, sql string, limit int) ([]map[string]any, error) {
	result, err := whdataQuery(fileKey, sql, limit)
	if err != nil {
		return nil, err
	}
	raw, ok := result["rows"].([]any)
	if !ok {
		return nil, fmt.Errorf("whdataRows: query result missing rows array (got keys %v). Prefer whdataRows over ranging whdataQuery.", mapKeys(result))
	}
	rows := make([]map[string]any, 0, len(raw))
	for i, item := range raw {
		row, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("whdataRows: row %d is not an object", i)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func mapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// whdataInspect inspects an uploaded data file (operation: overview | schema | sample).
func whdataInspect(fileKey string, operation string, tableName string, limit int) (map[string]any, error) {
	return whagonsCall("data.inspect", "", map[string]any{"fileKey": fileKey, "operation": operation, "tableName": tableName, "limit": limit})
}

// whdataProfile computes per-column statistics for an uploaded data file.
func whdataProfile(fileKey string, tableName string) (map[string]any, error) {
	return whagonsCall("data.profile", "", map[string]any{"fileKey": fileKey, "tableName": tableName})
}

func whagonsCall(kind string, path string, args any) (map[string]any, error) {
	request, err := json.Marshal(map[string]any{
		"kind": kind,
		"path": path,
		"args": args,
	})
	if err != nil {
		return nil, err
	}
	sandboxRPCMu.Lock()
	defer sandboxRPCMu.Unlock()
	fmt.Println("` + rpcPrefix + `" + string(request))
	responseLine, err := sandboxRPCReader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	var response struct {
		OK bool ` + "`json:\"ok\"`" + `
		Result any ` + "`json:\"result\"`" + `
		Error string ` + "`json:\"error\"`" + `
	}
	if err := json.Unmarshal([]byte(responseLine), &response); err != nil {
		return nil, err
	}
	if !response.OK {
		return nil, fmt.Errorf(response.Error)
	}
	if result, ok := response.Result.(map[string]any); ok {
		return result, nil
	}
	return map[string]any{"value": response.Result}, nil
}

`
}

func minimalGoEnv() []string {
	keep := []string{"PATH", "HOME", "TMPDIR", "GOCACHE", "GOMODCACHE", "GOROOT", "GOPATH"}
	env := make([]string, 0, len(keep)+2)
	for _, key := range keep {
		if value := os.Getenv(key); value != "" {
			env = append(env, key+"="+value)
		}
	}
	env = append(env, "CGO_ENABLED=0")
	return env
}

func normalizeResult(decoded map[string]any, duration int64) gonvex.GoSandboxResult {
	result := gonvex.GoSandboxResult{DurationMs: duration}
	if ok, _ := decoded["ok"].(bool); ok {
		result.OK = true
	} else {
		result.OK = false
	}
	if text, _ := decoded["summary"].(string); text != "" {
		result.Summary = text
	}
	if text, _ := decoded["error"].(string); text != "" {
		result.Error = annotateSandboxError(text)
	}
	if raw, ok := decoded["result"].(map[string]any); ok {
		result.Result = raw
	} else if raw, ok := decoded["result"]; ok {
		result.Result = map[string]any{"value": raw}
	}
	return result
}

// annotateSandboxError appends short, actionable recovery hints for the common
// mistakes assistants make in sandbox_run_go. Keep hints terse — they are shown
// in the tool error the model reads on the next turn.
func annotateSandboxError(message string) string {
	msg := strings.TrimSpace(message)
	if msg == "" {
		return msg
	}
	lower := strings.ToLower(msg)
	if strings.Contains(msg, "Hint:") || strings.Contains(msg, "hint:") {
		return msg
	}

	var hints []string
	switch {
	case strings.Contains(lower, "cannot range over") && strings.Contains(lower, "map[string]any"):
		hints = append(hints, "Hint: whdataQuery returns a result map, not rows. Use rows, err := whdataRows(fileKey, sql, limit) and range over rows.")
	case strings.Contains(lower, "cannot index") && (strings.Contains(lower, "interface type any") || strings.Contains(lower, "map index")):
		hints = append(hints, "Hint: prefer whdataRows(fileKey, sql, limit) ([]map[string]any). whdataQuery returns {ok,columns,rowCount,rows}, not a slice.")
	case strings.Contains(lower, "cannot use") && strings.Contains(lower, "as string value in map index"):
		hints = append(hints, "Hint: map keys are strings. Prefer whdataRows(...), or read result[\"rows\"].([]any) from whdataQuery.")
	case strings.Contains(lower, "multiple-value") && (strings.Contains(lower, "whagonsaction") || strings.Contains(lower, "whagonsquery") || strings.Contains(lower, "whagonsmutation") || strings.Contains(lower, "whdataquery") || strings.Contains(lower, "whdatarows")):
		hints = append(hints, "Hint: helpers return (value, error). Example: res, err := whagonsAction(...); if err != nil { return fail(err.Error()) }; return res, nil")
	case strings.Contains(lower, "not enough return values") && strings.Contains(lower, "want (any, error)"):
		hints = append(hints, "Hint: Run() returns (any, error). Use return value, nil or return fail(\"message\").")
	case strings.Contains(lower, "declared and not used"):
		hints = append(hints, "Hint: unused locals are compile errors. Remove them or assign to _.")
	}

	if len(hints) == 0 {
		return msg
	}
	return msg + "\n\n" + strings.Join(hints, "\n")
}

type limitedBuffer struct {
	bytes.Buffer
	limit     int64
	truncated bool
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if b.limit <= 0 {
		return b.Buffer.Write(p)
	}
	remaining := b.limit - int64(b.Buffer.Len())
	if remaining <= 0 {
		b.truncated = true
		return len(p), nil
	}
	if int64(len(p)) > remaining {
		b.truncated = true
		_, _ = b.Buffer.Write(p[:remaining])
		return len(p), nil
	}
	return b.Buffer.Write(p)
}
