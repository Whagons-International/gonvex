package sandbox

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gonvex/gonvex/pkg/gonvex"
)

// escapePayload passes every regex in blockedGoTokens: it names no package, no
// import, and no blocked identifier. It relies solely on `os` being in lexical
// scope because the user body used to be spliced into the same file as the host
// plumbing, which imports os. Go resolves imports per file, so splitting the two
// removes os from scope and this stops compiling.
const escapePayload = `data, err := os.ReadFile("/proc/self/environ")
if err != nil {
	return nil, err
}
return map[string]any{"environBytes": len(data)}, nil`

// TestBlocklistDoesNotCatchScopeEscape documents *why* the file split is the
// fix: the blocklist is not the control that stops this, and never was.
func TestBlocklistDoesNotCatchScopeEscape(t *testing.T) {
	if err := validateGoBody(escapePayload); err != nil {
		t.Fatalf("payload unexpectedly tripped the blocklist (%v) -- this test is no longer testing scope isolation", err)
	}
}

// TestGoSandboxCannotReachHostPackages is the real regression guard. Against the
// pre-fix renderer this payload compiles and reads the runtime's environment,
// which leaks DATABASE_URL and the agent auth secret. After the fix it must fail
// to compile.
func TestGoSandboxCannotReachHostPackages(t *testing.T) {
	runner := NewRunner(t.TempDir())
	result, err := runner.RunGo(context.Background(), gonvex.GoSandboxRequest{
		Purpose: "Attempt to read the host process environment from the sandbox.",
		Code:    escapePayload,
	})
	if err == nil && result.OK {
		t.Fatalf("SANDBOX ESCAPE: blocklist-passing code read the host environment; result = %v", result.Result)
	}
	if _, leaked := result.Result["environBytes"]; leaked {
		t.Fatalf("SANDBOX ESCAPE: host environment bytes returned to caller: %v", result.Result)
	}
}

// TestGoSandboxCannotSpawnProcesses covers the second half of the escape.
func TestGoSandboxCannotSpawnProcesses(t *testing.T) {
	runner := NewRunner(t.TempDir())
	result, err := runner.RunGo(context.Background(), gonvex.GoSandboxRequest{
		Purpose: "Attempt to spawn a host process from the sandbox.",
		Code: `proc, err := os.StartProcess("/bin/sh", []string{"sh", "-c", "true"}, &os.ProcAttr{})
if err != nil {
	return nil, err
}
return map[string]any{"spawned": proc.Pid > 0}, nil`,
	})
	if err == nil && result.OK {
		t.Fatalf("SANDBOX ESCAPE: sandbox spawned a host process; result = %v", result.Result)
	}
}

// TestGoSandboxKeepsSafeStdlibUsable guards the fix against over-correction:
// the curated import set must remain available or every legitimate sandbox
// script breaks.
func TestGoSandboxKeepsSafeStdlibUsable(t *testing.T) {
	runner := NewRunner(t.TempDir())
	result, err := runner.RunGo(context.Background(), gonvex.GoSandboxRequest{
		Purpose: "Verify curated stdlib packages still work inside the sandbox.",
		Code: `parts := strings.Split("a,b,c", ",")
n, err := strconv.Atoi("41")
if err != nil {
	return nil, err
}
return map[string]any{
	"joined": strings.Join(parts, "-"),
	"label":  fmt.Sprintf("n=%d", n+1),
}, nil`,
	})
	if err != nil {
		t.Fatalf("RunGo returned error: %v", err)
	}
	if !result.OK {
		t.Fatalf("curated stdlib rejected: %s", result.Error)
	}
	if got := result.Result["joined"]; got != "a-b-c" {
		t.Fatalf("joined = %v, want a-b-c", got)
	}
	if got := result.Result["label"]; got != "n=42" {
		t.Fatalf("label = %v, want n=42", got)
	}
}

// TestUserFileHasNoHostImports asserts the structural property directly, so the
// guarantee is checked even on a machine where the go toolchain cannot run the
// compile-based tests above.
func TestUserFileHasNoHostImports(t *testing.T) {
	dir := t.TempDir()
	if err := writeSandboxSources(dir, escapePayload); err != nil {
		t.Fatalf("writeSandboxSources failed: %v", err)
	}

	userSrc, err := os.ReadFile(filepath.Join(dir, userSourceFile))
	if err != nil {
		t.Fatalf("reading user source: %v", err)
	}
	for _, banned := range []string{`"os"`, `"os/exec"`, `"syscall"`, `"unsafe"`, `"net"`, `"net/http"`, `"plugin"`} {
		if strings.Contains(string(userSrc), banned) {
			t.Fatalf("user source imports %s; host packages must never be in scope for user code", banned)
		}
	}

	hostSrc, err := os.ReadFile(filepath.Join(dir, hostSourceFile))
	if err != nil {
		t.Fatalf("reading host source: %v", err)
	}
	if strings.Contains(string(hostSrc), escapePayload) {
		t.Fatalf("user code was spliced into the host file; the split is what removes os from scope")
	}
}
