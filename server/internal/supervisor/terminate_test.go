//go:build !windows

package supervisor

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const drainWorkerHelperEnv = "GONVEX_SUPERVISOR_TEST_DRAIN_WORKER"

func TestHandlerIsUnavailableBeforeStart(t *testing.T) {
	supervisor, err := New(context.Background(), Config{Executable: "/bin/false"})
	if err != nil {
		t.Fatal(err)
	}
	defer supervisor.Close()
	for _, path := range []string{"/healthz", "/anything"} {
		recorder := httptest.NewRecorder()
		supervisor.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusServiceUnavailable || recorder.Header().Get("Retry-After") != "1" {
			t.Fatalf("%s before start = %d (Retry-After %q), want 503 with Retry-After 1", path, recorder.Code, recorder.Header().Get("Retry-After"))
		}
	}
}

func TestHandlerAnswersWhenOnlyWorkerIsDraining(t *testing.T) {
	supervisor, err := New(context.Background(), Config{Executable: "/bin/false"})
	if err != nil {
		t.Fatal(err)
	}
	defer supervisor.Close()
	worker := &workerProcess{address: "127.0.0.1:1", drained: make(chan struct{})}
	worker.beginDrain()
	supervisor.active.Store(worker)

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		recorder := httptest.NewRecorder()
		supervisor.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/hello", nil))
		done <- recorder
	}()
	select {
	case recorder := <-done:
		if recorder.Code != http.StatusServiceUnavailable || recorder.Header().Get("Retry-After") != "1" {
			t.Fatalf("draining worker response = %d (Retry-After %q), want 503 with Retry-After 1", recorder.Code, recorder.Header().Get("Retry-After"))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler spun while the only worker was draining")
	}
}

func TestIsWebSocketUpgrade(t *testing.T) {
	cases := []struct {
		connection, upgrade string
		want                bool
	}{
		{"Upgrade", "websocket", true},
		{"keep-alive, Upgrade", "WebSocket", true},
		{"keep-alive", "websocket", false},
		{"Upgrade", "h2c", false},
		{"", "", false},
	}
	for _, testCase := range cases {
		request := httptest.NewRequest(http.MethodGet, "/ws", nil)
		if testCase.connection != "" {
			request.Header.Set("Connection", testCase.connection)
		}
		if testCase.upgrade != "" {
			request.Header.Set("Upgrade", testCase.upgrade)
		}
		if got := isWebSocketUpgrade(request); got != testCase.want {
			t.Fatalf("isWebSocketUpgrade(Connection=%q, Upgrade=%q) = %v, want %v", testCase.connection, testCase.upgrade, got, testCase.want)
		}
	}
}

// TestTerminateKeepsServingWhileWebSocketsDrain covers the gateway's SIGTERM
// path: health flips to 503 at once, new upgrades are refused with 503, plain
// HTTP keeps reaching the worker, and Terminate returns only after the worker
// closed the WebSocket that was open when termination began.
func TestTerminateKeepsServingWhileWebSocketsDrain(t *testing.T) {
	if os.Getenv(drainWorkerHelperEnv) == "1" {
		runDrainWorkerHelper()
		return
	}
	const workerCloseDelay = 1500 * time.Millisecond
	supervisor, err := Start(context.Background(), Config{
		Executable:     os.Args[0],
		Args:           []string{"-test.run=^TestTerminateKeepsServingWhileWebSocketsDrain$"},
		Env:            []string{drainWorkerHelperEnv + "=1", "GONVEX_SUPERVISOR_TEST_CLOSE_DELAY=" + workerCloseDelay.String()},
		StartupTimeout: time.Minute,
		DrainTimeout:   5 * time.Second,
		HealthInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start supervisor: %v", err)
	}
	defer supervisor.Close()
	gateway := httptest.NewServer(supervisor.Handler())
	defer gateway.Close()
	client := &http.Client{Timeout: 2 * time.Second}

	if status, _ := get(t, client, gateway.URL+"/healthz"); status != http.StatusOK {
		t.Fatalf("healthz before termination = %d, want 200", status)
	}
	socket, status := dialUpgrade(t, gateway.URL)
	if status != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade before termination = %d, want 101", status)
	}
	defer socket.Close()

	terminated := make(chan time.Duration, 1)
	started := time.Now()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		supervisor.Terminate(ctx)
		terminated <- time.Since(started)
	}()
	waitFor(t, func() bool { return supervisor.Terminating() })

	if status, header := get(t, client, gateway.URL+"/healthz"); status != http.StatusServiceUnavailable || header.Get("Retry-After") != "1" {
		t.Fatalf("healthz while terminating = %d, want 503 with Retry-After 1", status)
	}
	if status, _ := get(t, client, gateway.URL+"/hello"); status != http.StatusOK {
		t.Fatalf("plain HTTP while terminating = %d, want 200", status)
	}
	refused, status := dialUpgrade(t, gateway.URL)
	refused.Close()
	if status != http.StatusServiceUnavailable {
		t.Fatalf("upgrade while terminating = %d, want 503", status)
	}
	select {
	case elapsed := <-terminated:
		t.Fatalf("Terminate returned after %s while a WebSocket was still open", elapsed)
	default:
	}

	// The worker closes the socket after its drain delay; Terminate follows.
	_ = socket.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.Copy(io.Discard, socket); err != nil {
		t.Fatalf("WebSocket was not closed by the worker drain: %v", err)
	}
	// Like a browser answering the close, finish our half of the connection;
	// the gateway's proxy holds the tunnel until both directions end.
	socket.Close()
	select {
	case elapsed := <-terminated:
		if elapsed < workerCloseDelay {
			t.Fatalf("Terminate returned after %s, before the worker drained (%s)", elapsed, workerCloseDelay)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Terminate did not return after the WebSocket drained")
	}
	if status, _ := get(t, client, gateway.URL+"/hello"); status != http.StatusOK {
		t.Fatalf("plain HTTP after the drain = %d, want 200", status)
	}

	// A recycle requested now must not start a replacement worker.
	supervisor.RequestRecycle()
	time.Sleep(200 * time.Millisecond)
	before := supervisor.active.Load()
	if status, _ := get(t, client, gateway.URL+"/hello"); status != http.StatusOK || supervisor.active.Load() != before {
		t.Fatalf("recycle during termination replaced the worker or broke HTTP (status %d)", status)
	}
	pid := before.command.Process.Pid
	supervisor.Close()
	waitForProcessExit(t, pid)
}

func get(t *testing.T, client *http.Client, target string) (int, http.Header) {
	t.Helper()
	response, err := client.Get(target)
	if err != nil {
		t.Fatalf("GET %s: %v", target, err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	return response.StatusCode, response.Header
}

// dialUpgrade sends a WebSocket upgrade through the gateway and returns the
// raw connection with the response status.
func dialUpgrade(t *testing.T, gatewayURL string) (net.Conn, int) {
	t.Helper()
	parsed, err := url.Parse(gatewayURL)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := net.Dial("tcp", parsed.Host)
	if err != nil {
		t.Fatal(err)
	}
	_ = connection.SetDeadline(time.Now().Add(3 * time.Second))
	fmt.Fprintf(connection, "GET /ws HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n", parsed.Host)
	response, err := http.ReadResponse(bufio.NewReader(connection), nil)
	if err != nil {
		connection.Close()
		t.Fatalf("read upgrade response: %v", err)
	}
	_ = connection.SetDeadline(time.Time{})
	return connection, response.StatusCode
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// runDrainWorkerHelper is a minimal supervised worker: it holds upgraded
// connections open, and on SIGTERM (the drain signal) closes them after a
// delay while still serving HTTP. SIGINT shuts it down.
func runDrainWorkerHelper() {
	closeDelay, _ := time.ParseDuration(os.Getenv("GONVEX_SUPERVISOR_TEST_CLOSE_DELAY"))
	var mu sync.Mutex
	sockets := []net.Conn{}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(response http.ResponseWriter, _ *http.Request) {
		_, _ = response.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /hello", func(response http.ResponseWriter, _ *http.Request) {
		_, _ = response.Write([]byte("hello"))
	})
	mux.HandleFunc("GET /ws", func(response http.ResponseWriter, request *http.Request) {
		if !strings.EqualFold(request.Header.Get("Upgrade"), "websocket") {
			http.Error(response, "upgrade required", http.StatusBadRequest)
			return
		}
		connection, buffered, err := response.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		_, _ = buffered.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
		_ = buffered.Flush()
		mu.Lock()
		sockets = append(sockets, connection)
		mu.Unlock()
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	listener, err := InheritedWorkerListener()
	if err != nil || listener == nil {
		fmt.Fprintln(os.Stderr, "drain test worker did not inherit a listener", err)
		os.Exit(1)
	}
	drain := make(chan os.Signal, 1)
	signal.Notify(drain, syscall.SIGTERM)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	go func() {
		<-drain
		time.Sleep(closeDelay)
		mu.Lock()
		for _, connection := range sockets {
			_ = connection.Close()
		}
		mu.Unlock()
	}()
	go func() {
		<-stop
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()
	if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}
