package main

import (
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"strings"
	"syscall"
	"time"
)

// profilingBindRetry bounds how long a worker waits for the profiling port.
// During a supervised recycle the draining worker keeps the port until it
// exits (at most the supervisor's drain deadlines), so the replacement retries
// instead of giving up on the first "address in use".
const (
	profilingBindRetryEvery = 2 * time.Second
	profilingBindRetryFor   = 2 * time.Minute
)

// startProfilingListener exposes net/http/pprof on a separate loopback-only
// listener when GONVEX_PPROF_ADDR is set (load testing and incident
// profiling). It never shares the public handler and refuses non-loopback
// addresses, so it is unreachable from outside the container.
func startProfilingListener() {
	addr := strings.TrimSpace(os.Getenv("GONVEX_PPROF_ADDR"))
	if addr == "" {
		return
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		slog.Error("pprof listener disabled: invalid GONVEX_PPROF_ADDR", "error", err)
		return
	}
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		slog.Error("pprof listener disabled: GONVEX_PPROF_ADDR must be a loopback IP", "addr", addr)
		return
	}
	runtime.SetMutexProfileFraction(5)
	runtime.SetBlockProfileRate(100_000)
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	go func() {
		deadline := time.Now().Add(profilingBindRetryFor)
		for {
			listener, err := net.Listen("tcp", addr)
			if err == nil {
				slog.Info("pprof listening", "addr", addr)
				if err := http.Serve(listener, mux); err != nil {
					slog.Error("pprof listener stopped", "error", err)
				}
				return
			}
			if !errors.Is(err, syscall.EADDRINUSE) || time.Now().After(deadline) {
				slog.Error("pprof listener disabled", "addr", addr, "error", err)
				return
			}
			time.Sleep(profilingBindRetryEvery)
		}
	}()
}
