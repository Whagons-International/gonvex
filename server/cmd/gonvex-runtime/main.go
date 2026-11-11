package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gonvex/gonvex/server/internal/config"
	"github.com/gonvex/gonvex/server/internal/dbpool"
	gonvexruntime "github.com/gonvex/gonvex/server/internal/server"
	"github.com/gonvex/gonvex/server/internal/supervisor"
)

func main() {
	cfg := config.FromEnv()
	if reloadSupervisorEnabled() {
		if err := runSupervisor(cfg); err != nil {
			slog.Error("gonvex runtime supervisor stopped", "error", err)
			os.Exit(1)
		}
		return
	}
	runWorker(cfg)
}

func reloadSupervisorEnabled() bool {
	enabled := strings.TrimSpace(strings.ToLower(os.Getenv("GONVEX_RELOAD_SUPERVISOR")))
	worker := strings.TrimSpace(os.Getenv("GONVEX_RUNTIME_WORKER"))
	return (enabled == "1" || enabled == "true" || enabled == "yes" || enabled == "on") && worker != "1"
}

// terminationGrace is how long the gateway keeps its listener open after the
// worker's WebSocket drain window, for slow closes and in-flight requests.
// With the default 12s window the whole SIGTERM sequence stays well inside
// the 30s `docker stop` timeout.
const (
	terminationGrace      = 3 * time.Second
	maxTerminationDrain   = 20 * time.Second
	gatewayShutdownBudget = 5 * time.Second
)

func runSupervisor(cfg config.Config) error {
	runtimeSupervisor, err := supervisor.New(context.Background(), supervisor.Config{})
	if err != nil {
		return fmt.Errorf("start runtime supervisor: %w", err)
	}
	defer runtimeSupervisor.Close()

	// Listen before the first worker is ready: /healthz (and everything else)
	// answers 503 until a worker is active, instead of refusing connections.
	server := gonvexruntime.NewHTTPServer(cfg.Addr, runtimeSupervisor.Handler())
	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- server.ListenAndServe()
	}()

	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	slog.Info("starting supervised gonvex runtime gateway", "addr", cfg.Addr)
	started := make(chan error, 1)
	go func() {
		started <- runtimeSupervisor.Start()
	}()
	for ready := false; ; {
		select {
		case err := <-started:
			if err != nil {
				shutdownGateway(server, gatewayShutdownBudget)
				return fmt.Errorf("start runtime supervisor: %w", err)
			}
			ready = true
			slog.Info("supervised gonvex runtime gateway ready", "addr", cfg.Addr)
			continue
		case received := <-signals:
			if received == syscall.SIGTERM && ready {
				terminateGateway(runtimeSupervisor, signals)
			}
			shutdownGateway(server, gatewayShutdownBudget)
			return nil
		case err := <-serverErrors:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				return fmt.Errorf("serve runtime gateway: %w", err)
			}
			return nil
		}
	}
}

// terminateGateway drains the active worker's WebSockets while the gateway
// keeps accepting connections: the container stays routable until it exits,
// so closing the listener first would turn every request in the drain window
// into a refused connection (a 502 at the proxy). A second signal cuts the
// drain short.
func terminateGateway(runtimeSupervisor *supervisor.Supervisor, signals <-chan os.Signal) {
	budget := min(gonvexruntime.WebSocketDrainWindow()+terminationGrace, maxTerminationDrain)
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	go func() {
		select {
		case <-signals:
			slog.Warn("second shutdown signal; ending WebSocket drain early")
			cancel()
		case <-ctx.Done():
		}
	}()
	runtimeSupervisor.Terminate(ctx)
}

func shutdownGateway(server *http.Server, budget time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		slog.Warn("graceful gateway shutdown timed out", "error", err)
	}
}

func runWorker(cfg config.Config) {
	runtime, err := gonvexruntime.NewRequired(cfg)
	if err != nil {
		slog.Error("gonvex runtime startup failed", "error", err)
		os.Exit(1)
	}

	defer runtime.Close()

	server := gonvexruntime.NewHTTPServer(cfg.Addr, runtime.Handler())
	listener, err := supervisor.InheritedWorkerListener()
	if err != nil {
		slog.Error("gonvex runtime listener inheritance failed", "error", err)
		os.Exit(1)
	}
	serverErrors := make(chan error, 1)
	go func() {
		if listener != nil {
			serverErrors <- server.Serve(listener)
			return
		}
		serverErrors <- server.ListenAndServe()
	}()

	// A supervised worker keeps SIGTERM as the "drain" signal: the supervisor
	// sends it after activating the replacement worker so this process closes
	// its WebSockets gradually (code 1012) instead of dropping them all when it
	// exits. SIGINT remains the immediate shutdown. Standalone runtimes keep
	// the historic behavior where both signals shut down immediately.
	supervised := strings.TrimSpace(os.Getenv("GONVEX_RUNTIME_WORKER")) == "1"
	stopSignals := []os.Signal{os.Interrupt, syscall.SIGTERM}
	var drainSignals chan os.Signal
	if supervised {
		stopSignals = []os.Signal{os.Interrupt}
		drainSignals = make(chan os.Signal, 1)
		signal.Notify(drainSignals, syscall.SIGTERM)
	}
	ctx, stop := signal.NotifyContext(context.Background(), stopSignals...)
	defer stop()
	slog.Info("starting gonvex runtime", "addr", cfg.Addr)
	for {
		select {
		case <-drainSignals:
			drainSignals = nil
			// The replacement worker (or container) is already serving and has
			// its own connection budget against the same PostgreSQL: give back
			// idle connections and keep none before draining sockets.
			released := dbpool.BeginDrain()
			slog.Info("worker draining: released idle database connections", "idleClosed", released.IdleClosed, "inUse", released.InUse)
			go runtime.DrainWebSockets(0)
			continue
		case <-ctx.Done():
			shutdownContext, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if err := server.Shutdown(shutdownContext); err != nil {
				slog.Warn("graceful HTTP shutdown timed out", "error", err)
			}
		case err := <-serverErrors:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("gonvex runtime stopped", "error", err)
				os.Exit(1)
			}
		}
		return
	}
}
