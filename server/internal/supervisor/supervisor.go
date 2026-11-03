package supervisor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	// Plugin compiles on a busy host took more than 5 minutes on 2026-11-02 and
	// the new container killed itself mid-deploy; allow a slow cold start.
	defaultStartupTimeout = 30 * time.Minute
	defaultDrainTimeout   = 20 * time.Second
	defaultHealthInterval = 250 * time.Millisecond
	recycleWorkerHeader   = "X-Gonvex-Recycle-Worker"
	workerListenerFDEnv   = "GONVEX_WORKER_LISTENER_FD"
)

// Config controls the permanent gateway and its recyclable runtime workers.
// Executable, Args, and Env are injectable so the process lifecycle can be
// stress-tested without opening plugins in the test (gateway) process.
type Config struct {
	Executable     string
	Args           []string
	Env            []string
	StartupTimeout time.Duration
	DrainTimeout   time.Duration
	HealthInterval time.Duration
}

type workerProcess struct {
	address string
	command *exec.Cmd
	done    chan error

	requestsMu sync.Mutex
	requests   int
	draining   bool
	drained    chan struct{}
	drainedSet bool
}

type workerContextKey struct{}

// Supervisor owns a tiny reverse-proxy gateway. Project plugins are loaded
// only by child runtime processes, allowing the OS to reclaim every plugin
// mapping when a successful dev sync replaces the active worker.
type Supervisor struct {
	ctx       context.Context
	cancel    context.CancelFunc
	config    Config
	active    atomic.Pointer[workerProcess]
	recycle   chan struct{}
	exits     chan *workerProcess
	proxy     *httputil.ReverseProxy
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// Start boots the initial worker and waits for readiness before returning a
// gateway handler. The caller owns the public HTTP listener.
func Start(parent context.Context, config Config) (*Supervisor, error) {
	if parent == nil {
		parent = context.Background()
	}
	if strings.TrimSpace(config.Executable) == "" {
		executable, err := os.Executable()
		if err != nil {
			return nil, fmt.Errorf("resolve runtime executable: %w", err)
		}
		config.Executable = executable
	}
	if config.StartupTimeout <= 0 {
		config.StartupTimeout = defaultStartupTimeout
	}
	if config.DrainTimeout <= 0 {
		config.DrainTimeout = defaultDrainTimeout
	}
	if config.HealthInterval <= 0 {
		config.HealthInterval = defaultHealthInterval
	}

	ctx, cancel := context.WithCancel(parent)
	supervisor := &Supervisor{
		ctx:     ctx,
		cancel:  cancel,
		config:  config,
		recycle: make(chan struct{}, 1),
		exits:   make(chan *workerProcess, 8),
	}
	supervisor.proxy = supervisor.newProxy()

	worker, err := supervisor.startWorker()
	if err != nil {
		cancel()
		return nil, err
	}
	supervisor.active.Store(worker)
	supervisor.wg.Add(1)
	go supervisor.run()
	return supervisor, nil
}

func (s *Supervisor) Handler() http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		for {
			worker := s.active.Load()
			if worker == nil {
				http.Error(response, "runtime worker is starting", http.StatusServiceUnavailable)
				return
			}
			if !worker.acquire() {
				continue
			}
			if s.active.Load() != worker {
				worker.release()
				continue
			}
			defer worker.release()
			request = request.WithContext(context.WithValue(request.Context(), workerContextKey{}, worker))
			s.proxy.ServeHTTP(response, request)
			return
		}
	})
}

func (s *Supervisor) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		s.cancel()
		s.wg.Wait()
	})
}

func (s *Supervisor) RequestRecycle() {
	select {
	case s.recycle <- struct{}{}:
	default:
	}
}

func (s *Supervisor) newProxy() *httputil.ReverseProxy {
	placeholder := &url.URL{Scheme: "http", Host: "127.0.0.1"}
	proxy := httputil.NewSingleHostReverseProxy(placeholder)
	baseDirector := proxy.Director
	proxy.Director = func(request *http.Request) {
		baseDirector(request)
		worker, _ := request.Context().Value(workerContextKey{}).(*workerProcess)
		if worker == nil {
			worker = s.active.Load()
			if worker == nil {
				return
			}
		}
		request.URL.Scheme = "http"
		request.URL.Host = worker.address
	}
	proxy.ModifyResponse = func(response *http.Response) error {
		request := response.Request
		// The worker reports whether this sync actually dlopen'd a new plugin
		// module (or attempted to and failed, which can poison the process).
		// A /dev/sync whose bundle hash is unchanged loads nothing, so
		// recycling for it would only manufacture a reconnect wave.
		workerLoadedBundle := response.Header.Get(recycleWorkerHeader) == "1"
		if request != nil && request.Method == http.MethodPost && request.URL.Path == "/dev/sync" && request.URL.Query().Get("dryRun") != "true" && workerLoadedBundle {
			s.RequestRecycle()
		}
		return nil
	}
	proxy.ErrorHandler = func(response http.ResponseWriter, _ *http.Request, err error) {
		slog.Warn("runtime worker proxy failed", "error", err)
		http.Error(response, "runtime worker unavailable", http.StatusServiceUnavailable)
	}
	return proxy
}

func (s *Supervisor) run() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			worker := s.active.Swap(nil)
			s.stopWorker(worker)
			return
		case <-s.recycle:
			s.replaceActiveWorker()
		case worker := <-s.exits:
			if s.active.CompareAndSwap(worker, nil) {
				slog.Warn("runtime worker exited unexpectedly; replacing it", "pid", worker.command.Process.Pid)
				s.replaceActiveWorker()
			}
		}
	}
}

func (s *Supervisor) replaceActiveWorker() {
	old := s.active.Load()
	for s.ctx.Err() == nil {
		replacement, err := s.startWorker()
		if err == nil {
			s.active.Store(replacement)
			slog.Info("activated recyclable runtime worker", "pid", replacement.command.Process.Pid)
			if old != nil && old != replacement {
				old.beginDrain()
				s.stopWorker(old)
			}
			return
		}
		slog.Error("replacement runtime worker failed readiness; keeping current worker", "error", err)
		select {
		case <-s.ctx.Done():
			return
		case <-time.After(time.Second):
		}
		old = s.active.Load()
	}
}

func (s *Supervisor) startWorker() (*workerProcess, error) {
	address, listener, listenerFile, err := reserveLoopbackListener()
	if err != nil {
		return nil, err
	}
	defer listener.Close()
	defer listenerFile.Close()
	command := exec.Command(s.config.Executable, s.config.Args...)
	command.Env = workerEnvironment(os.Environ(), s.config.Env, address)
	command.ExtraFiles = []*os.File{listenerFile}
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	worker := &workerProcess{address: address, command: command, done: make(chan error, 1), drained: make(chan struct{})}
	if err := command.Start(); err != nil {
		return nil, fmt.Errorf("start runtime worker: %w", err)
	}
	go func() {
		worker.done <- command.Wait()
		close(worker.done)
		select {
		case s.exits <- worker:
		case <-s.ctx.Done():
		}
	}()

	if err := s.waitForReadiness(worker); err != nil {
		s.stopWorker(worker)
		return nil, err
	}
	return worker, nil
}

func (s *Supervisor) waitForReadiness(worker *workerProcess) error {
	deadline := time.NewTimer(s.config.StartupTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(s.config.HealthInterval)
	defer ticker.Stop()
	client := &http.Client{Timeout: 2 * time.Second}
	healthURL := "http://" + worker.address + "/healthz"

	for {
		request, err := http.NewRequestWithContext(s.ctx, http.MethodGet, healthURL, nil)
		if err != nil {
			return err
		}
		response, requestErr := client.Do(request)
		if requestErr == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case err := <-worker.done:
			if err == nil {
				err = errors.New("worker exited before readiness")
			}
			return fmt.Errorf("runtime worker exited before readiness: %w", err)
		case <-deadline.C:
			return fmt.Errorf("runtime worker readiness timed out after %s", s.config.StartupTimeout)
		case <-ticker.C:
		}
	}
}

func (s *Supervisor) stopWorker(worker *workerProcess) {
	if worker == nil || worker.command == nil || worker.command.Process == nil {
		return
	}
	drained := worker.beginDrain()
	// Give in-flight HTTP requests a brief window to finish before any signal:
	// a worker that exits promptly on SIGTERM (one predating the drain signal)
	// must not be killed mid-request. Live WebSockets pin the request count, so
	// this grace expires quickly whenever there is anything left to drain.
	grace := time.NewTimer(min(2*time.Second, s.config.DrainTimeout/2))
	select {
	case <-drained:
	case <-grace.C:
	}
	grace.Stop()
	// SIGTERM asks a supervised worker to close its WebSockets gradually with
	// close code 1012 (service restart) so clients reconnect staggered instead
	// of as one wave. Workers that predate the drain signal treat it as an
	// immediate graceful shutdown, which the deadline below already assumed.
	_ = worker.command.Process.Signal(syscall.SIGTERM)
	timer := time.NewTimer(s.config.DrainTimeout)
	select {
	case <-drained:
	case <-timer.C:
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}

	_ = worker.command.Process.Signal(os.Interrupt)
	shutdownTimer := time.NewTimer(s.config.DrainTimeout)
	defer shutdownTimer.Stop()
	select {
	case <-worker.done:
		return
	case <-shutdownTimer.C:
		_ = worker.command.Process.Kill()
		<-worker.done
	}
}

func (worker *workerProcess) acquire() bool {
	worker.requestsMu.Lock()
	defer worker.requestsMu.Unlock()
	if worker.draining {
		return false
	}
	worker.requests++
	return true
}

func (worker *workerProcess) release() {
	worker.requestsMu.Lock()
	defer worker.requestsMu.Unlock()
	if worker.requests > 0 {
		worker.requests--
	}
	worker.closeDrainedLocked()
}

func (worker *workerProcess) beginDrain() <-chan struct{} {
	worker.requestsMu.Lock()
	defer worker.requestsMu.Unlock()
	worker.draining = true
	worker.closeDrainedLocked()
	return worker.drained
}

func (worker *workerProcess) closeDrainedLocked() {
	if worker.draining && worker.requests == 0 && !worker.drainedSet {
		close(worker.drained)
		worker.drainedSet = true
	}
}

func reserveLoopbackListener() (string, net.Listener, *os.File, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, nil, fmt.Errorf("reserve runtime worker address: %w", err)
	}
	fileProvider, ok := listener.(interface{ File() (*os.File, error) })
	if !ok {
		listener.Close()
		return "", nil, nil, fmt.Errorf("runtime worker listener %T cannot be inherited", listener)
	}
	listenerFile, err := fileProvider.File()
	if err != nil {
		listener.Close()
		return "", nil, nil, fmt.Errorf("duplicate runtime worker listener: %w", err)
	}
	return listener.Addr().String(), listener, listenerFile, nil
}

// InheritedWorkerListener returns the already-bound loopback listener passed
// by the gateway. Holding the socket open across exec eliminates the
// choose-port/close/bind race during worker startup.
func InheritedWorkerListener() (net.Listener, error) {
	value := strings.TrimSpace(os.Getenv(workerListenerFDEnv))
	if value == "" {
		return nil, nil
	}
	fd, err := strconv.Atoi(value)
	if err != nil || fd < 3 {
		return nil, fmt.Errorf("invalid inherited runtime worker listener fd %q", value)
	}
	file := os.NewFile(uintptr(fd), "gonvex-runtime-worker-listener")
	if file == nil {
		return nil, fmt.Errorf("open inherited runtime worker listener fd %d", fd)
	}
	listener, err := net.FileListener(file)
	_ = file.Close()
	if err != nil {
		return nil, fmt.Errorf("inherit runtime worker listener: %w", err)
	}
	return listener, nil
}

func workerEnvironment(base []string, additions []string, address string) []string {
	values := make(map[string]string, len(base)+len(additions)+2)
	for _, entry := range append(append([]string{}, base...), additions...) {
		name, value, found := strings.Cut(entry, "=")
		if found && name != "" {
			values[name] = value
		}
	}
	values["GONVEX_ADDR"] = address
	values["GONVEX_RUNTIME_WORKER"] = "1"
	values[workerListenerFDEnv] = "3"

	environment := make([]string, 0, len(values))
	for name, value := range values {
		environment = append(environment, name+"="+value)
	}
	return environment
}
