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

	// websockets counts upgrade requests proxied to this worker that are still
	// open. Gateway termination waits for it to reach zero.
	websockets  atomic.Int64
	drainSignal sync.Once
	exitedCh    chan struct{}
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
	// lifecycleMu orders Start's WaitGroup registration against Close's
	// cancellation, so Close never misses a start that is still booting.
	lifecycleMu sync.Mutex
	wg          sync.WaitGroup
	// terminating is set once the gateway itself is shutting down (SIGTERM
	// from the container runtime). Health turns 503 and new WebSocket upgrades
	// are refused so load balancers and reconnecting clients move to the
	// replacement container, while ordinary HTTP keeps being served.
	terminating atomic.Bool
}

// Start boots the initial worker and waits for readiness before returning a
// gateway handler. The caller owns the public HTTP listener.
func Start(parent context.Context, config Config) (*Supervisor, error) {
	supervisor, err := New(parent, config)
	if err != nil {
		return nil, err
	}
	if err := supervisor.Start(); err != nil {
		supervisor.Close()
		return nil, err
	}
	return supervisor, nil
}

// New prepares a supervisor without starting a worker. Its Handler can serve
// immediately: until Start activates a ready worker every request, /healthz
// included, gets 503 so the public listener can open before plugins load.
func New(parent context.Context, config Config) (*Supervisor, error) {
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
	return supervisor, nil
}

// Start boots the initial worker, waits for its readiness, and activates it.
// Close (from another goroutine) aborts a pending start.
func (s *Supervisor) Start() error {
	s.lifecycleMu.Lock()
	if s.ctx.Err() != nil {
		s.lifecycleMu.Unlock()
		return s.ctx.Err()
	}
	s.wg.Add(1)
	s.lifecycleMu.Unlock()
	worker, err := s.startWorker()
	if err != nil {
		s.wg.Done()
		return err
	}
	if s.ctx.Err() != nil {
		s.stopWorker(worker)
		s.wg.Done()
		return s.ctx.Err()
	}
	s.active.Store(worker)
	go s.run()
	return nil
}

func (s *Supervisor) Handler() http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		terminating := s.terminating.Load()
		if terminating && request.URL.Path == "/healthz" {
			writeUnavailable(response, "runtime gateway is shutting down")
			return
		}
		upgrade := isWebSocketUpgrade(request)
		if terminating && upgrade {
			// Send reconnecting clients to the replacement container instead of
			// re-attaching them to a worker that is draining its sockets.
			writeUnavailable(response, "runtime gateway is shutting down")
			return
		}
		for {
			worker := s.active.Load()
			if worker == nil {
				writeUnavailable(response, "runtime worker is starting")
				return
			}
			if !worker.acquire() {
				if s.active.Load() == worker {
					// The only worker is draining and no replacement is active:
					// answer instead of spinning until it is swapped out.
					writeUnavailable(response, "runtime worker is draining")
					return
				}
				continue
			}
			if s.active.Load() != worker {
				worker.release()
				continue
			}
			defer worker.release()
			if upgrade {
				worker.websockets.Add(1)
				defer worker.websockets.Add(-1)
			}
			request = request.WithContext(context.WithValue(request.Context(), workerContextKey{}, worker))
			s.proxy.ServeHTTP(response, request)
			return
		}
	})
}

// Terminate starts a graceful gateway shutdown and blocks until the active
// worker's WebSockets have drained or ctx ends. From the first call on,
// /healthz answers 503 and new WebSocket upgrades get 503 with Retry-After,
// while every other request is still proxied to the active worker. The worker
// is asked to close its sockets gradually (SIGTERM, close code 1012). Worker
// recycles are suppressed from now on. The caller keeps its listener open
// until Terminate returns, then shuts it down and calls Close.
func (s *Supervisor) Terminate(ctx context.Context) {
	if s == nil {
		return
	}
	s.terminating.Store(true)
	worker := s.active.Load()
	if worker == nil || worker.command == nil || worker.command.Process == nil {
		return
	}
	open := worker.websockets.Load()
	slog.Info("runtime gateway terminating: draining worker WebSockets", "pid", worker.command.Process.Pid, "websockets", open)
	worker.signalDrain()
	started := time.Now()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for worker.websockets.Load() > 0 {
		select {
		case <-ctx.Done():
			slog.Warn("runtime gateway WebSocket drain timed out", "remaining", worker.websockets.Load(), "elapsed", time.Since(started))
			return
		case <-worker.exited():
			return
		case <-ticker.C:
		}
	}
	slog.Info("runtime gateway WebSocket drain complete", "websockets", open, "elapsed", time.Since(started))
}

// Terminating reports whether Terminate has been called.
func (s *Supervisor) Terminating() bool {
	return s != nil && s.terminating.Load()
}

func writeUnavailable(response http.ResponseWriter, message string) {
	response.Header().Set("Retry-After", "1")
	http.Error(response, message, http.StatusServiceUnavailable)
}

func isWebSocketUpgrade(request *http.Request) bool {
	if !strings.EqualFold(strings.TrimSpace(request.Header.Get("Upgrade")), "websocket") {
		return false
	}
	for _, value := range request.Header.Values("Connection") {
		for _, token := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), "upgrade") {
				return true
			}
		}
	}
	return false
}

func (s *Supervisor) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		s.lifecycleMu.Lock()
		s.cancel()
		s.lifecycleMu.Unlock()
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
			if s.terminating.Load() {
				slog.Info("runtime gateway terminating; ignoring worker recycle request")
				continue
			}
			s.replaceActiveWorker()
		case worker := <-s.exits:
			if s.active.CompareAndSwap(worker, nil) {
				if s.terminating.Load() {
					slog.Warn("runtime worker exited while the gateway terminates", "pid", worker.command.Process.Pid)
					continue
				}
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
		if err == nil && s.terminating.Load() {
			// The gateway began terminating while the replacement booted: the
			// current worker is already draining its WebSockets and keeps
			// serving HTTP until exit, so a fresh worker would only add load.
			slog.Info("runtime gateway terminating; discarding replacement worker", "pid", replacement.command.Process.Pid)
			s.stopWorker(replacement)
			return
		}
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
	worker := &workerProcess{address: address, command: command, done: make(chan error, 1), drained: make(chan struct{}), exitedCh: make(chan struct{})}
	if err := command.Start(); err != nil {
		return nil, fmt.Errorf("start runtime worker: %w", err)
	}
	go func() {
		err := command.Wait()
		close(worker.exitedCh)
		worker.done <- err
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
	worker.signalDrain()
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

// signalDrain sends the drain signal (SIGTERM) once. A supervised worker
// treats it as "close WebSockets gradually"; a second one would be ignored.
func (worker *workerProcess) signalDrain() {
	worker.drainSignal.Do(func() {
		_ = worker.command.Process.Signal(syscall.SIGTERM)
	})
}

// exited is closed once the worker process has been reaped.
func (worker *workerProcess) exited() <-chan struct{} {
	if worker.exitedCh == nil {
		return nil
	}
	return worker.exitedCh
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
