package pkg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cnuss/nanokube/pkg/nanokube"
	v1 "github.com/cnuss/nanokube/pkg/v1"
	"github.com/emicklei/go-restful/v3"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	nodeutil "k8s.io/component-helpers/node/util"
	kubeletapp "k8s.io/kubernetes/cmd/kubelet/app"
	kubeletoptions "k8s.io/kubernetes/cmd/kubelet/app/options"
	"k8s.io/kubernetes/pkg/capabilities"
	"k8s.io/kubernetes/pkg/kubelet"
	kubeletserver "k8s.io/kubernetes/pkg/kubelet/server"
)

const shutdownTimeout = 30 * time.Second

type KubeletImpl struct {
	ctx     context.Context
	cancel  context.CancelCauseFunc
	cmd     *cobra.Command
	options v1.Options

	version     string
	versionOnce sync.Once

	runOnce sync.Once

	canceled     chan struct{}
	canceledOnce sync.Once
	cancelMu     sync.Mutex
	cancelHooks  [][]func(context.Context)
	cancelOnce   sync.Once

	exitCode     chan int
	exitCodeOnce sync.Once

	host     v1.Host
	hostOnce sync.Once

	kube     *KubeImpl
	kubeOnce sync.Once

	apiserver         v1.ApiServer
	apiserverProvided chan struct{}

	storage         v1.Storage
	storageProvided chan struct{}

	backends     sync.Map // map[v1.BackendName]v1.Backend
	backendsOnce sync.Once

	services     []*restful.WebService
	servicesOnce sync.Once

	staticPods     []*corev1.Pod
	staticPodsOnce sync.Once

	dirs    sync.Map
	files   sync.Map
	tunnels sync.Map

	pidfileWritten bool
	detachOnce     sync.Once

	httpServer     *http.Server
	httpServerOnce sync.Once

	listener     net.Listener
	listenerOnce sync.Once

	server     *kubeletserver.Server
	serverOnce sync.Once

	bootstrap     kubelet.Bootstrap
	bootstrapOnce sync.Once
}

// keepAliveListener mirrors the private tcpKeepAliveListener in
// k8s.io/apiserver/pkg/server: enables TCP keep-alive on accepted
// connections so half-dead clients get cleaned up.
type keepAliveListener struct {
	net.Listener
	period time.Duration
}

func (ln keepAliveListener) Accept() (net.Conn, error) {
	c, err := ln.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(ln.period)
	}
	return c, nil
}

var _ v1.Kubelet = &KubeletImpl{}

func NewKubelet(cmd *cobra.Command) v1.Kubelet {
	ctx, cancel := context.WithCancelCause(context.Background())

	cmd.SetVersionTemplate("{{.Version}}\n")

	options := nanokube.NewOptions(cmd)

	kubelet := &KubeletImpl{
		ctx:               ctx,
		cancel:            cancel,
		canceled:          make(chan struct{}),
		exitCode:          make(chan int, 1),
		options:           options,
		cmd:               cmd,
		apiserverProvided: make(chan struct{}),
		storageProvided:   make(chan struct{}),
	}

	// TODO: consider moving this goroutine into Done() behind a sync.Once so the exit-code orchestration lives next to the channel it publishes on.
	go func() {
		<-ctx.Done()
		cause := context.Cause(ctx)
		code := 0
		var err v1.Error
		if errors.As(cause, &err) {
			fmt.Fprintf(os.Stderr, "nanokube exiting: %v\n", err)
			code = err.ExitStatus()
		}
		// Drain only services that were actually wired up. If cancellation
		// fires before WithApiServer/WithStorage (e.g. the stop subcommand
		// or an early-startup error), the provided channel is still open
		// and there is nothing to await.
		select {
		case <-kubelet.apiserverProvided:
			<-kubelet.apiserver.Done()
		default:
		}
		select {
		case <-kubelet.storageProvided:
			<-kubelet.storage.Done()
		default:
		}
		kubelet.exitCodeOnce.Do(func() {
			kubelet.exitCode <- code
		})
	}()

	ran := false
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		nanokube.SetupLogging(kubelet.Options().Verbosity())
		nanokube.Log.Info("starting", "version", kubelet.Version())
		ran = true
		return nil
	}

	startCmd := &cobra.Command{
		Use:   "start",
		Short: "Start nanokube in the background",
		RunE: func(cmd *cobra.Command, args []string) error {
			ran = true
			bin, err := os.Executable()
			if err != nil {
				return fmt.Errorf("failed to resolve current executable: %w", err)
			}

			logPath := options.FilePathAt(v1.LogFile(options))
			logFile, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
			if err != nil {
				return fmt.Errorf("failed to open log file %s: %w", logPath, err)
			}
			defer logFile.Close()

			devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
			if err != nil {
				return fmt.Errorf("failed to open /dev/null: %w", err)
			}
			defer devNull.Close()

			childArgs := append([]string{bin}, append(options.Args(), args...)...)
			env := append(os.Environ(), fmt.Sprintf("NANOKUBE_LAUNCHER_PID=%d", os.Getpid()))

			child, err := os.StartProcess(bin, childArgs, &os.ProcAttr{
				Dir:   string(options.DataDir()),
				Env:   env,
				Files: []*os.File{devNull, logFile, logFile},
				Sys:   &syscall.SysProcAttr{Setsid: true},
			})
			if err != nil {
				return fmt.Errorf("failed to spawn nanokube: %w", err)
			}

			parentSigs := make(chan os.Signal, 1)
			signal.Notify(parentSigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
			defer signal.Stop(parentSigs)

			ready := make(chan os.Signal, 1)
			signal.Notify(ready, syscall.SIGUSR1)
			defer signal.Stop(ready)

			childExit := make(chan *os.ProcessState, 1)
			go func() {
				state, _ := child.Wait()
				childExit <- state
			}()

			tailFile, err := os.OpenFile(logPath, os.O_RDONLY|os.O_CREATE, 0o644)
			if err != nil {
				nanokube.Log.Warn("could not tail nanokube log", "path", logPath, "error", err)
			} else {
				defer tailFile.Close()
				tailFile.Seek(0, io.SeekEnd)
			}

			drain := func() {
				if tailFile == nil {
					return
				}
				if _, err := io.Copy(os.Stdout, tailFile); err != nil && !errors.Is(err, io.EOF) {
					nanokube.Log.Warn("error tailing nanokube log", "error", err)
				}
			}

			tick := time.NewTicker(100 * time.Millisecond)
			defer tick.Stop()

			nanokube.Log.Info("nanokube spawned, waiting for ready", "pid", child.Pid)
			for {
				select {
				case <-tick.C:
					drain()
				case <-ready:
					drain()
					nanokube.Log.Info("nanokube is ready", "pid", child.Pid)
					os.Exit(0)
				case sig := <-parentSigs:
					nanokube.Log.Info("forwarding signal", "signal", sig, "pid", child.Pid)
					if err := child.Signal(sig); err != nil {
						nanokube.Log.Warn("failed to forward signal", "signal", sig, "error", err)
					}
				case state := <-childExit:
					drain()
					code := 1
					if state != nil && state.Exited() {
						code = state.ExitCode()
					}
					nanokube.Log.Warn("nanokube exited", "pid", child.Pid, "code", code)
					os.Exit(code)
				}
			}
		},
	}
	options.RunFlags(startCmd) // TODO(incomplete): this feels wierd
	cmd.AddCommand(startCmd)

	cmd.AddCommand(&cobra.Command{
		Use:   "stop",
		Short: "Stop a running nanokube",
		RunE: func(cmd *cobra.Command, args []string) error {
			ran = true
			pidPath := options.FilePathAt(v1.PidFile(options))
			data, err := os.ReadFile(pidPath)
			if errors.Is(err, os.ErrNotExist) {
				nanokube.Log.Info("not running", "pidfile", pidPath)
				os.Exit(0)
			}
			if err != nil {
				return fmt.Errorf("failed to read pidfile %s: %w", pidPath, err)
			}
			pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
			if err != nil {
				return fmt.Errorf("invalid pid in %s: %w", pidPath, err)
			}
			if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
				if errors.Is(err, syscall.ESRCH) {
					nanokube.Log.Info("not running, removing stale pidfile", "pid", pid, "pidfile", pidPath)
					os.Remove(pidPath)
					os.Exit(0)
				}
				return fmt.Errorf("failed to signal pid %d: %w", pid, err)
			}
			nanokube.Log.Info("sent SIGTERM to nanokube", "pid", pid)
			os.Exit(0)
			return nil
		},
	})

	if err := cmd.ExecuteContext(ctx); err != nil {
		kubelet.Cancel(nanokube.NewError(err).WithCode(1))
	} else if !ran {
		os.Exit(0)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		sig := <-sigCh
		nanokube.Log.Info("shutdown initiated", "signal", sig)
		kubelet.Cancel(nil)
	}()

	return kubelet
}

func (k *KubeletImpl) Context() context.Context {
	return k.ctx
}

func (k *KubeletImpl) Cancel(reason v1.Error) {
	k.canceledOnce.Do(func() { close(k.canceled) })
	k.runCancelHooks()
	if k.pidfileWritten {
		pidPath := k.Options().FilePathAt(v1.PidFile(k.Options()))
		if err := os.Remove(pidPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			nanokube.Log.Warn("failed to remove pidfile", "path", pidPath, "error", err)
		}
	}
	k.cancel(reason)
	var fatal v1.Error
	if errors.As(reason, &fatal) {
		runtime.Goexit()
	}
}

func (k *KubeletImpl) Detach() {
	k.detachOnce.Do(func() {
		pidStr := os.Getenv("NANOKUBE_LAUNCHER_PID")
		if pidStr == "" {
			return
		}
		ppid, err := strconv.Atoi(pidStr)
		if err != nil {
			nanokube.Log.Warn("invalid NANOKUBE_LAUNCHER_PID", "value", pidStr, "error", err)
			return
		}
		if err := syscall.Kill(ppid, syscall.SIGUSR1); err != nil {
			nanokube.Log.Warn("failed to signal launcher", "pid", ppid, "error", err)
		}
	})
}

func (k *KubeletImpl) Canceled() <-chan struct{} {
	return k.canceled
}

func (k *KubeletImpl) OnCancel(fns ...func(ctx context.Context)) v1.Kubelet {
	k.cancelMu.Lock()
	defer k.cancelMu.Unlock()
	k.cancelHooks = append(k.cancelHooks, fns)
	return k
}

func (k *KubeletImpl) OnReady(service v1.ServiceName, fns ...func(ctx context.Context)) v1.Kubelet {
	for _, fn := range fns {
		var ch <-chan struct{}

		switch service {
		case v1.APIServerService:
			ch = k.ApiServer().Ready()
		case v1.Node:
			ch = k.Kube().NodeReady()
		default:
			k.Cancel(nanokube.NewError(fmt.Errorf("unknown service %s", service)))
			return k
		}

		go func(fn func(ctx context.Context), ch <-chan struct{}) {
			for {
				select {
				case <-ch:
					fn(k.Context())
					return
				case <-k.Canceled():
					return
				}
			}
		}(fn, ch)
	}
	return k
}

func (k *KubeletImpl) runCancelHooks() {
	k.cancelOnce.Do(func() {
		k.cancelMu.Lock()
		groups := append([][]func(context.Context){}, k.cancelHooks...)
		k.cancelMu.Unlock()

		hookCtx, hookCancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer hookCancel()

		for groupIdx, group := range groups {
			var wg sync.WaitGroup
			for _, hook := range group {
				wg.Add(1)
				go func(fn func(context.Context)) {
					defer wg.Done()
					defer func() {
						if r := recover(); r != nil {
							nanokube.Log.Error("cancel hook panicked", "group", groupIdx, "recover", r, "stack", string(debug.Stack()))
						}
					}()
					fn(hookCtx)
				}(hook)
			}

			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()

			select {
			case <-done:
			case <-hookCtx.Done():
				nanokube.Log.Warn("cancel hook group timed out", "group", groupIdx, "timeout", shutdownTimeout)
				return
			}
		}
	})
}

func (k *KubeletImpl) Done() <-chan int {
	return k.exitCode
}

func (k *KubeletImpl) Options() v1.Options {
	return k.options
}

func (k *KubeletImpl) Version() string {
	k.versionOnce.Do(func() {
		k.version = k.cmd.Version
	})
	return k.version
}

func (k *KubeletImpl) Tunnel() v1.Tunnel {
	// DEVNOTE: Formerly had separate tunnels for the API server and kubelet
	//          Leaving scaffolding in place to allow for multiple tunnels in the future, but for now just return a single shared tunnel.
	tunnelName := v1.SharedTunnel

	tunnel, _ := k.tunnels.LoadOrStore(tunnelName, func() v1.Tunnel {
		return NewTunnel(k, tunnelName)
	}())
	return tunnel.(v1.Tunnel)
}

func (k *KubeletImpl) Host() v1.Host {
	k.hostOnce.Do(func() {
		k.host = nanokube.NewHost(k)
	})
	return k.host
}

func (k *KubeletImpl) Kube() v1.Kube {
	k.kubeOnce.Do(func() {
		k.kube = newKube(k)
	})
	return k.kube
}

func (k *KubeletImpl) Client() v1.Client {
	return k.ApiServer().Client()
}

func (k *KubeletImpl) WithApiServer(apiserver v1.ApiServer) v1.Kubelet {
	k.apiserver = apiserver
	close(k.apiserverProvided)
	return k
}

func (k *KubeletImpl) ApiServer() v1.ApiServer {
	<-k.apiserverProvided
	return k.apiserver
}

func (k *KubeletImpl) WithStorage(storage v1.Storage) v1.Kubelet {
	k.storage = storage
	close(k.storageProvided)
	return k
}

func (k *KubeletImpl) Storage() v1.Storage {
	<-k.storageProvided
	return k.storage
}

func (k *KubeletImpl) Backend(name v1.BackendName) v1.Backend {
	if backend, ok := k.backends.Load(name); ok {
		return backend.(v1.Backend)
	}
	return nil
}

func (k *KubeletImpl) Backends() map[v1.BackendName]v1.Backend {
	k.backendsOnce.Do(func() {
		for _, detect := range v1.Backends {
			backend := detect(k)
			if backend != nil {
				nanokube.Log.Info("backend detected", "backend", backend.Name())
				k.WithBackend(backend.Name(), backend)
			}
		}
	})

	backends := make(map[v1.BackendName]v1.Backend)
	k.backends.Range(func(key, value any) bool {
		name := key.(v1.BackendName)
		backend := value.(v1.Backend)
		backends[name] = backend
		return true
	})
	return backends
}

func (k *KubeletImpl) Services(baseURL *url.URL) []*restful.WebService {
	k.servicesOnce.Do(func() {
		services := []*restful.WebService{}
		for _, backend := range k.Backends() {
			services = append(services, backend.WithBaseURL(baseURL.JoinPath(string(backend.Name()))).Services()...)
		}
		k.services = services
	})
	return k.services
}

func (k *KubeletImpl) DefaultBackend() v1.Backend {
	for _, backend := range k.Backends() {
		return backend
	}
	// TODO(incomplete): better info on backends searched
	// TODO(partial): better CTA on how to add backends
	k.Cancel(nanokube.NewError(fmt.Errorf("no backends detected")))
	return nil
}

func (k *KubeletImpl) WithBackend(name v1.BackendName, backend v1.Backend) v1.Kubelet {
	k.backends.Store(name, backend)
	return k
}

func (k *KubeletImpl) StaticPods() []*corev1.Pod {
	k.staticPodsOnce.Do(func() {
		k.staticPods = []*corev1.Pod{}
		mountPath := string(k.Options().DataDir())
		volumes := []corev1.Volume{
			{Name: "nanokube-home", VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: string(mountPath),
				},
			}},
		}
		volumeMounts := []corev1.VolumeMount{
			{Name: "nanokube-home", MountPath: string(mountPath)},
		}

		k.staticPods = append(k.staticPods, &corev1.Pod{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Pod",
				APIVersion: "v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      k.Options().Name(),
				Namespace: "kube-system",
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:         "controller-manager",
						Image:        "registry.k8s.io/kube-controller-manager:" + k.Version(),
						Command:      []string{"kube-controller-manager"},
						Args:         k.Kube().Args(v1.ControllerManagerService, mountPath),
						VolumeMounts: volumeMounts,
					},
					{
						Name:         "scheduler",
						Image:        "registry.k8s.io/kube-scheduler:" + k.Version(),
						Command:      []string{"kube-scheduler"},
						Args:         k.Kube().Args(v1.SchedulerService, mountPath),
						VolumeMounts: volumeMounts,
					},
				},
				Volumes: volumes,
			},
		})
	})
	return k.staticPods
}

func (k *KubeletImpl) Bootstrap() kubelet.Bootstrap {
	k.bootstrapOnce.Do(func() {
		kubeDeps := k.Kube().KubeletDependencies()

		server := &kubeletoptions.KubeletServer{
			KubeletFlags:         *k.Kube().KubeletFlags(),
			KubeletConfiguration: *k.Kube().KubeletConfiguration(),
		}

		hostname, _ := nodeutil.GetHostname(server.HostnameOverride)
		nodeName := types.NodeName(hostname)
		nodeIPs, _, _ := nodeutil.ParseNodeIPArgument(server.NodeIP, server.CloudProvider)

		klet, err := kubeletapp.CreateAndInitKubelet(k.ctx, server, kubeDeps, hostname, nodeName, nodeIPs)
		if err != nil {
			k.Cancel(nanokube.NewError(fmt.Errorf("create kubelet: %w", err)).WithCode(1))
			return
		}

		k.bootstrap = klet
	})
	return k.bootstrap
}

func (k *KubeletImpl) Run() v1.Kubelet {
	k.runOnce.Do(func() {
		pidPath := k.Options().FilePathAt(v1.PidFile(k.Options()))
		if err := os.WriteFile(pidPath, []byte(strconv.Itoa(os.Getpid())), 0o644); err != nil {
			nanokube.Log.Warn("failed to write pidfile", "path", pidPath, "error", err)
		} else {
			k.pidfileWritten = true
		}

		for _, pod := range k.StaticPods() {
			data, err := json.Marshal(pod)
			if err != nil {
				k.Cancel(nanokube.NewError(fmt.Errorf("Failed to marshal static pod %s: %v", pod.Name, err)))
				return
			}
			if err := os.WriteFile(filepath.Join(k.Kube().KubeletConfiguration().StaticPodPath, pod.Name+".json"), data, 0o644); err != nil {
				k.Cancel(nanokube.NewError(fmt.Errorf("Failed to write static pod manifest %s: %v", pod.Name, err)))
				return
			}
		}

		capabilities.Initialize(capabilities.Capabilities{AllowPrivileged: true})

		go func() {
			ln := keepAliveListener{Listener: k.Kube().SecureServing().Listener, period: 1 * time.Minute}
			if err := k.HTTPServer().ServeTLS(ln, k.Kube().CertFilePath(), k.Kube().KeyFilePath()); err != nil && !errors.Is(err, http.ErrServerClosed) {
				k.Cancel(nanokube.NewError(fmt.Errorf("kubelet HTTP server error: %w", err)).WithCode(1))
			}
		}()
		go k.Bootstrap().Run(k.ctx, k.Kube().KubeletDependencies().PodConfig.Updates())
		go k.Bootstrap().ListenAndServePodResources(k.ctx)
		go k.Bootstrap().ListenAndServePods(k.ctx)
	})
	return k
}

func (k *KubeletImpl) Server() *kubeletserver.Server {
	k.serverOnce.Do(func() {
		kl := k.Bootstrap().(*kubelet.Kubelet)
		ksrv := kubeletserver.NewServer(k.ctx, kl, kl.ResourceAnalyzer(), kl.HealthCheckers(), kl.Flagz(), k.Kube().KubeletDependencies().Auth, k.Kube().KubeletConfiguration())
		ksrv.InstallTracingFilter(k.Kube().KubeletDependencies().TracerProvider)

		//
		// DEVNOTE: add streaming handlers
		//          TODO(partial): consider providing these directly and disable API Server management of /exec /attach /portforward
		//
		for _, ws := range k.Services(k.Tunnel().URL()) {
			ksrv.Restful().Add(ws)
			nanokube.Log.Info("registered service", "service", ws.RootPath())
		}

		//
		// DEVNOTE: we combine the kubelet server and the kubernetes apiserver together
		//          TODO(partial): add kubelet auth
		//
		ksrv.Restful().Handle("/", k.ApiServer().Handler())
		nanokube.Log.Info("registered API server")

		k.server = &ksrv
	})
	return k.server
}

func (k *KubeletImpl) HTTPServer() *http.Server {
	k.httpServerOnce.Do(func() {
		tunnel := k.Tunnel()
		k.httpServer = &http.Server{
			Addr:      net.JoinHostPort(tunnel.LocalIP().String(), strconv.FormatUint(uint64(tunnel.LocalPort()), 10)),
			TLSConfig: k.Kube().TLSConfig(),
			Handler:   k.Server(),
		}
		k.httpServer.RegisterOnShutdown(func() {
			if err := k.ApiServer().Destroy(); err != nil {
				nanokube.Log.Warn("failed to destroy API server", "error", err)
			}
		})
	})
	return k.httpServer
}
