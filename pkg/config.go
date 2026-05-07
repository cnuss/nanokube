package pkg

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	"github.com/cnuss/nanokube/pkg/nanokube"
	v1 "github.com/cnuss/nanokube/pkg/v1"
	"github.com/emicklei/go-restful/v3"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	noopoteltrace "go.opentelemetry.io/otel/trace/noop"
	"k8s.io/client-go/tools/record"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/version"
	"k8s.io/klog/v2"
	kubeletapp "k8s.io/kubernetes/cmd/kubelet/app"
	kubeletoptions "k8s.io/kubernetes/cmd/kubelet/app/options"
	"k8s.io/kubernetes/pkg/kubelet"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	containertest "k8s.io/kubernetes/pkg/kubelet/container/testing"
	probetest "k8s.io/kubernetes/pkg/kubelet/prober/testing"
	"k8s.io/kubernetes/pkg/kubelet/server"
	kubeletutil "k8s.io/kubernetes/pkg/kubelet/util"
	"k8s.io/kubernetes/pkg/util/oom"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/configmap"
	"k8s.io/kubernetes/pkg/volume/csi"
	"k8s.io/kubernetes/pkg/volume/downwardapi"
	"k8s.io/kubernetes/pkg/volume/emptydir"
	"k8s.io/kubernetes/pkg/volume/fc"
	"k8s.io/kubernetes/pkg/volume/git_repo"
	"k8s.io/kubernetes/pkg/volume/hostpath"
	"k8s.io/kubernetes/pkg/volume/iscsi"
	"k8s.io/kubernetes/pkg/volume/local"
	"k8s.io/kubernetes/pkg/volume/nfs"
	"k8s.io/kubernetes/pkg/volume/portworx"
	"k8s.io/kubernetes/pkg/volume/projected"
	"k8s.io/kubernetes/pkg/volume/secret"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/kubernetes/pkg/volume/util/subpath"
	"k8s.io/mount-utils"
)

const shutdownTimeout = 30 * time.Second

type ConfigImpl struct {
	ctx     context.Context
	cancel  context.CancelCauseFunc
	cmd     *cobra.Command
	options v1.Options

	canceled     chan struct{}
	canceledOnce sync.Once
	cancelMu     sync.Mutex
	cancelHooks  [][]func(context.Context)
	cancelOnce   sync.Once

	host     v1.Host
	hostOnce sync.Once

	kube     *KubeImpl
	kubeOnce sync.Once

	kubeletOnce          sync.Once
	kubeletReady         chan struct{}
	kubeletFlags         *kubeletoptions.KubeletFlags
	kubeletConfiguration *kubeletconfig.KubeletConfiguration
	kubeletDependencies  *kubelet.Dependencies

	backends     sync.Map // map[v1.BackendName]v1.Backend
	backendsOnce sync.Once

	services     []*restful.WebService
	servicesOnce sync.Once

	dirs    sync.Map
	files   sync.Map
	tunnels sync.Map
}

var _ v1.Config = &ConfigImpl{}

func NewConfig() v1.Config {
	ctx, cancel := context.WithCancelCause(context.Background())

	pflag.CommandLine = pflag.NewFlagSet(os.Args[0], pflag.ContinueOnError)
	cmd := &cobra.Command{
		Use:           "nanokube [flags]",
		Short:         "nanokube is a fully functional Kubernetes cluster that runs natively on your machine",
		Version:       version.Get().GitVersion,
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	options := nanokube.NewOptions(cmd)

	config := &ConfigImpl{
		ctx:      ctx,
		cancel:   cancel,
		canceled: make(chan struct{}),
		options:  options,
		cmd:      cmd,
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		sig := <-sigCh
		nanokube.Log.Info("shutdown initiated", "signal", sig)
		config.Cancel(nil)
	}()

	go func() {
		<-ctx.Done()
		signal.Stop(sigCh)
		cause := context.Cause(ctx)
		var err v1.Error
		if errors.As(cause, &err) {
			fmt.Fprintln(os.Stderr, fmt.Sprintf("nanokube exiting: %v", err))
			os.Exit(err.ExitStatus())
		}
		os.Exit(0)
	}()

	ran := false
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		ran = true
		// TODO(future): subprocess management
		return nil
	}

	if err := cmd.ExecuteContext(ctx); err != nil {
		config.Cancel(nanokube.NewError(err).WithCode(1))
	} else if !ran {
		config.Cancel(nil)
	}

	return config
}

func (c *ConfigImpl) Context() context.Context {
	return c.ctx
}

func (c *ConfigImpl) Cancel(reason v1.Error) {
	c.canceledOnce.Do(func() { close(c.canceled) })
	c.runCancelHooks()
	c.cancel(reason)
	var fatal v1.Error
	if errors.As(reason, &fatal) {
		runtime.Goexit()
	}
}

func (c *ConfigImpl) Canceled() <-chan struct{} {
	return c.canceled
}

func (c *ConfigImpl) OnCancel(fns ...func(ctx context.Context)) v1.Config {
	c.cancelMu.Lock()
	defer c.cancelMu.Unlock()
	c.cancelHooks = append(c.cancelHooks, fns)
	return c
}

func (c *ConfigImpl) runCancelHooks() {
	c.cancelOnce.Do(func() {
		c.cancelMu.Lock()
		groups := append([][]func(context.Context){}, c.cancelHooks...)
		c.cancelMu.Unlock()

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

func (c *ConfigImpl) Done() <-chan struct{} {
	return c.Context().Done()
}

func (c *ConfigImpl) Options() v1.Options {
	return c.options
}

func (c *ConfigImpl) Version() string {
	return c.cmd.Version
}

func (c *ConfigImpl) Tunnel(service v1.ServiceName) v1.Tunnel {
	tunnel, _ := c.tunnels.LoadOrStore(service, func() v1.Tunnel {
		return NewTunnel(c, service)
	}())
	return tunnel.(v1.Tunnel)
}

func (c *ConfigImpl) Host() v1.Host {
	c.hostOnce.Do(func() {
		c.host = nanokube.NewHost(c)
	})
	return c.host
}

func (c *ConfigImpl) Kube() v1.Kube {
	c.kubeOnce.Do(func() {
		c.kube = newKube(c)
	})
	return c.kube
}

func (c *ConfigImpl) WithApiServer(apiserver v1.ApiServer) v1.Config {
	c.Kube().WithApiServer(apiserver)
	return c
}

func (c *ConfigImpl) ApiServer() v1.ApiServer {
	return c.Kube().ApiServer()
}

func (c *ConfigImpl) WithStorage(storage v1.Storage) v1.Config {
	c.Kube().WithStorage(storage)
	return c
}

func (c *ConfigImpl) Storage() v1.Storage {
	return c.Kube().Storage()
}

func (c *ConfigImpl) Backend(name v1.BackendName) v1.Backend {
	if backend, ok := c.backends.Load(name); ok {
		return backend.(v1.Backend)
	}
	return nil
}

func (c *ConfigImpl) Backends() map[v1.BackendName]v1.Backend {
	c.backendsOnce.Do(func() {
		for _, detect := range v1.Backends {
			backend := detect(c)
			if backend != nil {
				nanokube.Log.Info("backend detected", "backend", backend.Name())
				c.WithBackend(backend.Name(), backend)
			}
		}
	})

	backends := make(map[v1.BackendName]v1.Backend)
	c.backends.Range(func(key, value any) bool {
		name := key.(v1.BackendName)
		backend := value.(v1.Backend)
		backends[name] = backend
		return true
	})
	return backends
}

func (c *ConfigImpl) Services(baseURL *url.URL) []*restful.WebService {
	c.servicesOnce.Do(func() {
		services := []*restful.WebService{}
		for _, backend := range c.Backends() {
			services = append(services, backend.WithBaseURL(baseURL.JoinPath(string(backend.Name()))).Services()...)
		}
		c.services = services
	})
	return c.services
}

func (c *ConfigImpl) DefaultBackend() v1.Backend {
	for _, backend := range c.Backends() {
		return backend
	}
	return nil
}

func (c *ConfigImpl) WithBackend(name v1.BackendName, backend v1.Backend) v1.Config {
	c.backends.Store(name, backend)
	return c
}

func (c *ConfigImpl) ensureKubelet() {
	c.kubeletOnce.Do(func() {
		c.kubeletReady = make(chan struct{})
		tunnel := c.Tunnel(v1.KubeletService)

		c.kubeletFlags = kubeletoptions.NewKubeletFlags()
		c.kubeletFlags.CloudProvider = "external"
		c.kubeletFlags.HostnameOverride = tunnel.Hostname()
		c.kubeletFlags.NodeLabels = make(map[string]string) // TODO(incomplete): add labels
		c.kubeletFlags.NodeIP = tunnel.LocalIP().String()
		c.kubeletFlags.RootDirectory = c.Options().DataDirAt(v1.DataDirKubelet)

		if cfg, err := kubeletoptions.NewKubeletConfiguration(); err == nil {
			c.kubeletConfiguration = cfg
		} else {
			c.kubeletConfiguration = &kubeletconfig.KubeletConfiguration{}
		}
		if c.Options().Standalone() {
			c.kubeletConfiguration.RegisterNode = false
		}
		c.kubeletConfiguration.Address = tunnel.LocalIP().String()
		c.kubeletConfiguration.ClusterDomain = tunnel.Domain()
		// TODO(incomplete): probe a container to get resolv.conf
		c.kubeletConfiguration.ClusterDNS = []string{"1.1.1.1"}
		c.kubeletConfiguration.PodLogsDir = c.Options().DataDirAt(v1.DataDirLogs)
		c.kubeletConfiguration.Port = tunnel.LocalPort()
		c.kubeletConfiguration.ReadOnlyPort = 0
		c.kubeletConfiguration.StaticPodPath = c.Options().DataDirAt(v1.DataDirStaticPods)

		c.kubeletDependencies = &kubelet.Dependencies{
			// These dependencies are required by the kubelet
			RemoteRuntimeService: nil,
			RemoteImageService:   nil,
			CAdvisorInterface:    nil,
			ContainerManager:     nil,
			TLSOptions:           nil,
			// These are needed for non-standalone mode
			KubeClient:      nil,
			HeartbeatClient: nil,
			// Use fake implementations for the rest of the dependencies
			ProbeManager:              probetest.FakeManager{},
			OSInterface:               &containertest.FakeOS{},
			VolumePlugins:             volumePlugins(),
			OOMAdjuster:               oom.NewFakeOOMAdjuster(),
			Mounter:                   &mount.FakeMounter{},
			Subpather:                 &subpath.FakeSubpath{},
			HostUtil:                  hostutil.NewFakeHostUtil(nil),
			PodStartupLatencyTracker:  kubeletutil.NewPodStartupLatencyTracker(),
			NodeStartupLatencyTracker: kubeletutil.NewNodeStartupLatencyTracker(),
			TracerProvider:            noopoteltrace.NewTracerProvider(),
			Recorder:                  &record.FakeRecorder{},
		}
		if !c.Options().Standalone() {
			c.kubeletDependencies.KubeClient = c.Kube().Client()
			c.kubeletDependencies.HeartbeatClient = c.Kube().Client()
		} else {
			c.kubeletDependencies.KubeClient = nil
			c.kubeletDependencies.HeartbeatClient = nil
			c.kubeletDependencies.EventClient = nil
		}
		c.kubeletDependencies.RemoteRuntimeService = c.DefaultBackend().Driver()
		c.kubeletDependencies.RemoteImageService = c.DefaultBackend().Driver()
		c.kubeletDependencies.CAdvisorInterface = c.DefaultBackend()
		c.kubeletDependencies.ContainerManager = c.DefaultBackend().Manager()
		c.kubeletDependencies.TLSOptions = &server.TLSOptions{
			Config: &tls.Config{
				NextProtos: func() []string {
					if !v1.HTTP2 {
						return []string{"http/1.1"}
					}
					return []string{"h2", "http/1.1"}
				}(),
				MinVersion: func() uint16 {
					if v, err := cliflag.TLSVersion(c.kubeletConfiguration.TLSMinVersion); err == nil {
						return v
					}
					return cliflag.DefaultTLSVersion()
				}(),
				CipherSuites: func() []uint16 {
					if v, err := cliflag.TLSCipherSuites(c.kubeletConfiguration.TLSCipherSuites); err == nil {
						return v
					}
					return nil
				}(),
			},
			CertFile: filepath.Join(string(c.Options().DataDir()), string(v1.CertFile)),
			KeyFile:  filepath.Join(string(c.Options().DataDir()), string(v1.KeyFile)),
		}
		c.kubeletDependencies.ProbeManager = nil
		c.kubeletDependencies.Services = c.Services(tunnel.URL())
		c.kubeletDependencies.VolumePlugins = c.Host().VolumePlugins()
		c.kubeletDependencies.OSInterface = c.Host()
		c.kubeletDependencies.Mounter = c.Host()
		c.kubeletDependencies.Subpather = c.Host()
		c.kubeletDependencies.HostUtil = c.Host()

		c.Kube()
		c.kube.bindKubelet(c)
	})
}

func (c *ConfigImpl) KubeletReady() <-chan struct{} {
	c.ensureKubelet()
	return c.kubeletReady
}

func (c *ConfigImpl) KubeletFlags() *kubeletoptions.KubeletFlags {
	c.ensureKubelet()
	return c.kubeletFlags
}

func (c *ConfigImpl) KubeletConfiguration() *kubeletconfig.KubeletConfiguration {
	c.ensureKubelet()
	return c.kubeletConfiguration
}

func (c *ConfigImpl) KubeletDependencies() *kubelet.Dependencies {
	c.ensureKubelet()
	return c.kubeletDependencies
}

func (c *ConfigImpl) KubeletRun(ctx context.Context) {
	c.ensureKubelet()
	if err := kubeletapp.RunKubelet(ctx, &kubeletoptions.KubeletServer{
		KubeletFlags:         *c.kubeletFlags,
		KubeletConfiguration: *c.kubeletConfiguration,
	}, c.kubeletDependencies); err != nil {
		klog.Fatalf("Failed to run Kubelet: %v. Exiting.", err)
	}
	close(c.kubeletReady)
	<-ctx.Done()
}

func volumePlugins() []volume.VolumePlugin {
	allPlugins := []volume.VolumePlugin{}
	allPlugins = append(allPlugins, emptydir.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, git_repo.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, hostpath.ProbeVolumePlugins(volume.VolumeConfig{})...)
	allPlugins = append(allPlugins, nfs.ProbeVolumePlugins(volume.VolumeConfig{})...)
	allPlugins = append(allPlugins, secret.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, iscsi.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, downwardapi.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, fc.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, configmap.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, projected.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, portworx.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, local.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, csi.ProbeVolumePlugins()...)
	return allPlugins
}
