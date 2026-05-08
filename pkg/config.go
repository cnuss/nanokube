package pkg

import (
	"context"
	"encoding/json"
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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/component-base/version"
	kubeletapp "k8s.io/kubernetes/cmd/kubelet/app"
	kubeletoptions "k8s.io/kubernetes/cmd/kubelet/app/options"
)

const shutdownTimeout = 30 * time.Second

type ConfigImpl struct {
	ctx     context.Context
	cancel  context.CancelCauseFunc
	cmd     *cobra.Command
	options v1.Options

	runOnce sync.Once

	canceled     chan struct{}
	canceledOnce sync.Once
	cancelMu     sync.Mutex
	cancelHooks  [][]func(context.Context)
	cancelOnce   sync.Once

	host     v1.Host
	hostOnce sync.Once

	kube     *KubeImpl
	kubeOnce sync.Once

	backends     sync.Map // map[v1.BackendName]v1.Backend
	backendsOnce sync.Once

	services     []*restful.WebService
	servicesOnce sync.Once

	staticPods     []*corev1.Pod
	staticPodsOnce sync.Once

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

func (c *ConfigImpl) OnReady(service v1.ServiceName, fns ...func(ctx context.Context)) v1.Config {
	for _, fn := range fns {
		var ch <-chan struct{}

		switch service {
		case v1.APIServerService:
			ch = c.Kube().ApiServer().Ready()
		case v1.Node:
			ch = c.Kube().NodeReady()
		default:
			c.Cancel(nanokube.NewError(fmt.Errorf("unknown service %s", service)))
			return c
		}

		go func(fn func(ctx context.Context), ch <-chan struct{}) {
			for {
				select {
				case <-ch:
					fn(c.Context())
					return
				case <-c.Canceled():
					return
				}
			}
		}(fn, ch)
	}
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

func (c *ConfigImpl) StaticPods() []*corev1.Pod {
	c.staticPodsOnce.Do(func() {
		c.staticPods = []*corev1.Pod{}
		mountPath := string(c.Options().DataDir())
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

		c.staticPods = append(c.staticPods, &corev1.Pod{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Pod",
				APIVersion: "v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      c.Options().Name(),
				Namespace: "kube-system",
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:         "controller-manager",
						Image:        "registry.k8s.io/kube-controller-manager:v1.35.0", // TODO(incomplete): add version
						Command:      []string{"kube-controller-manager"},
						Args:         c.Kube().Args(v1.ControllerManagerService, mountPath),
						VolumeMounts: volumeMounts,
					},
					{
						Name:         "scheduler",
						Image:        "registry.k8s.io/kube-scheduler:v1.35.0", // TODO(incomplete): add version
						Command:      []string{"kube-scheduler"},
						Args:         c.Kube().Args(v1.SchedulerService, mountPath),
						VolumeMounts: volumeMounts,
					},
				},
				Volumes: volumes,
			},
		})
	})
	return c.staticPods
}

func (c *ConfigImpl) Run() v1.Config {
	c.runOnce.Do(func() {
		for _, pod := range c.StaticPods() {
			data, err := json.Marshal(pod)
			if err != nil {
				c.Cancel(nanokube.NewError(fmt.Errorf("Failed to marshal static pod %s: %v", pod.Name, err)))
				return
			}
			if err := os.WriteFile(filepath.Join(c.Kube().KubeletConfiguration().StaticPodPath, pod.Name+".json"), data, 0o644); err != nil {
				c.Cancel(nanokube.NewError(fmt.Errorf("Failed to write static pod manifest %s: %v", pod.Name, err)))
				return
			}
		}
		if err := kubeletapp.RunKubelet(c.ctx, &kubeletoptions.KubeletServer{
			KubeletFlags:         *c.Kube().KubeletFlags(),
			KubeletConfiguration: *c.Kube().KubeletConfiguration(),
		}, c.Kube().KubeletDependencies()); err != nil {
			c.Cancel(nanokube.NewError(err).WithCode(1))
		}
	})
	return c
}

// func volumePlugins() []volume.VolumePlugin {
// 	allPlugins := []volume.VolumePlugin{}
// 	allPlugins = append(allPlugins, emptydir.ProbeVolumePlugins()...)
// 	allPlugins = append(allPlugins, git_repo.ProbeVolumePlugins()...)
// 	allPlugins = append(allPlugins, hostpath.ProbeVolumePlugins(volume.VolumeConfig{})...)
// 	allPlugins = append(allPlugins, nfs.ProbeVolumePlugins(volume.VolumeConfig{})...)
// 	allPlugins = append(allPlugins, secret.ProbeVolumePlugins()...)
// 	allPlugins = append(allPlugins, iscsi.ProbeVolumePlugins()...)
// 	allPlugins = append(allPlugins, downwardapi.ProbeVolumePlugins()...)
// 	allPlugins = append(allPlugins, fc.ProbeVolumePlugins()...)
// 	allPlugins = append(allPlugins, configmap.ProbeVolumePlugins()...)
// 	allPlugins = append(allPlugins, projected.ProbeVolumePlugins()...)
// 	allPlugins = append(allPlugins, portworx.ProbeVolumePlugins()...)
// 	allPlugins = append(allPlugins, local.ProbeVolumePlugins()...)
// 	allPlugins = append(allPlugins, csi.ProbeVolumePlugins()...)
// 	return allPlugins
// }
