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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeletapp "k8s.io/kubernetes/cmd/kubelet/app"
	kubeletoptions "k8s.io/kubernetes/cmd/kubelet/app/options"
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

var _ v1.Kubelet = &KubeletImpl{}

func NewKubelet(cmd *cobra.Command) v1.Kubelet {
	ctx, cancel := context.WithCancelCause(context.Background())

	options := nanokube.NewOptions(cmd)

	kubelet := &KubeletImpl{
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
		kubelet.Cancel(nil)
	}()

	go func() {
		<-ctx.Done()
		signal.Stop(sigCh)
		cause := context.Cause(ctx)
		var err v1.Error
		if errors.As(cause, &err) {
			fmt.Fprintf(os.Stderr, "nanokube exiting: %v\n", err)
			os.Exit(err.ExitStatus())
		}
		os.Exit(0)
	}()

	ran := false
	cmd.PreRunE = func(cmd *cobra.Command, args []string) error {
		nanokube.SetupLogging(kubelet.Options().Verbosity())
		nanokube.Log.Info("starting", "version", kubelet.Version())
		return nil
	}
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		ran = true
		// TODO(future): subprocess management
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
			args = append(options.Args(), args...)
			nanokube.Log.Info("start", "bin", bin, "args", args)
			kubelet.Cancel(nanokube.NewError(fmt.Errorf("not implemented")))
			return nil
		},
	}
	options.RunFlags(startCmd) // TODO(incomplete): this feels wierd

	cmd.AddCommand(startCmd)
	cmd.AddCommand(&cobra.Command{
		Use:   "stop",
		Short: "Stop nanokube background processes",
		RunE: func(cmd *cobra.Command, args []string) error {
			ran = true
			kubelet.Cancel(nanokube.NewError(fmt.Errorf("not implemented: stop")))
			return nil
		},
	})

	if err := cmd.ExecuteContext(ctx); err != nil {
		kubelet.Cancel(nanokube.NewError(err).WithCode(1))
	} else if !ran {
		kubelet.Cancel(nil)
	}

	return kubelet
}

func (k *KubeletImpl) Context() context.Context {
	return k.ctx
}

func (k *KubeletImpl) Cancel(reason v1.Error) {
	k.canceledOnce.Do(func() { close(k.canceled) })
	k.runCancelHooks()
	k.cancel(reason)
	var fatal v1.Error
	if errors.As(reason, &fatal) {
		runtime.Goexit()
	}
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
			ch = k.Kube().ApiServer().Ready()
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

func (k *KubeletImpl) Done() <-chan struct{} {
	return k.Context().Done()
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

func (k *KubeletImpl) Tunnel(service v1.ServiceName) v1.Tunnel {
	tunnel, _ := k.tunnels.LoadOrStore(service, func() v1.Tunnel {
		return NewTunnel(k, service)
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

func (k *KubeletImpl) WithApiServer(apiserver v1.ApiServer) v1.Kubelet {
	k.Kube().WithApiServer(apiserver)
	return k
}

func (k *KubeletImpl) ApiServer() v1.ApiServer {
	return k.Kube().ApiServer()
}

func (k *KubeletImpl) WithStorage(storage v1.Storage) v1.Kubelet {
	k.Kube().WithStorage(storage)
	return k
}

func (k *KubeletImpl) Storage() v1.Storage {
	return k.Kube().Storage()
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

func (k *KubeletImpl) Run() v1.Kubelet {
	k.runOnce.Do(func() {
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
		if err := kubeletapp.RunKubelet(k.ctx, &kubeletoptions.KubeletServer{
			KubeletFlags:         *k.Kube().KubeletFlags(),
			KubeletConfiguration: *k.Kube().KubeletConfiguration(),
		}, k.Kube().KubeletDependencies()); err != nil {
			k.Cancel(nanokube.NewError(err).WithCode(1))
		}
	})
	return k
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
