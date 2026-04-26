package pkg

import (
	"context"
	"errors"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"

	"github.com/cnuss/nanokube/pkg/nanokube"
	v1 "github.com/cnuss/nanokube/pkg/v1"
	"github.com/emicklei/go-restful/v3"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"k8s.io/component-base/version"
)

type ConfigImpl struct {
	ctx     context.Context
	stop    context.CancelFunc
	cancel  context.CancelCauseFunc
	cmd     *cobra.Command
	options v1.Options

	kube     v1.Kube
	kubeOnce sync.Once

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
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	ctx, cancel := context.WithCancelCause(ctx)

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
		ctx:     ctx,
		cancel:  cancel,
		stop:    stop,
		options: options,
		cmd:     cmd,
	}

	ran := false
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		ran = true
		if options.Clean() {
			os.RemoveAll(string(options.DataDir()))
		}
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
	c.cancel(reason)
	var fatal v1.Error
	if errors.As(reason, &fatal) {
		runtime.Goexit()
	}
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
	if tunnel, ok := c.tunnels.Load(service); ok {
		return tunnel.(v1.Tunnel)
	}
	tunnel := NewTunnel(c, service)
	c.tunnels.Store(service, tunnel)
	return tunnel
}

func (c *ConfigImpl) Kube() v1.Kube {
	c.kubeOnce.Do(func() {
		c.kube = newKube(c)
	})
	return c.kube
}

func (c *ConfigImpl) WithKubelet(kubelet v1.Kubelet) v1.Config {
	c.Kube().WithKubelet(kubelet)
	return c
}

func (c *ConfigImpl) Kubelet() v1.Kubelet {
	return c.Kube().Kubelet()
}

func (c *ConfigImpl) WithApiServer(apiserver v1.ApiServer) v1.Config {
	c.Kube().WithApiServer(apiserver)
	return c
}

func (c *ConfigImpl) ApiServer() v1.ApiServer {
	return c.Kube().ApiServer()
}

func (c *ConfigImpl) WithStorageFactory(storagefactory v1.StorageFactory) v1.Config {
	c.Kube().WithStorageFactory(storagefactory)
	return c
}

func (c *ConfigImpl) StorageFactory() v1.StorageFactory {
	return c.Kube().StorageFactory()
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
