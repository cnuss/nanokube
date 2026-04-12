package pkg

import (
	"context"
	"os"
	"sync"

	"github.com/cnuss/nanokube/pkg/nanokube"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"k8s.io/component-base/version"
	"k8s.io/kubernetes/pkg/kubemark"
)

type Config interface {
	Context() context.Context
	Cancel(reason error)

	Options() nanokube.Options
	Version() string

	Tunnel() Tunnel

	Kube() Kube
	Crid() Crid

	WithKubelet(kubelet *kubemark.HollowKubelet) Config
	WithApiServer(apiserver nanokube.ApiServer) Config
	WithStorageFactory(storagefactory nanokube.StorageFactory) Config
}

type ConfigImpl struct {
	ctx     context.Context
	cancel  context.CancelCauseFunc
	cmd     *cobra.Command
	options nanokube.Options

	crid     Crid
	cridOnce sync.Once

	kube     Kube
	kubeOnce sync.Once

	dirs  sync.Map
	files sync.Map
}

var _ Config = &ConfigImpl{}

func NewConfig(ctx context.Context) (Config, context.CancelCauseFunc, error) {
	ctx, cancel := context.WithCancelCause(ctx)
	var config *ConfigImpl = nil

	pflag.CommandLine = pflag.NewFlagSet(os.Args[0], pflag.ContinueOnError)
	cmd := &cobra.Command{
		Use:           "nanokube [flags]",
		Short:         "nanokube is a fully functional Kubernetes cluster that runs natively on your machine",
		Version:       version.Get().GitVersion,
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	options := nanokube.NewOptions(cmd)

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		if options.Clean() {
			os.RemoveAll(options.DataDir())
		}

		config = &ConfigImpl{
			ctx:     ctx,
			cancel:  cancel,
			options: options,
			cmd:     cmd,
		}
		return nil
	}

	if err := cmd.ExecuteContext(ctx); err != nil {
		return nil, cancel, err
	}

	if config == nil {
		os.Exit(0)
	}

	return config, cancel, nil
}

func (c *ConfigImpl) Context() context.Context {
	return c.ctx
}

func (c *ConfigImpl) Cancel(reason error) {
	c.cancel(reason)
}

func (c *ConfigImpl) Options() nanokube.Options {
	return c.options
}

func (c *ConfigImpl) Version() string {
	return c.cmd.Version
}

func (c *ConfigImpl) Tunnel() Tunnel {
	return NewTunnel(c)
}

func (c *ConfigImpl) Crid() Crid {
	c.cridOnce.Do(func() {
		c.crid = newCrid(c)
	})
	return c.crid
}

func (c *ConfigImpl) Kube() Kube {
	c.kubeOnce.Do(func() {
		c.kube = newKube(c)
	})
	return c.kube
}

func (c *ConfigImpl) WithKubelet(kubelet *kubemark.HollowKubelet) Config {
	c.Kube().WithKubelet(kubelet)
	return c
}

func (c *ConfigImpl) WithApiServer(apiserver nanokube.ApiServer) Config {
	c.Kube().WithApiServer(apiserver)
	return c
}

func (c *ConfigImpl) WithStorageFactory(storagefactory nanokube.StorageFactory) Config {
	c.Kube().WithStorageFactory(storagefactory)
	return c
}
