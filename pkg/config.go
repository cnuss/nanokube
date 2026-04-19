package pkg

import (
	"context"
	"errors"
	"os"
	"runtime"
	"sync"

	"github.com/cnuss/nanokube/pkg/nanokube"
	v1 "github.com/cnuss/nanokube/pkg/v1"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"k8s.io/component-base/version"
	"k8s.io/kubernetes/pkg/kubemark"
)

type ConfigImpl struct {
	ctx     context.Context
	cancel  context.CancelCauseFunc
	cmd     *cobra.Command
	options v1.Options

	crid     v1.Crid
	cridOnce sync.Once

	kube     v1.Kube
	kubeOnce sync.Once

	dirs  sync.Map
	files sync.Map
}

var _ v1.Config = &ConfigImpl{}

func NewConfig(ctx context.Context) (v1.Config, context.CancelCauseFunc, error) {
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
			os.RemoveAll(string(options.DataDir()))
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
	var fatal *FatalError
	if errors.As(reason, &fatal) {
		runtime.Goexit()
	}
}

func (c *ConfigImpl) Options() v1.Options {
	return c.options
}

func (c *ConfigImpl) Version() string {
	return c.cmd.Version
}

func (c *ConfigImpl) Crid() v1.Crid {
	c.cridOnce.Do(func() {
		c.crid = newCrid(c)
	})
	return c.crid
}

func (c *ConfigImpl) NewTunnel() v1.Tunnel {
	return NewTunnel(c)
}

func (c *ConfigImpl) Kube() v1.Kube {
	c.kubeOnce.Do(func() {
		c.kube = newKube(c)
	})
	return c.kube
}

func (c *ConfigImpl) WithKubelet(kubelet *kubemark.HollowKubelet) v1.Config {
	c.Kube().WithKubelet(kubelet)
	return c
}

func (c *ConfigImpl) WithApiServer(apiserver v1.ApiServer) v1.Config {
	c.Kube().WithApiServer(apiserver)
	return c
}

func (c *ConfigImpl) WithStorageFactory(storagefactory v1.StorageFactory) v1.Config {
	c.Kube().WithStorageFactory(storagefactory)
	return c
}
