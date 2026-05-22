package kubernetes

import (
	"context"
	"maps"
	"slices"

	v1 "github.com/cnuss/nanokube/pkg/v1"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/cmd/kube-apiserver/app"
	"k8s.io/kubernetes/cmd/kube-apiserver/app/options"
)

type ApiServerCommand struct {
	*cobra.Command
	ctx v1.Nanokube
}

func NewApiServerCommand(ctx v1.Nanokube) *ApiServerCommand {
	a := &ApiServerCommand{
		ctx: ctx,
	}
	a.Command = app.NewAPIServerCommand(ctx, a.Run())
	a.Command.RunE = a.RunE()
	return a
}

func (a *ApiServerCommand) Run() func(context.Context, options.CompletedOptions) error {
	return func(_ context.Context, o options.CompletedOptions) error {
		config, err := app.NewConfig(o)
		if err != nil {
			return err
		}
		completed, err := config.Complete()
		if err != nil {
			return err
		}
		server, err := app.CreateServerChain(completed)
		if err != nil {
			return err
		}
		prepared, err := server.PrepareRun()
		if err != nil {
			return err
		}
		return prepared.Run(a.ctx)
	}
}

func (a *ApiServerCommand) RunE() func(cmd *cobra.Command, args []string) error {
	logger := klog.FromContext(a.ctx)
	settings := make(map[string]string)
	flagSet := a.Command.Flags()

	flagSet.VisitAll(func(f *pflag.Flag) {
		if f.Name == "help" {
			return
		}
		settings[f.Name] = f.Value.String()
		flagSet.MarkHidden(f.Name)
	})
	keys := slices.Sorted(maps.Keys(settings))

	inner := a.Command.RunE
	return func(cmd *cobra.Command, args []string) (rerr error) {
		for _, k := range keys {
			logger.Info("apiserver flag", "name", k, "value", settings[k])
		}

		if err := inner(cmd, args); err != nil {
			return err
		}

		return nil
	}
}
