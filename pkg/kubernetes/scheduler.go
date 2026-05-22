package kubernetes

import (
	"context"

	v1 "github.com/cnuss/nanokube/pkg/v1"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"
	"k8s.io/kubernetes/cmd/kube-scheduler/app/options"
)

type SchedulerCommand struct {
	*Command
	ctx       v1.Nanokube
	apiserver *ApiServerCommand
}

func NewSchedulerCommand(ctx v1.Nanokube, apiserver *ApiServerCommand) *SchedulerCommand {
	c := &SchedulerCommand{
		ctx:       ctx,
		apiserver: apiserver,
	}
	c.Command = newCommand(ctx, app.NewSchedulerCommand(ctx, c.Run())).
		WithNeed(apiserver.Command)
	return c
}

func (c *SchedulerCommand) Run() func(context.Context, *options.Options, ...app.Option) error {
	return func(_ context.Context, opts *options.Options, registryOptions ...app.Option) error {
		cc, sched, err := app.Setup(c.ctx, opts, registryOptions...)
		if err != nil {
			return err
		}

		return app.Run(c.ctx, cc, sched)
	}
}
