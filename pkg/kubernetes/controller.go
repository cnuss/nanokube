package kubernetes

import (
	"context"

	v1 "github.com/cnuss/nanokube/pkg/v1"
	"k8s.io/kubernetes/cmd/kube-controller-manager/app"
	"k8s.io/kubernetes/cmd/kube-controller-manager/app/config"
)

type ControllerCommand struct {
	*Command
	ctx       v1.Nanokube
	apiserver *ApiServerCommand
}

func NewControllerCommand(ctx v1.Nanokube, apiserver *ApiServerCommand) *ControllerCommand {
	c := &ControllerCommand{
		ctx:       ctx,
		apiserver: apiserver,
	}
	c.Command = newCommand(ctx, app.NewControllerManagerCommand(ctx, c.Run())).
		WithNeed(apiserver.Command)
	return c
}

func (c *ControllerCommand) Run() func(context.Context, *config.CompletedConfig) error {
	return func(_ context.Context, cc *config.CompletedConfig) error {
		return app.Run(c.ctx, cc)
	}
}
