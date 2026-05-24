package kubernetes

// import (
// 	"context"

// 	v1 "github.com/cnuss/nanokube/pkg/v1"
// 	"k8s.io/kubernetes/cmd/kube-controller-manager/app"
// 	"k8s.io/kubernetes/cmd/kube-controller-manager/app/config"
// )

// type ControllerCommand struct {
// 	*Command
// 	nano v1.Nanokube
// }

// func NewControllerCommand(nano v1.Nanokube, apiserver *ApiServerCommand) *ControllerCommand {
// 	c := &ControllerCommand{nano: nano}
// 	c.Command = newCommand(nano, app.NewControllerManagerCommand(nano, c.Run())).
// 		WithNeed(apiserver.Command)
// 	return c
// }

// func (c *ControllerCommand) Run() func(context.Context, *config.CompletedConfig) error {
// 	return func(ctx context.Context, cc *config.CompletedConfig) error {
// 		return app.Run(ctx, cc)
// 	}
// }
