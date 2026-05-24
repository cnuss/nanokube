package kubernetes

// import (
// 	"context"

// 	v1 "github.com/cnuss/nanokube/pkg/v1"
// 	"k8s.io/kubernetes/cmd/kube-apiserver/app"
// 	"k8s.io/kubernetes/cmd/kube-apiserver/app/options"
// )

// type ApiServerCommand struct {
// 	*Command
// 	nano v1.Nanokube
// }

// func NewApiServerCommand(nano v1.Nanokube, storage *StorageCommand) *ApiServerCommand {
// 	c := &ApiServerCommand{nano: nano}
// 	c.Command = newCommand(nano, app.NewAPIServerCommand(nano, c.Run())).
// 		WithNeed(storage.Command)
// 	return c
// }

// func (c *ApiServerCommand) Run() func(context.Context, options.CompletedOptions) error {
// 	return func(ctx context.Context, o options.CompletedOptions) error {
// 		config, err := app.NewConfig(o)
// 		if err != nil {
// 			return err
// 		}
// 		completed, err := config.Complete()
// 		if err != nil {
// 			return err
// 		}
// 		server, err := app.CreateServerChain(completed)
// 		if err != nil {
// 			return err
// 		}
// 		prepared, err := server.PrepareRun()
// 		if err != nil {
// 			return err
// 		}
// 		return prepared.Run(ctx)
// 	}
// }
