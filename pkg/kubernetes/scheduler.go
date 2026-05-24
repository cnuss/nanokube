package kubernetes

// import (
// 	"context"

// 	v1 "github.com/cnuss/nanokube/pkg/v1"
// 	"k8s.io/kubernetes/cmd/kube-scheduler/app"
// 	"k8s.io/kubernetes/cmd/kube-scheduler/app/options"
// )

// type SchedulerCommand struct {
// 	*Command
// 	nano v1.Nanokube
// }

// func NewSchedulerCommand(nano v1.Nanokube, apiserver *ApiServerCommand) *SchedulerCommand {
// 	c := &SchedulerCommand{nano: nano}
// 	c.Command = newCommand(nano, app.NewSchedulerCommand(nano, c.Run())).
// 		WithNeed(apiserver.Command)
// 	return c
// }

// func (c *SchedulerCommand) Run() func(context.Context, *options.Options, ...app.Option) error {
// 	return func(ctx context.Context, opts *options.Options, registryOptions ...app.Option) error {
// 		cc, sched, err := app.Setup(ctx, opts, registryOptions...)
// 		if err != nil {
// 			return err
// 		}
// 		return app.Run(ctx, cc, sched)
// 	}
// }
