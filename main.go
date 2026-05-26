package main

import (
	"context"
	"errors"
	"os"

	"github.com/cnuss/nanokube/pkg"
	"github.com/cnuss/nanokube/pkg/driver/awslambda"
	"github.com/cnuss/nanokube/pkg/driver/docker"
	"github.com/cnuss/nanokube/pkg/driver/podman"
	v1 "github.com/cnuss/nanokube/pkg/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/component-base/cli"
	"k8s.io/klog/v2"
	apiserver "k8s.io/kubernetes/cmd/kube-apiserver/app"
	controllermanager "k8s.io/kubernetes/cmd/kube-controller-manager/app"
	scheduler "k8s.io/kubernetes/cmd/kube-scheduler/app"
	kubelet "k8s.io/kubernetes/cmd/kubelet/app"
)

var (
	ctx    context.Context
	cancel context.CancelCauseFunc
)

func init() {
	ctx, cancel = context.WithCancelCause(genericapiserver.SetupSignalContext())
	wait.NeverStop = ctx.Done()
	setupLogging(cancel)

	if !v1.HTTP2 {
		os.Setenv("DISABLE_HTTP2", "true")
	}

	v1.Backends = append(v1.Backends, docker.Detect(context.Background()))
	v1.Backends = append(v1.Backends, podman.Detect(context.Background()))
	v1.Backends = append(v1.Backends, awslambda.Detect(context.Background()))
}

func main() {
	command := pkg.NewNanokubeCommand(ctx).
		WithRunCommand(apiserver.NewAPIServerCommand(context.Background())).
		WithRunCommand(controllermanager.NewControllerManagerCommand(context.Background())).
		WithRunCommand(scheduler.NewSchedulerCommand(context.Background())).
		WithRunCommand(kubelet.NewKubeletCommand(context.Background()))
	code := cli.Run(command.Command)
	if errs := command.Nano().Errors(); errs != nil {
		klog.ErrorS(errors.Join(errs...), "encountered errors during execution")
	}
	os.Exit(code)
}
