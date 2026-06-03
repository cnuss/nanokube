package main

import (
	"context"
	"errors"
	"os"
	"syscall"

	daemonize "github.com/cnuss/daemonize"
	"github.com/cnuss/nanokube/pkg"
	"github.com/cnuss/nanokube/pkg/driver/awslambda"
	"github.com/cnuss/nanokube/pkg/driver/docker"
	"github.com/cnuss/nanokube/pkg/driver/podman"
	"github.com/cnuss/nanokube/pkg/klogz"
	v1 "github.com/cnuss/nanokube/pkg/v1"
	"k8s.io/component-base/cli"
	"k8s.io/klog/v2"
	apiserver "k8s.io/kubernetes/cmd/kube-apiserver/app"
	controllermanager "k8s.io/kubernetes/cmd/kube-controller-manager/app"
	scheduler "k8s.io/kubernetes/cmd/kube-scheduler/app"
	kubelet "k8s.io/kubernetes/cmd/kubelet/app"
)

func init() {
	// Register the zerolog-backed "nanokube" log format before any component
	// registers its logging flags (which freezes the format registry).
	if err := klogz.Register(); err != nil {
		klog.ErrorS(err, "unable to register nanokube log format")
	}

	if !v1.HTTP2 {
		os.Setenv("DISABLE_HTTP2", "true")
	}

	v1.Backends = append(v1.Backends, docker.Detect(context.Background()))
	v1.Backends = append(v1.Backends, podman.Detect(context.Background()))
	v1.Backends = append(v1.Backends, awslambda.Detect(context.Background()))
}

func main() {
	command := pkg.NewNanokubeCommand(context.Background()).
		With(apiserver.NewAPIServerCommand(context.Background())).
		With(controllermanager.NewControllerManagerCommand(context.Background())).
		With(scheduler.NewSchedulerCommand(context.Background())).
		With(kubelet.NewKubeletCommand(context.Background()))

	code := cli.Run(daemonize.FromCobra(command.Command).
		WithShutdownSignal(syscall.SIGINT, syscall.SIGTERM).
		DetachOn(command.StartHooksDone()))
	if errs := command.Nano().Errors(); errs != nil {
		klog.ErrorS(errors.Join(errs...), "encountered errors during execution")
	}
	os.Exit(code)
}
