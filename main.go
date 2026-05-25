package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/cnuss/nanokube/pkg"
	"github.com/cnuss/nanokube/pkg/driver/awslambda"
	"github.com/cnuss/nanokube/pkg/driver/docker"
	"github.com/cnuss/nanokube/pkg/driver/podman"
	"github.com/cnuss/nanokube/pkg/kubernetes"
	"github.com/cnuss/nanokube/pkg/nanokube"
	v1 "github.com/cnuss/nanokube/pkg/v1"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/component-base/cli"
	logsapi "k8s.io/component-base/logs/api/v1"
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
	klog.OsExit = func(code int) {
		cancel(fmt.Errorf("exiting with code %d", code))
	}
	logsapi.ReapplyHandling = logsapi.ReapplyHandlingIgnoreUnchanged

	if !v1.HTTP2 {
		os.Setenv("DISABLE_HTTP2", "true")
	}

	v1.Backends = append(v1.Backends, docker.Detect)
	v1.Backends = append(v1.Backends, podman.Detect)
	v1.Backends = append(v1.Backends, awslambda.Detect)
}

func run(ctx context.Context, cmd *kubernetes.Command) error {
	errCh := make(chan error, 1)
	go func() {
		if code := cli.Run(cmd.Command); code != 0 {
			errCh <- fmt.Errorf("%s exited with code %d", cmd.Command.Name(), code)
			return
		}
		errCh <- nil
	}()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func main() {
	command := pkg.NewNanokubeCommand(ctx).
		WithRunCommand(apiserver.NewAPIServerCommand(context.Background())).
		WithRunCommand(controllermanager.NewControllerManagerCommand(context.Background())).
		WithRunCommand(scheduler.NewSchedulerCommand(context.Background())).
		WithRunCommand(kubelet.NewKubeletCommand(context.Background()))
	code := cli.Run(command.Command)
	if errs := command.Nano().Errors(); errs != nil {
		nanokube.Log.Error("encountered errors during execution", "errors", errors.Join(errs...))
	}
	os.Exit(code)
}

// func oldmain() {
// 	nano, cancel := pkg.NewNanokube(genericapiserver.SetupSignalContext())
// 	g, _ := errgroup.WithContext(nano)
// 	defer cancel(nil)

// 	storage := kubernetes.NewStorageCommand(nano)
// 	apiserver := kubernetes.NewApiServerCommand(nano, storage)
// 	controller := kubernetes.NewControllerCommand(nano, apiserver)
// 	_ = controller
// 	scheduler := kubernetes.NewSchedulerCommand(nano, apiserver)
// 	_ = scheduler
// 	kubelet := kubernetes.NewKubeletCommand(nano, apiserver)
// 	_ = kubelet

// 	g.Go(func() error {
// 		return run(storage.Context(), storage.
// 			WithFlag("backend", "etcd3"),
// 		)
// 	})
// 	g.Go(func() error {
// 		return run(apiserver.Context(), apiserver.
// 			WithFlag("advertise-address", nano.Tunnel().LocalIP().String()).
// 			WithFlag("allow-privileged", "true").
// 			WithFlag("api-audiences", fmt.Sprintf("https://%s", nano.Tunnel().FQDN())).
// 			WithFlag("authorization-mode", "Node,RBAC").
// 			WithFlag("endpoint-reconciler-type", "none").
// 			WithFlag("etcd-servers", fmt.Sprintf("http://127.0.0.1:%d", storage.Port())). // TODO(lazify)
// 			WithFlag("external-hostname", nano.Tunnel().FQDN()).
// 			WithFlag("kubelet-port", fmt.Sprintf("%d", nano.Tunnel().LocalPort())).
// 			WithFlag("tls-cert-file", nano.CertFilePath()).
// 			WithFlag("tls-private-key-file", nano.KeyFilePath()).
// 			WithFlag("secure-port", fmt.Sprintf("%d", nano.Tunnel().LocalPort())).
// 			WithFlag("service-account-issuer", fmt.Sprintf("https://%s", nano.Tunnel().FQDN())).
// 			WithFlag("service-account-key-file", nano.KeyFilePath()).
// 			WithFlag("service-account-signing-key-file", nano.KeyFilePath()),
// 		)
// 	})
// 	g.Go(func() error {
// 		return nil
// 		// return run(controller.Context(), controller.
// 		// 	WithFlag("foo", "bar"),
// 		// )
// 	})
// 	g.Go(func() error {
// 		return nil
// 		// return run(scheduler.Context(), scheduler.
// 		// 	WithFlag("foo", "bar"),
// 		// )
// 	})
// 	g.Go(func() error {
// 		return nil
// 		// return run(kubelet.Context(), kubelet.
// 		// 	WithFlag("cloud-provider", "external").
// 		// 	WithFlag("cert-dir", nano.Options().DataDirAt(v1.DataDirCerts)).
// 		// 	WithFlag("hostname-override", nano.KubeletHostname()). // TODO(incomplete): set NodeName
// 		// 	WithFlag("node-labels", "").                           // TODO(incomplete): moar labels
// 		// 	WithFlag("node-ip", nano.Tunnel().LocalIP().String()).
// 		// 	WithFlag("port", fmt.Sprintf("%d", nano.Tunnel().LocalPort())).
// 		// 	WithFlag("preferred-address-types", "ExternalDNS").
// 		// 	WithFlag("read-only-port", "0").
// 		// 	WithFlag("root-dir", nano.Options().DataDirAt(v1.DataDirKubelet)),
// 		// )
// 	})

// 	if err := g.Wait(); err != nil {
// 		fmt.Fprintln(os.Stderr, err)
// 		os.Exit(1)
// 	}
// }
