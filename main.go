package main

import (
	"context"
	"fmt"
	"os"

	"github.com/cnuss/nanokube/pkg"
	"github.com/cnuss/nanokube/pkg/driver/awslambda"
	"github.com/cnuss/nanokube/pkg/driver/docker"
	"github.com/cnuss/nanokube/pkg/driver/podman"
	"github.com/cnuss/nanokube/pkg/kubernetes"
	v1 "github.com/cnuss/nanokube/pkg/v1"
	"golang.org/x/sync/errgroup"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/component-base/cli"
)

func init() {
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
	nano, cancel := pkg.NewNanokube(genericapiserver.SetupSignalContext())
	g, _ := errgroup.WithContext(nano)
	defer cancel(nil)

	storage := kubernetes.NewStorageCommand(nano)
	apiserver := kubernetes.NewApiServerCommand(nano, storage)
	controller := kubernetes.NewControllerCommand(nano, apiserver)
	scheduler := kubernetes.NewSchedulerCommand(nano, apiserver)
	kubelet := kubernetes.NewKubeletCommand(nano, apiserver)

	g.Go(func() error {
		return run(storage.Context(), storage.
			Command,
		)
	})
	g.Go(func() error {
		return run(apiserver.Context(), apiserver.
			Command.
			WithFlag("foo", "bar"),
		)
	})
	g.Go(func() error {
		return run(controller.Context(), controller.
			Command.
			WithFlag("foo", "bar"),
		)
	})
	g.Go(func() error {
		return run(scheduler.Context(), scheduler.
			Command.
			WithFlag("foo", "bar"),
		)
	})
	g.Go(func() error {
		return run(kubelet.Context(), kubelet.
			Command.
			WithFlag("cloud-provider", "external").
			WithFlag("hostname-override", nano.KubeletHostname()). // TODO(incomplete): set NodeName
			WithFlag("node-labels", "").                           // TODO(incomplete): moar labels
			WithFlag("node-ip", nano.Tunnel().LocalIP().String()).
			WithFlag("root-dir", nano.Options().DataDirAt(v1.DataDirKubelet)).
			WithFlag("cert-dir", nano.Options().DataDirAt(v1.DataDirCerts)).
			WithFlag("tls-cert-file", nano.CertFilePath()).
			WithFlag("tls-private-key-file", nano.KeyFilePath()),
		)
	})

	if err := g.Wait(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
