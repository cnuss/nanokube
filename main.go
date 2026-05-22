package main

import (
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

func main() {
	nano, cancel := pkg.NewNanokube(genericapiserver.SetupSignalContext())
	defer cancel(nil)
	g := new(errgroup.Group)

	kubelet := kubernetes.NewKubeletCommand(nano)
	apiserver := kubernetes.NewApiServerCommand(nano)

	g.Go(func() error {
		defer cancel(nil)
		if code := cli.Run(kubelet.Command); code != 0 {
			err := fmt.Errorf("kubelet exited with code %d", code)
			cancel(err)
			return err
		}
		return nil
	})

	g.Go(func() error {
		defer cancel(nil)
		if code := cli.Run(apiserver.Command); code != 0 {
			err := fmt.Errorf("apiserver exited with code %d", code)
			cancel(err)
			return err
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
