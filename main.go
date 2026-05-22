package main

import (
	"os"

	"github.com/cnuss/nanokube/pkg"
	"github.com/cnuss/nanokube/pkg/driver/awslambda"
	"github.com/cnuss/nanokube/pkg/driver/docker"
	"github.com/cnuss/nanokube/pkg/driver/podman"
	"github.com/cnuss/nanokube/pkg/kubernetes"
	v1 "github.com/cnuss/nanokube/pkg/v1"
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
	nano := pkg.NewNanokube()
	cmd := kubernetes.NewKubeletCommand(nano)
	code := cli.Run(cmd)
	os.Exit(code)
}
