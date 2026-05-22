package kubernetes

import (
	"context"
	"time"

	v1 "github.com/cnuss/nanokube/pkg/v1"
	"github.com/spf13/pflag"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/component-base/featuregate"
	"k8s.io/kubernetes/cmd/kubelet/app"
	"k8s.io/kubernetes/cmd/kubelet/app/options"
	"k8s.io/kubernetes/pkg/kubelet"
	"k8s.io/kubernetes/pkg/kubelet/server"
)

type KubeletCommand struct {
	*Command
	ctx       v1.Nanokube
	apiserver *ApiServerCommand
}

func NewKubeletCommand(ctx v1.Nanokube, apiserver *ApiServerCommand) *KubeletCommand {
	c := &KubeletCommand{
		ctx:       ctx,
		apiserver: apiserver,
	}
	flagSet := pflag.NewFlagSet(server.ComponentKubelet, pflag.ContinueOnError)
	c.Command = newCommand(ctx, app.NewKubeletCommand(ctx, flagSet, c.Run())).
		WithNeed(apiserver.Command).
		WithFlagSet(flagSet)
	return c
}

func (c *KubeletCommand) Run() func(context.Context, *options.KubeletServer, *kubelet.Dependencies, featuregate.FeatureGate) error {
	return func(_ context.Context, ks *options.KubeletServer, d *kubelet.Dependencies, fg featuregate.FeatureGate) error {
		ks.Address = c.ctx.Tunnel().LocalIP().String()
		ks.ClusterDomain = c.ctx.Tunnel().Domain()
		ks.ClusterDNS = []string{"1.1.1.1"}
		ks.FileCheckFrequency = metav1.Duration{Duration: 1 * time.Second}
		ks.PodLogsDir = c.ctx.Options().DataDirAt(v1.DataDirLogs)
		ks.Port = c.ctx.Tunnel().LocalPort()
		ks.ReadOnlyPort = 0
		ks.RegisterNode = true
		ks.StaticPodPath = c.ctx.Options().DataDirAt(v1.DataDirStaticPods)
		d.RemoteImageService = c.ctx.DefaultBackend().Driver()
		d.RemoteRuntimeService = c.ctx.DefaultBackend().Driver()
		d.ContainerManager = c.ctx.DefaultBackend().Manager()
		d.CAdvisorInterface = c.ctx.DefaultBackend()
		d.VolumePlugins = c.ctx.Host().VolumePlugins()
		d.OSInterface = c.ctx.Host()
		d.Mounter = c.ctx.Host()
		d.Subpather = c.ctx.Host()
		d.HostUtil = c.ctx.Host()
		d.Recorder = c.ctx
		return app.Run(c.ctx, ks, d, fg)
	}
}
