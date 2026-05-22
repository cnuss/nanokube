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
	nano v1.Nanokube
}

func NewKubeletCommand(nano v1.Nanokube, apiserver *ApiServerCommand) *KubeletCommand {
	c := &KubeletCommand{nano: nano}
	flagSet := pflag.NewFlagSet(server.ComponentKubelet, pflag.ContinueOnError)
	c.Command = newCommand(nano, app.NewKubeletCommand(nano, flagSet, c.Run())).
		WithFlagSet(flagSet).
		WithNeed(apiserver.Command)
	return c
}

func (c *KubeletCommand) Run() func(context.Context, *options.KubeletServer, *kubelet.Dependencies, featuregate.FeatureGate) error {
	return func(ctx context.Context, ks *options.KubeletServer, d *kubelet.Dependencies, fg featuregate.FeatureGate) error {
		ks.Address = c.nano.Tunnel().LocalIP().String()
		ks.ClusterDomain = c.nano.Tunnel().Domain()
		ks.ClusterDNS = []string{"1.1.1.1"}
		ks.FileCheckFrequency = metav1.Duration{Duration: 1 * time.Second}
		ks.PodLogsDir = c.nano.Options().DataDirAt(v1.DataDirLogs)
		ks.Port = c.nano.Tunnel().LocalPort()
		ks.ReadOnlyPort = 0
		ks.RegisterNode = true
		ks.StaticPodPath = c.nano.Options().DataDirAt(v1.DataDirStaticPods)
		d.RemoteImageService = c.nano.DefaultBackend().Driver()
		d.RemoteRuntimeService = c.nano.DefaultBackend().Driver()
		d.ContainerManager = c.nano.DefaultBackend().Manager()
		d.CAdvisorInterface = c.nano.DefaultBackend()
		d.VolumePlugins = c.nano.Host().VolumePlugins()
		d.OSInterface = c.nano.Host()
		d.Mounter = c.nano.Host()
		d.Subpather = c.nano.Host()
		d.HostUtil = c.nano.Host()
		d.Recorder = c.nano
		return app.Run(ctx, ks, d, fg)
	}
}
