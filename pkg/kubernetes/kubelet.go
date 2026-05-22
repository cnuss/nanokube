package kubernetes

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	v1 "github.com/cnuss/nanokube/pkg/v1"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/component-base/featuregate"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/cmd/kubelet/app"
	"k8s.io/kubernetes/cmd/kubelet/app/options"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet"
	"k8s.io/kubernetes/pkg/kubelet/server"
)

type KubeletCommand struct {
	*cobra.Command
	ctx     v1.Nanokube
	flagSet *pflag.FlagSet
}

func NewKubeletCommand(ctx v1.Nanokube) *KubeletCommand {
	k := &KubeletCommand{
		ctx:     ctx,
		flagSet: pflag.NewFlagSet(server.ComponentKubelet, pflag.ContinueOnError),
	}
	k.Command = app.NewKubeletCommand(ctx, k.flagSet, k.Run())
	k.Command.RunE = k.RunE()
	return k
}

func (k *KubeletCommand) Run() func(context.Context, *options.KubeletServer, *kubelet.Dependencies, featuregate.FeatureGate) error {
	return func(_ context.Context, ks *options.KubeletServer, d *kubelet.Dependencies, fg featuregate.FeatureGate) error {
		ks.Address = k.ctx.Tunnel().LocalIP().String()
		ks.ClusterDomain = k.ctx.Tunnel().Domain()
		ks.ClusterDNS = []string{"1.1.1.1"}
		ks.FileCheckFrequency = metav1.Duration{Duration: 1 * time.Second}
		ks.PodLogsDir = k.ctx.Options().DataDirAt(v1.DataDirLogs)
		ks.Port = k.ctx.Tunnel().LocalPort()
		ks.ReadOnlyPort = 0
		ks.RegisterNode = true
		ks.StaticPodPath = k.ctx.Options().DataDirAt(v1.DataDirStaticPods)
		d.RemoteImageService = k.ctx.DefaultBackend().Driver()
		d.RemoteRuntimeService = k.ctx.DefaultBackend().Driver()
		d.CAdvisorInterface = k.ctx.DefaultBackend()
		d.ContainerManager = k.ctx.DefaultBackend().Manager()
		d.VolumePlugins = k.ctx.Host().VolumePlugins()
		d.OSInterface = k.ctx.Host()
		d.Mounter = k.ctx.Host()
		d.Subpather = k.ctx.Host()
		d.HostUtil = k.ctx.Host()
		d.Recorder = k.ctx
		return app.Run(k.ctx, ks, d, fg)
	}
}

func (k *KubeletCommand) RunE() func(cmd *cobra.Command, args []string) error {
	logger := klog.FromContext(k.ctx)
	settings := make(map[string]string)

	k.flagSet.Set("cloud-provider", "external")
	k.flagSet.Set("hostname-override", k.ctx.KubeletHostname())
	k.flagSet.Set("node-labels", "") // TODO(incomplete): add labels
	k.flagSet.Set("node-ip", k.ctx.Tunnel().LocalIP().String())
	k.flagSet.Set("root-dir", k.ctx.Options().DataDirAt(v1.DataDirKubelet))
	k.flagSet.Set("cert-dir", k.ctx.Options().DataDirAt(v1.DataDirCerts))
	k.flagSet.Set("tls-cert-file", k.ctx.CertFilePath())
	k.flagSet.Set("tls-private-key-file", k.ctx.KeyFilePath())
	k.flagSet.Set("feature-gates", func() string {
		features := []string{
			fmt.Sprintf("%s=true", features.KubeletInUserNamespace),
			// TODO(incomplete): more feature gates
		}
		return strings.Join(features, ",")
	}())

	k.flagSet.VisitAll(func(f *pflag.Flag) {
		if f.Name == "help" {
			return
		}
		settings[f.Name] = f.Value.String()
		k.flagSet.MarkHidden(f.Name)
	})

	keys := slices.Sorted(maps.Keys(settings))

	inner := k.Command.RunE
	return func(cmd *cobra.Command, args []string) (rerr error) {
		for _, k := range keys {
			logger.V(8).Info("kubelet flag", "name", k, "value", settings[k])
		}

		if err := inner(cmd, args); err != nil {
			return err
		}

		return nil
	}
}
