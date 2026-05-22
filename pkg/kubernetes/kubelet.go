package kubernetes

import (
	"context"
	"fmt"
	"maps"
	"os"
	"runtime/debug"
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

func NewKubeletCommand(ctx v1.Nanokube) *cobra.Command {
	flagSet := pflag.NewFlagSet(server.ComponentKubelet, pflag.ContinueOnError)
	command := app.NewKubeletCommand(ctx, flagSet, Run(ctx))
	command.RunE = RunE(ctx, command, flagSet)
	return command
}

func Run(ctx v1.Nanokube) func(context.Context, *options.KubeletServer, *kubelet.Dependencies, featuregate.FeatureGate) error {
	return func(_ context.Context, ks *options.KubeletServer, d *kubelet.Dependencies, fg featuregate.FeatureGate) error {
		ks.Address = ctx.Tunnel().LocalIP().String()
		ks.ClusterDomain = ctx.Tunnel().Domain()
		ks.ClusterDNS = []string{"1.1.1.1"}
		ks.FileCheckFrequency = metav1.Duration{Duration: 1 * time.Second}
		ks.PodLogsDir = ctx.Options().DataDirAt(v1.DataDirLogs)
		ks.Port = ctx.Tunnel().LocalPort()
		ks.ReadOnlyPort = 0
		ks.RegisterNode = true
		ks.StaticPodPath = ctx.Options().DataDirAt(v1.DataDirStaticPods)
		d.RemoteImageService = ctx.DefaultBackend().Driver()
		d.RemoteRuntimeService = ctx.DefaultBackend().Driver()
		d.CAdvisorInterface = ctx.DefaultBackend()
		d.ContainerManager = ctx.DefaultBackend().Manager()
		d.VolumePlugins = ctx.Host().VolumePlugins()
		d.OSInterface = ctx.Host()
		d.Mounter = ctx.Host()
		d.Subpather = ctx.Host()
		d.HostUtil = ctx.Host()
		d.Recorder = ctx
		return app.Run(ctx, ks, d, fg)
	}
}

// func Run(ctx context.Context, ks *options.KubeletServer, d *kubelet.Dependencies, fg featuregate.FeatureGate) error {
// 	d.RemoteImageService = nano.DefaultBackend().Driver()
// 	d.RemoteRuntimeService = nano.DefaultBackend().Driver()
// 	return app.Run(ctx, ks, d, fg)
// }

func RunE(ctx v1.Nanokube, command *cobra.Command, flagSet *pflag.FlagSet) func(cmd *cobra.Command, args []string) error {
	logger := klog.FromContext(ctx)
	settings := make(map[string]string)

	flagSet.Set("cloud-provider", "external")
	flagSet.Set("hostname-override", ctx.KubeletHostname())
	flagSet.Set("node-labels", "") // TODO(incomplete): add labels
	flagSet.Set("node-ip", ctx.Tunnel().LocalIP().String())
	flagSet.Set("root-dir", ctx.Options().DataDirAt(v1.DataDirKubelet))
	flagSet.Set("cert-dir", ctx.Options().DataDirAt(v1.DataDirCerts))
	flagSet.Set("tls-cert-file", ctx.CertFilePath())
	flagSet.Set("tls-private-key-file", ctx.KeyFilePath())
	flagSet.Set("feature-gates", func() string {
		features := []string{
			fmt.Sprintf("%s=true", features.KubeletInUserNamespace),
			// TODO(incomplete): more feature gates
		}
		return strings.Join(features, ",")
	}())

	flagSet.VisitAll(func(f *pflag.Flag) {
		if f.Name == "help" {
			return
		}
		settings[f.Name] = f.Value.String()
		flagSet.MarkHidden(f.Name)
	})

	keys := slices.Sorted(maps.Keys(settings))

	inner := command.RunE
	return func(cmd *cobra.Command, args []string) (rerr error) {
		defer func() {
			if r := recover(); r != nil {
				fmt.Fprintf(os.Stderr, "!!!!! Panic: %v\n%s\n", r, debug.Stack())
				if e, ok := r.(error); ok {
					rerr = e
				} else {
					rerr = fmt.Errorf("panic: %v", r)
				}
			}
		}()

		for _, k := range keys {
			logger.Info("kubelet flag", "name", k, "value", settings[k])
		}

		if err := inner(cmd, args); err != nil {
			return err
		}

		return nil
	}
}
