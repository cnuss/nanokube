package kubernetes

import (
	"context"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"

	v1 "github.com/cnuss/nanokube/pkg/v1"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
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
		d.RemoteImageService = ctx.DefaultBackend().Driver()
		d.RemoteRuntimeService = ctx.DefaultBackend().Driver()
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

	flagSet.Set("root-dir", func() string {
		dir, _ := os.UserCacheDir()
		return dir
	}())
	flagSet.Set("cert-dir", func() string {
		dir, _ := os.UserCacheDir()
		return dir
	}())
	flagSet.Set("volume-plugin-dir", func() string {
		dir, _ := os.UserCacheDir()
		return dir
	}())
	flagSet.Set("feature-gates", func() string {
		features := []string{
			fmt.Sprintf("%s=true", features.KubeletInUserNamespace),
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
	return func(cmd *cobra.Command, args []string) error {
		for _, k := range keys {
			logger.Info("kubelet flag", "name", k, "value", settings[k])
		}

		if err := inner(cmd, args); err != nil {
			return err
		}

		return nil
	}
}
