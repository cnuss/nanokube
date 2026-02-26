package main

import (
	"context"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/cnuss/nanokube/pkg/config"
	"github.com/cnuss/nanokube/pkg/cri"
	"github.com/cnuss/nanokube/pkg/etcd"
	"github.com/cnuss/nanokube/pkg/kubernetes"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var options = config.NewOptions()

var rootCmd = &cobra.Command{
	Use:           "nanokube [flags]",
	Short:         "A minimal Kubernetes distribution",
	SilenceUsage:  true,
	SilenceErrors: true,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		return options.Validate()
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		log.Info().Str("data", options.DataDir).Str("name", options.Name).Msg("starting up")
		log.Debug().Msg("debug logging enabled")

		cfg := config.NewConfig(options)

		cfg.SetCRI(cri.NewCRI(cfg.DataDir, cfg.Name, options.Clean))
		cfg.Components = append(cfg.Components, cfg.CRI)
		cfg.Components = append(cfg.Components, etcd.NewEtcd(cfg))
		cfg.Components = append(cfg.Components, kubernetes.NewAPIServer(cfg))
		cfg.Components = append(cfg.Components, kubernetes.NewControllerManager(cfg))
		cfg.Components = append(cfg.Components, kubernetes.NewScheduler(cfg))

		if options.Kubelet {
			cfg.Components = append(cfg.Components, kubernetes.NewKubelet(cfg))
		}

		for _, c := range cfg.Components {
			if err := c.Start(ctx); err != nil {
				return err
			}
		}

		<-ctx.Done()
		log.Info().Msg("shutting down")

		for i := len(cfg.Components) - 1; i >= 0; i-- {
			cfg.Components[i].Stop()
		}
		return nil
	},
}

func init() {
	rootCmd.Flags().StringVar(&options.Name, "name", "nanokube", "cluster name")
	rootCmd.Flags().CountVarP(&options.Verbosity, "verbose", "v", "verbosity (-v debug, -vv trace)")
	rootCmd.Flags().BoolVar(&options.Clean, "clean", false, "clean data directory before starting")
	rootCmd.Flags().BoolVar(&options.Kubelet, "kubelet", true, "start the kubelet component")
	rootCmd.Flags().StringVar(&options.DataDir, "data", func() string {
		home, err := os.UserHomeDir()
		if err != nil {
			log.Fatal().Err(err).Msg("failed to get home dir")
		}
		return filepath.Join(home, ".nanokube")
	}(), "data directory")
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rootCmd, logger, cleanup := component.Setup(rootCmd)
	rootCmd.SetContext(ctx)
	log.Logger = logger
	defer cleanup()

	if err := rootCmd.Execute(); err != nil {
		if err.Error() != "context canceled" {
			log.Fatal().Err(err).Msg("fatal error")
		}
	}
}
