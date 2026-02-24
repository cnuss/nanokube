package main

import (
	"os"
	"path/filepath"

	"github.com/cnuss/nanokube/pkg/config"
	"github.com/cnuss/nanokube/pkg/cri"
	"github.com/cnuss/nanokube/pkg/etcd"
	"github.com/cnuss/nanokube/pkg/kubernetes"
	"github.com/rs/zerolog"
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
		log.Info().Str("data", options.DataDir).Str("name", options.Name).Msg("starting up")
		log.Debug().Msg("debug logging enabled")

		cfg, ctx, stop := options.Config()
		defer stop()

		cfg.SetCRI(cri.NewCRI(cfg.DataDir))
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
		return nil
	},
}

func init() {
	rootCmd.Flags().StringVar(&options.Name, "name", "nanokube", "cluster name")
	rootCmd.Flags().BoolVarP(&options.Verbose, "verbose", "v", false, "enable debug logging")
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
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	if err := rootCmd.Execute(); err != nil {
		if err.Error() != "context canceled" {
			log.Fatal().Err(err).Msg("fatal error")
		}
	}
}
