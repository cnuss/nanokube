package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/cnuss/nanokube/pkg/config"
	"github.com/cnuss/nanokube/pkg/crid"
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
		sigCtx := cmd.Context()
		log.Info().Str("data", options.DataDir).Str("name", options.Name).Msg("starting up")
		log.Debug().Msg("debug logging enabled")

		cfg := config.NewConfig(options)
		if err := cfg.SetCRID(crid.NewCRID(sigCtx, cfg.Name, cfg.DataDir)); err != nil {
			return fmt.Errorf("failed to initialize CRID: %w", err)
		}

		cfg.Components = append(cfg.Components, cfg.CRID)
		cfg.Components = append(cfg.Components, etcd.NewEtcd(cfg))
		cfg.Components = append(cfg.Components, kubernetes.NewAPIServer(cfg))
		cfg.Components = append(cfg.Components, kubernetes.NewControllerManager(cfg))
		cfg.Components = append(cfg.Components, kubernetes.NewScheduler(cfg))

		if options.Kubelet {
			cfg.Components = append(cfg.Components, kubernetes.NewKubelet(cfg))
		}

		// Each component gets its own context so we can cancel them
		// in reverse order during shutdown, keeping dependencies alive.
		cancels := make([]context.CancelFunc, len(cfg.Components))
		started := 0
		for i, c := range cfg.Components {
			compCtx, cancel := context.WithCancel(context.Background())
			cancels[i] = cancel

			// Allow ctrl+c to abort startup
			done := make(chan error, 1)
			go func() {
				s, err := c.Start(compCtx)
				if err != nil {
					done <- err
					return
				}
				<-s
				done <- nil
			}()

			select {
			case err := <-done:
				if err != nil {
					cancel()
					return err
				}
				started = i + 1
			case <-sigCtx.Done():
				cancel()
				log.Info().Msg("startup interrupted")
				goto shutdown
			}
		}

		<-sigCtx.Done()
	shutdown:
		log.Info().Msg("shutting down")

		names := make([]string, len(cfg.Components))
		for i, c := range cfg.Components {
			names[i] = fmt.Sprintf("%T", c)
		}
		for i := started - 1; i >= 0; i-- {
			log.Info().Str("component", names[i]).Msg("stopping")
			cancels[i]()
			<-cfg.Components[i].Stop()
			log.Info().Str("component", names[i]).Msg("stopped")
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
