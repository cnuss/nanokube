package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/cnuss/nanokube/internal"
	"github.com/cnuss/nanokube/pkg/config"
	"github.com/cnuss/nanokube/pkg/cri"
	"github.com/cnuss/nanokube/pkg/cri/docker"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var (
	name    string
	verbose bool
	clean   bool
	dataDir string
)

var rootCmd = &cobra.Command{
	Use:           "nanokube [flags]",
	Short:         "A minimal Kubernetes distribution",
	SilenceUsage:  true,
	SilenceErrors: true,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if verbose {
			zerolog.SetGlobalLevel(zerolog.DebugLevel)
		}

		if dataDir == "" {
			home, err := os.UserHomeDir()
			if err != nil {
				return fmt.Errorf("failed to get home dir: %w", err)
			}
			dataDir = filepath.Join(home, ".nanokube")
		}

		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		log.Debug().Msg("debug logging enabled")
		log.Info().Str("data", dataDir).Msg("starting nanokube")

		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		cfg := config.NewConfig(ctx, name, dataDir, verbose, clean)

		// Set up CRI server if a Docker socket is available
		var criServer *cri.Server
		if dockerSock := cri.DetectDockerSocket(); dockerSock != "" {
			backend := docker.New(dockerSock)
			streamRuntime := docker.NewStreamingRuntime(backend)
			criServer = cri.NewServer(dataDir, backend, streamRuntime)
			cfg.Runtime.SetCRIEndpoint("unix://" + criServer.SocketPath())
		}

		components := []internal.Component{
			internal.NewEtcd(cfg),
			internal.NewAPIServer(cfg),
			internal.NewControllerManager(cfg),
			internal.NewScheduler(cfg),
		}

		if criServer != nil {
			components = append(components, internal.NewCRI(cfg, criServer))
		}

		components = append(components, internal.NewKubelet(cfg))

		for _, c := range components {
			if err := c.Start(); err != nil {
				return err
			}
		}

		<-ctx.Done()
		cfg.Runtime.Stop()
		log.Info().Msg("shutting down")
		return nil
	},
}

func init() {
	rootCmd.Flags().StringVar(&name, "name", "nanokube", "cluster name")
	rootCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "enable debug logging")
	rootCmd.Flags().StringVar(&dataDir, "data", "", "data directory (default: ~/.nanokube)")
	rootCmd.Flags().BoolVar(&clean, "clean", false, "clean data directory before starting")
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
