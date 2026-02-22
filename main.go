package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/cnuss/nanokube/internal"
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
	Use:   "nanokube [flags]",
	Short: "A minimal Kubernetes distribution",
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

		if clean {
			if err := os.RemoveAll(dataDir); err != nil {
				return fmt.Errorf("failed to clean data dir: %w", err)
			}
		}

		if err := os.MkdirAll(dataDir, 0755); err != nil {
			return fmt.Errorf("failed to create data dir: %w", err)
		}

		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		log.Debug().Msg("debug logging enabled")
		log.Info().Str("data", dataDir).Msg("starting nanokube")

		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		// Create config and generate certs
		config := internal.NewConfig(name, dataDir, verbose)
		if err := config.Generate(); err != nil {
			return err
		}

		components := []internal.Component{
			internal.NewEtcd(config),
			internal.NewAPIServer(config),
			internal.NewControllerManager(config),
			internal.NewScheduler(config),
		}

		for _, c := range components {
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
		os.Exit(1)
	}
}
