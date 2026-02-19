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
	docker  bool
	podman  bool
	crio    bool
	verbose bool
	clean   bool
	dataDir string
	runtime internal.Runtime
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

		switch {
		case podman:
			return fmt.Errorf("podman is not supported")
		case crio:
			return fmt.Errorf("cri-o is not supported")
		case docker:
			runtime = internal.RuntimeDocker
		}

		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		log.Debug().Msg("debug logging enabled")
		log.Info().Str("runtime", runtime.String()).Str("data", dataDir).Msg("starting nanokube")

		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		etcd := internal.NewEtcd(dataDir, verbose)
		if err := etcd.Start(ctx); err != nil {
			return err
		}

		<-ctx.Done()
		log.Info().Msg("shutting down")

		return etcd.Stop()
	},
}

func init() {
	rootCmd.Flags().BoolVar(&docker, "docker", true, "use Docker as container runtime")
	rootCmd.Flags().BoolVar(&podman, "podman", false, "use Podman as container runtime")
	rootCmd.Flags().BoolVar(&crio, "cri-o", false, "use CRI-O as container runtime")
	rootCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "enable debug logging")
	rootCmd.Flags().StringVar(&dataDir, "data", "", "data directory (default: ~/.nanokube)")
	rootCmd.Flags().BoolVar(&clean, "clean", false, "clean data directory before starting")
	rootCmd.MarkFlagsMutuallyExclusive("docker", "podman", "cri-o")
}

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
