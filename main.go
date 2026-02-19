package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
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
	runtime internal.Runtime
)

var rootCmd = &cobra.Command{
	Use:   "nanokube [flags]",
	Short: "A minimal Kubernetes distribution",
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if verbose {
			zerolog.SetGlobalLevel(zerolog.DebugLevel)
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
	Run: func(cmd *cobra.Command, args []string) {
		log.Debug().Msg("debug logging enabled")
		log.Info().Str("runtime", runtime.String()).Msg("starting nanokube")

		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		<-ctx.Done()
		log.Info().Msg("shutting down")
	},
}

func init() {
	rootCmd.Flags().BoolVar(&docker, "docker", true, "use Docker as container runtime")
	rootCmd.Flags().BoolVar(&podman, "podman", false, "use Podman as container runtime")
	rootCmd.Flags().BoolVar(&crio, "cri-o", false, "use CRI-O as container runtime")
	rootCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "enable debug logging")
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
