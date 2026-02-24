package main

import (
	"io"
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

		cfg.SetCRI(cri.NewCRI(cfg.DataDir, cfg.Name))
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
	rootCmd.Flags().CountVarP(&options.Verbosity, "verbose", "v", "verbosity level (-v debug, -vv trace)")
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

// teeFile creates a pipe that copies writes to both orig and logFile.
func teeFile(orig *os.File, logFile *os.File) *os.File {
	r, w, err := os.Pipe()
	if err != nil {
		return orig
	}
	go func() {
		mw := io.MultiWriter(orig, logFile)
		io.Copy(mw, r)
	}()
	return w
}

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	// Parse flags early so we can set up clean + logging before RunE
	rootCmd.ParseFlags(os.Args[1:])

	switch {
	case options.Verbosity >= 2:
		zerolog.SetGlobalLevel(zerolog.TraceLevel)
	case options.Verbosity == 1:
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	// Clean data directory if requested
	if options.Clean {
		os.RemoveAll(options.DataDir)
	}
	os.MkdirAll(options.DataDir, 0755)

	// Set up log file mirroring — all output (zerolog + klog + etcd) goes to disk
	logFile, err := os.OpenFile(filepath.Join(options.DataDir, options.Name+".log"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err == nil {
		multi := zerolog.MultiLevelWriter(
			zerolog.ConsoleWriter{Out: os.Stderr},
			logFile,
		)
		log.Logger = zerolog.New(multi).With().Timestamp().Logger()
		os.Stderr = teeFile(os.Stderr, logFile)
		os.Stdout = teeFile(os.Stdout, logFile)
		defer logFile.Close()
	}

	if err := rootCmd.Execute(); err != nil {
		if err.Error() != "context canceled" {
			log.Fatal().Err(err).Msg("fatal error")
		}
	}
}
