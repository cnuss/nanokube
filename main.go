package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/cnuss/nanokube/pkg/config"
	"github.com/cnuss/nanokube/pkg/crid"
	"github.com/cnuss/nanokube/pkg/kubernetes"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
)

var featureGates = map[string]bool{
	"APIServerIdentity":         false,
	"RuntimeClassInImageCriApi": false,
	"KubeletInUserNamespace":    true,
}

var rootCmd = &cobra.Command{
	Use:           "nanokube [flags]",
	Short:         "A minimal Kubernetes distribution",
	SilenceUsage:  true,
	SilenceErrors: true,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if options.DataDir == "" {
			options.DataDir = config.DefaultDataDir(options.Name)
		}
		return options.Validate()
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		sigCtx := cmd.Context()
		log.Info().Str("data", options.DataDir).Str("name", options.Name).Msg("starting up")
		log.Debug().Msg("debug logging enabled")

		cr := crid.NewCRID(sigCtx, options.Name, options.DataDir, options.Clean)

		components := []component.Component{
			cr,
			// etcd.NewEtcd(cr.Certs(), cr.DataDir(), options.Verbosity),
			// kubernetes.NewAPIServer(cr.Certs(), featureGates, options.Verbosity),
			// kubernetes.NewControllerManager(cr.Certs(), cr.Files().Kubeconfig, featureGates, options.Verbosity),
			// kubernetes.NewScheduler(cr.Certs(), cr.Files().Kubeconfig, featureGates, options.Verbosity),
			kubernetes.NewManifests(cr),
		}

		if options.Kubelet {
			components = append(components, kubernetes.NewKubelet(cr, featureGates))
		}

		// Each component gets its own context so we can cancel them
		// in reverse order during shutdown, keeping dependencies alive.
		cancels := make([]context.CancelFunc, len(components))
		started := 0
		for i, c := range components {
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
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "╔══════════════════════════════════════════════════╗")
		fmt.Fprintln(os.Stderr, "║          SIGNAL RECEIVED — SHUTTING DOWN         ║")
		fmt.Fprintln(os.Stderr, "║                                                  ║")
		fmt.Fprintln(os.Stderr, "║  Initiating graceful shutdown sequence...        ║")
		fmt.Fprintln(os.Stderr, "║  Components will be stopped in reverse order.    ║")
		fmt.Fprintln(os.Stderr, "╚══════════════════════════════════════════════════╝")
		fmt.Fprintln(os.Stderr, "")

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()

		names := make([]string, len(components))
		for i, c := range components {
			names[i] = fmt.Sprintf("%T", c)
		}
		for i := started - 1; i >= 0; i-- {
			log.Info().Str("component", names[i]).Msg("stopping")
			cancels[i]()
			<-components[i].Stop(shutdownCtx)
			log.Info().Str("component", names[i]).Msg("stopped")
		}
		return nil
	},
}

var options *config.Options

func init() {
	options = config.NewOptions(rootCmd)
}

func main() {
	// Prevent Kubernetes components from killing the process via klog.Fatal/FlushAndExit.
	// In an embedded binary, no component should call os.Exit — the shutdown loop handles cleanup.
	// runtime.Goexit terminates the calling goroutine without affecting others.
	klog.OsExit = func(code int) {
		log.Warn().Int("code", code).Msg("klog.OsExit intercepted")
		runtime.Goexit()
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer stop()

	// Replace wait.NeverStop so all k8s wait.Until/JitterUntil loops
	// (kubelet syncLoop, node status, lease controller, etc.) exit on shutdown.
	stopCh := make(chan struct{})
	wait.NeverStop = stopCh
	go func() {
		<-ctx.Done()
		close(stopCh)
	}()

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
