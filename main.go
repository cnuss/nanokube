package main

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/cnuss/nanokube/pkg"
	"github.com/cnuss/nanokube/pkg/awslambda"
	"github.com/cnuss/nanokube/pkg/docker"
	"github.com/cnuss/nanokube/pkg/nanokube"
	"github.com/cnuss/nanokube/pkg/podman"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/util/retry"
	cloudproviderapi "k8s.io/cloud-provider/api"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubemark"
)

// var featureGates = map[string]bool{
// 	"APIServerIdentity":         false,
// 	"RuntimeClassInImageCriApi": false,
// 	"KubeletInUserNamespace":    true,
// }

// var rootCmd = &cobra.Command{
// 	Use:           "nanokube [flags]",
// 	Short:         "A minimal Kubernetes distribution",
// 	SilenceUsage:  true,
// 	SilenceErrors: true,
// 	PreRunE: func(cmd *cobra.Command, args []string) error {
// 		if options.DataDir == "" {
// 			options.DataDir = config.DefaultDataDir(options.Name)
// 		}
// 		return options.Validate()
// 	},
// 	RunE: func(cmd *cobra.Command, args []string) error {
// 		sigCtx := cmd.Context()
// 		log.Info().Str("data", options.DataDir).Str("name", options.Name).Msg("starting up")
// 		log.Debug().Msg("debug logging enabled")

// 		crid := crid.NewCRID(sigCtx, options.Name, options.DataDir, options.Clean)
// 		etcd := etcd.NewEtcd(crid.Certs(), crid.DataDirs().Etcd)
// 		manifests := kubernetes.NewManifests(crid)

// 		components := []component.Component{etcd, crid, manifests}

// 		if options.Kubelet {
// 			components = append(components, kubernetes.NewKubelet(crid, featureGates))
// 		}

// 		// Each component gets its own context so we can cancel them
// 		// in reverse order during shutdown, keeping dependencies alive.
// 		cancels := make([]context.CancelFunc, len(components))
// 		started := 0
// 		for i, c := range components {
// 			compCtx, cancel := context.WithCancel(context.Background())
// 			cancels[i] = cancel

// 			// Allow ctrl+c to abort startup
// 			done := make(chan error, 1)
// 			go func() {
// 				s, err := c.Start(compCtx)
// 				if err != nil {
// 					done <- err
// 					return
// 				}
// 				<-s
// 				done <- nil
// 			}()

// 			select {
// 			case err := <-done:
// 				if err != nil {
// 					cancel()
// 					return err
// 				}
// 				started = i + 1
// 			case <-sigCtx.Done():
// 				cancel()
// 				log.Info().Msg("startup interrupted")
// 				goto shutdown
// 			}
// 		}

// 		<-sigCtx.Done()
// 	shutdown:
// 		fmt.Fprintln(os.Stderr, "")
// 		fmt.Fprintln(os.Stderr, "╔══════════════════════════════════════════════════╗")
// 		fmt.Fprintln(os.Stderr, "║          SIGNAL RECEIVED — SHUTTING DOWN         ║")
// 		fmt.Fprintln(os.Stderr, "║                                                  ║")
// 		fmt.Fprintln(os.Stderr, "║  Initiating graceful shutdown sequence...        ║")
// 		fmt.Fprintln(os.Stderr, "║  Components will be stopped in reverse order.    ║")
// 		fmt.Fprintln(os.Stderr, "╚══════════════════════════════════════════════════╝")
// 		fmt.Fprintln(os.Stderr, "")

// 		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
// 		defer shutdownCancel()

// 		names := make([]string, len(components))
// 		for i, c := range components {
// 			names[i] = fmt.Sprintf("%T", c)
// 		}
// 		for i := started - 1; i >= 0; i-- {
// 			log.Debug().Str("component", names[i]).Msg("stopping")
// 			cancels[i]()
// 			<-components[i].Stop(shutdownCtx)
// 			log.Debug().Str("component", names[i]).Msg("stopped")
// 		}
// 		return nil
// 	},
// }

// var options *config.Options

// func init() {
// 	options = config.NewOptions(rootCmd)
// }

func init() {
	// System Runtimes
	pkg.Runtimes["docker"] = docker.Detect
	pkg.Runtimes["podman"] = podman.Detect

	// Cloud Runtimes
	pkg.Runtimes["awslambda"] = awslambda.Detect
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer stop()

	config, cancel, err := pkg.NewConfig(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	nanokube.SetupLogging(config.Options().Verbosity())
	nanokube.Log.Info("starting nanokube", "version", config.Version())

	// Override wait.NeverStop
	stopCh := make(chan struct{})
	wait.NeverStop = stopCh
	go func() {
		<-config.Context().Done()
		close(stopCh)
	}()

	klog.OsExit = func(code int) {
		cancel(pkg.NewFatalError(fmt.Errorf("klog exited with code %d", code)).WithCode(code))
		runtime.Goexit()
	}

	defer func() {
		cancel(nil)
		if cause := context.Cause(config.Context()); cause != nil {
			var fatal *pkg.FatalError
			if errors.As(cause, &fatal) {
				fmt.Fprintln(os.Stderr, fatal)
				os.Exit(fatal.Code())
			}
		}
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil && ctx.Err() == nil {
				buf := make([]byte, 1<<16)
				buf = buf[:runtime.Stack(buf, true)]
				fmt.Fprintf(os.Stderr, "panic during init: %v\n%s\n", r, buf)
				panic(r)
			}
		}()
		config = config.
			WithStorageFactory(
				nanokube.NewStorageFactory(ctx, config.Options())).
			WithApiServer(nanokube.NewApiServer(
				config.Options(),
				config.Kube().ApiServerOptions(),
				config.Kube().StorageFactory())).
			WithKubelet(kubemark.NewHollowKubelet(
				config.Kube().KubeletFlags(),
				config.Kube().KubeletConfiguration(),
				config.Kube().Client().Clientset(),
				config.Kube().Client().WithHeartbeat(30*time.Second).Clientset(),
				config.Crid().DefaultBackend(),
				config.Crid().DefaultBackend().Driver(),
				config.Crid().DefaultBackend().Driver(),
				config.Crid().DefaultBackend().Manager(),
			))

		go run(ctx, cancel, config)
		go updateKubeconfig(config)
		go updateNode(config)

		<-config.Context().Done()
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}
}

func run(ctx context.Context, cancel context.CancelCauseFunc, config pkg.Config) {
	// DEVNOTE: everything runs implicitly when we call their accessors
	config.Kube().Kubelet().Run(ctx)
	cancel(pkg.NewFatalError(fmt.Errorf("kubelet exited unexpectedly")))
}

func updateNode(config pkg.Config) {
	ref := <-config.Kube().NodeReady()
	if ref == nil {
		return
	}

	nodes := config.Kube().Client().CoreV1().Nodes()
	tunnel := config.Kube().KubeletTunnel()

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		node, err := nodes.Get(config.Context(), ref.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		node.Spec.Taints = slices.DeleteFunc(node.Spec.Taints, func(t v1.Taint) bool {
			return t.Key == cloudproviderapi.TaintExternalCloudProvider
		})
		_, err = nodes.Update(config.Context(), node, metav1.UpdateOptions{})
		return err
	}); err != nil {
		nanokube.Log.Warn("failed to update node taints", "name", ref.Name, "error", err)
	}

	if _, err := nodes.ApplyStatus(
		config.Context(),
		corev1ac.Node(ref.Name).WithStatus(
			corev1ac.NodeStatus().WithAddresses(
				corev1ac.NodeAddress().WithType(v1.NodeHostName).WithAddress(func() string {
					a, _ := os.Hostname()
					a, _, _ = strings.Cut(a, ".")
					return a
				}()),
				corev1ac.NodeAddress().WithType(v1.NodeInternalDNS).WithAddress(func() string {
					a, _ := os.Hostname()
					return a
				}()),
				corev1ac.NodeAddress().WithType(v1.NodeExternalDNS).WithAddress(func() string {
					a := fmt.Sprintf("%s:%d", tunnel.FQDN(), 443)
					return a
				}()),
			),
		),
		metav1.ApplyOptions{FieldManager: config.Options().Name(), Force: true},
	); err != nil {
		nanokube.Log.Warn("failed to apply node status", "name", ref.Name, "error", err)
	}

	nanokube.Log.Info("node is ready", "name", ref.Name, "fqdn", tunnel.FQDN())
}

func updateKubeconfig(config pkg.Config) {
	kubeconfig, err := clientcmd.LoadFromFile(clientcmd.RecommendedHomeFile)
	if err != nil {
		kubeconfig = clientcmdapi.NewConfig()
	}

	internal := config.Kube().Client().WithTunnel(config.Kube().ApiServerTunnel(), true)
	external := config.Kube().Client().WithTunnel(config.Kube().ApiServerTunnel(), false)

	if err := internal.WriteKubeconfig(filepath.Join(string(config.Options().DataDir()), string(nanokube.KubeconfigFile))); err != nil {
		nanokube.Log.Error("failed to write internal kubeconfig", "path", string(config.Options().DataDir())+string(nanokube.KubeconfigFile), "error", err)
	}

	current := external.Kubeconfig(config.Options().Name())
	maps.Copy(kubeconfig.Clusters, current.Clusters)
	maps.Copy(kubeconfig.AuthInfos, current.AuthInfos)
	maps.Copy(kubeconfig.Contexts, current.Contexts)
	kubeconfig.CurrentContext = current.CurrentContext

	if err := clientcmd.WriteToFile(*kubeconfig, clientcmd.RecommendedHomeFile); err != nil {
		nanokube.Log.Error("failed to update kubeconfig", "path", clientcmd.RecommendedHomeFile, "error", err)
	} else {
		nanokube.Log.Info("kubeconfig updated", "path", clientcmd.RecommendedHomeFile)
	}
}
