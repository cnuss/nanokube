package pkg

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"

	v1 "github.com/cnuss/nanokube/pkg/v1"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	apiextensionsapiserver "k8s.io/apiextensions-apiserver/pkg/apiserver"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/util/webhook"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
	cloudproviderapi "k8s.io/cloud-provider/api"
	"k8s.io/component-base/featuregate"
	"k8s.io/klog/v2"
	aggregatorscheme "k8s.io/kube-aggregator/pkg/apiserver/scheme"
	apiserver "k8s.io/kubernetes/cmd/kube-apiserver/app"
	apiserveroptions "k8s.io/kubernetes/cmd/kube-apiserver/app/options"
	controllermanager "k8s.io/kubernetes/cmd/kube-controller-manager/app"
	controllermanagerconfig "k8s.io/kubernetes/cmd/kube-controller-manager/app/config"
	scheduler "k8s.io/kubernetes/cmd/kube-scheduler/app"
	schedulerappconfig "k8s.io/kubernetes/cmd/kube-scheduler/app/config"
	kubelet "k8s.io/kubernetes/cmd/kubelet/app"
	kubeletoptions "k8s.io/kubernetes/cmd/kubelet/app/options"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/controlplane"
	controlplaneapiserver "k8s.io/kubernetes/pkg/controlplane/apiserver"
	generatedopenapi "k8s.io/kubernetes/pkg/generated/openapi"
	kubeletcore "k8s.io/kubernetes/pkg/kubelet"
	kubeletconfiginternal "k8s.io/kubernetes/pkg/kubelet/apis/config"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/config"
	kubeletserver "k8s.io/kubernetes/pkg/kubelet/server"
	sched "k8s.io/kubernetes/pkg/scheduler"
)

// featureGates is applied process-wide (the kube: gate is a singleton
// shared by every in-process component) via [Command.WithRunCommand].
var featureGates = []string{
	// DEVNOTE: for cloudflared support:
	"kube:ExtendWebSocketsToKubelet=false",
	"kube:PortForwardWebsockets=false",
	"kube:TranslateStreamCloseWebsocketRequests=false",
	"kube:WatchList=false",
	"kube:WatchListClient=false",
	// DEVNOTE: general:
	"kube:KubeletInUserNamespace=true",
}

var kubeletPaths = []string{
	"/attach/",
	"/checkpoint/",
	"/containerLogs/",
	"/debug/",
	"/exec/",
	"/logs/",
	"/metrics/",
	"/pods/",
	"/portForward/",
	"/run/",
	"/runningpods/",
	"/stats/",
}

type Command struct {
	*cobra.Command
	ctx v1.Nanokube

	run        *cobra.Command
	start      *cobra.Command
	byName     map[string]*cobra.Command
	startHooks map[string]startHookFn

	apiserverRunOnce         sync.Once
	controllerManagerRunOnce sync.Once
	schedulerRunOnce         sync.Once
	kubeletRunOnce           sync.Once
	featureGatesOnce         sync.Once

	kubeletServer         kubeletserver.Server
	kubeletServerProvided chan struct{}

	kubeconfigReady chan struct{}
}

type (
	hookCtx     = genericapiserver.PostStartHookContext
	startHookFn = func(errFn func(error), doneFn func()) func(hookCtx) error
)

func NewNanokubeCommand(ctx context.Context) *Command {
	nano := NewNanokube(ctx)

	c := &Command{
		Command: &cobra.Command{
			Use:  "nanokube",
			Long: "all-in-one kubernetes binary",
		},
		ctx:                   nano,
		byName:                make(map[string]*cobra.Command),
		startHooks:            make(map[string]startHookFn),
		kubeletServerProvided: make(chan struct{}),
		kubeconfigReady:       make(chan struct{}),
	}
	c.Command.SetContext(nano)

	c.run = &cobra.Command{
		Use:   "run",
		Short: "run a kubernetes component in silo",
	}
	c.run.SetContext(nano)
	c.Command.AddCommand(c.run)

	start := &cobra.Command{
		Use:   "start",
		Short: "start kubernetes components",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Propagate -v (and a few other klog flags) from start down
			// to each component's own flag set. They're inherited as
			// persistent flags on start but each component reads from
			// its own local flag value via logsapi.
			for _, flag := range []string{"v", "vmodule", "log-flush-frequency"} {
				f := cmd.Flag(flag)
				if f == nil || !f.Changed {
					continue
				}
				for _, child := range c.byName {
					if child.Flags().Lookup(flag) != nil {
						child.Flags().Set(flag, f.Value.String())
					}
				}
			}
			c.startHooks["kube-controller-manager-cmd"] = runEHook(c.kubeconfigReady, c.byName["kube-controller-manager"])
			c.startHooks["kube-scheduler-cmd"] = runEHook(c.kubeconfigReady, c.byName["kube-scheduler"])
			c.startHooks["kubelet-cmd"] = runEHook(c.kubeconfigReady, c.byName["kubelet"])
			c.startHooks["update-kubeconfig"] = fnHook("update-kubeconfig", nil, c.updateKubeconfig)
			c.startHooks["untaint-node"] = fnHook("untaint-node", c.Nano().NodeReady(), c.untaintNode)
			c.startHooks["update-node-status"] = fnHook("update-node-status", c.Nano().NodeReady(), c.updateNodeStatus)
			return c.byName["kube-apiserver"].RunE(cmd, args)
		},
	}
	start.SetContext(nano)
	c.Command.AddCommand(start)

	return c
}

func (c *Command) Nano() v1.Nanokube {
	return c.ctx
}

func (c *Command) Cancel(err error) {
	c.ctx.CancelErr(err)
}

// WithRunCommand registers cmd under `nanokube run <name>`. Returns the
// receiver for chaining.
func (c *Command) WithRunCommand(cmd *cobra.Command) *Command {
	cmd.SetContext(c.Context())
	name := cmd.Name()
	c.byName[name] = cmd

	// Route every component's logging through nanokube's registered format.
	// Skipped when NANOKUBE_PRETTY=0 so the raw klog format leaks through for
	// side-by-side comparison.
	if os.Getenv("NANOKUBE_PRETTY") != "0" {
		if f := cmd.Flags().Lookup("logging-format"); f != nil {
			cmd.Flags().Set("logging-format", "nanokube")
		}
	}

	// Apply [featureGates] once. The kube: gate is a process-wide
	// singleton shared by every in-process component, so setting it on any
	// one component's --feature-gates flag mutates the same registry.
	c.featureGatesOnce.Do(func() {
		if cmd.Flags().Lookup("feature-gates") != nil {
			cmd.Flags().Set("feature-gates", strings.Join(featureGates, ","))
		}
	})

	switch name {
	case "kube-controller-manager":
		cmd.Flags().Set("authentication-skip-lookup", "true")
		cmd.Flags().Set("leader-elect", "true")
		cmd.Flags().Set("root-ca-file", c.Nano().RootCaFilePath())
		cmd.Flags().Set("service-account-private-key-file", c.Nano().KeyFilePath())
		cmd.Flags().Set("secure-port", "0")
		cmd.Flags().Set("use-service-account-credentials", "false")
		c.controllerManagerRunOnce.Do(func() {
			run := controllermanager.Run
			controllermanager.Run = func(ctx context.Context, cfg *controllermanagerconfig.CompletedConfig) error {
				return run(ctx, cfg)
			}
		})
	case "kube-scheduler":
		cmd.Flags().Set("authentication-skip-lookup", "true")
		cmd.Flags().Set("leader-elect", "true")
		cmd.Flags().Set("secure-port", "0")
		c.schedulerRunOnce.Do(func() {
			run := scheduler.Run
			scheduler.Run = func(ctx context.Context, cc *schedulerappconfig.CompletedConfig, sched *sched.Scheduler) error {
				return run(ctx, cc, sched)
			}
		})
	case "kube-apiserver":
		cmd.Flags().Set("allow-privileged", "true")
		cmd.Flags().Set("api-audiences", "https://kubernetes.default.svc")
		cmd.Flags().Set("authorization-mode", "RBAC,Node")
		cmd.Flags().Set("bind-address", c.Nano().Tunnel().LocalIP().String())            // TODO(lazify): move to runOnce
		cmd.Flags().Set("etcd-servers", strings.Join(c.Nano().Storage().Servers(), ",")) // TODO(lazify): move to runOnce
		cmd.Flags().Set("kubelet-preferred-address-types", "ExternalDNS")
		cmd.Flags().Set("secure-port", fmt.Sprintf("%d", c.Nano().Tunnel().LocalPort())) // TODO(lazify): move to runOnce
		cmd.Flags().Set("service-account-key-file", c.Nano().KeyFilePath())
		cmd.Flags().Set("service-account-signing-key-file", c.Nano().KeyFilePath())
		cmd.Flags().Set("service-account-issuer", "https://kubernetes.default.svc")
		cmd.Flags().Set("storage-media-type", "application/json")
		cmd.Flags().Set("tls-cert-file", c.Nano().CertFilePath())
		cmd.Flags().Set("tls-private-key-file", c.Nano().KeyFilePath())
		c.apiserverRunOnce.Do(func() {
			apiserver.Run = func(ctx context.Context, opts apiserveroptions.CompletedOptions) error {
				config := &apiserver.Config{Options: opts}

				genericConfig, versionedInformers, storageFactory, err := controlplaneapiserver.BuildGenericConfig(
					opts.CompletedOptions,
					[]*runtime.Scheme{legacyscheme.Scheme, apiextensionsapiserver.Scheme, aggregatorscheme.Scheme},
					controlplane.DefaultAPIResourceConfigSource(),
					generatedopenapi.GetOpenAPIDefinitions,
				)
				if err != nil {
					return err
				}

				kubeAPIs, serviceResolver, pluginInitializer, err := apiserver.CreateKubeAPIServerConfig(opts, c.Nano().Storage().SetConfig(genericConfig), c.Nano().SetSharedInformerFactory(versionedInformers), storageFactory)
				if err != nil {
					return err
				}
				config.KubeAPIs = kubeAPIs

				apiExtensions, err := controlplaneapiserver.CreateAPIExtensionsConfig(*kubeAPIs.ControlPlane.Generic, kubeAPIs.ControlPlane.VersionedInformers, pluginInitializer, opts.CompletedOptions, opts.MasterCount,
					serviceResolver, webhook.NewDefaultAuthenticationInfoResolverWrapper(kubeAPIs.ControlPlane.ProxyTransport, kubeAPIs.ControlPlane.Generic.EgressSelector, kubeAPIs.ControlPlane.Generic.LoopbackClientConfig, kubeAPIs.ControlPlane.Generic.TracerProvider))
				if err != nil {
					return err
				}
				config.ApiExtensions = apiExtensions

				aggregator, err := controlplaneapiserver.CreateAggregatorConfig(*kubeAPIs.ControlPlane.Generic, opts.CompletedOptions, kubeAPIs.ControlPlane.VersionedInformers, serviceResolver, kubeAPIs.ControlPlane.ProxyTransport, kubeAPIs.ControlPlane.Extra.PeerProxy, pluginInitializer)
				if err != nil {
					return err
				}
				config.Aggregator = aggregator

				completed, err := config.Complete()
				if err != nil {
					return err
				}

				server, err := apiserver.CreateServerChain(completed)
				if err != nil {
					return err
				}

				go func() {
					<-c.kubeletServerProvided
					for _, p := range kubeletPaths {
						klog.Infof("registering kubelet handler for path prefix %q", p)
						server.GenericAPIServer.Handler.NonGoRestfulMux.UnlistedHandlePrefix(p, &c.kubeletServer)
					}
				}()

				var wg sync.WaitGroup

				for name, startHook := range c.startHooks {
					wg.Add(1)
					server.GenericAPIServer.AddPostStartHook(name, func(ctx hookCtx) error {
						return startHook(c.Cancel, wg.Done)(ctx)
					})
				}

				prepared, err := server.PrepareRun()
				if err != nil {
					return err
				}
				err = prepared.Run(ctx)
				wg.Wait()

				// TODO(partial): find a better spot for storage shutdown
				c.Nano().Storage().Shutdown()

				return err
			}
		})
	case "kubelet":
		cmd.Flags().Set("cloud-provider", "external")
		cmd.Flags().Set("enable-server", "false")
		cmd.Flags().Set("read-only-port", "0")
		cmd.Flags().Set("root-dir", c.Nano().Options().DataDirAt(v1.DataDirKubelet))
		cmd.Flags().Set("tls-cert-file", c.Nano().CertFilePath())
		cmd.Flags().Set("tls-private-key-file", c.Nano().KeyFilePath())
		c.kubeletRunOnce.Do(func() {
			run := kubelet.Run
			kubelet.Run = func(ctx context.Context, ks *kubeletoptions.KubeletServer, deps *kubeletcore.Dependencies, fg featuregate.FeatureGate) error {
				ks.ClusterDNS = []string{"1.1.1.1"} // TODO(partial): install coredns
				ks.ClusterDomain = "cluster.local"  // TODO(partial): install coredns
				ks.PodLogsDir = c.Nano().Options().DataDirAt(v1.DataDirLogs)
				ks.Port = 443
				ks.RegisterNode = true
				deps.CAdvisorInterface = c.Nano().DefaultBackend()
				deps.ContainerManager = c.Nano().DefaultBackend().Manager()
				deps.HostUtil = c.Nano().Host()
				deps.Mounter = c.Nano().Host()
				deps.OSInterface = c.Nano().Host()
				deps.ProbeManager = nil
				deps.RemoteImageService = c.Nano().DefaultBackend().Driver()
				deps.RemoteRuntimeService = c.Nano().DefaultBackend().Driver()
				deps.Recorder = c.Nano()
				deps.Subpather = c.Nano().Host()
				deps.VolumePlugins = c.Nano().Host().VolumePlugins()
				return run(ctx, ks, deps, fg)
			}
			start := kubelet.StartKubelet
			kubelet.StartKubelet = func(ctx context.Context, k kubeletcore.Bootstrap, podCfg *kubeletconfig.PodConfig, kubeCfg *kubeletconfiginternal.KubeletConfiguration, kubeDeps *kubeletcore.Dependencies, enableServer bool) {
				kl := k.(*kubeletcore.Kubelet)
				c.kubeletServer = kubeletserver.NewServer(ctx, kl, kl.ResourceAnalyzer(), kl.HealthCheckers(), kl.Flagz(), kubeDeps.Auth, kubeCfg)
				close(c.kubeletServerProvided)
				start(ctx, k, podCfg, kubeCfg, kubeDeps, enableServer)
			}
		})
	}

	c.run.AddCommand(cmd)
	return c
}

func (c *Command) updateKubeconfig(ctx hookCtx) error {
	kubeconfigPath := c.Nano().WithLoopback(ctx.LoopbackClientConfig).KubeconfigPath() // TODO(partial): use WriteKubeconfig on v1.Client
	c.Nano().Client().WithTunnel(c.Nano().Tunnel(), false).WriteKubeconfig(clientcmd.RecommendedHomeFile)
	c.byName["kube-controller-manager"].Flags().Set("kubeconfig", kubeconfigPath)
	c.byName["kube-controller-manager"].Flags().Set("authorization-kubeconfig", kubeconfigPath)
	c.byName["kube-controller-manager"].Flags().Set("authentication-kubeconfig", kubeconfigPath)
	c.byName["kube-scheduler"].Flags().Set("kubeconfig", kubeconfigPath)
	c.byName["kube-scheduler"].Flags().Set("authorization-kubeconfig", kubeconfigPath)
	c.byName["kube-scheduler"].Flags().Set("authentication-kubeconfig", kubeconfigPath)
	c.byName["kubelet"].Flags().Set("kubeconfig", kubeconfigPath)
	close(c.kubeconfigReady)
	return nil
}

func (c *Command) untaintNode(ctx hookCtx) error {
	nodeRef := c.Nano().NodeRef()
	if nodeRef == nil {
		return fmt.Errorf("node reference not found")
	}
	nodes := c.Nano().Client().CoreV1().Nodes()

	// DEVNOTE: we retry because a few other controllers might be updating the node
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		node, err := nodes.Get(ctx.Context, nodeRef.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		node.Spec.Taints = slices.DeleteFunc(node.Spec.Taints, func(t corev1.Taint) bool {
			return t.Key == cloudproviderapi.TaintExternalCloudProvider
		})
		_, err = nodes.Update(ctx.Context, node, metav1.UpdateOptions{})
		return err
	}); err != nil {
		return fmt.Errorf("failed to remove taints from node: %w", err)
	}

	return nil
}

func (c *Command) updateNodeStatus(ctx hookCtx) error {
	nodeRef := c.Nano().NodeRef()
	if nodeRef == nil {
		return fmt.Errorf("node reference not found")
	}
	nodes := c.Nano().Client().CoreV1().Nodes()

	// DEVNOTE: we retry because a few other controllers might be updating the node
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		_, err := nodes.ApplyStatus(ctx.Context, corev1ac.Node(nodeRef.Name).WithStatus(
			corev1ac.NodeStatus().
				WithAddresses(
					corev1ac.NodeAddress().WithType(corev1.NodeHostName).WithAddress(func() string {
						a, _ := os.Hostname()
						a, _, _ = strings.Cut(a, ".")
						return a
					}()),
					corev1ac.NodeAddress().WithType(corev1.NodeInternalDNS).WithAddress(func() string {
						a, _ := os.Hostname()
						return a
					}()),
					corev1ac.NodeAddress().WithType(corev1.NodeExternalDNS).WithAddress(func() string {
						// a := fmt.Sprintf("%s:%d", tunnel.FQDN(), 443)
						a := c.Nano().Tunnel().FQDN()
						return a
					}()),
				),
		), metav1.ApplyOptions{FieldManager: c.Name(), Force: true})
		return err
	}); err != nil {
		return fmt.Errorf("failed to update node status: %w", err)
	}

	return nil
}

func fnHook(name string, waitCh <-chan struct{}, fn func(hookCtx) error) startHookFn {
	return func(errFn func(error), doneFn func()) func(hookCtx) error {
		return func(hctx hookCtx) error {
			go func() {
				if waitCh != nil {
					<-waitCh
				}
				klog.InfoS("starting", "hook", name)
				defer doneFn()
				if err := fn(hctx); err != nil {
					klog.ErrorS(err, "errored", "hook", name)
					if errFn != nil {
						errFn(err)
					}
				} else {
					klog.InfoS("completed", "hook", name)
				}
			}()
			return nil
		}
	}
}

func runEHook(waitCh <-chan struct{}, cmd *cobra.Command) startHookFn {
	return func(errFn func(error), doneFn func()) func(hookCtx) error {
		return func(hctx hookCtx) error {
			go func() {
				if waitCh != nil {
					<-waitCh
				}
				klog.InfoS("starting", "command", cmd.Name())
				defer doneFn()
				if err := cmd.RunE(cmd, []string{}); err != nil {
					klog.ErrorS(err, "errored", "command", cmd.Name())
					if errFn != nil {
						errFn(err)
					}
				} else {
					klog.InfoS("completed", "command", cmd.Name())
				}
			}()
			return nil
		}
	}
}
