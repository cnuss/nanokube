package pkg

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	v1 "github.com/cnuss/nanokube/pkg/v1"
	"github.com/spf13/cobra"
	apiextensionsapiserver "k8s.io/apiextensions-apiserver/pkg/apiserver"
	"k8s.io/apimachinery/pkg/runtime"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/util/webhook"
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
	sched "k8s.io/kubernetes/pkg/scheduler"
)

type Command struct {
	*cobra.Command
	ctx v1.Nanokube

	run    *cobra.Command
	start  *cobra.Command
	byName map[string]*cobra.Command
	hooks  map[string]*cobra.Command

	apiserverRunOnce         sync.Once
	controllerManagerRunOnce sync.Once
	schedulerRunOnce         sync.Once
	kubeletRunOnce           sync.Once
}

func NewNanokubeCommand(ctx context.Context) *Command {
	nano := NewNanokube(ctx)
	byName := make(map[string]*cobra.Command)
	hooks := make(map[string]*cobra.Command)

	root := &cobra.Command{
		Use:  "nanokube",
		Long: "all-in-one kubernetes binary",
	}
	root.SetContext(nano)

	run := &cobra.Command{
		Use:   "run",
		Short: "run a kubernetes component in silo",
	}
	run.SetContext(nano)
	root.AddCommand(run)

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
				for _, child := range byName {
					if child.Flags().Lookup(flag) != nil {
						child.Flags().Set(flag, f.Value.String())
					}
				}
			}
			hooks["run-kube-controller-manager"] = byName["kube-controller-manager"]
			hooks["run-kube-scheduler"] = byName["kube-scheduler"]
			hooks["run-kubelet"] = byName["kubelet"]
			return byName["kube-apiserver"].RunE(cmd, args)
		},
	}
	start.SetContext(nano)
	root.AddCommand(start)

	command := &Command{
		Command: root,
		ctx:     nano,
		run:     run,
		start:   start,
		byName:  byName,
		hooks:   hooks,
	}

	return command
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
		cmd.Flags().Set("api-audiences", "https://kubernetes.default.svc")
		cmd.Flags().Set("authorization-mode", "RBAC,Node")
		cmd.Flags().Set("bind-address", c.Nano().Tunnel().LocalIP().String())
		cmd.Flags().Set("etcd-servers", strings.Join(c.Nano().Storage().Servers(), ","))
		cmd.Flags().Set("secure-port", fmt.Sprintf("%d", c.Nano().Tunnel().LocalPort()))
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

				kubeconfigReady := make(chan struct{})
				server.GenericAPIServer.AddPostStartHook("update-kubeconfig", func(context genericapiserver.PostStartHookContext) error {
					kubeconfigPath := c.Nano().WithLoopback(context.LoopbackClientConfig).KubeconfigPath()
					c.byName["kube-controller-manager"].Flags().Set("kubeconfig", kubeconfigPath)
					c.byName["kube-controller-manager"].Flags().Set("authorization-kubeconfig", kubeconfigPath)
					c.byName["kube-controller-manager"].Flags().Set("authentication-kubeconfig", kubeconfigPath)
					c.byName["kube-scheduler"].Flags().Set("kubeconfig", kubeconfigPath)
					c.byName["kube-scheduler"].Flags().Set("authorization-kubeconfig", kubeconfigPath)
					c.byName["kube-scheduler"].Flags().Set("authentication-kubeconfig", kubeconfigPath)
					c.byName["kubelet"].Flags().Set("kubeconfig", kubeconfigPath)
					close(kubeconfigReady)
					return nil
				})

				var wg sync.WaitGroup
				for name, hook := range c.hooks {
					wg.Add(1)
					server.GenericAPIServer.AddPostStartHook(name, func(hctx genericapiserver.PostStartHookContext) error {
						go func() {
							defer wg.Done()
							<-kubeconfigReady
							hook.SetContext(hctx.Context)
							if err := hook.RunE(hook, nil); err != nil {
								klog.ErrorS(err, "component errored", "component", hook.Name())
								c.Cancel(err)
							} else {
								klog.InfoS("component stopped", "component", hook.Name())
							}
						}()
						klog.InfoS("component started", "component", hook.Name())
						return nil
					})
				}

				prepared, err := server.PrepareRun()
				if err != nil {
					return err
				}
				err = prepared.Run(ctx)
				wg.Wait()
				c.Nano().Storage().Shutdown()
				return err
			}
		})
	case "kubelet":
		cmd.Flags().Set("cloud-provider", "external")
		cmd.Flags().Set("root-dir", c.Nano().Options().DataDirAt(v1.DataDirKubelet))
		cmd.Flags().Set("tls-cert-file", c.Nano().CertFilePath())
		cmd.Flags().Set("tls-private-key-file", c.Nano().KeyFilePath())
		c.kubeletRunOnce.Do(func() {
			run := kubelet.Run
			kubelet.Run = func(ctx context.Context, ks *kubeletoptions.KubeletServer, deps *kubeletcore.Dependencies, fg featuregate.FeatureGate) error {
				ks.PodLogsDir = c.Nano().Options().DataDirAt(v1.DataDirLogs)
				ks.ReadOnlyPort = 0
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
		})
	}

	c.run.AddCommand(cmd)
	return c
}
