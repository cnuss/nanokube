package pkg

import (
	"context"
	"strings"
	"sync"

	v1 "github.com/cnuss/nanokube/pkg/v1"
	"github.com/spf13/cobra"
	apiextensionsapiserver "k8s.io/apiextensions-apiserver/pkg/apiserver"
	"k8s.io/apimachinery/pkg/runtime"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/util/webhook"
	"k8s.io/component-base/featuregate"
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

	ctx    v1.Nanokube
	cancel context.CancelCauseFunc

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
	nano, cancel := NewNanokube(ctx)
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
			hooks["kube-controller-manager"] = byName["kube-controller-manager"]
			hooks["kube-scheduler"] = byName["kube-scheduler"]
			return byName["kube-apiserver"].RunE(cmd, args)
		},
	}
	start.SetContext(nano)
	root.AddCommand(start)

	command := &Command{
		Command: root,
		ctx:     nano,
		cancel:  cancel,
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

// WithRunCommand registers cmd under `nanokube run <name>`. Returns the
// receiver for chaining.
func (c *Command) WithRunCommand(cmd *cobra.Command) *Command {
	cmd.SetContext(c.Context())
	name := cmd.Name()
	c.byName[name] = cmd

	switch name {
	case "kube-controller-manager":
		cmd.Flag("authentication-skip-lookup").Value.Set("true")
		cmd.Flag("leader-elect").Value.Set("true")
		cmd.Flag("root-ca-file").Value.Set(c.Nano().RootCaFilePath())
		cmd.Flag("service-account-private-key-file").Value.Set(c.Nano().KeyFilePath())
		cmd.Flag("secure-port").Value.Set("0")
		cmd.Flag("use-service-account-credentials").Value.Set("false")
		c.controllerManagerRunOnce.Do(func() {
			run := controllermanager.Run
			controllermanager.Run = func(ctx context.Context, cfg *controllermanagerconfig.CompletedConfig) error {
				return run(ctx, cfg)
			}
		})
	case "kube-scheduler":
		cmd.Flag("authentication-skip-lookup").Value.Set("true")
		cmd.Flag("leader-elect").Value.Set("true")
		cmd.Flag("secure-port").Value.Set("0")
		c.schedulerRunOnce.Do(func() {
			run := scheduler.Run
			scheduler.Run = func(ctx context.Context, cc *schedulerappconfig.CompletedConfig, sched *sched.Scheduler) error {
				return run(ctx, cc, sched)
			}
		})
	case "kube-apiserver":
		cmd.Flag("api-audiences").Value.Set("https://kubernetes.default.svc")
		cmd.Flag("authorization-mode").Value.Set("RBAC,Node")
		cmd.Flag("etcd-servers").Value.Set(strings.Join(c.Nano().Storage().Servers(), ","))
		cmd.Flag("service-account-key-file").Value.Set(c.Nano().KeyFilePath())
		cmd.Flag("service-account-signing-key-file").Value.Set(c.Nano().KeyFilePath())
		cmd.Flag("service-account-issuer").Value.Set("https://kubernetes.default.svc")
		cmd.Flag("storage-media-type").Value.Set("application/json")
		cmd.Flag("tls-cert-file").Value.Set(c.Nano().CertFilePath())
		cmd.Flag("tls-private-key-file").Value.Set(c.Nano().KeyFilePath())
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
					kubeconfigPath := c.Nano().KubeconfigPath(context.LoopbackClientConfig)
					c.byName["kube-controller-manager"].Flag("kubeconfig").Value.Set(kubeconfigPath)
					c.byName["kube-controller-manager"].Flag("authorization-kubeconfig").Value.Set(kubeconfigPath)
					c.byName["kube-controller-manager"].Flag("authentication-kubeconfig").Value.Set(kubeconfigPath)
					c.byName["kube-scheduler"].Flag("kubeconfig").Value.Set(kubeconfigPath)
					c.byName["kube-scheduler"].Flag("authorization-kubeconfig").Value.Set(kubeconfigPath)
					c.byName["kube-scheduler"].Flag("authentication-kubeconfig").Value.Set(kubeconfigPath)
					close(kubeconfigReady)
					return nil
				})

				server.GenericAPIServer.AddPostStartHook("run-kcm", func(context genericapiserver.PostStartHookContext) error {
					<-kubeconfigReady
					kcm := c.byName["kube-controller-manager"]
					kcm.SetContext(context.Context)
					go kcm.RunE(kcm, nil)
					return nil
				})

				prepared, err := server.PrepareRun()
				if err != nil {
					return err
				}
				return prepared.Run(ctx)
			}
		})
	case "kubelet":
		c.kubeletRunOnce.Do(func() {
			run := kubelet.Run
			kubelet.Run = func(ctx context.Context, ks *kubeletoptions.KubeletServer, deps *kubeletcore.Dependencies, fg featuregate.FeatureGate) error {
				return run(ctx, ks, deps, fg)
			}
		})
	}

	c.run.AddCommand(cmd)
	return c
}
