package pkg

import (
	"context"
	"fmt"
	"strings"
	"sync"

	v1 "github.com/cnuss/nanokube/pkg/v1"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
	apiextensionsapiserver "k8s.io/apiextensions-apiserver/pkg/apiserver"
	"k8s.io/apimachinery/pkg/runtime"
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

	byName    map[string]*cobra.Command
	deps      map[*cobra.Command][]*cobra.Command
	inner     map[*cobra.Command]func(*cobra.Command, []string) error
	cancelFor map[*cobra.Command]context.CancelFunc
	stopped   map[*cobra.Command]chan struct{}

	apiserverRunOnce         sync.Once
	kubeletRunOnce           sync.Once
	controllerManagerRunOnce sync.Once
	schedulerRunOnce         sync.Once
}

func NewNanokubeCommand(ctx context.Context) *Command {
	nano, cancel := NewNanokube(ctx)
	cmd := &cobra.Command{
		Use:  "nanokube",
		Long: "all-in-one kubernetes binary",
	}
	cmd.SetContext(nano)

	c := &Command{
		Command:   cmd,
		ctx:       nano,
		cancel:    cancel,
		byName:    map[string]*cobra.Command{},
		deps:      map[*cobra.Command][]*cobra.Command{},
		inner:     map[*cobra.Command]func(*cobra.Command, []string) error{},
		cancelFor: map[*cobra.Command]context.CancelFunc{},
		stopped:   map[*cobra.Command]chan struct{}{},
	}

	// no-arg form: launch kubelet + controller + scheduler (apiserver pulled in via deps)
	cmd.RunE = func(*cobra.Command, []string) error {
		return c.launch(
			c.byName["kubelet"],
			c.byName["kube-controller-manager"],
			c.byName["kube-scheduler"],
		)
	}

	return c
}

func (c *Command) Nano() v1.Nanokube {
	return c.ctx
}

// dependentsOf returns commands that have `cmd` in their deps. Computed at
// shutdown time (lazy) so AddCommand order doesn't matter.
func (c *Command) dependentsOf(cmd *cobra.Command) []*cobra.Command {
	var out []*cobra.Command
	for other, deps := range c.deps {
		for _, d := range deps {
			if d == cmd {
				out = append(out, other)
				break
			}
		}
	}
	return out
}

func (c *Command) AddCommand(cmds ...*cobra.Command) {
	for _, cmd := range cmds {
		// Each child gets its own ctx derived from the root, plus a `stopped`
		// chan that closes when its RunE returns. The shutdown goroutine
		// below holds the cancel until every dependent has stopped.
		childCtx, childCancel := context.WithCancel(c.Context())
		cmd.SetContext(childCtx)
		c.cancelFor[cmd] = childCancel
		c.stopped[cmd] = make(chan struct{})

		name := cmd.Name()
		c.byName[name] = cmd

		switch name {
		case "kube-controller-manager":
			c.deps[cmd] = []*cobra.Command{c.byName["kube-apiserver"]}
			c.controllerManagerRunOnce.Do(func() {
				run := controllermanager.Run
				controllermanager.Run = func(ctx context.Context, cfg *controllermanagerconfig.CompletedConfig) error {
					fmt.Printf("controllermanager run")
					return run(ctx, cfg)
				}
			})
		case "kube-scheduler":
			c.deps[cmd] = []*cobra.Command{c.byName["kube-apiserver"]}
			c.schedulerRunOnce.Do(func() {
				run := scheduler.Run
				scheduler.Run = func(ctx context.Context, cc *schedulerappconfig.CompletedConfig, sched *sched.Scheduler) error {
					fmt.Printf("!!! scheduler run")
					return run(ctx, cc, sched)
				}
			})
		case "kube-apiserver":
			cmd.Flag("api-audiences").Value.Set("https://kubernetes.default.svc") // TODO: fix
			cmd.Flag("authorization-mode").Value.Set("RBAC,Node")
			cmd.Flag("etcd-servers").Value.Set(strings.Join(c.Nano().Storage().Servers(), ","))
			cmd.Flag("service-account-key-file").Value.Set(c.Nano().KeyFilePath())         // TODO: fix
			cmd.Flag("service-account-signing-key-file").Value.Set(c.Nano().KeyFilePath()) // TODO: fix
			cmd.Flag("service-account-issuer").Value.Set("https://kubernetes.default.svc") // TODO: fix
			cmd.Flag("storage-media-type").Value.Set("application/json")
			cmd.Flag("tls-cert-file").Value.Set(c.Nano().CertFilePath())
			cmd.Flag("tls-private-key-file").Value.Set(c.Nano().KeyFilePath())
			c.apiserverRunOnce.Do(func() {
				apiserver.Run = func(ctx context.Context, opts apiserveroptions.CompletedOptions) error {
					config := &apiserver.Config{
						Options: opts,
					}

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
					fmt.Printf("!!! kubelet run")
					return run(ctx, ks, deps, fg)
				}
			})
		default:
			// Unknown command — register as-is, no RunE rewrapping, no gating.
			c.Command.AddCommand(cmd)
			continue
		}

		// Stash the upstream RunE wrapped to close stopped when it returns,
		// then replace cmd.RunE with one that launches target + transitive deps.
		upstream := cmd.RunE
		target := cmd
		c.inner[cmd] = func(cmd *cobra.Command, args []string) error {
			defer close(c.stopped[cmd])
			return upstream(cmd, args)
		}
		cmd.RunE = func(*cobra.Command, []string) error {
			return c.launch(target)
		}
		c.Command.AddCommand(cmd)

		// Reverse-dep shutdown: when root ctx fires, wait for every command
		// that depends on me to close `stopped`, then cancel my own ctx.
		go func(cmd *cobra.Command) {
			<-c.Context().Done()
			for _, dep := range c.dependentsOf(cmd) {
				<-c.stopped[dep]
			}
			c.cancelFor[cmd]()
		}(cmd)
	}
}

// launch runs `cmds` plus their transitive deps as concurrent goroutines and
// waits for all of them. Each goroutine calls the command's RunE directly so
// cobra doesn't re-parse os.Args.
func (c *Command) launch(cmds ...*cobra.Command) error {
	seen := map[*cobra.Command]bool{}
	var order []*cobra.Command
	var visit func(*cobra.Command)
	visit = func(cmd *cobra.Command) {
		if cmd == nil || seen[cmd] {
			return
		}
		seen[cmd] = true
		for _, d := range c.deps[cmd] {
			visit(d)
		}
		order = append(order, cmd)
	}
	for _, cmd := range cmds {
		visit(cmd)
	}

	g := new(errgroup.Group)
	for _, cmd := range order {
		g.Go(func() error {
			err := c.inner[cmd](cmd, nil)
			if err != nil {
				c.cancel(fmt.Errorf("%s: %w", cmd.Name(), err))
			}
			return err
		})
	}
	err := g.Wait()
	c.ctx.Storage().Shutdown()
	return err
}
