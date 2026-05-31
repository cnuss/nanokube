package pkg

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	v1 "github.com/cnuss/nanokube/pkg/v1"
	"github.com/spf13/cobra"
	"github.com/stoewer/go-strcase"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsapiserver "k8s.io/apiextensions-apiserver/pkg/apiserver"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/endpoints/handlers"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/util/webhook"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	rbacv1ac "k8s.io/client-go/applyconfigurations/rbac/v1"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
	cloudproviderapi "k8s.io/cloud-provider/api"
	"k8s.io/component-base/featuregate"
	logsapi "k8s.io/component-base/logs/api/v1"
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
// shared by every in-process component) via [Command.With].
var featureGates = []string{
	// DEVNOTE: for cloudflared support:
	"kube:ExtendWebSocketsToKubelet=false",
	"kube:PortForwardWebsockets=false",
	"kube:TranslateStreamCloseWebsocketRequests=false",
	"kube:WatchList=false",
	"kube:WatchListClient=false",
	// DEVNOTE: general:
	"kube:KubeletInUserNamespace=true",
	"kube:GracefulNodeShutdown=true",
	"kube:GracefulNodeShutdownBasedOnPodPriority=true",
	"kube:ControllerManagerReleaseLeaderElectionLockOnExit=true",
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

	startHooks     map[string]startHookFn
	runHooks       map[string]startHookFn
	stopHooks      map[string]stopHookFn
	startHooksDone chan struct{}

	featureGatesOnce sync.Once

	apiserverOnce             sync.Once
	apiServerProvided         chan struct{}
	controllerManagerOnce     sync.Once
	controllerManagerProvided chan struct{}
	schedulerOnce             sync.Once
	schedulerProvided         chan struct{}
	kubeletOnce               sync.Once
	kubeletProvided           chan struct{}

	kubeletServer         kubeletserver.Server
	kubeletServerProvided chan struct{}

	paths         []string
	pathsProvided chan struct{}

	drainDone       chan struct{}
	kubeconfigReady chan struct{}
}

type (
	hookCtx     = genericapiserver.PostStartHookContext
	startHookFn = func(errFn func(error), doneFn func()) func(hookCtx) error
	stopHookFn  = func() error
)

func NewNanokubeCommand(ctx context.Context) *Command {
	nano := NewNanokube(ctx)
	c := &Command{
		Command: &cobra.Command{
			Use:  "nanokube",
			Long: "all-in-one kubernetes binary",
		},
		ctx:                       nano,
		startHooks:                make(map[string]startHookFn),
		runHooks:                  make(map[string]startHookFn),
		stopHooks:                 make(map[string]stopHookFn),
		startHooksDone:            make(chan struct{}),
		apiServerProvided:         make(chan struct{}),
		controllerManagerProvided: make(chan struct{}),
		schedulerProvided:         make(chan struct{}),
		kubeletProvided:           make(chan struct{}),
		paths:                     make([]string, 0),
		pathsProvided:             make(chan struct{}),
		kubeletServerProvided:     make(chan struct{}),
		drainDone:                 make(chan struct{}),
		kubeconfigReady:           make(chan struct{}),
	}
	wait.NeverStop = c.drainDone
	klog.OsExit = func(code int) {
		c.Cancel(fmt.Errorf("klog exit code %d", code))
	}
	logsapi.ReapplyHandling = logsapi.ReapplyHandlingIgnoreUnchanged
	c.Command.SetContext(nano)

	run := &cobra.Command{
		Use:   "run",
		Short: "run a kubernetes component",
	}
	run.SetContext(c.Nano())
	c.Command.AddCommand(run)

	c.Command.RunE = func(cmd *cobra.Command, args []string) error {
		c.runHooks["start-kube-controller-manager"] = c.runEHook(c.kubeconfigReady, c.ControllerManagerCommand())
		c.runHooks["start-kube-scheduler"] = c.runEHook(c.kubeconfigReady, c.SchedulerCommand())
		c.runHooks["start-kubelet"] = c.runEHook(c.kubeconfigReady, c.KubeletCommand())
		c.startHooks["update-kubeconfig"] = startHook("update-kubeconfig", nil, c.updateKubeconfig)
		c.startHooks["untaint-node"] = startHook("untaint-node", c.Nano().NodeReady(), c.untaintNode)
		c.startHooks["update-node-status"] = startHook("update-node-status", c.Nano().NodeReady(), c.updateNodeStatus)
		c.startHooks["update-kubelet-rbac"] = startHook("kubelet-rbac", c.pathsProvided, c.updateKubeletRBAC("system:anonymous")) // TODO(partial): use a real subject and tighten permissions
		c.stopHooks["drain-node"] = stopHook("drain-node", c.drainNode)
		return c.ApiServerCommand().RunE(cmd, args)
	}
	return c
}

func (c *Command) Nano() v1.Nanokube {
	return c.ctx
}

func (c *Command) Cancel(err error) {
	c.ctx.CancelErr(err)
}

func (c *Command) StartHooksDone() <-chan struct{} {
	return c.startHooksDone
}

func (c *Command) With(cmd *cobra.Command) *Command {
	cmd.SetContext(c.Context())
	name := cmd.Name()
	c.featureGatesOnce.Do(func() {
		if cmd.Flags().Lookup("feature-gates") != nil {
			cmd.Flags().Set("feature-gates", strings.Join(featureGates, ","))
		}
	})

	switch name {
	case "kube-controller-manager":
		c.controllerManagerOnce.Do(func() {
			cmd.Flags().Set("authentication-skip-lookup", "true")
			cmd.Flags().Set("controller-shutdown-timeout", "0")
			cmd.Flags().Set("leader-elect", "true")
			cmd.Flags().Set("root-ca-file", c.Nano().RootCaFilePath())
			cmd.Flags().Set("service-account-private-key-file", c.Nano().KeyFilePath())
			cmd.Flags().Set("secure-port", "0")
			cmd.Flags().Set("use-service-account-credentials", "false")
			run := controllermanager.Run
			controllermanager.Run = func(ctx context.Context, cfg *controllermanagerconfig.CompletedConfig) error {
				return run(ctx, cfg)
			}
			c.RunCommand().AddCommand(cmd)
			close(c.controllerManagerProvided)
		})
	case "kube-scheduler":
		c.schedulerOnce.Do(func() {
			cmd.Flags().Set("authentication-skip-lookup", "true")
			cmd.Flags().Set("leader-elect", "true")
			cmd.Flags().Set("secure-port", "0")
			run := scheduler.Run
			scheduler.Run = func(ctx context.Context, cc *schedulerappconfig.CompletedConfig, sched *sched.Scheduler) error {
				return run(ctx, cc, sched)
			}
			c.RunCommand().AddCommand(cmd)
			close(c.schedulerProvided)
		})
	case "kube-apiserver":
		c.apiserverOnce.Do(func() {
			cmd.Flags().Set("allow-privileged", "true")
			cmd.Flags().Set("api-audiences", "https://kubernetes.default.svc")
			cmd.Flags().Set("authorization-mode", "RBAC,Node")
			cmd.Flags().Set("etcd-servers", "http://") // TODO(partial): mark as not required
			cmd.Flags().Set("kubelet-preferred-address-types", "ExternalDNS")
			cmd.Flags().Set("service-account-key-file", c.Nano().KeyFilePath())
			cmd.Flags().Set("service-account-signing-key-file", c.Nano().KeyFilePath())
			cmd.Flags().Set("service-account-issuer", "https://kubernetes.default.svc")
			cmd.Flags().Set("shutdown-delay-duration", "10s")
			cmd.Flags().Set("shutdown-send-retry-after", "true")
			cmd.Flags().Set("shutdown-watch-termination-grace-period", "10s")
			cmd.Flags().Set("storage-media-type", "application/json")
			cmd.Flags().Set("tls-cert-file", c.Nano().CertFilePath())
			cmd.Flags().Set("tls-private-key-file", c.Nano().KeyFilePath())
			apiserver.Run = func(ctx context.Context, opts apiserveroptions.CompletedOptions) error {
				opts.SecureServing.BindAddress = c.Nano().Tunnel().LocalIP()
				opts.SecureServing.BindPort = c.Nano().Tunnel().LocalPort()
				opts.Etcd.StorageConfig.Transport.ServerList = c.Nano().Storage().Servers()
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

				// TODO(partial): is this the right place for this?
				c.Nano().Tunnel().WithListener(server.GenericAPIServer.SecureServingInfo.Listener)

				go func() {
					<-c.kubeletServerProvided
					defer close(c.pathsProvided)
					for _, p := range kubeletPaths {
						server.GenericAPIServer.Handler.NonGoRestfulMux.UnlistedHandlePrefix(p, &c.kubeletServer)
						c.paths = append(c.paths, p)
					}
					for _, ws := range c.Nano().Services(c.Nano().Tunnel().URL()) {
						server.GenericAPIServer.Handler.GoRestfulContainer.Add(ws)
						c.paths = append(c.paths, ws.RootPath())
					}
					for name, hook := range c.stopHooks {
						server.GenericAPIServer.AddPreShutdownHook(name, hook)
					}
				}()

				var startWg, runWg sync.WaitGroup

				for name, h := range c.startHooks {
					startWg.Add(1)
					server.GenericAPIServer.AddPostStartHook(name, func(ctx hookCtx) error {
						return h(c.Cancel, startWg.Done)(ctx)
					})
				}
				for name, h := range c.runHooks {
					runWg.Add(1)
					server.GenericAPIServer.AddPostStartHook(name, func(ctx hookCtx) error {
						return h(c.Cancel, runWg.Done)(ctx)
					})
				}
				go func() {
					startWg.Wait()
					close(c.startHooksDone)
				}()

				prepared, err := server.PrepareRun()
				if err != nil {
					return err
				}
				err = prepared.Run(ctx)
				startWg.Wait()
				runWg.Wait()

				// TODO(partial): find a better spot for storage shutdown
				c.Nano().Storage().Shutdown()
				return err
			}
			c.RunCommand().AddCommand(cmd)
			close(c.apiServerProvided)
		})
	case "kubelet":
		c.kubeletOnce.Do(func() {
			cmd.Flags().Set("cloud-provider", "external")
			cmd.Flags().Set("enable-server", "false")
			cmd.Flags().Set("read-only-port", "0")
			cmd.Flags().Set("root-dir", c.Nano().Options().DataDirAt(v1.DataDirKubelet))
			cmd.Flags().Set("tls-cert-file", c.Nano().CertFilePath())
			cmd.Flags().Set("tls-private-key-file", c.Nano().KeyFilePath())
			run := kubelet.Run
			kubelet.Run = func(ctx context.Context, ks *kubeletoptions.KubeletServer, deps *kubeletcore.Dependencies, fg featuregate.FeatureGate) error {
				ks.ClusterDNS = []string{"1.1.1.1"} // TODO(partial): install coredns
				ks.ClusterDomain = c.Nano().Tunnel().Domain()
				// ks.HostnameOverride = c.Nano().Tunnel().Hostname()
				ks.PodLogsDir = c.Nano().Options().DataDirAt(v1.DataDirLogs)
				ks.Port = 443
				ks.RegisterNode = true
				ks.ShutdownGracePeriod = metav1.Duration{Duration: 60 * time.Second}
				ks.ShutdownGracePeriodCriticalPods = metav1.Duration{Duration: 30 * time.Second}
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
				go func() {
					<-ctx.Done()
					gcCtx, cancel := context.WithTimeout(context.Background(), handlers.MaxWatchTimeout)
					defer cancel()
					if err := kl.ContainerGC().DeleteAllUnusedContainers(gcCtx); err != nil {
						klog.ErrorS(err, "shutdown container GC failed")
					}
					kl.NodeLeaseController().Stop()
				}()
			}
			c.RunCommand().AddCommand(cmd)
			close(c.kubeletProvided)
		})
	}

	return c
}

func (c *Command) RunCommand() *cobra.Command {
	for _, cmd := range c.Commands() {
		if cmd.Name() == "run" {
			return cmd
		}
	}
	panic("run command not found")
}

func (c *Command) ApiServerCommand() *cobra.Command {
	<-c.apiServerProvided
	for _, cmd := range c.RunCommand().Commands() {
		if cmd.Name() == "kube-apiserver" {
			return cmd
		}
	}
	panic("kube-apiserver command not found")
}

func (c *Command) ControllerManagerCommand() *cobra.Command {
	<-c.controllerManagerProvided
	for _, cmd := range c.RunCommand().Commands() {
		if cmd.Name() == "kube-controller-manager" {
			return cmd
		}
	}
	panic("kube-controller-manager command not found")
}

func (c *Command) SchedulerCommand() *cobra.Command {
	<-c.schedulerProvided
	for _, cmd := range c.RunCommand().Commands() {
		if cmd.Name() == "kube-scheduler" {
			return cmd
		}
	}
	panic("kube-scheduler command not found")
}

func (c *Command) KubeletCommand() *cobra.Command {
	<-c.kubeletProvided
	for _, cmd := range c.RunCommand().Commands() {
		if cmd.Name() == "kubelet" {
			return cmd
		}
	}
	panic("kubelet command not found")
}

func (c *Command) updateKubeconfig(ctx hookCtx) error {
	kubeconfigPath := c.Nano().WithLoopback(ctx.LoopbackClientConfig).KubeconfigPath() // TODO(partial): use WriteKubeconfig on v1.Client
	c.Nano().Client().WithTunnel(c.Nano().Tunnel(), false).WriteKubeconfig(clientcmd.RecommendedHomeFile)
	c.ControllerManagerCommand().Flags().Set("kubeconfig", kubeconfigPath)
	c.ControllerManagerCommand().Flags().Set("authorization-kubeconfig", kubeconfigPath)
	c.ControllerManagerCommand().Flags().Set("authentication-kubeconfig", kubeconfigPath)
	c.SchedulerCommand().Flags().Set("kubeconfig", kubeconfigPath)
	c.SchedulerCommand().Flags().Set("authorization-kubeconfig", kubeconfigPath)
	c.SchedulerCommand().Flags().Set("authentication-kubeconfig", kubeconfigPath)
	c.KubeletCommand().Flags().Set("kubeconfig", kubeconfigPath)
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
						a := c.Nano().Tunnel().Hostname()
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

func (c *Command) updateKubeletRBAC(subject string) func(ctx hookCtx) error {
	return func(ctx hookCtx) error {
		const aggregateRoleName = "system:kubelet"
		const aggregateLabelKey = "rbac.nanokube.io/aggregate-to-kubelet"

		rbac := c.Nano().Client().RbacV1()
		opts := metav1.ApplyOptions{FieldManager: c.Name(), Force: true}
		subjectSlug := strings.TrimPrefix(subject, "system:")

		for _, p := range c.paths {
			base := strings.TrimSuffix(p, "/")
			segments := strings.Split(strings.TrimPrefix(base, "/"), "/")
			for i, seg := range segments {
				segments[i] = strcase.KebabCase(seg)
			}
			roleName := aggregateRoleName + ":" + strings.Join(segments, ":")
			role := rbacv1ac.ClusterRole(roleName).
				WithLabels(map[string]string{aggregateLabelKey: "true"}).
				WithRules(rbacv1ac.PolicyRule().
					WithVerbs("*").
					WithNonResourceURLs(base, base+"/*"))
			if _, err := rbac.ClusterRoles().Apply(ctx.Context, role, opts); err != nil {
				return fmt.Errorf("apply cluster role %q: %w", roleName, err)
			}
		}

		aggregate := rbacv1ac.ClusterRole(aggregateRoleName).
			WithAggregationRule(rbacv1ac.AggregationRule().
				WithClusterRoleSelectors(metav1ac.LabelSelector().
					WithMatchLabels(map[string]string{aggregateLabelKey: "true"})))
		if _, err := rbac.ClusterRoles().Apply(ctx.Context, aggregate, opts); err != nil {
			return fmt.Errorf("apply aggregate cluster role %q: %w", aggregateRoleName, err)
		}

		binding := rbacv1ac.ClusterRoleBinding(aggregateRoleName + ":" + subjectSlug).
			WithRoleRef(rbacv1ac.RoleRef().
				WithAPIGroup(rbacv1.GroupName).
				WithKind("ClusterRole").
				WithName(aggregateRoleName)).
			WithSubjects(rbacv1ac.Subject().
				WithAPIGroup(rbacv1.GroupName).
				WithKind(rbacv1.UserKind).
				WithName(subject))
		if _, err := rbac.ClusterRoleBindings().Apply(ctx.Context, binding, opts); err != nil {
			return fmt.Errorf("apply cluster role binding %q: %w", *binding.Name, err)
		}
		return nil
	}
}

func (c *Command) drainNode() error {
	defer close(c.drainDone)

	nodeRef := c.Nano().NodeRef()
	if nodeRef == nil {
		return fmt.Errorf("node reference not found")
	}
	ctx, cancel := context.WithTimeout(context.Background(), handlers.MaxWatchTimeout)
	defer cancel()
	client := c.Nano().Client()

	node := corev1ac.Node(nodeRef.Name).WithSpec(corev1ac.NodeSpec().WithUnschedulable(true))
	if _, err := client.CoreV1().Nodes().Apply(ctx, node, metav1.ApplyOptions{FieldManager: c.Name(), Force: true}); err != nil {
		return fmt.Errorf("cordon node: %w", err)
	}
	pods, err := client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + nodeRef.Name,
	})
	if err != nil {
		return fmt.Errorf("list pods on node: %w", err)
	}
	pending := make([]corev1.Pod, 0, len(pods.Items))
	for _, pod := range pods.Items {
		if pod.Annotations[corev1.MirrorPodAnnotationKey] != "" {
			continue
		}
		if slices.ContainsFunc(pod.OwnerReferences, func(o metav1.OwnerReference) bool {
			return o.Kind == "DaemonSet"
		}) {
			continue
		}
		eviction := &policyv1.Eviction{
			ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace},
		}
		if err := client.PolicyV1().Evictions(pod.Namespace).Evict(ctx, eviction); err != nil {
			klog.ErrorS(err, "evict pod failed", "pod", klog.KObj(&pod))
			continue
		}
		pending = append(pending, pod)
	}

	// Wait for the kubelet to actually tear down — Eviction only schedules
	// deletion; containers don't disappear until the kubelet sync loop
	// runs and the runtime removes them.
	if err := wait.PollUntilContextCancel(ctx, 200*time.Millisecond, true, func(ctx context.Context) (bool, error) {
		remaining := pending[:0]
		for _, pod := range pending {
			_, err := client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				continue
			}
			remaining = append(remaining, pod)
		}
		pending = remaining
		return len(pending) == 0, nil
	}); err != nil {
		klog.ErrorS(err, "drain wait timed out", "node", nodeRef.Name, "remaining", len(pending))
	}
	return nil
}

func (c *Command) runEHook(waitCh <-chan struct{}, cmd *cobra.Command) startHookFn {
	klog.InfoS("registering RunE hook", "hook", cmd.Name())
	return func(errFn func(error), doneFn func()) func(hookCtx) error {
		return func(hctx hookCtx) error {
			go func() {
				klog.InfoS("waiting to start command", "command", cmd.Name())
				if waitCh != nil {
					<-waitCh
				}

				ctx, cancel := context.WithCancel(context.Background())
				cmd.SetContext(ctx)
				go func() {
					<-c.drainDone
					cancel()
				}()

				defer doneFn()
				klog.InfoS("starting command", "command", cmd.Name())
				if err := cmd.RunE(cmd, []string{}); err != nil {
					klog.ErrorS(err, "errored", "command", cmd.Name())
					if errFn != nil {
						errFn(err)
					}
				}
			}()
			return nil
		}
	}
}

func startHook(name string, waitCh <-chan struct{}, fn func(hookCtx) error) startHookFn {
	klog.InfoS("registering start hook", "hook", name)
	return func(errFn func(error), doneFn func()) func(hookCtx) error {
		return func(hctx hookCtx) error {
			go func() {
				klog.InfoS("waiting to start hook", "hook", name)
				if waitCh != nil {
					<-waitCh
				}
				defer doneFn()
				klog.InfoS("starting hook", "hook", name)
				if err := fn(hctx); err != nil {
					klog.ErrorS(err, "errored", "hook", name)
					if errFn != nil {
						errFn(err)
					}
				}
			}()
			return nil
		}
	}
}

func stopHook(name string, fn func() error) stopHookFn {
	klog.InfoS("registering stop hook", "hook", name)
	return func() error {
		if err := fn(); err != nil {
			klog.ErrorS(err, "errored", "pre-shutdown hook", name)
			return err
		}
		return nil
	}
}
