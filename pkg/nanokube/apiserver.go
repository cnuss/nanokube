package nanokube

import (
	"context"
	"crypto/x509"
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"

	apiextensionsapiserver "k8s.io/apiextensions-apiserver/pkg/apiserver"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/storage"
	"k8s.io/apiserver/pkg/util/webhook"
	"k8s.io/kube-aggregator/pkg/apiserver"
	aggregatorscheme "k8s.io/kube-aggregator/pkg/apiserver/scheme"
	"k8s.io/kubernetes/cmd/kube-apiserver/app"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/controlplane"
	controlplaneapiserver "k8s.io/kubernetes/pkg/controlplane/apiserver"
	generatedopenapi "k8s.io/kubernetes/pkg/generated/openapi"

	v1 "github.com/cnuss/nanokube/pkg/v1"
)

type ApiServerImpl struct {
	ctx     context.Context
	kubelet v1.Kubelet

	appConfig     *app.Config
	appConfigOnce sync.Once

	caCerts     []*x509.Certificate
	caCertsOnce sync.Once

	client      v1.Client
	clientOnce  sync.Once
	clientReady chan struct{}

	ready     chan struct{}
	readyOnce sync.Once

	done chan struct{}

	serverConfig          *server.Config
	sharedInformerFactory informers.SharedInformerFactory
	defaultStorageFactory *storage.DefaultStorageFactory
	genericConfigOnce     sync.Once

	controlplaneConfig      *controlplane.Config
	serviceResolver         apiserver.ServiceResolver
	pluginInitializers      []admission.PluginInitializer
	kubeAPIServerConfigOnce sync.Once

	apiExtensions     *apiextensionsapiserver.Config
	apiExtensionsOnce sync.Once

	apiServerConfig     *apiserver.Config
	apiServerConfigOnce sync.Once

	apiAggregator     *apiserver.APIAggregator
	apiAggregatorOnce sync.Once
}

var _ v1.ApiServer = &ApiServerImpl{}

func NewApiServer(kubelet v1.Kubelet) v1.ApiServer {
	ctx, cancel := context.WithCancel(context.Background())
	context.AfterFunc(kubelet.Context(), cancel)

	return &ApiServerImpl{
		ctx:         ctx,
		kubelet:     kubelet,
		ready:       make(chan struct{}),
		clientReady: make(chan struct{}),
		done:        make(chan struct{}),
	}
}

func (a *ApiServerImpl) Context() context.Context {
	return a.ctx
}

func (a *ApiServerImpl) Ready() <-chan struct{} {
	a.readyOnce.Do(func() {
		go func() {
			<-Await(a.ctx, a.clientReady)
			for {
				if _, err := a.client.CoreV1().Namespaces().Get(a.ctx, "kube-system", metav1.GetOptions{}); err == nil {
					Log.Info("apiserver is ready")
					close(a.ready)
					break
				}
				select {
				case <-a.ctx.Done():
					return
				case <-time.After(1 * time.Second):
				}
			}
		}()
	})
	return a.ready
}

func (a *ApiServerImpl) Done() <-chan struct{} {
	return a.done
}

func (a *ApiServerImpl) genericConfig() (*server.Config, informers.SharedInformerFactory, *storage.DefaultStorageFactory) {
	a.genericConfigOnce.Do(func() {
		opts := a.kubelet.Kube().ApiServerOptions()
		genericConfig, versionedInformers, storageFactory, err := controlplaneapiserver.BuildGenericConfig(
			opts.CompletedOptions,
			[]*runtime.Scheme{legacyscheme.Scheme, apiextensionsapiserver.Scheme, aggregatorscheme.Scheme},
			controlplane.DefaultAPIResourceConfigSource(),
			generatedopenapi.GetOpenAPIDefinitions,
		)
		if err != nil {
			a.kubelet.Cancel(NewError(fmt.Errorf("failed to build generic config: %w", err)))
			return
		}
		a.serverConfig = genericConfig
		a.sharedInformerFactory = versionedInformers
		a.defaultStorageFactory = a.kubelet.Storage().WithFactory(storageFactory).Factory()
	})
	return a.serverConfig, a.sharedInformerFactory, a.defaultStorageFactory
}

func (a *ApiServerImpl) ServerConfig() *server.Config {
	serverConfig, _, _ := a.genericConfig()
	return serverConfig
}

func (a *ApiServerImpl) SharedInformerFactory() informers.SharedInformerFactory {
	_, versionedInformers, _ := a.genericConfig()
	return versionedInformers
}

func (a *ApiServerImpl) DefaultStorageFactory() *storage.DefaultStorageFactory {
	_, _, storageFactory := a.genericConfig()
	return a.kubelet.Storage().WithFactory(storageFactory).Factory()
}

func (a *ApiServerImpl) kubeAPIServerConfig() (*controlplane.Config, apiserver.ServiceResolver, []admission.PluginInitializer) {
	a.kubeAPIServerConfigOnce.Do(func() {
		opts := a.kubelet.Kube().ApiServerOptions()
		controlplaneConfig, serviceResolver, pluginInitializers, err := app.CreateKubeAPIServerConfig(*opts, a.ServerConfig(), a.SharedInformerFactory(), a.DefaultStorageFactory())
		if err != nil {
			a.kubelet.Cancel(NewError(fmt.Errorf("failed to create kube-apiserver config: %w", err)))
			return
		}
		a.controlplaneConfig = controlplaneConfig
		a.serviceResolver = serviceResolver
		a.pluginInitializers = pluginInitializers
	})
	return a.controlplaneConfig, a.serviceResolver, a.pluginInitializers
}

func (a *ApiServerImpl) ControlPlaneConfig() *controlplane.Config {
	controlplaneConfig, _, _ := a.kubeAPIServerConfig()
	controlplaneConfig.ControlPlane.StorageFactory = a.kubelet.Storage()
	return controlplaneConfig
}

func (a *ApiServerImpl) ServiceResolver() apiserver.ServiceResolver {
	_, serviceResolver, _ := a.kubeAPIServerConfig()
	return serviceResolver
}

func (a *ApiServerImpl) PluginInitializers() []admission.PluginInitializer {
	_, _, pluginInitializers := a.kubeAPIServerConfig()
	return pluginInitializers
}

func (a *ApiServerImpl) ApiExtensions() *apiextensionsapiserver.Config {
	a.apiExtensionsOnce.Do(func() {
		opts := a.kubelet.Kube().ApiServerOptions()
		apiExtensions, err := controlplaneapiserver.CreateAPIExtensionsConfig(*a.ControlPlaneConfig().ControlPlane.Generic, a.ControlPlaneConfig().ControlPlane.VersionedInformers, a.PluginInitializers(), opts.CompletedOptions, opts.MasterCount,
			a.ServiceResolver(), webhook.NewDefaultAuthenticationInfoResolverWrapper(a.ControlPlaneConfig().ControlPlane.ProxyTransport, a.ControlPlaneConfig().ControlPlane.Generic.EgressSelector, a.ControlPlaneConfig().ControlPlane.Generic.LoopbackClientConfig, a.ControlPlaneConfig().ControlPlane.Generic.TracerProvider))
		if err != nil {
			a.kubelet.Cancel(NewError(fmt.Errorf("failed to create API extensions config: %w", err)))
			return
		}
		a.apiExtensions = apiExtensions
	})
	return a.apiExtensions
}

func (a *ApiServerImpl) ApiServerConfig() *apiserver.Config {
	a.apiServerConfigOnce.Do(func() {
		opts := a.kubelet.Kube().ApiServerOptions()
		apiServerConfig, err := controlplaneapiserver.CreateAggregatorConfig(*a.ControlPlaneConfig().ControlPlane.Generic, opts.CompletedOptions, a.ControlPlaneConfig().ControlPlane.VersionedInformers, a.ServiceResolver(), a.ControlPlaneConfig().ControlPlane.ProxyTransport, a.ControlPlaneConfig().ControlPlane.Extra.PeerProxy, a.PluginInitializers())
		if err != nil {
			a.kubelet.Cancel(NewError(fmt.Errorf("failed to create aggregator config: %w", err)))
			return
		}
		a.apiServerConfig = apiServerConfig
	})
	return a.apiServerConfig
}

func (a *ApiServerImpl) APIAggregator() *apiserver.APIAggregator {
	a.apiAggregatorOnce.Do(func() {
		config := &app.Config{
			Options:       *a.kubelet.Kube().ApiServerOptions(),
			KubeAPIs:      a.ControlPlaneConfig(),
			ApiExtensions: a.ApiExtensions(),
			Aggregator:    a.ApiServerConfig(),
		}
		completed, err := config.Complete()
		if err != nil {
			a.kubelet.Cancel(NewError(fmt.Errorf("failed to complete API server config: %w", err)))
			return
		}

		server, err := app.CreateServerChain(completed)
		if err != nil {
			a.kubelet.Cancel(NewError(fmt.Errorf("failed to create API server: %w", err)))
			return
		}

		<-Await(a.ctx, a.kubelet.Storage().Ready())

		prepared, err := server.PrepareRun()
		if err != nil {
			a.kubelet.Cancel(NewError(fmt.Errorf("failed to prepare API server: %w", err)))
			return
		}

		a.apiAggregator = prepared.APIAggregator

		go func() {
			defer close(a.done)
			gs := a.apiAggregator.GenericAPIServer
			defer gs.Destroy()

			// TODO(partial):Inlined from preparedGenericAPIServer.RunWithContext. Skipped, all afe for single-replica no-LB no-audit:
			// - ShutdownDelay: gives an upstream LB time to drain this replica before the listener closes.
			// - SendRetryAfter: answers new requests with 429+Retry-After during drain instead of dropping.
			// - Active-Watch Drain: rate-limits closing long-running watches to spread reconnect load across replicas.
			// - MuxAndDiscoveryComplete: signal that all delegates have installed routes (closes a startup /apis 404 race).
			// - Audit Backend: structured per-request audit logging to file/webhook.
			// - UDS Profiling: pprof over a Unix-domain socket gated by filesystem ACLs.
			internalStopCh := make(chan struct{})
			stoppedCh, listenerStoppedCh, err := gs.SecureServingInfo.Serve(gs.Handler, gs.ShutdownTimeout, internalStopCh)
			if err != nil {
				close(internalStopCh)
				a.kubelet.Cancel(NewError(fmt.Errorf("apiserver listener: %w", err)).WithCode(1))
				return
			}
			go func() { <-a.ctx.Done(); close(internalStopCh) }()

			gs.RunPostStartHooks(a.ctx)

			<-a.ctx.Done()

			if err := gs.RunPreShutdownHooks(); err != nil {
				Log.Warn("apiserver pre-shutdown hooks failed", "error", err)
			}

			<-listenerStoppedCh
			<-stoppedCh
			Log.Info("apiserver shut down")
		}()
	})
	return a.apiAggregator
}

func (a *ApiServerImpl) Client() v1.Client {
	a.clientOnce.Do(func() {
		loopback := a.APIAggregator().GenericAPIServer.LoopbackClientConfig
		a.client = NewClient(loopback)
		close(a.clientReady)
	})

	<-Await(a.ctx, a.clientReady, a.Ready())
	return a.client
}

func (a *ApiServerImpl) CACerts() []*x509.Certificate {
	a.caCertsOnce.Do(func() {
		a.caCerts = a.kubelet.Tunnel(v1.KubeletTunnel).CACerts()
		// TODO(incomplete): add generated apiserver.crt to CA certs
	})
	return a.caCerts
}
