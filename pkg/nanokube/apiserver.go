package nanokube

import (
	"context"
	"crypto/x509"
	"fmt"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"

	apiextensionsapiserver "k8s.io/apiextensions-apiserver/pkg/apiserver"
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
	client        v1.Client
	appConfigOnce sync.Once

	caCerts     []*x509.Certificate
	caCertsOnce sync.Once

	runOnce sync.Once
	ready   chan struct{}
	done    chan struct{}

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
}

var _ v1.ApiServer = &ApiServerImpl{}

func NewApiServer(kubelet v1.Kubelet) v1.ApiServer {
	ctx, cancel := context.WithCancel(context.Background())
	context.AfterFunc(kubelet.Context(), cancel)

	return &ApiServerImpl{
		ctx:     ctx,
		kubelet: kubelet,
		ready:   make(chan struct{}),
		done:    make(chan struct{}),
	}
}

func (a *ApiServerImpl) Context() context.Context {
	return a.ctx
}

func (a *ApiServerImpl) Ready() <-chan struct{} {
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

func (a *ApiServerImpl) Tunnel() v1.Tunnel {
	return a.kubelet.Tunnel(v1.KubeletTunnel)
}

func (a *ApiServerImpl) Client() v1.Client {
	a.runOnce.Do(func() {
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

		container := server.GenericAPIServer.Handler.GoRestfulContainer
		// ws := new(restful.WebService)
		// mux := server.GenericAPIServer.Handler.NonGoRestfulMux
		for _, ws := range a.kubelet.Services(a.Tunnel().URL()) {
			container.Add(ws)
			Log.Info("added API service", "rootPath", ws.RootPath())
		}

		<-Await(a.ctx, a.kubelet.Storage().Ready())

		prepared, err := server.PrepareRun()
		if err != nil {
			a.kubelet.Cancel(NewError(fmt.Errorf("failed to prepare API server: %w", err)))
			return
		}

		loopback := prepared.GenericAPIServer.LoopbackClientConfig
		a.client = NewClient(loopback)

		go func() {
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

		go func() {
			defer close(a.done)
			if err := prepared.Run(a.ctx); err != nil {
				a.kubelet.Cancel(NewError(fmt.Errorf("API server exited with error: %w", err)).WithCode(1))
			}
			Log.Info("apiserver shut down")
		}()
	})

	<-a.Ready()
	return a.client
}

func (a *ApiServerImpl) CACerts() []*x509.Certificate {
	a.caCertsOnce.Do(func() {
		a.caCerts = a.kubelet.Tunnel(v1.KubeletTunnel).CACerts()
		// TODO(incomplete): add generated apiserver.crt to CA certs
	})
	return a.caCerts
}
