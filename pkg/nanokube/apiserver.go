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

func (h *ApiServerImpl) Context() context.Context {
	return h.ctx
}

func (h *ApiServerImpl) Ready() <-chan struct{} {
	return h.ready
}

func (h *ApiServerImpl) Done() <-chan struct{} {
	return h.done
}

func (h *ApiServerImpl) genericConfig() (*server.Config, informers.SharedInformerFactory, *storage.DefaultStorageFactory) {
	h.genericConfigOnce.Do(func() {
		opts := h.kubelet.Kube().ApiServerOptions()
		genericConfig, versionedInformers, storageFactory, err := controlplaneapiserver.BuildGenericConfig(
			opts.CompletedOptions,
			[]*runtime.Scheme{legacyscheme.Scheme, apiextensionsapiserver.Scheme, aggregatorscheme.Scheme},
			controlplane.DefaultAPIResourceConfigSource(),
			generatedopenapi.GetOpenAPIDefinitions,
		)
		if err != nil {
			h.kubelet.Cancel(NewError(fmt.Errorf("failed to build generic config: %w", err)))
			return
		}
		h.serverConfig = genericConfig
		h.sharedInformerFactory = versionedInformers
		h.defaultStorageFactory = h.kubelet.Storage().WithFactory(storageFactory).Factory()
	})
	return h.serverConfig, h.sharedInformerFactory, h.defaultStorageFactory
}

func (h *ApiServerImpl) ServerConfig() *server.Config {
	serverConfig, _, _ := h.genericConfig()
	return serverConfig
}

func (h *ApiServerImpl) SharedInformerFactory() informers.SharedInformerFactory {
	_, versionedInformers, _ := h.genericConfig()
	return versionedInformers
}

func (h *ApiServerImpl) DefaultStorageFactory() *storage.DefaultStorageFactory {
	_, _, storageFactory := h.genericConfig()
	return h.kubelet.Storage().WithFactory(storageFactory).Factory()
}

func (h *ApiServerImpl) kubeAPIServerConfig() (*controlplane.Config, apiserver.ServiceResolver, []admission.PluginInitializer) {
	h.kubeAPIServerConfigOnce.Do(func() {
		opts := h.kubelet.Kube().ApiServerOptions()
		controlplaneConfig, serviceResolver, pluginInitializers, err := app.CreateKubeAPIServerConfig(*opts, h.ServerConfig(), h.SharedInformerFactory(), h.DefaultStorageFactory())
		if err != nil {
			h.kubelet.Cancel(NewError(fmt.Errorf("failed to create kube-apiserver config: %w", err)))
			return
		}
		h.controlplaneConfig = controlplaneConfig
		h.serviceResolver = serviceResolver
		h.pluginInitializers = pluginInitializers
	})
	return h.controlplaneConfig, h.serviceResolver, h.pluginInitializers
}

func (h *ApiServerImpl) ControlPlaneConfig() *controlplane.Config {
	controlplaneConfig, _, _ := h.kubeAPIServerConfig()
	controlplaneConfig.ControlPlane.StorageFactory = h.kubelet.Storage()
	return controlplaneConfig
}

func (h *ApiServerImpl) ServiceResolver() apiserver.ServiceResolver {
	_, serviceResolver, _ := h.kubeAPIServerConfig()
	return serviceResolver
}

func (h *ApiServerImpl) PluginInitializers() []admission.PluginInitializer {
	_, _, pluginInitializers := h.kubeAPIServerConfig()
	return pluginInitializers
}

func (h *ApiServerImpl) ApiExtensions() *apiextensionsapiserver.Config {
	h.apiExtensionsOnce.Do(func() {
		opts := h.kubelet.Kube().ApiServerOptions()
		apiExtensions, err := controlplaneapiserver.CreateAPIExtensionsConfig(*h.ControlPlaneConfig().ControlPlane.Generic, h.ControlPlaneConfig().ControlPlane.VersionedInformers, h.PluginInitializers(), opts.CompletedOptions, opts.MasterCount,
			h.ServiceResolver(), webhook.NewDefaultAuthenticationInfoResolverWrapper(h.ControlPlaneConfig().ControlPlane.ProxyTransport, h.ControlPlaneConfig().ControlPlane.Generic.EgressSelector, h.ControlPlaneConfig().ControlPlane.Generic.LoopbackClientConfig, h.ControlPlaneConfig().ControlPlane.Generic.TracerProvider))
		if err != nil {
			h.kubelet.Cancel(NewError(fmt.Errorf("failed to create API extensions config: %w", err)))
			return
		}
		h.apiExtensions = apiExtensions
	})
	return h.apiExtensions
}

func (h *ApiServerImpl) ApiServerConfig() *apiserver.Config {
	h.apiServerConfigOnce.Do(func() {
		opts := h.kubelet.Kube().ApiServerOptions()
		apiServerConfig, err := controlplaneapiserver.CreateAggregatorConfig(*h.ControlPlaneConfig().ControlPlane.Generic, opts.CompletedOptions, h.ControlPlaneConfig().ControlPlane.VersionedInformers, h.ServiceResolver(), h.ControlPlaneConfig().ControlPlane.ProxyTransport, h.ControlPlaneConfig().ControlPlane.Extra.PeerProxy, h.PluginInitializers())
		if err != nil {
			h.kubelet.Cancel(NewError(fmt.Errorf("failed to create aggregator config: %w", err)))
			return
		}
		h.apiServerConfig = apiServerConfig
	})
	return h.apiServerConfig
}

func (h *ApiServerImpl) Tunnel() v1.Tunnel {
	return h.kubelet.Tunnel(v1.APIServerService)
}

func (h *ApiServerImpl) Client() v1.Client {
	h.runOnce.Do(func() {
		config := &app.Config{
			Options:       *h.kubelet.Kube().ApiServerOptions(),
			KubeAPIs:      h.ControlPlaneConfig(),
			ApiExtensions: h.ApiExtensions(),
			Aggregator:    h.ApiServerConfig(),
		}
		completed, err := config.Complete()
		if err != nil {
			h.kubelet.Cancel(NewError(fmt.Errorf("failed to complete API server config: %w", err)))
			return
		}

		server, err := app.CreateServerChain(completed)
		if err != nil {
			h.kubelet.Cancel(NewError(fmt.Errorf("failed to create API server: %w", err)))
			return
		}

		<-Await(h.ctx, h.kubelet.Storage().Ready())

		prepared, err := server.PrepareRun()
		if err != nil {
			h.kubelet.Cancel(NewError(fmt.Errorf("failed to prepare API server: %w", err)))
			return
		}

		loopback := prepared.GenericAPIServer.LoopbackClientConfig
		h.client = NewClient(loopback)

		go func() {
			for {
				if _, err := h.client.CoreV1().Namespaces().Get(h.ctx, "kube-system", metav1.GetOptions{}); err == nil {
					Log.Info("apiserver is ready")
					close(h.ready)
					break
				}
				select {
				case <-h.ctx.Done():
					return
				case <-time.After(1 * time.Second):
				}
			}
		}()

		go func() {
			defer close(h.done)
			if err := prepared.Run(h.ctx); err != nil {
				h.kubelet.Cancel(NewError(fmt.Errorf("API server exited with error: %w", err)).WithCode(1))
			}
			Log.Info("apiserver shut down")
		}()
	})

	<-h.Ready()
	return h.client
}

func (h *ApiServerImpl) CACerts() []*x509.Certificate {
	h.caCertsOnce.Do(func() {
		h.caCerts = h.kubelet.Tunnel(v1.APIServerService).CACerts()
		// TODO(incomplete): add generated apiserver.crt to CA certs
	})
	return h.caCerts
}
