package nanokube

import (
	"context"
	"crypto/x509"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"

	apiextensionsapiserver "k8s.io/apiextensions-apiserver/pkg/apiserver"
	"k8s.io/apiserver/pkg/util/webhook"
	aggregatorscheme "k8s.io/kube-aggregator/pkg/apiserver/scheme"
	"k8s.io/kubernetes/cmd/kube-apiserver/app"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/controlplane"
	controlplaneapiserver "k8s.io/kubernetes/pkg/controlplane/apiserver"
	generatedopenapi "k8s.io/kubernetes/pkg/generated/openapi"

	v1 "github.com/cnuss/nanokube/pkg/v1"
)

type ApiServerImpl struct {
	kubelet   v1.Kubelet
	appConfig *app.Config
	storage   v1.Storage

	client v1.Client

	caCerts     []*x509.Certificate
	caCertsOnce sync.Once

	runOnce sync.Once
	ready   chan struct{}
}

var _ v1.ApiServer = &ApiServerImpl{}

func NewApiServer(kubelet v1.Kubelet) v1.ApiServer {
	opts := kubelet.Kube().ApiServerOptions()
	c := &app.Config{
		Options: *opts,
	}

	genericConfig, versionedInformers, storageFactory, err := controlplaneapiserver.BuildGenericConfig(
		opts.CompletedOptions,
		[]*runtime.Scheme{legacyscheme.Scheme, apiextensionsapiserver.Scheme, aggregatorscheme.Scheme},
		controlplane.DefaultAPIResourceConfigSource(),
		generatedopenapi.GetOpenAPIDefinitions,
	)
	if err != nil {
		klog.Fatalf("Failed to build generic config: %v", err)
	}

	storage := kubelet.Kube().Storage().WithFactory(storageFactory)

	kubeAPIs, serviceResolver, pluginInitializer, err := app.CreateKubeAPIServerConfig(c.Options, genericConfig, versionedInformers, storage.Factory())
	if err != nil {
		klog.Fatalf("Failed to create kube-apiserver config: %v", err)
	}
	c.KubeAPIs = kubeAPIs

	apiExtensions, err := controlplaneapiserver.CreateAPIExtensionsConfig(*kubeAPIs.ControlPlane.Generic, kubeAPIs.ControlPlane.VersionedInformers, pluginInitializer, opts.CompletedOptions, opts.MasterCount,
		serviceResolver, webhook.NewDefaultAuthenticationInfoResolverWrapper(kubeAPIs.ControlPlane.ProxyTransport, kubeAPIs.ControlPlane.Generic.EgressSelector, kubeAPIs.ControlPlane.Generic.LoopbackClientConfig, kubeAPIs.ControlPlane.Generic.TracerProvider))
	if err != nil {
		klog.Fatalf("Failed to create API extensions config: %v", err)
	}
	c.ApiExtensions = apiExtensions

	aggregator, err := controlplaneapiserver.CreateAggregatorConfig(*kubeAPIs.ControlPlane.Generic, opts.CompletedOptions, kubeAPIs.ControlPlane.VersionedInformers, serviceResolver, kubeAPIs.ControlPlane.ProxyTransport, kubeAPIs.ControlPlane.Extra.PeerProxy, pluginInitializer)
	if err != nil {
		klog.Fatalf("Failed to create aggregator config: %v", err)
	}
	c.Aggregator = aggregator

	return &ApiServerImpl{
		kubelet:   kubelet,
		appConfig: c,
		storage:   storage,
		ready:     make(chan struct{}),
	}
}

func (h *ApiServerImpl) Ready() <-chan struct{} {
	return h.ready
}

func (h *ApiServerImpl) Config() *app.Config {
	return h.appConfig
}

func (h *ApiServerImpl) Tunnel() v1.Tunnel {
	return h.kubelet.Tunnel(v1.APIServerService)
}

func (h *ApiServerImpl) Client(ctx context.Context) v1.Client {
	<-h.storage.Ready()

	h.runOnce.Do(func() {
		completed, err := h.appConfig.Complete()
		if err != nil {
			klog.Fatalf("Failed to complete API server config: %v", err)
		}

		server, err := app.CreateServerChain(completed)
		if err != nil {
			klog.Fatalf("Failed to create API server: %v", err)
		}

		prepared, err := server.PrepareRun()
		if err != nil {
			klog.Fatalf("Failed to prepare API server: %v", err)
		}

		loopback := prepared.GenericAPIServer.LoopbackClientConfig
		h.client = NewClient(loopback)

		go func() {
			for {
				if _, err := h.client.CoreV1().Namespaces().Get(ctx, "kube-system", metav1.GetOptions{}); err == nil {
					Log.Info("apiserver is ready")
					close(h.ready)
					break
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(1 * time.Second):
				}
			}
		}()

		go func() {
			if err := prepared.Run(ctx); err != nil {
				klog.Fatalf("API server exited with error: %v", err)
			}
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
