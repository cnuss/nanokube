package nanokube

import (
	"context"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"

	apiextensionsapiserver "k8s.io/apiextensions-apiserver/pkg/apiserver"
	"k8s.io/apiserver/pkg/util/webhook"
	aggregatorscheme "k8s.io/kube-aggregator/pkg/apiserver/scheme"
	"k8s.io/kubernetes/cmd/kube-apiserver/app"
	"k8s.io/kubernetes/cmd/kube-apiserver/app/options"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/controlplane"
	controlplaneapiserver "k8s.io/kubernetes/pkg/controlplane/apiserver"
	generatedopenapi "k8s.io/kubernetes/pkg/generated/openapi"
)

type ApiServer interface {
	Client(ctx context.Context) Client
	Config() *app.Config
	Ready() chan struct{}
}

type ApiServerImpl struct {
	config  *app.Config
	storage StorageFactory

	client Client

	runOnce sync.Once
	ready   chan struct{}
}

var _ ApiServer = &ApiServerImpl{}

func NewApiServer(opts *options.CompletedOptions, storage StorageFactory) ApiServer {
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

	kubeAPIs, serviceResolver, pluginInitializer, err := app.CreateKubeAPIServerConfig(c.Options, genericConfig, versionedInformers, storage.WithDefault(storageFactory).Default())
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
		config:  c,
		storage: storage,
		ready:   make(chan struct{}),
	}
}

func (h *ApiServerImpl) Ready() chan struct{} {
	return h.ready
}

func (h *ApiServerImpl) Config() *app.Config {
	return h.config
}

func (h *ApiServerImpl) Client(ctx context.Context) Client {
	<-h.storage.Ready()

	h.runOnce.Do(func() {
		completed, err := h.config.Complete()
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
				if version, err := h.client.Discovery().ServerVersion(); err == nil {
					Log.Info("apiserver is ready", "host", loopback.Host, "version", version.GitVersion)
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
