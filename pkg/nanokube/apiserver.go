package nanokube

import (
	"context"

	"k8s.io/apiserver/pkg/server"
	"k8s.io/klog/v2"
	"k8s.io/kube-aggregator/pkg/apiserver"

	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/server/storage"
	"k8s.io/kubernetes/cmd/kube-apiserver/app"
	"k8s.io/kubernetes/cmd/kube-apiserver/app/options"
	"k8s.io/kubernetes/pkg/controlplane"
)

type ApiServer struct {
	Config       *controlplane.Config
	Resolver     apiserver.ServiceResolver
	Initializers []admission.PluginInitializer
}

func NewApiServer(opts *options.CompletedOptions, cfg *server.Config, storage *storage.DefaultStorageFactory) *ApiServer {
	controlPlaneConfig, serviceResolver, pluginInitializers, err := app.CreateKubeAPIServerConfig(
		*opts,
		cfg,
		nil, // TODO: SharedInformerFactory
		storage,
	)
	if err != nil {
		klog.Fatalf("Failed to create kube-apiserver config: %v", err)
	}

	return &ApiServer{
		Config:       controlPlaneConfig,
		Resolver:     serviceResolver,
		Initializers: pluginInitializers,
	}
}

// Run starts the API server and blocks.
func (h *ApiServer) Run(ctx context.Context) {
	select {}
}
