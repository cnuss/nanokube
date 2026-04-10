package nanokube

import (
	"context"

	"k8s.io/kubernetes/pkg/controlplane/apiserver/options"
)

type ApiServerDeps struct{}

type HollowApiServer struct{}

func NewHollowApiServer(ctx context.Context, opts *options.Options) *HollowApiServer {
	return &HollowApiServer{}
}

func (h *HollowApiServer) Run(ctx context.Context) {
	select {}
}
