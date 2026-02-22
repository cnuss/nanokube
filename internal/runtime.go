package internal

import (
	"context"

	"github.com/rs/zerolog/log"
)

type Runtime struct {
	ctx context.Context
}

func NewRuntime(ctx context.Context) *Runtime {
	r := &Runtime{ctx: ctx}
	log.Info().Str("runtime", r.Name()).Msg("using container runtime")
	return r
}

func (r *Runtime) Name() string {
	return "cri-dockerd"
}

func (r *Runtime) ContainerRuntimeEndpoint() string {
	return "unix:///var/run/cri-dockerd.sock"
}
