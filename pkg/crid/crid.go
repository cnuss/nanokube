package crid

import (
	"context"
	"fmt"
	"time"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/cnuss/nanokube/pkg/crid/backend"
	"github.com/cnuss/nanokube/pkg/crid/docker"
	"github.com/cnuss/nanokube/pkg/crid/podman"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
)

type CRID struct {
	ctx    context.Context
	cancel context.CancelFunc
	stop   func() bool
	log    component.Logger

	backends    map[backend.Runtime]backend.Backend
	broadcaster record.EventBroadcaster
}

var _ component.Component = &CRID{}

func NewCRID(ctx context.Context, dataDir string) *CRID {
	log := component.NewLogger("crid")
	log.Info().Msg("initializing")

	crid := &CRID{ctx: ctx, log: log, backends: detectBackends(ctx, dataDir)}
	return crid
}

func (c *CRID) Start(ctx context.Context) (component.Started, error) {
	c.log.Info().Msg("starting")

	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.stop = context.AfterFunc(c.ctx, func() { cancel() })

	c.broadcaster = record.NewBroadcaster(record.WithContext(ctx))

	for name, backend := range c.backends {
		c.log.Info().Str("backend", string(name)).Msg("starting backend")
		if err := backend.Start(ctx, c.broadcaster); err != nil {
			cancel()
			return nil, fmt.Errorf("backend %s: %w", name, err)
		}
		c.log.Info().Str("backend", string(name)).Msg("backend started")
	}

	return component.Ready(), nil
}

func (c *CRID) Stop() component.Stopped {
	c.log.Info().Msg("stopping")

	return component.NotReady(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if c.broadcaster != nil {
			c.broadcaster.Shutdown()
		}

		for name, backend := range c.backends {
			c.log.Info().Str("backend", string(name)).Msg("stopping backend")
			if err := backend.Stop(ctx); err != nil {
				c.log.Error().Str("backend", string(name)).Err(err).Msg("error stopping backend")
				continue
			}
			c.log.Info().Str("backend", string(name)).Msg("backend stopped")
		}

		if c.stop != nil {
			c.stop()
		}
		if c.cancel != nil {
			c.cancel()
		}
	})
}

func (c *CRID) WithClient(client clientset.Interface) *CRID {
	if c.broadcaster != nil {
		c.broadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: client.CoreV1().Events("")})
		c.log.Info().Msg("event sink connected")
	}
	return c
}

func (c *CRID) Backends() map[backend.Runtime]backend.Backend {
	return c.backends
}

func (c *CRID) Backend(runtime backend.Runtime) backend.Backend {
	if b, ok := c.backends[runtime]; ok {
		return b
	}
	c.log.Warn().Str("runtime", string(runtime)).Msg("requested backend not found, using noop")
	return &backend.NoopBackend{}
}

func (c *CRID) DefaultBackend() backend.Backend {
	for _, b := range c.backends {
		return b
	}
	c.log.Warn().Msg("no backends found, using noop")
	return &backend.NoopBackend{}
}

func detectBackends(ctx context.Context, dataDir string) map[backend.Runtime]backend.Backend {
	backends := make(map[backend.Runtime]backend.Backend)

	if b := docker.Detect(ctx, dataDir); b != nil {
		backends[backend.Docker] = b
	}

	if b := podman.Detect(ctx, dataDir); b != nil {
		backends[backend.Podman] = b
	}

	return backends
}
