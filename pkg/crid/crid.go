package crid

import (
	"context"
	"crypto/tls"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/cnuss/nanokube/pkg/config"
	"github.com/cnuss/nanokube/pkg/crid/backend"
	"github.com/cnuss/nanokube/pkg/crid/docker"
	"github.com/cnuss/nanokube/pkg/crid/podman"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/kubelet/server"
)

type CRID struct {
	ctx    context.Context
	cancel context.CancelFunc
	stop   func() bool
	log    component.Logger

	name    string
	dataDir string
	clean   bool

	broadcaster record.EventBroadcaster

	backends     map[backend.Runtime]backend.Backend
	backendsOnce sync.Once

	hosts     backend.Hosts
	hostsOnce sync.Once

	files     *config.Files
	filesOnce sync.Once

	certs     *config.Certs
	certsOnce sync.Once

	dataDirs     config.DataDirs
	dataDirsOnce sync.Once
}

func NewCRID(ctx context.Context, name, dataDir string, clean bool) *CRID {
	log := component.NewLogger("crid")
	log.Info().Msg("initializing")
	return &CRID{ctx: ctx, log: log, name: name, dataDir: dataDir, clean: clean}
}

func (c *CRID) Start(ctx context.Context) (component.Started, error) {
	c.log.Info().Msg("starting")

	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.stop = context.AfterFunc(c.ctx, func() { cancel() })

	c.broadcaster = record.NewBroadcaster(record.WithContext(ctx))

	for name, backend := range c.Backends() {
		c.log.Info().Str("backend", string(name)).Msg("starting backend")
		if err := backend.Start(ctx, c.Hosts(), c.broadcaster, filepath.Join(c.dataDir, "plugins"), filepath.Join(c.dataDir, "plugins_registry"), c.clean); err != nil {
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

		// Stop backends — shuts down gRPC/CRI, preventing the kubelet
		// from creating new containers during final cleanup.
		for name, backend := range c.Backends() {
			c.log.Info().Str("backend", string(name)).Msg("stopping backend")
			if err := backend.Stop(ctx); err != nil {
				c.log.Error().Str("backend", string(name)).Err(err).Msg("error stopping backend")
				continue
			}
			c.log.Info().Str("backend", string(name)).Msg("backend stopped")
		}

		if c.broadcaster != nil {
			c.broadcaster.Shutdown()
		}
		if c.stop != nil {
			c.stop()
		}
		if c.cancel != nil {
			c.cancel()
		}
	})
}

func (c *CRID) WithKubeClient(client clientset.Interface) {
	go func() {
		c.log.Info().Msg("waiting for API server")
		for {
			if _, err := client.Discovery().ServerVersion(); err == nil {
				break
			}
			select {
			case <-c.ctx.Done():
				return
			case <-time.After(1 * time.Second):
			}
		}
		c.log.Info().Msg("API server reachable")

		// Delete stale kubernetes service if ClusterIP doesn't match the current service CIDR
		if svc := c.DefaultBackend().IPAM().Service(); svc != nil {
			if existing, err := client.CoreV1().Services("default").Get(c.ctx, "kubernetes", metav1.GetOptions{}); err == nil {
				if existing.Spec.ClusterIP != svc.IP.String() {
					c.log.Info().Str("old", existing.Spec.ClusterIP).Str("new", svc.IP.String()).Msg("deleting stale kubernetes service")
					client.CoreV1().Services("default").Delete(c.ctx, "kubernetes", metav1.DeleteOptions{})
				}
			}
		}

		if c.broadcaster != nil {
			c.broadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: client.CoreV1().Events("")})
			c.log.Info().Msg("event sink connected")
		}

		for _, b := range c.Backends() {
			b.WithKubeClient(client).CSI().StartProvisioner(c.ctx, client, b.Name() == c.DefaultBackend().Name())
		}
	}()
}

func (c *CRID) Context() context.Context { return c.ctx }
func (c *CRID) Name() string             { return c.name }
func (c *CRID) DataDir() string          { return c.dataDir }
func (c *CRID) DataDirs() config.DataDirs {
	c.dataDirsOnce.Do(func() {
		c.dataDirs = config.NewDataDirs(c.dataDir)
	})
	return c.dataDirs
}

func (c *CRID) Files() *config.Files {
	c.filesOnce.Do(func() {
		c.files = config.NewFiles(c.dataDir)
	})
	return c.files
}

func (c *CRID) Certs() *config.Certs {
	c.certsOnce.Do(func() {
		c.certs = config.NewCerts(c.name, c.dataDir, c.Hosts().Hostname(), c.DefaultBackend().IPAM().Service().IP)
	})
	return c.certs
}

func (c *CRID) TLSOptions() *server.TLSOptions {
	return &server.TLSOptions{
		Config:   &tls.Config{MinVersion: tls.VersionTLS12},
		CertFile: c.Certs().CertPath(),
		KeyFile:  c.Certs().KeyPath(),
	}
}

func (c *CRID) Backends() map[backend.Runtime]backend.Backend {
	c.backendsOnce.Do(func() {
		c.backends = detectBackends(c.ctx, c.name, c.dataDir)
	})
	return c.backends
}

func (c *CRID) Backend(runtime backend.Runtime) backend.Backend {
	if b, ok := c.Backends()[runtime]; ok {
		return b
	}
	c.log.Warn().Str("runtime", string(runtime)).Msg("requested backend not found, using noop")
	return &backend.NoopBackend{}
}

func (c *CRID) Hosts() backend.Hosts {
	c.hostsOnce.Do(func() {
		h, err := NewHosts(c.ctx, c.Backends())
		if err != nil {
			c.log.Error().Err(err).Msg("failed to initialize hosts")
			return
		}
		c.hosts = h
	})
	return c.hosts
}

func (c *CRID) DefaultBackend() backend.Backend {
	for _, b := range c.Backends() {
		return b
	}
	c.log.Warn().Msg("no backends found, using noop")
	return &backend.NoopBackend{}
}

func detectBackends(ctx context.Context, name, dataDir string) map[backend.Runtime]backend.Backend {
	backends := make(map[backend.Runtime]backend.Backend)

	if b := docker.Detect(ctx, name, dataDir); b != nil {
		backends[backend.Docker] = b
	}

	if b := podman.Detect(ctx, name, dataDir); b != nil {
		backends[backend.Podman] = b
	}

	return backends
}

var _ component.Component = &CRID{}
