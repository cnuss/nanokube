package crid

import (
	"context"
	"crypto/tls"
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/cnuss/nanokube/pkg/config"
	"github.com/cnuss/nanokube/pkg/crid/backend"
	_ "github.com/cnuss/nanokube/pkg/crid/docker"
	_ "github.com/cnuss/nanokube/pkg/crid/podman"
	clientset "k8s.io/client-go/kubernetes"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
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

	kubeconfig     string
	kubeconfigOnce sync.Once

	certs     *config.Certs
	certsOnce sync.Once

	dataDirs     config.DataDirs
	dataDirsOnce sync.Once
}

func NewCRID(ctx context.Context, name, dataDir string, clean bool) *CRID {
	log := component.NewLogger("crid")
	log.Debug().Msg("initializing")
	return &CRID{ctx: ctx, log: log, name: name, dataDir: dataDir, clean: clean}
}

func (c *CRID) Start(ctx context.Context) (component.Started, error) {
	c.log.Debug().Msg("starting")

	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.stop = context.AfterFunc(c.ctx, func() { cancel() })

	c.broadcaster = record.NewBroadcaster(record.WithContext(ctx))

	if len(c.Backends()) == 0 {
		supported := make([]string, 0, len(backend.Runtimes))
		for r := range backend.Runtimes {
			supported = append(supported, string(r))
		}
		return nil, fmt.Errorf("no container runtimes detected (supported: %s)", strings.Join(supported, ", "))
	}

	for name, backend := range c.Backends() {
		if err := backend.Start(ctx, c.Hosts(), c.broadcaster, filepath.Join(c.dataDir, "plugins"), filepath.Join(c.dataDir, "plugins_registry"), c.clean); err != nil {
			cancel()
			return nil, fmt.Errorf("backend %s: %w", name, err)
		}
	}

	return component.Ready(), nil
}

func (c *CRID) Stop(ctx context.Context) component.Stopped {
	c.log.Debug().Msg("stopping")

	return component.NotReady(func() {
		// Stop backends — shuts down gRPC/CRI, preventing the kubelet
		// from creating new containers during final cleanup.
		for name, backend := range c.Backends() {
			if err := backend.Stop(ctx); err != nil {
				c.log.Error().Str("backend", string(name)).Err(err).Msg("error stopping backend")
				continue
			}
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
		if c.broadcaster != nil {
			c.log.Debug().Msg("starting event broadcaster")
			c.broadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: client.CoreV1().Events("")})
		}

		for _, b := range c.Backends() {
			c.log.Debug().Str("backend", string(b.Name())).Msg("starting CSI provisioner for backend")
			b.WithKubeClient(client).CSI().StartProvisioner(c.ctx, client, b.Name() == c.DefaultBackend().Name())
		}
	}()
}

func (c *CRID) Context() context.Context { return c.ctx }
func (c *CRID) Name() string             { return c.name }
func (c *CRID) DataDir() string          { return c.dataDir }
func (c *CRID) DataDirs() config.DataDirs {
	c.dataDirsOnce.Do(func() {
		c.dataDirs = config.NewDataDirs(c.name, c.dataDir)
	})
	return c.dataDirs
}

func (c *CRID) Kubeconfig() string {
	c.kubeconfigOnce.Do(func() {
		hostname := c.Hosts().Hostname()
		c.kubeconfig = filepath.Join(c.dataDir, "kubeconfig")

		cfg := clientcmdapi.Config{
			Clusters: map[string]*clientcmdapi.Cluster{
				c.name: {
					Server:                   fmt.Sprintf("https://%s:443", hostname),
					CertificateAuthorityData: c.Certs().Cert(),
				},
			},
			AuthInfos: map[string]*clientcmdapi.AuthInfo{
				c.name: {
					ClientCertificateData: c.Certs().Cert(),
					ClientKeyData:         c.Certs().Key(),
				},
			},
			Contexts: map[string]*clientcmdapi.Context{
				c.name: {
					Cluster:  c.name,
					AuthInfo: c.name,
				},
			},
			CurrentContext: c.name,
		}

		c.log.Debug().Str("path", c.kubeconfig).Msg("writing kubeconfig")
		clientcmd.WriteToFile(cfg, c.kubeconfig)

		// Merge into ~/.kube/config
		recommendedPath := clientcmd.RecommendedHomeFile
		dst, err := clientcmd.LoadFromFile(recommendedPath)
		if err != nil {
			dst = clientcmdapi.NewConfig()
		}
		dst.Clusters[c.name] = cfg.Clusters[c.name]
		dst.AuthInfos[c.name] = cfg.AuthInfos[c.name]
		dst.Contexts[c.name] = cfg.Contexts[c.name]
		dst.CurrentContext = c.name
		if err := clientcmd.WriteToFile(*dst, recommendedPath); err != nil {
			c.log.Warn().Err(err).Str("path", recommendedPath).Msg("failed to merge ~/.kube/config")
		} else {
			c.log.Debug().Str("path", recommendedPath).Str("context", c.name).Msg("merged kubeconfig")
		}
	})
	return c.kubeconfig
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

	for runtime, detect := range backend.Runtimes {
		if b := detect(ctx, name, dataDir); b != nil {
			backends[runtime] = b
		}
	}

	return backends
}

var _ component.Component = &CRID{}
