package nanokube

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"sync"
	"time"

	"k8s.io/client-go/informers"
	client "k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/tools/record"

	"github.com/cnuss/nanokube/pkg/tunnel"
	v1 "github.com/cnuss/nanokube/pkg/v1"
)

type ClientImpl struct {
	client.Interface

	ctx        context.Context
	clientset  *client.Clientset
	config     *rest.Config
	httpClient *http.Client

	informerFactory     informers.SharedInformerFactory
	informerFactoryOnce sync.Once

	sink     record.EventSink
	sinkOnce sync.Once
}

var _ v1.Client = &ClientImpl{}

func NewClient(ctx context.Context, config *rest.Config) v1.Client {
	httpClient, err := rest.HTTPClientFor(config)
	if err != nil {
		panic(err)
	}
	cs, err := client.NewForConfigAndClient(config, httpClient)
	if err != nil {
		panic(err)
	}
	return &ClientImpl{
		ctx:        ctx,
		Interface:  cs,
		clientset:  cs,
		config:     config,
		httpClient: httpClient,
	}
}

func NewClientForKubeconfig(ctx context.Context, kubeconfig *clientcmdapi.Config) v1.Client {
	cfg, err := clientcmd.NewDefaultClientConfig(*kubeconfig, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		panic(err)
	}
	return NewClient(ctx, cfg)
}

func (c *ClientImpl) Sink() record.EventSink {
	c.sinkOnce.Do(func() {
		c.sink = &typedcorev1.EventSinkImpl{Interface: c.Interface.CoreV1().Events("")}
	})
	return c.sink
}

func (c *ClientImpl) Clientset() *client.Clientset {
	return c.clientset
}

func (c *ClientImpl) WithHeartbeat(interval time.Duration) v1.Client {
	if c.config == nil {
		return c
	}
	return c.WithQps(float32(-1)).WithTimeout(interval)
}

func (c *ClientImpl) WithQps(qps float32) v1.Client {
	if c.config == nil {
		return c
	}
	cfg := rest.CopyConfig(c.config)
	cfg.QPS = qps
	cfg.Burst = int(qps * 2)
	cs, err := client.NewForConfigAndClient(cfg, c.httpClient)
	if err != nil {
		panic(err)
	}
	return &ClientImpl{Interface: cs, clientset: cs, config: cfg, httpClient: c.httpClient}
}

func (c *ClientImpl) WithTimeout(timeout time.Duration) v1.Client {
	if c.config == nil {
		return c
	}
	cfg := rest.CopyConfig(c.config)
	cfg.Timeout = timeout
	cs, err := client.NewForConfigAndClient(cfg, c.httpClient)
	if err != nil {
		panic(err)
	}
	return &ClientImpl{Interface: cs, clientset: cs, config: cfg, httpClient: c.httpClient}
}

func (c *ClientImpl) WithToken(token string) v1.Client {
	if c.config == nil {
		return c
	}
	cfg := rest.CopyConfig(c.config)
	cfg.BearerToken = token
	cs, err := client.NewForConfigAndClient(cfg, c.httpClient)
	if err != nil {
		panic(err)
	}
	return &ClientImpl{Interface: cs, clientset: cs, config: cfg, httpClient: c.httpClient}
}

func (c *ClientImpl) WithTunnel(tunnel tunnel.Tunnel, local bool) v1.Client {
	if c.config == nil {
		return c
	}

	cfg := rest.CopyConfig(c.config)
	if !local {
		cfg.Host = fmt.Sprintf("https://%s", tunnel.Hostname())
	} else {
		cfg.Host = fmt.Sprintf("https://%s:%d", tunnel.LocalIP(), tunnel.LocalPort())
	}
	cfg.CAData = nil
	cfg.Insecure = local
	cs, err := client.NewForConfigAndClient(cfg, c.httpClient)
	if err != nil {
		panic(err)
	}
	return &ClientImpl{Interface: cs, clientset: cs, config: cfg, httpClient: c.httpClient}
}

func (c *ClientImpl) Kubeconfig(name string) *clientcmdapi.Config {
	if c.config == nil {
		return &clientcmdapi.Config{}
	}
	cfg := clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{
			name: {
				Server: c.config.Host,
				CertificateAuthorityData: func() []byte {
					if c.config.Insecure {
						return nil
					}
					return c.config.CAData
				}(),
				InsecureSkipTLSVerify: c.config.Insecure,
			},
		},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{
			name: {
				ClientCertificateData: c.config.CertData,
				ClientKeyData:         c.config.KeyData,
				Token:                 c.config.BearerToken,
			},
		},
		Contexts: map[string]*clientcmdapi.Context{
			name: {
				Cluster:  name,
				AuthInfo: name,
			},
		},
		CurrentContext: name,
	}
	return &cfg
}

func (c *ClientImpl) WriteKubeconfig(name string) error {
	cfg := c.Kubeconfig(name)

	// Merge into the existing kubeconfig (honoring $KUBECONFIG) instead of
	// overwriting it, so other clusters/users/contexts are preserved.
	pathOptions := clientcmd.NewDefaultPathOptions()
	merged, err := pathOptions.GetStartingConfig()
	if err != nil {
		return fmt.Errorf("load kubeconfig: %w", err)
	}

	maps.Copy(merged.Clusters, cfg.Clusters)
	maps.Copy(merged.AuthInfos, cfg.AuthInfos)
	maps.Copy(merged.Contexts, cfg.Contexts)
	if cfg.CurrentContext != "" {
		merged.CurrentContext = cfg.CurrentContext
	}

	return clientcmd.ModifyConfig(pathOptions, *merged, false)
}
