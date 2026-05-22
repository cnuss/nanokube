package nanokube

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"k8s.io/client-go/informers"
	client "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

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
}

var _ v1.Client = &ClientImpl{}

func NewNoopClient(ctx context.Context) v1.Client {
	fakeCS := fake.NewSimpleClientset()
	return &ClientImpl{
		ctx:       ctx,
		Interface: fakeCS,
	}
}

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

func (c *ClientImpl) Ready() <-chan struct{} {
	ready := make(chan struct{})
	if c.clientset == nil {
		// noop client: always ready
		close(ready)
		return ready
	}
	go func() {
		defer close(ready)
		for {
			_, err := c.clientset.RESTClient().Get().AbsPath("/readyz").Do(c.ctx).Raw()
			if err == nil {
				return
			}
			select {
			case <-time.After(500 * time.Millisecond):
			case <-c.ctx.Done():
				return
			}
		}
	}()
	return ready
}

func (c *ClientImpl) Clientset() *client.Clientset {
	return c.clientset
}

func (c *ClientImpl) InformerFactory() informers.SharedInformerFactory {
	c.informerFactoryOnce.Do(func() {
		c.informerFactory = informers.NewSharedInformerFactory(c, 0)
	})
	return c.informerFactory
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

func (c *ClientImpl) WithTunnel(tunnel v1.Tunnel, local bool) v1.Client {
	if c.config == nil {
		return c
	}
	if !local {
		<-tunnel.Ready()
	}

	cfg := rest.CopyConfig(c.config)
	if !local {
		cfg.Host = fmt.Sprintf("https://%s", tunnel.FQDN())
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

func (c *ClientImpl) WriteKubeconfig(path string) error {
	cfg := c.Kubeconfig("nanokube")
	return clientcmd.WriteToFile(*cfg, path)
}
