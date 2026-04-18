package nanokube

import (
	"fmt"
	"net/http"
	"time"

	client "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

type Client interface {
	client.Interface
	Clientset() *client.Clientset
	WithHeartbeat(interval time.Duration) Client
	WithQps(qps float32) Client
	WithTimeout(timeout time.Duration) Client
	WithTunnel(tunnel Tunnel, local bool) Client

	Kubeconfig(name string) *clientcmdapi.Config
	WriteKubeconfig(path string) error
}

type ClientImpl struct {
	client.Interface
	clientset  *client.Clientset
	config     *rest.Config
	httpClient *http.Client
}

var _ Client = &ClientImpl{}

func NewClient(config *rest.Config) Client {
	httpClient, err := rest.HTTPClientFor(config)
	if err != nil {
		panic(err)
	}
	cs, err := client.NewForConfigAndClient(config, httpClient)
	if err != nil {
		panic(err)
	}
	return &ClientImpl{
		Interface:  cs,
		clientset:  cs,
		config:     config,
		httpClient: httpClient,
	}
}

func (c *ClientImpl) Clientset() *client.Clientset {
	return c.clientset
}

func (c *ClientImpl) WithHeartbeat(interval time.Duration) Client {
	return c.WithQps(float32(-1)).WithTimeout(interval)
}

func (c *ClientImpl) WithQps(qps float32) Client {
	cfg := rest.CopyConfig(c.config)
	cfg.QPS = qps
	cfg.Burst = int(qps * 2)
	cs, err := client.NewForConfigAndClient(cfg, c.httpClient)
	if err != nil {
		panic(err)
	}
	return &ClientImpl{Interface: cs, clientset: cs, config: cfg, httpClient: c.httpClient}
}

func (c *ClientImpl) WithTimeout(timeout time.Duration) Client {
	cfg := rest.CopyConfig(c.config)
	cfg.Timeout = timeout
	cs, err := client.NewForConfigAndClient(cfg, c.httpClient)
	if err != nil {
		panic(err)
	}
	return &ClientImpl{Interface: cs, clientset: cs, config: cfg, httpClient: c.httpClient}
}

func (c *ClientImpl) WithTunnel(tunnel Tunnel, local bool) Client {
	if !local {
		<-tunnel.Ready()
	}

	cfg := rest.CopyConfig(c.config)
	if !local {
		cfg.Host = fmt.Sprintf("https://%s", tunnel.FQDN())
		cfg.CAData = nil
		cfg.Insecure = false
	} else {
		cfg.Host = fmt.Sprintf("https://%s:%d", tunnel.LocalHost(), tunnel.LocalPort())
		cfg.CAData = nil
		cfg.Insecure = true
	}
	cs, err := client.NewForConfigAndClient(cfg, c.httpClient)
	if err != nil {
		panic(err)
	}
	return &ClientImpl{Interface: cs, clientset: cs, config: cfg, httpClient: c.httpClient}
}

func (c *ClientImpl) Kubeconfig(name string) *clientcmdapi.Config {
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
