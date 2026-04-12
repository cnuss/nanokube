package nanokube

import (
	"net/http"
	"time"

	client "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Client interface {
	client.Interface
	Clientset() *client.Clientset
	WithHeartbeat(interval time.Duration) Client
	WithQps(qps float32) Client
	WithTimeout(timeout time.Duration) Client
}

type ClientImpl struct {
	client.Interface
	clientset  *client.Clientset
	config     *rest.Config
	httpClient *http.Client
}

// Clientset implements [Client].
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
