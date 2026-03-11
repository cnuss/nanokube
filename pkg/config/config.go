package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/cnuss/nanokube/pkg/crid"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

var logger = component.NewLogger("config")

type Config struct {
	Name         string
	DataDir      string
	Verbosity    int
	Certs        *certs
	FeatureGates map[string]bool
	Components   []component.Component
	CRID         *crid.CRID
}

func NewConfig(options *Options) *Config {
	logger.Info().Str("dir", options.DataDir).Msg("generating configuration")

	return &Config{
		Name:      options.Name,
		DataDir:   options.DataDir,
		Verbosity: options.Verbosity,
		FeatureGates: map[string]bool{
			"APIServerIdentity":         false,
			"RuntimeClassInImageCriApi": false,
		},
		Certs:      &certs{Name: options.Name, DataDir: options.DataDir},
		Components: []component.Component{},
	}
}

func (c *Config) SetCRID(crid *crid.CRID) error {
	host, err := crid.DefaultBackend().HostInfo()
	if err != nil {
		return fmt.Errorf("crid host probe: %w", err)
	}
	c.CRID = crid
	c.Certs.Hostname = host.Hostname
	return nil
}

func (c *Config) Kubeconfig() clientcmdapi.Config {
	return clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{
			c.Name: {
				Server:                   "https://127.0.0.1:6443",
				CertificateAuthorityData: c.Certs.Cert(),
			},
		},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{
			c.Name: {
				ClientCertificateData: c.Certs.Cert(),
				ClientKeyData:         c.Certs.Key(),
			},
		},
		Contexts: map[string]*clientcmdapi.Context{
			c.Name: {
				Cluster:  c.Name,
				AuthInfo: c.Name,
			},
		},
		CurrentContext: c.Name,
	}
}

func (c *Config) KubeconfigPath() string {
	config := c.Kubeconfig()
	path := filepath.Join(c.DataDir, "kubeconfig")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		clientcmd.WriteToFile(config, path)
	}
	c.mergeKubeconfig(config)
	return path
}

// mergeKubeconfig upserts this cluster's entries into ~/.kube/config
// so that kubectl can reach the cluster without --kubeconfig.
func (c *Config) mergeKubeconfig(src clientcmdapi.Config) {
	dst, err := clientcmd.LoadFromFile(clientcmd.RecommendedHomeFile)
	if err != nil {
		dst = clientcmdapi.NewConfig()
	}

	dst.Clusters[c.Name] = src.Clusters[c.Name]
	dst.AuthInfos[c.Name] = src.AuthInfos[c.Name]
	dst.Contexts[c.Name] = src.Contexts[c.Name]
	dst.CurrentContext = c.Name

	if err := os.MkdirAll(filepath.Dir(clientcmd.RecommendedHomeFile), 0755); err != nil {
		logger.Warn().Err(err).Msg("failed to create ~/.kube directory")
		return
	}
	if err := clientcmd.WriteToFile(*dst, clientcmd.RecommendedHomeFile); err != nil {
		logger.Warn().Err(err).Msg("failed to write ~/.kube/config")
	}
}

func (c *Config) KubeArgs() []string {
	gates := make([]string, 0, len(c.FeatureGates))
	for k, v := range c.FeatureGates {
		gates = append(gates, fmt.Sprintf("%s=%t", k, v))
	}
	args := []string{
		"--feature-gates=" + strings.Join(gates, ","),
	}
	args = append(args, fmt.Sprintf("--v=%d", c.Verbosity*2))
	return args
}
