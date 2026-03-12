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
	DataDirs     DataDirs
	Files        Files
	Verbosity    int
	FeatureGates map[string]bool
	Components   []component.Component
	CRID         *crid.CRID
}

func NewConfig(options *Options) *Config {
	logger.Info().Str("dir", options.DataDir).Msg("generating configuration")

	return &Config{
		Name:      options.Name,
		DataDir:   options.DataDir,
		DataDirs:  NewDataDirs(options.DataDir),
		Files:     NewFiles(options.DataDir),
		Verbosity: options.Verbosity,
		FeatureGates: map[string]bool{
			"APIServerIdentity":         false,
			"RuntimeClassInImageCriApi": false,
		},
		Components: []component.Component{},
	}
}

func (c *Config) SetCRID(crid *crid.CRID) error {
	host, err := crid.DefaultBackend().HostInfo()
	if err != nil {
		return fmt.Errorf("crid host probe: %w", err)
	}
	c.WithHostname(host.Hostname).CRID = crid.WithPluginDirs(c.DataDirs.Plugins, c.DataDirs.PluginsRegistry)
	return nil
}

func (c *Config) Kubeconfig() clientcmdapi.Config {
	return clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{
			c.Name: {
				Server:                   "https://127.0.0.1:6443",
				CertificateAuthorityData: c.Certs().Cert(),
			},
		},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{
			c.Name: {
				ClientCertificateData: c.Certs().Cert(),
				ClientKeyData:         c.Certs().Key(),
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
	if _, err := os.Stat(c.Files.Kubeconfig); os.IsNotExist(err) {
		clientcmd.WriteToFile(config, c.Files.Kubeconfig)
	}
	c.mergeKubeconfig(config)
	return c.Files.Kubeconfig
}

func (c *Config) WithHostname(hostname string) *Config {
	c.Files.Certs.Hostname = hostname
	return c
}

func (c *Config) Certs() *certs {
	return c.Files.Certs
}

// mergeKubeconfig upserts this cluster's entries into ~/.kube/config
// so that kubectl can reach the cluster without --kubeconfig.
func (c *Config) mergeKubeconfig(src clientcmdapi.Config) {
	dst, err := clientcmd.LoadFromFile(c.Files.RecommendedHomeFile)
	if err != nil {
		dst = clientcmdapi.NewConfig()
	}

	dst.Clusters[c.Name] = src.Clusters[c.Name]
	dst.AuthInfos[c.Name] = src.AuthInfos[c.Name]
	dst.Contexts[c.Name] = src.Contexts[c.Name]
	dst.CurrentContext = c.Name

	if err := clientcmd.WriteToFile(*dst, c.Files.RecommendedHomeFile); err != nil {
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

type DataDirs struct {
	Root               string
	RecommendedHomeDir string
	Manifests          string
	Plugins            string
	PluginsRegistry    string
	Volumes            string
	Logs               string
	PKI                string
}

func NewDataDirs(dataDir string) DataDirs {
	dirs := DataDirs{
		Root:               dataDir,
		RecommendedHomeDir: filepath.Dir(clientcmd.RecommendedHomeFile),
		Manifests:          filepath.Join(dataDir, "manifests"),
		Plugins:            filepath.Join(dataDir, "plugins"),
		PluginsRegistry:    filepath.Join(dataDir, "plugins_registry"),
		Volumes:            filepath.Join(dataDir, "volumes"),
		Logs:               filepath.Join(dataDir, "logs"),
		PKI:                filepath.Join(dataDir, "pki"),
	}
	for _, d := range []string{dirs.Manifests, dirs.Plugins, dirs.PluginsRegistry, dirs.Volumes, dirs.Logs, dirs.PKI, filepath.Dir(clientcmd.RecommendedHomeFile)} {
		os.MkdirAll(d, 0o755)
	}
	return dirs
}

type Files struct {
	RecommendedHomeFile string
	Kubeconfig          string
	LogFile             string
	Certs               *certs
}

func NewFiles(dataDir string) Files {
	files := Files{
		RecommendedHomeFile: clientcmd.RecommendedHomeFile,
		Kubeconfig:          filepath.Join(dataDir, "kubeconfig"),
		LogFile:             filepath.Join(dataDir, "log"),
	}
	files.Certs = &certs{Name: "nanokube", DataDir: dataDir}
	return files
}

func (f *Files) CAFile() string {
	return f.Certs.CertPath()
}

func (f *Files) KeyFile() string {
	return f.Certs.KeyPath()
}
