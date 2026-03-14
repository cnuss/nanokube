package config

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/cnuss/nanokube/pkg/crid"
	"github.com/cnuss/nanokube/pkg/crid/backend"
	"k8s.io/client-go/tools/clientcmd"
)

var logger = component.NewLogger("config")

type Config struct {
	Name         string
	DataDir      string
	DataDirs     DataDirs
	Verbosity    int
	FeatureGates map[string]bool
	Components   []component.Component
	CRID         *crid.CRID

	files     *Files
	filesOnce sync.Once
}

func NewConfig(options *Options) *Config {
	logger.Info().Str("dir", options.DataDir).Msg("generating configuration")

	return &Config{
		Name:      options.Name,
		DataDir:   options.DataDir,
		DataDirs:  NewDataDirs(options.DataDir),
		Verbosity: options.Verbosity,
		FeatureGates: map[string]bool{
			"APIServerIdentity":         false,
			"RuntimeClassInImageCriApi": false,
		},
		Components: []component.Component{},
	}
}

func (c *Config) SetCRID(crid *crid.CRID) {
	c.CRID = crid
}

func (c *Config) Files() *Files {
	c.filesOnce.Do(func() {
		var hosts backend.Hosts
		if c.CRID != nil {
			hosts = c.CRID.Hosts()
		}
		c.files = NewFiles(c.DataDir, hosts)
	})
	return c.files
}

func (c *Config) Certs() *certs {
	return c.Files().Certs()
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

	dataDir   string
	hosts     backend.Hosts
	certs     *certs
	certsOnce sync.Once
}

func NewFiles(dataDir string, hosts backend.Hosts) *Files {
	return &Files{
		RecommendedHomeFile: clientcmd.RecommendedHomeFile,
		Kubeconfig:          filepath.Join(dataDir, "kubeconfig"),
		LogFile:             filepath.Join(dataDir, "log"),
		hosts:               hosts,
		dataDir:             dataDir,
	}
}

func (f *Files) Certs() *certs {
	f.certsOnce.Do(func() {
		f.certs = &certs{
			Name:     "nanokube",
			DataDir:  f.dataDir,
			Hostname: f.hosts.Hostname(),
		}
	})
	return f.certs
}

func (f *Files) CAFile() string {
	return f.Certs().CertPath()
}

func (f *Files) KeyFile() string {
	return f.Certs().KeyPath()
}
