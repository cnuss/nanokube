package config

import (
	"os"
	"path/filepath"

	"k8s.io/client-go/tools/clientcmd"
)

type DataDirs struct {
	Root               string
	RecommendedHomeDir string
	Etcd               string
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
		Etcd:               filepath.Join(dataDir, "etcd"),
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
