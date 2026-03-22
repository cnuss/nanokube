package config

import (
	"os"
	"path/filepath"
	"reflect"

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

func DefaultDataDir(name string) string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, "."+name)
}

func NewDataDirs(name, dataDir string) DataDirs {
	if dataDir == "" {
		dataDir = DefaultDataDir(name)
	}
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
	v := reflect.ValueOf(dirs)
	for i := 0; i < v.NumField(); i++ {
		if d := v.Field(i).String(); d != "" {
			os.MkdirAll(d, 0o755)
		}
	}
	return dirs
}
