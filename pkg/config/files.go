package config

import (
	"path/filepath"

	"k8s.io/client-go/tools/clientcmd"
)

type Files struct {
	RecommendedHomeFile string
	Kubeconfig          string
	LogFile             string
}

func NewFiles(dataDir string) *Files {
	return &Files{
		RecommendedHomeFile: clientcmd.RecommendedHomeFile,
		Kubeconfig:          filepath.Join(dataDir, "kubeconfig"),
		LogFile:             filepath.Join(dataDir, "log"),
	}
}
