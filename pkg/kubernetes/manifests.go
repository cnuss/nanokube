package kubernetes

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cnuss/nanokube/pkg/component"
	pkgconfig "github.com/cnuss/nanokube/pkg/config"
	"github.com/spf13/cobra"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	versionutil "k8s.io/component-base/version"
	sigyaml "sigs.k8s.io/yaml"
)

//go:embed kube-system.yaml
var kubeSystemManifest string

var manifestsLog = newLogger("manifests")

type Manifests struct {
	config *pkgconfig.Config
	cmd    *cobra.Command
}

func NewManifests(config *pkgconfig.Config) *Manifests {
	return &Manifests{
		config: config,
	}
}

func (m *Manifests) kubeconfig() clientcmdapi.Config {
	return clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{
			m.config.Name: {
				Server:                   "https://127.0.0.1:6443",
				CertificateAuthorityData: m.config.Certs().Cert(),
			},
		},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{
			m.config.Name: {
				ClientCertificateData: m.config.Certs().Cert(),
				ClientKeyData:         m.config.Certs().Key(),
			},
		},
		Contexts: map[string]*clientcmdapi.Context{
			m.config.Name: {
				Cluster:  m.config.Name,
				AuthInfo: m.config.Name,
			},
		},
		CurrentContext: m.config.Name,
	}
}

// writeKubeconfig writes the cluster kubeconfig to the data directory
// and merges it into ~/.kube/config so kubectl works without --kubeconfig.
func (m *Manifests) writeKubeconfig() {
	config := m.kubeconfig()
	kubeconfigPath := m.config.Files().Kubeconfig

	if _, err := os.Stat(kubeconfigPath); os.IsNotExist(err) {
		clientcmd.WriteToFile(config, kubeconfigPath)
	}

	// Merge into ~/.kube/config
	dst, err := clientcmd.LoadFromFile(m.config.Files().RecommendedHomeFile)
	if err != nil {
		dst = clientcmdapi.NewConfig()
	}
	dst.Clusters[m.config.Name] = config.Clusters[m.config.Name]
	dst.AuthInfos[m.config.Name] = config.AuthInfos[m.config.Name]
	dst.Contexts[m.config.Name] = config.Contexts[m.config.Name]
	dst.CurrentContext = m.config.Name
	if err := clientcmd.WriteToFile(*dst, m.config.Files().RecommendedHomeFile); err != nil {
		manifestsLog.Warn().Err(err).Msg("failed to write ~/.kube/config")
	}
}

func (m *Manifests) Start(ctx context.Context) (component.Started, error) {
	version := strings.SplitN(versionutil.Get().GitVersion, "-", 2)[0]
	manifestsLog.Info().Str("version", version).Msg("starting manifests")

	m.writeKubeconfig()

	pod := &v1.Pod{}
	if err := yaml.NewYAMLOrJSONDecoder(strings.NewReader(kubeSystemManifest), 4096).Decode(pod); err != nil {
		return nil, fmt.Errorf("parse kube-system manifest: %w", err)
	}

	for i, c := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == "api" || pod.Spec.Containers[i].Name == "controller" || pod.Spec.Containers[i].Name == "scheduler" {
			pod.Spec.Containers[i].Image = c.Image + ":" + version
		}
	}

	for i, vol := range pod.Spec.Volumes {
		if vol.Name == "etc-kubernetes" && vol.HostPath != nil {
			pod.Spec.Volumes[i].HostPath.Path = m.config.DataDir
		}
	}

	out, err := sigyaml.Marshal(pod)
	if err != nil {
		return nil, fmt.Errorf("marshal kube-system manifest: %w", err)
	}

	if err := os.WriteFile(filepath.Join(m.config.DataDirs.Manifests, "kube-system.yaml"), out, 0o644); err != nil {
		return nil, fmt.Errorf("write kube-system manifest: %w", err)
	}

	return component.Ready(), nil
}

func (m *Manifests) Stop() component.Stopped {
	return component.NotReady(func() {})
	// return component.Closed("tcp", "127.0.0.1:10259", nil)
}

func kubeArgs(config *pkgconfig.Config) []string {
	gates := make([]string, 0, len(config.FeatureGates))
	for k, v := range config.FeatureGates {
		gates = append(gates, fmt.Sprintf("%s=%t", k, v))
	}
	args := []string{
		"--feature-gates=" + strings.Join(gates, ","),
	}
	args = append(args, fmt.Sprintf("--v=%d", config.Verbosity*2))
	return args
}
