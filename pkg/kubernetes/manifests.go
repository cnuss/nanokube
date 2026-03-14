package kubernetes

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/cnuss/nanokube/pkg/crid"
	"github.com/cnuss/nanokube/pkg/crid/backend"
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
	crid *crid.CRID
	cmd  *cobra.Command
}

func NewManifests(crid *crid.CRID) *Manifests {
	return &Manifests{
		crid: crid,
	}
}

func (m *Manifests) kubeconfig() clientcmdapi.Config {
	return clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{
			m.crid.Name(): {
				Server:                   "https://127.0.0.1:6443",
				CertificateAuthorityData: m.crid.Certs().Cert(),
			},
		},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{
			m.crid.Name(): {
				ClientCertificateData: m.crid.Certs().Cert(),
				ClientKeyData:         m.crid.Certs().Key(),
			},
		},
		Contexts: map[string]*clientcmdapi.Context{
			m.crid.Name(): {
				Cluster:  m.crid.Name(),
				AuthInfo: m.crid.Name(),
			},
		},
		CurrentContext: m.crid.Name(),
	}
}

// writeKubeconfig writes the cluster kubeconfig to the data directory
// and merges it into ~/.kube/config so kubectl works without --kubeconfig.
func (m *Manifests) writeKubeconfig() {
	config := m.kubeconfig()
	kubeconfigPath := m.crid.Files().Kubeconfig

	if _, err := os.Stat(kubeconfigPath); os.IsNotExist(err) {
		clientcmd.WriteToFile(config, kubeconfigPath)
	}

	// Merge into ~/.kube/config
	dst, err := clientcmd.LoadFromFile(m.crid.Files().RecommendedHomeFile)
	if err != nil {
		dst = clientcmdapi.NewConfig()
	}
	dst.Clusters[m.crid.Name()] = config.Clusters[m.crid.Name()]
	dst.AuthInfos[m.crid.Name()] = config.AuthInfos[m.crid.Name()]
	dst.Contexts[m.crid.Name()] = config.Contexts[m.crid.Name()]
	dst.CurrentContext = m.crid.Name()
	if err := clientcmd.WriteToFile(*dst, m.crid.Files().RecommendedHomeFile); err != nil {
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
		if vol.HostPath == nil {
			continue
		}
		switch vol.Name {
		case "etc-kubernetes":
			pod.Spec.Volumes[i].HostPath.Path = m.crid.DataDir()
		case "var-lib-etcd":
			pod.Spec.Volumes[i].HostPath.Path = m.crid.DataDirs().Etcd
		}
	}

	// Inject host aliases so the kubelet includes them in its managed /etc/hosts
	if h := m.crid.Hosts(); h != nil {
		pod.Spec.HostAliases = h.HostAliases(backend.NetworkHost)
	}

	out, err := sigyaml.Marshal(pod)
	if err != nil {
		return nil, fmt.Errorf("marshal kube-system manifest: %w", err)
	}

	if err := os.WriteFile(filepath.Join(m.crid.DataDirs().Manifests, "kube-system.yaml"), out, 0o644); err != nil {
		return nil, fmt.Errorf("write kube-system manifest: %w", err)
	}

	return component.Ready(), nil
}

func (m *Manifests) Stop() component.Stopped {
	return component.NotReady(func() {})
	// return component.Closed("tcp", "127.0.0.1:10259", nil)
}

func kubeArgs(featureGates map[string]bool, verbosity int) []string {
	gates := make([]string, 0, len(featureGates))
	for k, v := range featureGates {
		gates = append(gates, fmt.Sprintf("%s=%t", k, v))
	}
	args := []string{
		"--feature-gates=" + strings.Join(gates, ","),
	}
	args = append(args, fmt.Sprintf("--v=%d", verbosity*2))
	return args
}
