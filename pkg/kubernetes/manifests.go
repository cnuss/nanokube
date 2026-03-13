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

func (m *Manifests) Start(ctx context.Context) (component.Started, error) {
	version := strings.SplitN(versionutil.Get().GitVersion, "-", 2)[0]
	manifestsLog.Info().Str("version", version).Msg("starting manifests")

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
