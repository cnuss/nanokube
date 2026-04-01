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
	"github.com/spf13/cobra"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
	versionutil "k8s.io/component-base/version"
	sigyaml "sigs.k8s.io/yaml"
)

//go:embed kube-system.yaml
var kubeSystemManifest string

type Manifests struct {
	log  component.Logger
	crid *crid.CRID
	cmd  *cobra.Command
}

func NewManifests(crid *crid.CRID) *Manifests {
	return &Manifests{
		log:  component.NewLogger("manifests"),
		crid: crid,
	}
}

func (m *Manifests) Start(ctx context.Context) (component.Started, error) {
	version := strings.SplitN(versionutil.Get().GitVersion, "-", 2)[0]
	m.log.Info().Str("version", version).Msg("starting manifests")

	pod := &v1.Pod{}
	if err := yaml.NewYAMLOrJSONDecoder(strings.NewReader(kubeSystemManifest), 4096).Decode(pod); err != nil {
		return nil, fmt.Errorf("parse kube-system manifest: %w", err)
	}
	pod.Name = m.crid.Name()

	svc := m.crid.DefaultBackend().IPAM().Service()
	envOverrides := map[string]string{
		"ADVERTISE_ADDRESS":        svc.IP.String(),
		"SERVICE_CLUSTER_IP_RANGE": svc.Net.String(),
	}

	for i, c := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == "api" || pod.Spec.Containers[i].Name == "controller" || pod.Spec.Containers[i].Name == "scheduler" {
			pod.Spec.Containers[i].Image = c.Image + ":" + version
		}
		for j, env := range c.Env {
			if val, ok := envOverrides[env.Name]; ok {
				pod.Spec.Containers[i].Env[j].Value = val
			}
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

	// Host aliases are now injected via the pod admit handler in container_manager.go
	// if h := m.crid.Hosts(); h != nil {
	// 	pod.Spec.HostAliases = h.HostAliases(ctx, backend.NetworkHost)
	// }

	out, err := sigyaml.Marshal(pod)
	if err != nil {
		return nil, fmt.Errorf("marshal kube-system manifest: %w", err)
	}

	if err := os.WriteFile(filepath.Join(m.crid.DataDirs().Manifests, "kube-system.yaml"), out, 0o644); err != nil {
		return nil, fmt.Errorf("write kube-system manifest: %w", err)
	}

	return component.Ready(), nil
}

func (m *Manifests) Stop(ctx context.Context) component.Stopped {
	return component.NotReady(func() {})
	// return component.Closed("tcp", "127.0.0.1:10259", nil)
}
