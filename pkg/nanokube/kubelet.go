package nanokube

import (
	"context"
	"crypto/tls"
	"path/filepath"
	"sync"

	noopoteltrace "go.opentelemetry.io/otel/trace/noop"
	"k8s.io/klog/v2"
	"k8s.io/mount-utils"

	"k8s.io/client-go/tools/record"
	cliflag "k8s.io/component-base/cli/flag"
	kubeletapp "k8s.io/kubernetes/cmd/kubelet/app"
	kubeletoptions "k8s.io/kubernetes/cmd/kubelet/app/options"
	"k8s.io/kubernetes/pkg/kubelet"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	containertest "k8s.io/kubernetes/pkg/kubelet/container/testing"
	probetest "k8s.io/kubernetes/pkg/kubelet/prober/testing"
	"k8s.io/kubernetes/pkg/kubelet/server"
	kubeletutil "k8s.io/kubernetes/pkg/kubelet/util"
	"k8s.io/kubernetes/pkg/util/oom"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/configmap"
	"k8s.io/kubernetes/pkg/volume/csi"
	"k8s.io/kubernetes/pkg/volume/downwardapi"
	"k8s.io/kubernetes/pkg/volume/emptydir"
	"k8s.io/kubernetes/pkg/volume/fc"
	"k8s.io/kubernetes/pkg/volume/git_repo"
	"k8s.io/kubernetes/pkg/volume/hostpath"
	"k8s.io/kubernetes/pkg/volume/iscsi"
	"k8s.io/kubernetes/pkg/volume/local"
	"k8s.io/kubernetes/pkg/volume/nfs"
	"k8s.io/kubernetes/pkg/volume/portworx"
	"k8s.io/kubernetes/pkg/volume/projected"
	"k8s.io/kubernetes/pkg/volume/secret"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/kubernetes/pkg/volume/util/subpath"

	v1 "github.com/cnuss/nanokube/pkg/v1"
)

type KubeletImpl struct {
	config v1.Config

	flags     *kubeletoptions.KubeletFlags
	flagsOnce sync.Once

	configuration     *kubeletconfig.KubeletConfiguration
	configurationOnce sync.Once

	dependencies     *kubelet.Dependencies
	dependenciesOnce sync.Once

	ready chan struct{}
}

var _ v1.Kubelet = &KubeletImpl{}

func NewKubelet(config v1.Config) v1.Kubelet {
	return &KubeletImpl{
		config: config,
		flags:  kubeletoptions.NewKubeletFlags(),
		configuration: func() *kubeletconfig.KubeletConfiguration {
			if c, err := kubeletoptions.NewKubeletConfiguration(); err == nil {
				return c
			}
			return &kubeletconfig.KubeletConfiguration{}
		}(),
		dependencies: &kubelet.Dependencies{
			// These dependencies are required by the kubelet
			RemoteRuntimeService: nil,
			RemoteImageService:   nil,
			CAdvisorInterface:    nil,
			ContainerManager:     nil,
			TLSOptions:           nil,
			// These are needed for non-standalone mode
			KubeClient:      nil,
			HeartbeatClient: nil,
			// Use fake implementations for the rest of the dependencies
			ProbeManager:              probetest.FakeManager{},
			OSInterface:               &containertest.FakeOS{},
			VolumePlugins:             volumePlugins(),
			OOMAdjuster:               oom.NewFakeOOMAdjuster(),
			Mounter:                   &mount.FakeMounter{},
			Subpather:                 &subpath.FakeSubpath{},
			HostUtil:                  hostutil.NewFakeHostUtil(nil),
			PodStartupLatencyTracker:  kubeletutil.NewPodStartupLatencyTracker(),
			NodeStartupLatencyTracker: kubeletutil.NewNodeStartupLatencyTracker(),
			TracerProvider:            noopoteltrace.NewTracerProvider(),
			Recorder:                  &record.FakeRecorder{},
		},
		ready: make(chan struct{}),
	}
}

func (k *KubeletImpl) Tunnel() v1.Tunnel {
	return k.config.Tunnel(v1.KubeletService)
}

func (k *KubeletImpl) Ready() <-chan struct{} {
	return k.ready
}

func (k *KubeletImpl) Flags() *kubeletoptions.KubeletFlags {
	k.flagsOnce.Do(func() {
		k.flags.CloudProvider = "external"
		k.flags.HostnameOverride = k.Tunnel().Hostname()
		k.flags.NodeLabels = make(map[string]string) // TODO(incomplete): add labels
		k.flags.NodeIP = k.Tunnel().LocalIP().String()
		k.flags.RootDirectory = k.config.Options().DataDirAt(v1.DataDirKubelet)
	})
	return k.flags
}

func (k *KubeletImpl) Configuration() *kubeletconfig.KubeletConfiguration {
	k.configurationOnce.Do(func() {
		if k.config.Options().Standalone() {
			k.configuration.RegisterNode = false
		}
		k.configuration.Address = k.Tunnel().LocalIP().String()
		k.configuration.ClusterDomain = k.Tunnel().Domain()
		// TODO(incomplete): probe a container to get resolv.conf
		k.configuration.ClusterDNS = []string{"1.1.1.1"}
		k.configuration.PodLogsDir = k.config.Options().DataDirAt(v1.DataDirLogs)
		k.configuration.Port = k.Tunnel().LocalPort()
		k.configuration.ReadOnlyPort = 0
		k.configuration.StaticPodPath = k.config.Options().DataDirAt(v1.DataDirStaticPods)
	})
	return k.configuration
}

func (k *KubeletImpl) Dependencies() *kubelet.Dependencies {
	k.dependenciesOnce.Do(func() {
		if !k.config.Options().Standalone() {
			k.dependencies.KubeClient = k.config.Kube().Client()
			k.dependencies.HeartbeatClient = k.config.Kube().Client()
		} else {
			k.dependencies.KubeClient = nil
			k.dependencies.HeartbeatClient = nil
			k.dependencies.EventClient = nil
		}

		// Required dependencies
		k.dependencies.RemoteRuntimeService = k.config.DefaultBackend().Driver()
		k.dependencies.RemoteImageService = k.config.DefaultBackend().Driver()
		k.dependencies.CAdvisorInterface = k.config.DefaultBackend()
		k.dependencies.ContainerManager = k.config.DefaultBackend().Manager()
		k.dependencies.TLSOptions = &server.TLSOptions{
			Config: &tls.Config{
				NextProtos: func() []string {
					if !v1.HTTP2 {
						return []string{"http/1.1"}
					}
					return []string{"h2", "http/1.1"}
				}(),
				MinVersion: func() uint16 {
					if v, err := cliflag.TLSVersion(k.Configuration().TLSMinVersion); err == nil {
						return v
					}
					return cliflag.DefaultTLSVersion()
				}(),
				CipherSuites: func() []uint16 {
					if v, err := cliflag.TLSCipherSuites(k.Configuration().TLSCipherSuites); err == nil {
						return v
					}
					return nil
				}(),
			},
			CertFile: filepath.Join(string(k.config.Options().DataDir()), string(v1.CertFile)),
			KeyFile:  filepath.Join(string(k.config.Options().DataDir()), string(v1.KeyFile)),
		}

		k.dependencies.ProbeManager = nil
		k.dependencies.Services = k.config.Services(k.Tunnel().URL())
		k.dependencies.VolumePlugins = k.config.Host().VolumePlugins()
		k.dependencies.OSInterface = k.config.Host()
		k.dependencies.Mounter = k.config.Host()
		k.dependencies.Subpather = k.config.Host()
		k.dependencies.HostUtil = k.config.Host()
	})
	return k.dependencies
}

func (k *KubeletImpl) Run(ctx context.Context) {
	if err := kubeletapp.RunKubelet(ctx, &kubeletoptions.KubeletServer{
		KubeletFlags:         *k.Flags(),
		KubeletConfiguration: *k.Configuration(),
	}, k.Dependencies()); err != nil {
		klog.Fatalf("Failed to run Kubelet: %v. Exiting.", err)
	}
	close(k.ready)
	<-ctx.Done()
}

func volumePlugins() []volume.VolumePlugin {
	allPlugins := []volume.VolumePlugin{}
	allPlugins = append(allPlugins, emptydir.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, git_repo.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, hostpath.ProbeVolumePlugins(volume.VolumeConfig{})...)
	allPlugins = append(allPlugins, nfs.ProbeVolumePlugins(volume.VolumeConfig{})...)
	allPlugins = append(allPlugins, secret.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, iscsi.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, downwardapi.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, fc.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, configmap.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, projected.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, portworx.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, local.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, csi.ProbeVolumePlugins()...)
	return allPlugins
}
