package nanokube

import (
	"context"

	noopoteltrace "go.opentelemetry.io/otel/trace/noop"
	"k8s.io/klog/v2"
	"k8s.io/mount-utils"

	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	internalapi "k8s.io/cri-api/pkg/apis"
	kubeletapp "k8s.io/kubernetes/cmd/kubelet/app"
	kubeletoptions "k8s.io/kubernetes/cmd/kubelet/app/options"
	"k8s.io/kubernetes/pkg/kubelet"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/cadvisor"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	containertest "k8s.io/kubernetes/pkg/kubelet/container/testing"
	probetest "k8s.io/kubernetes/pkg/kubelet/prober/testing"
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
	options v1.Options
	flags   *kubeletoptions.KubeletFlags
	config  *kubeletconfig.KubeletConfiguration
	deps    *kubelet.Dependencies

	ready chan struct{}
}

var _ v1.Kubelet = &KubeletImpl{}

func NewKubelet(
	options v1.Options,
	flags *kubeletoptions.KubeletFlags,
	config *kubeletconfig.KubeletConfiguration,
	client *clientset.Clientset,
	heartbeatClient *clientset.Clientset,
	cadvisorInterface cadvisor.Interface,
	imageService internalapi.ImageManagerService,
	runtimeService internalapi.RuntimeService,
	containerManager cm.ContainerManager,
) v1.Kubelet {
	d := &kubelet.Dependencies{
		KubeClient:                client,
		HeartbeatClient:           heartbeatClient,
		ProbeManager:              probetest.FakeManager{},
		RemoteRuntimeService:      runtimeService,
		RemoteImageService:        imageService,
		CAdvisorInterface:         cadvisorInterface,
		OSInterface:               &containertest.FakeOS{},
		ContainerManager:          containerManager,
		VolumePlugins:             volumePlugins(),
		TLSOptions:                nil,
		OOMAdjuster:               oom.NewFakeOOMAdjuster(),
		Mounter:                   &mount.FakeMounter{},
		Subpather:                 &subpath.FakeSubpath{},
		HostUtil:                  hostutil.NewFakeHostUtil(nil),
		PodStartupLatencyTracker:  kubeletutil.NewPodStartupLatencyTracker(),
		NodeStartupLatencyTracker: kubeletutil.NewNodeStartupLatencyTracker(),
		TracerProvider:            noopoteltrace.NewTracerProvider(),
		Recorder:                  &record.FakeRecorder{}, // With real recorder we attempt to read /dev/kmsg.
	}

	return &KubeletImpl{
		options: options,
		flags:   flags,
		config:  config,
		deps:    d,
		ready:   make(chan struct{}),
	}
}

func (k *KubeletImpl) Ready() <-chan struct{} {
	return k.ready
}

func (k *KubeletImpl) Flags() *kubeletoptions.KubeletFlags {
	return k.flags
}

func (k *KubeletImpl) Configuration() *kubeletconfig.KubeletConfiguration {
	return k.config
}

func (k *KubeletImpl) Deps() *kubelet.Dependencies {
	return k.deps
}

func (k *KubeletImpl) Run(ctx context.Context) {
	if err := kubeletapp.RunKubelet(ctx, &kubeletoptions.KubeletServer{
		KubeletFlags:         *k.flags,
		KubeletConfiguration: *k.config,
	}, k.deps); err != nil {
		klog.Fatalf("Failed to run Kubelet: %v. Exiting.", err)
	}
	close(k.ready)
	<-ctx.Done()
}

func volumePlugins() []volume.VolumePlugin {
	allPlugins := []volume.VolumePlugin{}
	allPlugins = append(allPlugins, emptydir.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, git_repo.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, hostpath.FakeProbeVolumePlugins(volume.VolumeConfig{})...)
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
