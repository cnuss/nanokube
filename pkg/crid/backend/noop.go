package backend

import (
	"context"
	"fmt"

	"github.com/cnuss/nanokube/pkg/crid/labels"
	"k8s.io/client-go/tools/record"
	internalapi "k8s.io/cri-api/pkg/apis"
	"k8s.io/kubelet/pkg/cri/streaming"
	"k8s.io/kubernetes/pkg/kubelet/cadvisor"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	"k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/prober"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/kubernetes/pkg/volume/util/subpath"
	"k8s.io/mount-utils"
)

// NoopBackend satisfies the Backend interface but returns nil/errors for
// every operation. Used as a safe fallback when no container runtime is detected.
type NoopBackend struct{}

var _ Backend = &NoopBackend{}

func (n *NoopBackend) Name() Runtime            { return "noop" }
func (n *NoopBackend) Context() context.Context { return context.Background() }
func (n *NoopBackend) Start(context.Context, record.EventBroadcaster) error {
	return fmt.Errorf("no backend")
}
func (n *NoopBackend) Stop(context.Context) error                { return nil }
func (n *NoopBackend) Labels() labels.LabelProvider              { return labels.NewLabels("noop") }
func (n *NoopBackend) Images() internalapi.ImageManagerService   { return nil }
func (n *NoopBackend) Containers() internalapi.RuntimeService    { return nil }
func (n *NoopBackend) ContainerManager() cm.ContainerManager     { return nil }
func (n *NoopBackend) VolumePlugin() volume.VolumePlugin         { return nil }
func (n *NoopBackend) Mounter() mount.Interface                  { return nil }
func (n *NoopBackend) Cadvisor() cadvisor.Interface              { return nil }
func (n *NoopBackend) OS() container.OSInterface                 { return nil }
func (n *NoopBackend) Subpath() subpath.Interface                { return nil }
func (n *NoopBackend) HostUtils() hostutil.HostUtils             { return nil }
func (n *NoopBackend) EventBroadcaster() record.EventBroadcaster { return nil }
func (n *NoopBackend) EventRecorder() record.EventRecorder       { return nil }
func (n *NoopBackend) Prober() prober.Manager                    { return nil }
func (n *NoopBackend) Streaming() streaming.Server               { return nil }
func (n *NoopBackend) HostInfo() (*HostInfo, error) {
	return &HostInfo{
		Hostname: "localhost",
		CpuInfo:  []CpuInfo{},
	}, nil
}
