package backend

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/pkg/kubelet/prober"
)

func NewProber(backend *BackendImpl) prober.Manager {
	prober := &ProberImpl{backend: backend}
	// TODO: start
	return prober
}

type ProberImpl struct {
	backend *BackendImpl
}

// AddPod implements [prober.Manager].
func (p *ProberImpl) AddPod(ctx context.Context, pod *v1.Pod) {
	panic("unimplemented")
}

// CleanupPods implements [prober.Manager].
func (p *ProberImpl) CleanupPods(desiredPods map[types.UID]sets.Empty) {
	panic("unimplemented")
}

// RemovePod implements [prober.Manager].
func (p *ProberImpl) RemovePod(pod *v1.Pod) {
	panic("unimplemented")
}

// StopLivenessAndStartup implements [prober.Manager].
func (p *ProberImpl) StopLivenessAndStartup(pod *v1.Pod) {
	panic("unimplemented")
}

// UpdatePodStatus implements [prober.Manager].
func (p *ProberImpl) UpdatePodStatus(context.Context, *v1.Pod, *v1.PodStatus) {
	panic("unimplemented")
}

var _ prober.Manager = &ProberImpl{}
