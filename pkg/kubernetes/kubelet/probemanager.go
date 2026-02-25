package kubelet

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
)

var probeLog = newLogger("probemanager")

// ProbeManager implements prober.Manager with no-op probe workers.
// All containers are marked Ready in UpdatePodStatus.
type ProbeManager struct{}

func (ProbeManager) AddPod(_ context.Context, pod *v1.Pod) {
	probeLog.Warn().Str("pod", pod.Name).Msg("AddPod not implemented")
}

func (ProbeManager) StopLivenessAndStartup(pod *v1.Pod) {
	probeLog.Warn().Str("pod", pod.Name).Msg("StopLivenessAndStartup not implemented")
}

func (ProbeManager) RemovePod(pod *v1.Pod) {
	probeLog.Warn().Str("pod", pod.Name).Msg("RemovePod not implemented")
}

func (ProbeManager) CleanupPods(_ map[types.UID]sets.Empty) {
	probeLog.Warn().Msg("CleanupPods not implemented")
}

func (ProbeManager) UpdatePodStatus(_ context.Context, _ *v1.Pod, podStatus *v1.PodStatus) {
	probeLog.Warn().Msg("UpdatePodStatus not implemented")
	for i := range podStatus.ContainerStatuses {
		podStatus.ContainerStatuses[i].Ready = true
	}
}
