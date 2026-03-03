package kubelet

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cnuss/nanokube/pkg/cri"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	kubelettypes "k8s.io/kubelet/pkg/types"
	"k8s.io/kubernetes/pkg/probe"
)

var probeLog = newLogger("probemanager")

type probeType int

const (
	probeReadiness probeType = iota
	probeLiveness
	probeStartup
)

func (pt probeType) String() string {
	switch pt {
	case probeReadiness:
		return "readiness"
	case probeLiveness:
		return "liveness"
	case probeStartup:
		return "startup"
	default:
		return "unknown"
	}
}

type resultKey struct {
	podUID        types.UID
	containerName string
	probeType     probeType
}

type resultState struct {
	success  bool
	runCount int // consecutive same-result count
}

type trackedPod struct {
	pod            *v1.Pod
	cancel         context.CancelFunc // cancels all workers
	livenessCancel context.CancelFunc // cancels liveness workers only
	startupCancel  context.CancelFunc // cancels startup workers only
}

// ProbeManager implements prober.Manager by executing all probes inside the
// target container via the CRI backend's ExecSync.
type ProbeManager struct {
	mu      sync.RWMutex
	pods    map[types.UID]*trackedPod
	results map[resultKey]*resultState

	backend cri.Backend
}

func NewProbeManager(backend cri.Backend) *ProbeManager {
	return &ProbeManager{
		pods:    make(map[types.UID]*trackedPod),
		results: make(map[resultKey]*resultState),
		backend: backend,
	}
}

func (pm *ProbeManager) AddPod(ctx context.Context, pod *v1.Pod) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	uid := pod.UID
	if _, exists := pm.pods[uid]; exists {
		return
	}

	podCtx, podCancel := context.WithCancel(ctx)
	livenessCtx, livenessCancel := context.WithCancel(podCtx)
	startupCtx, startupCancel := context.WithCancel(podCtx)

	tp := &trackedPod{
		pod:            pod,
		cancel:         podCancel,
		livenessCancel: livenessCancel,
		startupCancel:  startupCancel,
	}
	pm.pods[uid] = tp

	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		if c.ReadinessProbe != nil {
			go pm.worker(podCtx, uid, c, c.ReadinessProbe, probeReadiness)
		}
		if c.LivenessProbe != nil {
			go pm.worker(livenessCtx, uid, c, c.LivenessProbe, probeLiveness)
		}
		if c.StartupProbe != nil {
			go pm.worker(startupCtx, uid, c, c.StartupProbe, probeStartup)
		}
	}

	probeLog.Debug().Str("pod", pod.Name).Msg("added probe workers")
}

func (pm *ProbeManager) RemovePod(pod *v1.Pod) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.removePodLocked(pod.UID)
}

func (pm *ProbeManager) removePodLocked(uid types.UID) {
	tp, exists := pm.pods[uid]
	if !exists {
		return
	}
	tp.cancel()
	delete(pm.pods, uid)

	// clean up results for this pod
	for k := range pm.results {
		if k.podUID == uid {
			delete(pm.results, k)
		}
	}
}

func (pm *ProbeManager) CleanupPods(desiredPods map[types.UID]sets.Empty) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for uid := range pm.pods {
		if _, desired := desiredPods[uid]; !desired {
			pm.removePodLocked(uid)
		}
	}
}

func (pm *ProbeManager) StopLivenessAndStartup(pod *v1.Pod) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	tp, exists := pm.pods[pod.UID]
	if !exists {
		return
	}
	if tp.livenessCancel != nil {
		tp.livenessCancel()
	}
	if tp.startupCancel != nil {
		tp.startupCancel()
	}

	probeLog.Debug().Str("pod", pod.Name).Msg("stopped liveness and startup probes")
}

func (pm *ProbeManager) UpdatePodStatus(_ context.Context, pod *v1.Pod, podStatus *v1.PodStatus) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	// build a map of container spec by name for quick lookup
	specByName := make(map[string]*v1.Container, len(pod.Spec.Containers))
	for i := range pod.Spec.Containers {
		specByName[pod.Spec.Containers[i].Name] = &pod.Spec.Containers[i]
	}

	for i := range podStatus.ContainerStatuses {
		cs := &podStatus.ContainerStatuses[i]
		spec := specByName[cs.Name]
		running := cs.State.Running != nil

		// Determine Started
		started := false
		if running {
			if spec != nil && spec.StartupProbe != nil {
				key := resultKey{pod.UID, cs.Name, probeStartup}
				if rs, ok := pm.results[key]; ok && rs.success {
					started = true
				}
			} else {
				started = true
			}
		}
		cs.Started = &started

		// Determine Ready
		ready := false
		if started {
			if spec != nil && spec.ReadinessProbe != nil {
				key := resultKey{pod.UID, cs.Name, probeReadiness}
				if rs, ok := pm.results[key]; ok && rs.success {
					ready = true
				}
			} else {
				ready = true
			}
		}
		cs.Ready = ready
	}
}

func (pm *ProbeManager) worker(ctx context.Context, uid types.UID, container *v1.Container, probeSpec *v1.Probe, pt probeType) {
	initialDelay := time.Duration(probeSpec.InitialDelaySeconds) * time.Second
	period := time.Duration(probeSpec.PeriodSeconds) * time.Second
	if period <= 0 {
		period = 10 * time.Second
	}
	successThreshold := int(probeSpec.SuccessThreshold)
	if successThreshold <= 0 {
		successThreshold = 1
	}
	failureThreshold := int(probeSpec.FailureThreshold)
	if failureThreshold <= 0 {
		failureThreshold = 3
	}

	// wait for initial delay
	if initialDelay > 0 {
		select {
		case <-ctx.Done():
			return
		case <-time.After(initialDelay):
		}
	}

	ticker := time.NewTicker(period)
	defer ticker.Stop()

	// run immediately on first tick, then at period intervals
	for {
		pm.runProbe(ctx, uid, container, probeSpec, pt, successThreshold, failureThreshold)

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (pm *ProbeManager) runProbe(ctx context.Context, uid types.UID, container *v1.Container, probeSpec *v1.Probe, pt probeType, successThreshold, failureThreshold int) {
	pm.mu.RLock()
	tp, exists := pm.pods[uid]
	if !exists {
		pm.mu.RUnlock()
		return
	}
	podName := tp.pod.Name
	pm.mu.RUnlock()

	// If this is a readiness or liveness probe, check that startup probe passed first
	if pt == probeReadiness || pt == probeLiveness {
		if container.StartupProbe != nil {
			pm.mu.RLock()
			startupKey := resultKey{uid, container.Name, probeStartup}
			startupResult, ok := pm.results[startupKey]
			startupPassed := ok && startupResult.success
			pm.mu.RUnlock()
			if !startupPassed {
				return // startup probe hasn't passed yet
			}
		}
	}

	result := pm.executeProbe(ctx, probeSpec, container, uid, podName)

	pm.mu.Lock()
	defer pm.mu.Unlock()

	key := resultKey{uid, container.Name, pt}
	rs, ok := pm.results[key]
	if !ok {
		rs = &resultState{}
		pm.results[key] = rs
	}

	succeeded := result == probe.Success || result == probe.Warning
	if succeeded {
		if rs.success {
			rs.runCount++
		} else {
			rs.runCount = 1
		}
		if rs.runCount >= successThreshold {
			rs.success = true
		}
	} else {
		if !rs.success {
			rs.runCount++
		} else {
			rs.runCount = 1
		}
		if rs.runCount >= failureThreshold {
			if rs.success {
				probeLog.Info().
					Str("pod", podName).
					Str("container", container.Name).
					Str("probe", pt.String()).
					Msg("probe failed")
			}
			rs.success = false
		}
	}
}

// resolveContainerID finds the running container ID for a given pod UID and
// container name by querying the CRI backend with label selectors.
func (pm *ProbeManager) resolveContainerID(ctx context.Context, podUID types.UID, containerName string) (string, error) {
	if pm.backend == nil {
		return "", fmt.Errorf("no CRI backend available")
	}
	state := runtimeapi.ContainerState_CONTAINER_RUNNING
	containers, err := pm.backend.ListContainers(ctx, &runtimeapi.ContainerFilter{
		State: &runtimeapi.ContainerStateValue{State: state},
		LabelSelector: map[string]string{
			kubelettypes.KubernetesPodUIDLabel:        string(podUID),
			kubelettypes.KubernetesContainerNameLabel: containerName,
		},
	})
	if err != nil {
		return "", fmt.Errorf("list containers: %w", err)
	}
	if len(containers) == 0 {
		return "", fmt.Errorf("no running container found for %s/%s", podUID, containerName)
	}
	return containers[0].Id, nil
}

func (pm *ProbeManager) executeProbe(ctx context.Context, probeSpec *v1.Probe, container *v1.Container, podUID types.UID, podName string) probe.Result {
	handler := probeSpec.ProbeHandler
	timeout := time.Duration(probeSpec.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 1 * time.Second
	}
	timeoutSec := int(timeout.Seconds())
	if timeoutSec <= 0 {
		timeoutSec = 1
	}

	containerID, err := pm.resolveContainerID(ctx, podUID, container.Name)
	if err != nil {
		probeLog.Debug().Err(err).Str("pod", podName).Str("container", container.Name).Msg("resolve container for probe")
		return probe.Failure
	}

	var cmd []string

	switch {
	case handler.Exec != nil:
		cmd = handler.Exec.Command

	case handler.HTTPGet != nil:
		scheme := "http"
		if handler.HTTPGet.Scheme == v1.URISchemeHTTPS {
			scheme = "https"
		}
		port := handler.HTTPGet.Port.IntValue()
		if port == 0 {
			port, err = probe.ResolveContainerPort(handler.HTTPGet.Port, container)
			if err != nil {
				probeLog.Warn().Err(err).Str("pod", podName).Str("container", container.Name).Msg("http probe port error")
				return probe.Failure
			}
		}
		path := handler.HTTPGet.Path
		if path == "" {
			path = "/"
		}
		url := fmt.Sprintf("%s://localhost:%d%s", scheme, port, path)
		cmd = []string{"wget", "-q", "-O", "/dev/null", "-S", fmt.Sprintf("--timeout=%d", timeoutSec)}
		if host := handler.HTTPGet.Host; host != "" {
			cmd = append(cmd, "--header", fmt.Sprintf("Host: %s", host))
		}
		for _, h := range handler.HTTPGet.HTTPHeaders {
			cmd = append(cmd, "--header", fmt.Sprintf("%s: %s", h.Name, h.Value))
		}
		cmd = append(cmd, url)

	case handler.TCPSocket != nil:
		port, err := probe.ResolveContainerPort(handler.TCPSocket.Port, container)
		if err != nil {
			probeLog.Warn().Err(err).Str("pod", podName).Str("container", container.Name).Msg("tcp probe port error")
			return probe.Failure
		}
		cmd = []string{"nc", "-z", fmt.Sprintf("-w%d", timeoutSec), "localhost", fmt.Sprintf("%d", port)}

	case handler.GRPC != nil:
		probeLog.Warn().Str("pod", podName).Str("container", container.Name).Msg("grpc probe not supported, treating as success")
		return probe.Success

	default:
		return probe.Success
	}

	stdout, stderr, execErr := pm.backend.ExecSync(ctx, containerID, cmd, timeout)
	if execErr != nil {
		probeLog.Debug().
			Err(execErr).
			Str("pod", podName).
			Str("container", container.Name).
			Str("stdout", truncate(string(stdout), 200)).
			Str("stderr", truncate(string(stderr), 200)).
			Msg("probe exec failed")
		return probe.Failure
	}
	return probe.Success
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
