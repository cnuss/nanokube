package kubelet

import (
	"context"
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/pkg/probe"
	httpprobe "k8s.io/kubernetes/pkg/probe/http"
	tcpprobe "k8s.io/kubernetes/pkg/probe/tcp"
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
	podIP          string
	cancel         context.CancelFunc // cancels all workers
	livenessCancel context.CancelFunc // cancels liveness workers only
	startupCancel  context.CancelFunc // cancels startup workers only
}

// ProbeManager implements prober.Manager with real HTTP/TCP probe execution.
type ProbeManager struct {
	mu      sync.RWMutex
	pods    map[types.UID]*trackedPod
	results map[resultKey]*resultState

	httpProber httpprobe.Prober
	tcpProber  tcpprobe.Prober
}

func NewProbeManager() *ProbeManager {
	return &ProbeManager{
		pods:       make(map[types.UID]*trackedPod),
		results:    make(map[resultKey]*resultState),
		httpProber: httpprobe.New(true),
		tcpProber:  tcpprobe.New(),
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
	pm.mu.Lock()
	// cache the pod IP from the status
	if tp, exists := pm.pods[pod.UID]; exists && podStatus.PodIP != "" {
		tp.podIP = podStatus.PodIP
	}
	pm.mu.Unlock()

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
		pm.runProbe(uid, container, probeSpec, pt, successThreshold, failureThreshold)

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (pm *ProbeManager) runProbe(uid types.UID, container *v1.Container, probeSpec *v1.Probe, pt probeType, successThreshold, failureThreshold int) {
	pm.mu.RLock()
	tp, exists := pm.pods[uid]
	if !exists {
		pm.mu.RUnlock()
		return
	}
	podIP := tp.podIP
	podName := tp.pod.Name
	pm.mu.RUnlock()

	if podIP == "" {
		return // IP not yet assigned
	}

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

	result := pm.executeProbe(probeSpec, container, podIP, podName)

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

func (pm *ProbeManager) executeProbe(probeSpec *v1.Probe, container *v1.Container, podIP, podName string) probe.Result {
	handler := probeSpec.ProbeHandler

	switch {
	case handler.HTTPGet != nil:
		req, err := httpprobe.NewRequestForHTTPGetAction(handler.HTTPGet, container, podIP, "probe")
		if err != nil {
			probeLog.Warn().Err(err).Str("pod", podName).Str("container", container.Name).Msg("http probe request error")
			return probe.Failure
		}
		timeout := time.Duration(probeSpec.TimeoutSeconds) * time.Second
		if timeout <= 0 {
			timeout = 1 * time.Second
		}
		result, output, err := pm.httpProber.Probe(req, timeout)
		if result != probe.Success {
			probeLog.Debug().
				Str("pod", podName).
				Str("container", container.Name).
				Str("url", req.URL.String()).
				Str("result", string(result)).
				Str("output", truncate(output, 200)).
				Err(err).
				Msg("http probe")
		}
		return result

	case handler.TCPSocket != nil:
		port, err := probe.ResolveContainerPort(handler.TCPSocket.Port, container)
		if err != nil {
			probeLog.Warn().Err(err).Str("pod", podName).Str("container", container.Name).Msg("tcp probe port error")
			return probe.Failure
		}
		host := handler.TCPSocket.Host
		if host == "" {
			host = podIP
		}
		timeout := time.Duration(probeSpec.TimeoutSeconds) * time.Second
		if timeout <= 0 {
			timeout = 1 * time.Second
		}
		result, _, err := pm.tcpProber.Probe(host, port, timeout)
		if result != probe.Success {
			probeLog.Debug().
				Str("pod", podName).
				Str("container", container.Name).
				Str("host", fmt.Sprintf("%s:%d", host, port)).
				Str("result", string(result)).
				Err(err).
				Msg("tcp probe")
		}
		return result

	case handler.Exec != nil:
		probeLog.Warn().Str("pod", podName).Str("container", container.Name).Msg("exec probe not supported, treating as success")
		return probe.Success

	case handler.GRPC != nil:
		probeLog.Warn().Str("pod", podName).Str("container", container.Name).Msg("grpc probe not supported, treating as success")
		return probe.Success

	default:
		return probe.Success
	}
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
