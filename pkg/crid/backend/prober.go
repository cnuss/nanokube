package backend

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cnuss/nanokube/pkg/component"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/kubernetes/pkg/kubelet/prober"
	"k8s.io/kubernetes/pkg/probe"
)

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

func NewProber(backend *BackendImpl) prober.Manager {
	return &ProberImpl{
		backend: backend,
		log:     component.NewLogger("prober"),
		pods:    make(map[types.UID]*trackedPod),
		results: make(map[resultKey]*resultState),
	}
}

type ProberImpl struct {
	backend *BackendImpl
	log     component.Logger

	pods    map[types.UID]*trackedPod
	results map[resultKey]*resultState
	mu      sync.RWMutex
}

func (p *ProberImpl) AddPod(ctx context.Context, pod *v1.Pod) {
	p.mu.Lock()
	defer p.mu.Unlock()

	uid := pod.UID
	if _, exists := p.pods[uid]; exists {
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
	p.pods[uid] = tp

	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		if c.ReadinessProbe != nil {
			go p.worker(podCtx, uid, c, c.ReadinessProbe, probeReadiness)
		}
		if c.LivenessProbe != nil {
			go p.worker(livenessCtx, uid, c, c.LivenessProbe, probeLiveness)
		}
		if c.StartupProbe != nil {
			go p.worker(startupCtx, uid, c, c.StartupProbe, probeStartup)
		}
	}

	p.log.Debug().Str("pod", pod.Name).Msg("added probe workers")
}

func (p *ProberImpl) RemovePod(pod *v1.Pod) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.removePodLocked(pod.UID)
}

func (p *ProberImpl) removePodLocked(uid types.UID) {
	tp, exists := p.pods[uid]
	if !exists {
		return
	}
	tp.cancel()
	delete(p.pods, uid)

	// clean up results for this pod
	for k := range p.results {
		if k.podUID == uid {
			delete(p.results, k)
		}
	}
}

func (p *ProberImpl) CleanupPods(desiredPods map[types.UID]sets.Empty) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for uid := range p.pods {
		if _, desired := desiredPods[uid]; !desired {
			p.removePodLocked(uid)
		}
	}
}

func (p *ProberImpl) StopLivenessAndStartup(pod *v1.Pod) {
	p.mu.Lock()
	defer p.mu.Unlock()

	tp, exists := p.pods[pod.UID]
	if !exists {
		return
	}
	if tp.livenessCancel != nil {
		tp.livenessCancel()
	}
	if tp.startupCancel != nil {
		tp.startupCancel()
	}

	p.log.Debug().Str("pod", pod.Name).Msg("stopped liveness and startup probes")
}

func (p *ProberImpl) UpdatePodStatus(_ context.Context, pod *v1.Pod, podStatus *v1.PodStatus) {
	p.mu.RLock()
	defer p.mu.RUnlock()

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
				if rs, ok := p.results[key]; ok && rs.success {
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
				if rs, ok := p.results[key]; ok && rs.success {
					ready = true
				}
			} else {
				ready = true
			}
		}
		cs.Ready = ready
	}
}

func (p *ProberImpl) worker(ctx context.Context, uid types.UID, container *v1.Container, probeSpec *v1.Probe, pt probeType) {
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
		p.runProbe(ctx, uid, container, probeSpec, pt, successThreshold, failureThreshold)

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (p *ProberImpl) runProbe(ctx context.Context, uid types.UID, container *v1.Container, probeSpec *v1.Probe, pt probeType, successThreshold, failureThreshold int) {
	p.mu.RLock()
	tp, exists := p.pods[uid]
	if !exists {
		p.mu.RUnlock()
		return
	}
	podName := tp.pod.Name
	p.mu.RUnlock()

	// If this is a readiness or liveness probe, check that startup probe passed first
	if pt == probeReadiness || pt == probeLiveness {
		if container.StartupProbe != nil {
			p.mu.RLock()
			startupKey := resultKey{uid, container.Name, probeStartup}
			startupResult, ok := p.results[startupKey]
			startupPassed := ok && startupResult.success
			p.mu.RUnlock()
			if !startupPassed {
				return // startup probe hasn't passed yet
			}
		}
	}

	result := p.executeProbe(ctx, probeSpec, container, uid, podName)

	p.mu.Lock()
	defer p.mu.Unlock()

	key := resultKey{uid, container.Name, pt}
	rs, ok := p.results[key]
	if !ok {
		rs = &resultState{}
		p.results[key] = rs
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
				p.log.Info().
					Str("pod", podName).
					Str("container", container.Name).
					Str("probe", pt.String()).
					Msg("probe failed")
			}
			rs.success = false
		}
	}
}

// resolveSandboxID finds the running sandbox container ID for a given pod UID.
// Probes exec into the sandbox (busybox) rather than the app container, so
// probe tooling doesn't depend on the app image contents.
func (p *ProberImpl) resolveSandboxID(ctx context.Context, podUID types.UID) (string, error) {
	if p.backend == nil {
		return "", fmt.Errorf("no CRI backend available")
	}
	sandboxes, err := p.backend.containers.ListPodSandbox(ctx, &runtimeapi.PodSandboxFilter{
		State: &runtimeapi.PodSandboxStateValue{State: runtimeapi.PodSandboxState_SANDBOX_READY},
		LabelSelector: map[string]string{
			p.backend.Labels().UIDKey(): string(podUID),
		},
	})
	if err != nil {
		return "", fmt.Errorf("list sandboxes: %w", err)
	}
	if len(sandboxes) == 0 {
		return "", fmt.Errorf("no ready sandbox found for %s", podUID)
	}
	return sandboxes[0].Id, nil
}

// resolveContainerID finds the running container ID for a given pod UID and
// container name by querying the CRI backend with label selectors.
func (p *ProberImpl) resolveContainerID(ctx context.Context, podUID types.UID, containerName string) (string, error) {
	if p.backend == nil {
		return "", fmt.Errorf("no CRI backend available")
	}
	state := runtimeapi.ContainerState_CONTAINER_RUNNING
	containers, err := p.backend.containers.ListContainers(ctx, &runtimeapi.ContainerFilter{
		State: &runtimeapi.ContainerStateValue{State: state},
		LabelSelector: map[string]string{
			p.backend.Labels().UIDKey():  string(podUID),
			p.backend.Labels().NameKey(): containerName,
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

func (p *ProberImpl) executeProbe(ctx context.Context, probeSpec *v1.Probe, container *v1.Container, podUID types.UID, podName string) probe.Result {
	handler := probeSpec.ProbeHandler
	timeout := time.Duration(probeSpec.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 1 * time.Second
	}
	timeoutSec := int(timeout.Seconds())
	if timeoutSec <= 0 {
		timeoutSec = 1
	}

	var cmd []string
	var execTarget string // container ID to exec into

	switch {
	case handler.Exec != nil:
		// Exec probes run in the app container per Kubernetes spec
		containerID, err := p.resolveContainerID(ctx, podUID, container.Name)
		if err != nil {
			p.log.Debug().Err(err).Str("pod", podName).Str("container", container.Name).Msg("resolve container for probe")
			return probe.Failure
		}
		execTarget = containerID
		cmd = handler.Exec.Command

	case handler.HTTPGet != nil, handler.TCPSocket != nil, handler.GRPC != nil:
		// HTTP/TCP/GRPC probes exec into the sandbox (busybox) which shares
		// the pod network namespace and always has wget/nc available.
		sandboxID, err := p.resolveSandboxID(ctx, podUID)
		if err != nil {
			p.log.Debug().Err(err).Str("pod", podName).Msg("resolve sandbox for probe")
			return probe.Failure
		}
		execTarget = sandboxID

		if handler.HTTPGet != nil {
			scheme := "http"
			if handler.HTTPGet.Scheme == v1.URISchemeHTTPS {
				scheme = "https"
			}
			port := handler.HTTPGet.Port.IntValue()
			if port == 0 {
				port, err = probe.ResolveContainerPort(handler.HTTPGet.Port, container)
				if err != nil {
					p.log.Warn().Err(err).Str("pod", podName).Str("container", container.Name).Msg("http probe port error")
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
		} else if handler.TCPSocket != nil {
			port, err := probe.ResolveContainerPort(handler.TCPSocket.Port, container)
			if err != nil {
				p.log.Warn().Err(err).Str("pod", podName).Str("container", container.Name).Msg("tcp probe port error")
				return probe.Failure
			}
			cmd = []string{"nc", "-z", fmt.Sprintf("-w%d", timeoutSec), "localhost", fmt.Sprintf("%d", port)}
		} else {
			// GRPC: TCP connect fallback (does not speak the GRPC health protocol)
			cmd = []string{"nc", "-z", fmt.Sprintf("-w%d", timeoutSec), "localhost", fmt.Sprintf("%d", handler.GRPC.Port)}
		}

	default:
		p.log.Warn().Str("pod", podName).Str("container", container.Name).Msg("unknown probe handler")
		return probe.Failure
	}

	stdout, stderr, execErr := p.backend.containers.ExecSync(ctx, execTarget, cmd, timeout)
	if execErr != nil {
		p.log.Debug().
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

var _ prober.Manager = &ProberImpl{}
