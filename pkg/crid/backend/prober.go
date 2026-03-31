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
	"k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/prober"
	"k8s.io/kubernetes/pkg/kubelet/prober/results"
	"k8s.io/kubernetes/pkg/kubelet/status"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
)

// proberPodManager implements the minimal status.PodManager interface for the prober.
type proberPodManager struct{}

func (m *proberPodManager) GetPodByUID(uid types.UID) (*v1.Pod, bool)     { return nil, false }
func (m *proberPodManager) GetMirrorPodByPod(pod *v1.Pod) (*v1.Pod, bool) { return nil, false }
func (m *proberPodManager) TranslatePodUID(uid types.UID) kubetypes.ResolvedPodUID {
	return kubetypes.ResolvedPodUID(uid)
}

func (m *proberPodManager) GetUIDTranslations() (map[kubetypes.ResolvedPodUID]kubetypes.MirrorPodUID, map[kubetypes.MirrorPodUID]kubetypes.ResolvedPodUID) {
	return nil, nil
}

// proberDeletionSafety implements status.PodDeletionSafetyProvider.
type proberDeletionSafety struct{}

func (d *proberDeletionSafety) PodCouldHaveRunningContainers(pod *v1.Pod) bool { return true }

// proberStartupLatency implements status.PodStartupLatencyStateHelper.
type proberStartupLatency struct{}

func (l *proberStartupLatency) RecordStatusUpdated(pod *v1.Pod)        {}
func (l *proberStartupLatency) DeletePodStartupState(podUID types.UID) {}

func NewProber(backend *BackendImpl) prober.Manager {
	return &ProberImpl{
		backend: backend,
		log:     component.NewLogger("prober"),
		runner: &CommandRunnerImpl{
			backend: backend,
			log:     component.NewLogger("command-runner"),
		},
	}
}

type ProberImpl struct {
	backend   *BackendImpl
	log       component.Logger
	runner    *CommandRunnerImpl
	inner     prober.Manager
	innerOnce sync.Once
}

func (p *ProberImpl) Inner() prober.Manager {
	p.innerOnce.Do(func() {
		statusMgr := status.NewManager(
			p.backend.KubeClient(),
			&proberPodManager{},
			&proberDeletionSafety{},
			&proberStartupLatency{},
		)
		p.inner = prober.NewManager(
			statusMgr,
			results.NewManager(),
			results.NewManager(),
			results.NewManager(),
			p.runner,
			p.backend.EventRecorder(),
		)
	})
	return p.inner
}

type CommandRunnerImpl struct {
	backend *BackendImpl
	log     component.Logger
	// sandboxes maps app container IDs to their pod's sandbox container ID.
	sandboxes sync.Map // map[string]string
}

// AddPod rewrites HTTP/TCP/GRPC probes into exec probes so the upstream
// prober routes everything through RunInContainer → sandbox exec.
func (p *ProberImpl) AddPod(ctx context.Context, pod *v1.Pod) {
	rewritten := pod.DeepCopy()
	for i := range rewritten.Spec.Containers {
		c := &rewritten.Spec.Containers[i]
		rewriteProbe(c.LivenessProbe)
		rewriteProbe(c.ReadinessProbe)
		rewriteProbe(c.StartupProbe)
	}
	p.log.Info().Str("pod", pod.Name).Msg("AddPod")
	p.Inner().AddPod(ctx, rewritten)
}

func (p *ProberImpl) RemovePod(pod *v1.Pod) {
	p.Inner().RemovePod(pod)
}

func (p *ProberImpl) CleanupPods(desiredPods map[types.UID]sets.Empty) {
	p.Inner().CleanupPods(desiredPods)
}

func (p *ProberImpl) StopLivenessAndStartup(pod *v1.Pod) {
	p.Inner().StopLivenessAndStartup(pod)
}

func (p *ProberImpl) UpdatePodStatus(ctx context.Context, pod *v1.Pod, podStatus *v1.PodStatus) {
	p.Inner().UpdatePodStatus(ctx, pod, podStatus)
}

// rewriteProbe converts HTTP/TCP/GRPC probes into exec probes that run
// wget or nc inside the sandbox container (which shares the pod network namespace).
func rewriteProbe(probe *v1.Probe) {
	if probe == nil {
		return
	}

	timeout := probe.TimeoutSeconds
	if timeout <= 0 {
		timeout = 1
	}

	switch {
	case probe.HTTPGet != nil:
		scheme := "http"
		if probe.HTTPGet.Scheme == v1.URISchemeHTTPS {
			scheme = "https"
		}
		port := probe.HTTPGet.Port.IntValue()
		path := probe.HTTPGet.Path
		if path == "" {
			path = "/"
		}
		url := fmt.Sprintf("%s://localhost:%d%s", scheme, port, path)

		cmd := []string{"wget", "-q", "-O", "/dev/null", "-S", fmt.Sprintf("--timeout=%d", timeout)}
		if host := probe.HTTPGet.Host; host != "" {
			cmd = append(cmd, "--header", fmt.Sprintf("Host: %s", host))
		}
		for _, h := range probe.HTTPGet.HTTPHeaders {
			cmd = append(cmd, "--header", fmt.Sprintf("%s: %s", h.Name, h.Value))
		}
		cmd = append(cmd, url)

		probe.HTTPGet = nil
		probe.Exec = &v1.ExecAction{Command: cmd}

	case probe.TCPSocket != nil:
		port := probe.TCPSocket.Port.IntValue()
		probe.TCPSocket = nil
		probe.Exec = &v1.ExecAction{
			Command: []string{"nc", "-z", fmt.Sprintf("-w%d", timeout), "localhost", fmt.Sprintf("%d", port)},
		}

	case probe.GRPC != nil:
		port := probe.GRPC.Port
		probe.GRPC = nil
		probe.Exec = &v1.ExecAction{
			Command: []string{"nc", "-z", fmt.Sprintf("-w%d", timeout), "localhost", fmt.Sprintf("%d", port)},
		}
	}
}

// RunInContainer executes a command in the sandbox container that shares
// the pod's network namespace. The upstream prober passes the app container ID,
// but we resolve it to the sandbox for network probes.
func (c *CommandRunnerImpl) RunInContainer(ctx context.Context, id container.ContainerID, cmd []string, timeout time.Duration) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Resolve the sandbox for this container's pod
	sandboxID, err := c.resolveSandboxForContainer(ctx, id.ID)
	if err != nil {
		return nil, fmt.Errorf("resolve sandbox: %w", err)
	}

	stdout, stderr, err := c.backend.containers.ExecSync(ctx, sandboxID, cmd, timeout)
	if err != nil {
		return append(stdout, stderr...), err
	}
	return stdout, nil
}

// resolveSandboxForContainer finds the sandbox container that shares the network
// namespace with the given app container, using cached lookups.
func (c *CommandRunnerImpl) resolveSandboxForContainer(ctx context.Context, containerID string) (string, error) {
	// Check cache first
	if sandboxID, ok := c.sandboxes.Load(containerID); ok {
		return sandboxID.(string), nil
	}

	// Look up the container to find its pod UID
	resp, err := c.backend.containers.ContainerStatus(ctx, containerID, false)
	if err != nil {
		return "", fmt.Errorf("container status: %w", err)
	}
	podUID := resp.Status.Labels[c.backend.Labels().UIDKey()]
	if podUID == "" {
		return "", fmt.Errorf("no pod UID label on container %s", containerID)
	}

	// Find the sandbox for this pod
	sandboxes, err := c.backend.containers.ListPodSandbox(ctx, &runtimeapi.PodSandboxFilter{
		State: &runtimeapi.PodSandboxStateValue{State: runtimeapi.PodSandboxState_SANDBOX_READY},
		LabelSelector: map[string]string{
			c.backend.Labels().UIDKey(): podUID,
		},
	})
	if err != nil {
		return "", fmt.Errorf("list sandboxes: %w", err)
	}
	if len(sandboxes) == 0 {
		return "", fmt.Errorf("no ready sandbox for pod %s", podUID)
	}

	sandboxID := sandboxes[0].Id
	c.sandboxes.Store(containerID, sandboxID)
	return sandboxID, nil
}

var (
	_ prober.Manager          = &ProberImpl{}
	_ container.CommandRunner = &CommandRunnerImpl{}
)
