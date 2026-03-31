package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/cnuss/nanokube/pkg/crid/labels"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/server/healthz"
	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	podresourcesapi "k8s.io/kubelet/pkg/apis/podresources/v1"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	"k8s.io/kubernetes/pkg/kubelet/cm/resourceupdates"
	"k8s.io/kubernetes/pkg/kubelet/config"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
	"k8s.io/kubernetes/pkg/kubelet/status"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework"
)

func NewContainerManager(backend Backend) cm.ContainerManager {
	return &ContainerManagerImpl{
		backend: backend,
		log:     component.NewLogger("container-manager"),
	}
}

type ContainerManagerImpl struct {
	backend           Backend
	ctx               context.Context
	log               component.Logger
	node              *v1.Node
	activePods        cm.ActivePodsFunc
	getNode           cm.GetNodeFunc
	sourcesReady      config.SourcesReady
	podStatusProvider status.PodStatusProvider
	runtimeService    internalapi.RuntimeService
	updates           chan resourceupdates.Update
	updatesOnce       sync.Once
}

func (m *ContainerManagerImpl) Start(ctx context.Context, node *v1.Node, activePods cm.ActivePodsFunc, getNode cm.GetNodeFunc, sourcesReady config.SourcesReady, podStatusProvider status.PodStatusProvider, runtimeService internalapi.RuntimeService, localStorageCapacityIsolation bool) error {
	m.ctx = ctx
	m.node = node
	m.activePods = activePods
	m.getNode = getNode
	m.sourcesReady = sourcesReady
	m.podStatusProvider = podStatusProvider
	m.runtimeService = runtimeService

	if localStorageCapacityIsolation {
		return fmt.Errorf("localStorageCapacityIsolation is not supported")
	}

	m.log.Info().Str("node", node.Name).Msg("container manager started")
	return nil
}

func (m *ContainerManagerImpl) SystemCgroupsLimit() v1.ResourceList {
	m.log.Warn().Msg("SystemCgroupsLimit not implemented")
	return v1.ResourceList{}
}

func (m *ContainerManagerImpl) GetNodeConfig() cm.NodeConfig {
	m.log.Warn().Msg("GetNodeConfig not implemented")
	return cm.NodeConfig{}
}

func (m *ContainerManagerImpl) Status() cm.Status {
	return cm.Status{}
}

func (m *ContainerManagerImpl) NewPodContainerManager() cm.PodContainerManager {
	return &podContainerManager{
		ctx:     m.ctx,
		log:     component.NewLogger("pod-container-manager"),
		backend: m.backend,
	}
}

func (m *ContainerManagerImpl) GetMountedSubsystems() *cm.CgroupSubsystems {
	m.log.Warn().Msg("GetMountedSubsystems not implemented")
	return &cm.CgroupSubsystems{}
}

func (m *ContainerManagerImpl) GetQOSContainersInfo() cm.QOSContainersInfo {
	m.log.Warn().Msg("GetQOSContainersInfo not implemented")
	return cm.QOSContainersInfo{}
}

func (m *ContainerManagerImpl) GetNodeAllocatableReservation() v1.ResourceList {
	m.log.Trace().Msg("GetNodeAllocatableReservation not implemented")
	return nil
}

func (m *ContainerManagerImpl) GetCapacity(localStorageCapacityIsolation bool) v1.ResourceList {
	if localStorageCapacityIsolation {
		m.log.Error().Msg("localStorageCapacityIsolation is not supported")
		return v1.ResourceList{}
	}
	return m.GetNodeAllocatableAbsolute()
}

func (m *ContainerManagerImpl) GetDevicePluginResourceCapacity() (v1.ResourceList, v1.ResourceList, []string) {
	m.log.Trace().Msg("GetDevicePluginResourceCapacity not implemented")
	return nil, nil, nil
}

func (m *ContainerManagerImpl) UpdateQOSCgroups(logger klog.Logger) error {
	m.log.Warn().Msg("UpdateQOSCgroups not implemented")
	return nil
}

func (m *ContainerManagerImpl) GetResources(ctx context.Context, pod *v1.Pod, container *v1.Container) (*kubecontainer.RunContainerOptions, error) {
	m.log.Warn().Msg("GetResources not implemented")
	return &kubecontainer.RunContainerOptions{}, nil
}

func (m *ContainerManagerImpl) UpdatePluginResources(*schedulerframework.NodeInfo, *lifecycle.PodAdmitAttributes) error {
	m.log.Warn().Msg("UpdatePluginResources not implemented")
	return nil
}

func (m *ContainerManagerImpl) InternalContainerLifecycle() cm.InternalContainerLifecycle {
	return &containerLifecycle{
		log:     component.NewLogger("container-lifecycle"),
		backend: m.backend,
	}
}

func (m *ContainerManagerImpl) GetPodCgroupRoot() string {
	m.log.Warn().Msg("GetPodCgroupRoot not implemented")
	return ""
}

func (m *ContainerManagerImpl) GetPluginRegistrationHandlers() map[string]cache.PluginHandler {
	m.log.Warn().Msg("GetPluginRegistrationHandlers not implemented")
	return nil
}

func (m *ContainerManagerImpl) GetHealthCheckers() []healthz.HealthChecker {
	m.log.Warn().Msg("GetHealthCheckers not implemented")
	return nil
}

func (m *ContainerManagerImpl) ShouldResetExtendedResourceCapacity() bool {
	m.log.Warn().Msg("ShouldResetExtendedResourceCapacity not implemented")
	return false
}

func (m *ContainerManagerImpl) GetAllocateResourcesPodAdmitHandler() lifecycle.PodAdmitHandler {
	return &podAdmitHandler{
		backend: m.backend,
		log:     component.NewLogger("pod-admit-handler"),
	}
}

func (m *ContainerManagerImpl) GetNodeAllocatableAbsolute() v1.ResourceList {
	cap := v1.ResourceList{}
	if m.backend == nil {
		return cap
	}
	host, err := m.backend.HostInfo()
	if err != nil {
		m.log.Warn().Err(err).Msg("failed to get host information")
		return cap
	}
	cap[v1.ResourceCPU] = *resource.NewQuantity(int64(len(host.CpuInfo)), resource.DecimalSI)
	mem, err := host.MemInfo()
	if err != nil {
		m.log.Warn().Err(err).Msg("failed to get memory information")
		return cap
	}
	cap[v1.ResourceMemory] = *resource.NewQuantity(mem.MemTotal, resource.BinarySI)
	return cap
}

func (m *ContainerManagerImpl) PrepareDynamicResources(ctx context.Context, pod *v1.Pod) error {
	m.log.Warn().Msg("PrepareDynamicResources not implemented")
	return nil
}

func (m *ContainerManagerImpl) UnprepareDynamicResources(ctx context.Context, pod *v1.Pod) error {
	m.log.Warn().Msg("UnprepareDynamicResources not implemented")
	return nil
}

func (m *ContainerManagerImpl) PodMightNeedToUnprepareResources(UID types.UID) bool {
	m.log.Warn().Msg("PodMightNeedToUnprepareResources not implemented")
	return false
}

func (m *ContainerManagerImpl) UpdateAllocatedResourcesStatus(pod *v1.Pod, status *v1.PodStatus) {
	m.log.Warn().Msg("UpdateAllocatedResourcesStatus not implemented")
}

func (m *ContainerManagerImpl) Updates() <-chan resourceupdates.Update {
	m.updatesOnce.Do(func() {
		m.updates = make(chan resourceupdates.Update)
		events := m.backend.Subscribe()
		go func() {
			for {
				select {
				case <-m.ctx.Done():
					return
				case ev, ok := <-events:
					if !ok {
						return
					}
					if ev.Resource != ResourceContainer {
						continue
					}
					uid := m.backend.Labels().UID(ev.Attributes)
					if uid == "" {
						continue
					}
					m.log.Info().Str("action", string(ev.Action)).Str("id", ev.ID[:min(12, len(ev.ID))]).Str("pod", uid).Msg("update event")
					select {
					case m.updates <- resourceupdates.Update{PodUIDs: []string{uid}}:
					case <-m.ctx.Done():
						return
					}
				}
			}
		}()
	})
	return m.updates
}

func (m *ContainerManagerImpl) PodHasExclusiveCPUs(pod *v1.Pod) bool {
	m.log.Warn().Msg("PodHasExclusiveCPUs not implemented")
	return false
}

func (m *ContainerManagerImpl) ContainerHasExclusiveCPUs(pod *v1.Pod, container *v1.Container) bool {
	m.log.Warn().Msg("ContainerHasExclusiveCPUs not implemented")
	return false
}

// podresources.DevicesProvider
func (m *ContainerManagerImpl) UpdateAllocatedDevices() {
	m.log.Warn().Msg("UpdateAllocatedDevices not implemented")
}

func (m *ContainerManagerImpl) GetDevices(podUID, containerName string) []*podresourcesapi.ContainerDevices {
	m.log.Warn().Msg("GetDevices not implemented")
	return nil
}

func (m *ContainerManagerImpl) GetAllocatableDevices() []*podresourcesapi.ContainerDevices {
	m.log.Warn().Msg("GetAllocatableDevices not implemented")
	return nil
}

// podresources.CPUsProvider
func (m *ContainerManagerImpl) GetCPUs(podUID, containerName string) []int64 {
	m.log.Warn().Msg("GetCPUs not implemented")
	return nil
}

func (m *ContainerManagerImpl) GetAllocatableCPUs() []int64 {
	m.log.Warn().Msg("GetAllocatableCPUs not implemented")
	return nil
}

// podresources.MemoryProvider
func (m *ContainerManagerImpl) GetMemory(podUID, containerName string) []*podresourcesapi.ContainerMemory {
	m.log.Warn().Msg("GetMemory not implemented")
	return nil
}

func (m *ContainerManagerImpl) GetAllocatableMemory() []*podresourcesapi.ContainerMemory {
	m.log.Warn().Msg("GetAllocatableMemory not implemented")
	return nil
}

// podresources.DynamicResourcesProvider
func (m *ContainerManagerImpl) GetDynamicResources(pod *v1.Pod, container *v1.Container) []*podresourcesapi.DynamicResource {
	m.log.Warn().Msg("GetDynamicResources not implemented")
	return nil
}

// internalContainerLifecycle implements cm.InternalContainerLifecycle as a no-op.
type containerLifecycle struct {
	log     component.Logger
	backend Backend
}

func (i *containerLifecycle) PreCreateContainer(logger klog.Logger, pod *v1.Pod, container *v1.Container, containerConfig *runtimeapi.ContainerConfig) error {
	return nil
}

func (i *containerLifecycle) PreStartContainer(logger klog.Logger, pod *v1.Pod, container *v1.Container, containerID string) error {
	return nil
}

func (i *containerLifecycle) PostStopContainer(logger klog.Logger, containerID string) error {
	return nil
}

// podContainerManager implements cm.PodContainerManager.
type podContainerManager struct {
	ctx     context.Context
	log     component.Logger
	backend Backend
}

func (p *podContainerManager) GetPodContainerName(pod *v1.Pod) (cm.CgroupName, string) {
	if p.backend == nil {
		return nil, ""
	}
	sandboxes, err := p.backend.Containers().ListPodSandbox(p.ctx, &runtimeapi.PodSandboxFilter{
		LabelSelector: map[string]string{
			p.backend.Labels().UIDKey(): string(pod.UID),
		},
	})
	if err != nil {
		p.log.Warn().Err(err).Str("pod", pod.Name).Msg("failed to list sandboxes")
		return nil, ""
	}
	if len(sandboxes) == 0 {
		return nil, ""
	}
	return nil, sandboxes[0].Id
}

func (p *podContainerManager) EnsureExists(logger klog.Logger, pod *v1.Pod) error {
	return nil
}

func (p *podContainerManager) Exists(pod *v1.Pod) bool {
	p.log.Trace().Str("pod", pod.Name).Msg("checking if pod container exists")
	if p.backend == nil {
		return true
	}
	sandboxes, err := p.backend.Containers().ListPodSandbox(p.ctx, &runtimeapi.PodSandboxFilter{
		LabelSelector: map[string]string{
			p.backend.Labels().UIDKey(): string(pod.UID),
		},
	})
	if err != nil {
		p.log.Warn().Err(err).Str("pod", pod.Name).Msg("failed to list sandboxes")
		return true
	}
	return len(sandboxes) > 0
}

func (p *podContainerManager) Destroy(logger klog.Logger, name cm.CgroupName) error {
	p.log.Warn().Msg("Destroy not implemented")
	return nil
}

func (p *podContainerManager) ReduceCPULimits(logger klog.Logger, name cm.CgroupName) error {
	p.log.Warn().Msg("ReduceCPULimits not implemented")
	return nil
}

func (p *podContainerManager) GetAllPodsFromCgroups() (map[types.UID]cm.CgroupName, error) {
	p.log.Warn().Msg("GetAllPodsFromCgroups not implemented")
	return nil, nil
}

func (p *podContainerManager) IsPodCgroup(cgroupfs string) (bool, types.UID) {
	p.log.Warn().Msg("IsPodCgroup not implemented")
	return false, ""
}

func (p *podContainerManager) GetPodCgroupMemoryUsage(pod *v1.Pod) (uint64, error) {
	p.log.Warn().Msg("GetPodCgroupMemoryUsage not implemented")
	return 0, nil
}

func (p *podContainerManager) GetPodCgroupConfig(pod *v1.Pod, resource v1.ResourceName) (*cm.ResourceConfig, error) {
	p.log.Warn().Msg("GetPodCgroupConfig not implemented")
	return nil, nil
}

func (p *podContainerManager) SetPodCgroupConfig(logger klog.Logger, pod *v1.Pod, resourceConfig *cm.ResourceConfig) error {
	p.log.Warn().Msg("SetPodCgroupConfig not implemented")
	return nil
}

// podAdmitHandler implements lifecycle.PodAdmitHandler.
// Injects host aliases into pod specs before kubelet generates /etc/hosts.
type podAdmitHandler struct {
	backend Backend
	log     component.Logger
}

func (p *podAdmitHandler) Admit(attrs *lifecycle.PodAdmitAttributes) lifecycle.PodAdmitResult {
	pod := attrs.Pod
	hosts := p.backend.Hosts()
	p.log.Info().Any("attrs", attrs).Any("hosts", hosts.Entries(p.backend.Context(), NetworkBridge)).Msg("admitting pod")

	// Sanitize pod name — replace dots with hyphens for DNS compatibility
	if strings.Contains(pod.Name, ".") {
		pod.Name = strings.ReplaceAll(pod.Name, ".", "-")
	}

	if h := p.backend.Hosts(); h != nil {
		network := NetworkBridge
		if pod.Spec.HostNetwork {
			network = NetworkHost
		}
		pod.Spec.HostAliases = append(pod.Spec.HostAliases, h.HostAliases(p.backend.Context(), network)...)
	}

	// Inject security context as annotation so CreateContainer can apply it on
	// platforms where kubelet doesn't populate LinuxContainerConfig (e.g. macOS).
	if sc := pod.Spec.SecurityContext; sc != nil {
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}
		var user string
		if sc.RunAsUser != nil {
			user = strconv.FormatInt(*sc.RunAsUser, 10)
		}
		if sc.RunAsGroup != nil {
			user += ":" + strconv.FormatInt(*sc.RunAsGroup, 10)
		}
		if user != "" {
			pod.Annotations[p.backend.Labels().Prefix(labels.SecurityContextKey)] = user
		}
	}

	// Inject host aliases as annotation so RunPodSandbox can set ExtraHosts
	if len(pod.Spec.HostAliases) > 0 {
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}
		b, _ := json.Marshal(pod.Spec.HostAliases)
		pod.Annotations[p.backend.Labels().Prefix(labels.HostAliasesKey)] = string(b)
	}

	// Rewrite HTTP/TCP/GRPC probes to exec probes so the upstream kubelet prober
	// runs them via ExecSync in the sandbox (which shares the pod network namespace)
	// instead of connecting from the host to podIP (unreachable on macOS).
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		rewriteProbe(c.LivenessProbe)
		rewriteProbe(c.ReadinessProbe)
		rewriteProbe(c.StartupProbe)
	}

	// Inject container names as annotation so RunPodSandbox can set DNS aliases
	if len(pod.Spec.Containers) > 0 {
		names := make([]string, 0, len(pod.Spec.Containers))
		for _, c := range pod.Spec.Containers {
			names = append(names, c.Name)
		}
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}
		pod.Annotations[p.backend.Labels().Prefix(labels.DNSAliasesKey)] = strings.Join(names, ",")
	}

	return lifecycle.PodAdmitResult{Admit: true}
}

// SandboxExecSentinel is prepended to rewritten probe commands so that
// ExecSync can detect them and route execution to the pod sandbox container
// (which has busybox with wget/nc) instead of the app container.
const SandboxExecSentinel = "__sandbox__"

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

		cmd := []string{SandboxExecSentinel, "wget", "-q", "-O", "/dev/null", "-S", "--no-check-certificate", fmt.Sprintf("--timeout=%d", timeout)}
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
			Command: []string{SandboxExecSentinel, "nc", "-z", fmt.Sprintf("-w%d", timeout), "localhost", fmt.Sprintf("%d", port)},
		}

	case probe.GRPC != nil:
		port := probe.GRPC.Port
		probe.GRPC = nil
		probe.Exec = &v1.ExecAction{
			Command: []string{SandboxExecSentinel, "nc", "-z", fmt.Sprintf("-w%d", timeout), "localhost", fmt.Sprintf("%d", port)},
		}
	}
}

var _ cm.ContainerManager = &ContainerManagerImpl{}
