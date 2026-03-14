package backend

import (
	"context"
	"fmt"

	"github.com/cnuss/nanokube/pkg/component"
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

func NewContainerManager(backend *BackendImpl) cm.ContainerManager {
	return &ContainerManagerImpl{
		backend: backend,
		log:     component.NewLogger("container-manager"),
		updates: make(chan resourceupdates.Update, 64),
	}
}

type ContainerManagerImpl struct {
	backend           *BackendImpl
	ctx               context.Context
	log               component.Logger
	node              *v1.Node
	activePods        cm.ActivePodsFunc
	getNode           cm.GetNodeFunc
	sourcesReady      config.SourcesReady
	podStatusProvider status.PodStatusProvider
	runtimeService    internalapi.RuntimeService
	updates           chan resourceupdates.Update
	events            <-chan Event
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

	if m.backend != nil {
		m.events = m.backend.Subscribe()
		go m.streamContainerEvents(ctx)
	}

	return nil
}

func (m *ContainerManagerImpl) streamContainerEvents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-m.events:
			if !ok {
				return
			}
			if ev.Resource != ResourceContainer {
				continue
			}
			uid := m.backend.Labels().PodUID(ev.Attributes)
			if uid == "" {
				continue
			}
			m.log.Info().
				Str("pod", uid).
				Str("id", ev.ID[:min(12, len(ev.ID))]).
				Str("action", string(ev.Action)).
				Msg("container event")

			select {
			case m.updates <- resourceupdates.Update{PodUIDs: []string{uid}}:
			case <-ctx.Done():
				return
			}
		}
	}
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
		log: component.NewLogger("container-lifecycle"),
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
		log: component.NewLogger("pod-admit-handler"),
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
	log component.Logger
}

func (i *containerLifecycle) PreCreateContainer(logger klog.Logger, pod *v1.Pod, container *v1.Container, containerConfig *runtimeapi.ContainerConfig) error {
	i.log.Warn().Msg("PreCreateContainer not implemented")
	return nil
}

func (i *containerLifecycle) PreStartContainer(logger klog.Logger, pod *v1.Pod, container *v1.Container, containerID string) error {
	i.log.Warn().Msg("PreStartContainer not implemented")
	return nil
}

func (i *containerLifecycle) PostStopContainer(logger klog.Logger, containerID string) error {
	i.log.Warn().Msg("PostStopContainer not implemented")
	return nil
}

// podContainerManager implements cm.PodContainerManager.
type podContainerManager struct {
	ctx     context.Context
	log     component.Logger
	backend *BackendImpl
}

func (p *podContainerManager) GetPodContainerName(pod *v1.Pod) (cm.CgroupName, string) {
	if p.backend == nil {
		return nil, ""
	}
	sandboxes, err := p.backend.ContainerServer().ListPodSandbox(p.ctx, &runtimeapi.ListPodSandboxRequest{
		Filter: &runtimeapi.PodSandboxFilter{
			LabelSelector: map[string]string{
				p.backend.Labels().PodUIDKey(): string(pod.UID),
			},
		},
	})
	if err != nil {
		p.log.Warn().Err(err).Str("pod", pod.Name).Msg("failed to list sandboxes")
		return nil, ""
	}
	if len(sandboxes.GetItems()) == 0 {
		return nil, ""
	}
	return nil, sandboxes.GetItems()[0].Id
}

func (p *podContainerManager) EnsureExists(logger klog.Logger, pod *v1.Pod) error {
	return nil
}

func (p *podContainerManager) Exists(pod *v1.Pod) bool {
	p.log.Trace().Str("pod", pod.Name).Msg("checking if pod container exists")
	if p.backend == nil {
		return true
	}
	sandboxes, err := p.backend.ContainerServer().ListPodSandbox(p.ctx, &runtimeapi.ListPodSandboxRequest{
		Filter: &runtimeapi.PodSandboxFilter{
			LabelSelector: map[string]string{
				p.backend.Labels().PodUIDKey(): string(pod.UID),
			},
		},
	})
	if err != nil {
		p.log.Warn().Err(err).Str("pod", pod.Name).Msg("failed to list sandboxes")
		return true
	}
	return len(sandboxes.GetItems()) > 0
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

// noopPodAdmitHandler implements lifecycle.PodAdmitHandler.
type podAdmitHandler struct {
	log component.Logger
}

func (n *podAdmitHandler) Admit(attrs *lifecycle.PodAdmitAttributes) lifecycle.PodAdmitResult {
	n.log.Warn().Msg("Admit not implemented")
	return lifecycle.PodAdmitResult{Admit: true}
}

var _ cm.ContainerManager = &ContainerManagerImpl{}
