package kubelet

import (
	"context"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/cnuss/nanokube/pkg/cri"
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

// ContainerManager implements cm.ContainerManager for nanokube.
type ContainerManager struct {
	ctx               context.Context
	log               component.Logger
	backend           cri.Backend
	node              *v1.Node
	activePods        cm.ActivePodsFunc
	getNode           cm.GetNodeFunc
	sourcesReady      config.SourcesReady
	podStatusProvider status.PodStatusProvider
	runtimeService    internalapi.RuntimeService
}

var _ cm.ContainerManager = &ContainerManager{}

// NewContainerManager creates a ContainerManager backed by the CRI backend.
func NewContainerManager(ctx context.Context, backend cri.Backend) *ContainerManager {
	return &ContainerManager{ctx: ctx, backend: backend, log: component.NewLogger("container-manager")}
}

func (m *ContainerManager) Start(ctx context.Context, node *v1.Node, activePods cm.ActivePodsFunc, getNode cm.GetNodeFunc, sourcesReady config.SourcesReady, podStatusProvider status.PodStatusProvider, runtimeService internalapi.RuntimeService, _ bool) error {
	m.ctx = ctx
	m.node = node
	m.activePods = activePods
	m.getNode = getNode
	m.sourcesReady = sourcesReady
	m.podStatusProvider = podStatusProvider
	m.runtimeService = runtimeService

	m.log.Info().Str("node", node.Name).Msg("container manager started")
	return nil
}

func (m *ContainerManager) SystemCgroupsLimit() v1.ResourceList {
	m.log.Warn().Msg("SystemCgroupsLimit not implemented")
	return v1.ResourceList{}
}

func (m *ContainerManager) GetNodeConfig() cm.NodeConfig {
	m.log.Warn().Msg("GetNodeConfig not implemented")
	return cm.NodeConfig{}
}

func (m *ContainerManager) Status() cm.Status {
	m.log.Warn().Msg("Status not implemented")
	return cm.Status{}
}

func (m *ContainerManager) NewPodContainerManager() cm.PodContainerManager {
	return &podContainerManager{
		log: component.NewLogger("pod-container-manager"),
	}
}

func (m *ContainerManager) GetMountedSubsystems() *cm.CgroupSubsystems {
	m.log.Warn().Msg("GetMountedSubsystems not implemented")
	return &cm.CgroupSubsystems{}
}

func (m *ContainerManager) GetQOSContainersInfo() cm.QOSContainersInfo {
	m.log.Warn().Msg("GetQOSContainersInfo not implemented")
	return cm.QOSContainersInfo{}
}

func (m *ContainerManager) GetNodeAllocatableReservation() v1.ResourceList {
	m.log.Warn().Msg("GetNodeAllocatableReservation not implemented")
	return nil
}

func (m *ContainerManager) GetCapacity(localStorageCapacityIsolation bool) v1.ResourceList {
	m.log.Info().Bool("localStorageCapacityIsolation", localStorageCapacityIsolation).Msg("GetCapacity")
	return m.GetNodeAllocatableAbsolute()
}

func (m *ContainerManager) GetDevicePluginResourceCapacity() (v1.ResourceList, v1.ResourceList, []string) {
	m.log.Warn().Msg("GetDevicePluginResourceCapacity not implemented")
	return nil, nil, nil
}

func (m *ContainerManager) UpdateQOSCgroups(logger klog.Logger) error {
	m.log.Warn().Msg("UpdateQOSCgroups not implemented")
	return nil
}

func (m *ContainerManager) GetResources(ctx context.Context, pod *v1.Pod, container *v1.Container) (*kubecontainer.RunContainerOptions, error) {
	m.log.Warn().Msg("GetResources not implemented")
	return &kubecontainer.RunContainerOptions{}, nil
}

func (m *ContainerManager) UpdatePluginResources(*schedulerframework.NodeInfo, *lifecycle.PodAdmitAttributes) error {
	m.log.Warn().Msg("UpdatePluginResources not implemented")
	return nil
}

func (m *ContainerManager) InternalContainerLifecycle() cm.InternalContainerLifecycle {
	return &internalContainerLifecycle{
		log: component.NewLogger("internal-container-lifecycle"),
	}
}

func (m *ContainerManager) GetPodCgroupRoot() string {
	m.log.Warn().Msg("GetPodCgroupRoot not implemented")
	return ""
}

func (m *ContainerManager) GetPluginRegistrationHandlers() map[string]cache.PluginHandler {
	m.log.Warn().Msg("GetPluginRegistrationHandlers not implemented")
	return nil
}

func (m *ContainerManager) GetHealthCheckers() []healthz.HealthChecker {
	m.log.Warn().Msg("GetHealthCheckers not implemented")
	return nil
}

func (m *ContainerManager) ShouldResetExtendedResourceCapacity() bool {
	m.log.Warn().Msg("ShouldResetExtendedResourceCapacity not implemented")
	return false
}

func (m *ContainerManager) GetAllocateResourcesPodAdmitHandler() lifecycle.PodAdmitHandler {
	return &podAdmitHandler{
		log: component.NewLogger("pod-admit-handler"),
	}
}

func (m *ContainerManager) GetNodeAllocatableAbsolute() v1.ResourceList {
	cap := v1.ResourceList{}
	if m.backend == nil {
		return cap
	}
	cpus, memBytes, _, _, err := m.backend.HostInfo(m.ctx)
	if err != nil || cpus == 0 {
		m.log.Warn().Err(err).Msg("HostInfo unavailable")
		return cap
	}
	cap[v1.ResourceCPU] = *resource.NewQuantity(int64(cpus), resource.DecimalSI)
	cap[v1.ResourceMemory] = *resource.NewQuantity(memBytes, resource.BinarySI)
	return cap
}

func (m *ContainerManager) PrepareDynamicResources(ctx context.Context, pod *v1.Pod) error {
	m.log.Warn().Msg("PrepareDynamicResources not implemented")
	return nil
}

func (m *ContainerManager) UnprepareDynamicResources(ctx context.Context, pod *v1.Pod) error {
	m.log.Warn().Msg("UnprepareDynamicResources not implemented")
	return nil
}

func (m *ContainerManager) PodMightNeedToUnprepareResources(UID types.UID) bool {
	m.log.Warn().Msg("PodMightNeedToUnprepareResources not implemented")
	return false
}

func (m *ContainerManager) UpdateAllocatedResourcesStatus(pod *v1.Pod, status *v1.PodStatus) {
	m.log.Warn().Msg("UpdateAllocatedResourcesStatus not implemented")
}

func (m *ContainerManager) Updates() <-chan resourceupdates.Update {
	m.log.Warn().Msg("Updates not implemented")
	return nil
}

func (m *ContainerManager) PodHasExclusiveCPUs(pod *v1.Pod) bool {
	m.log.Warn().Msg("PodHasExclusiveCPUs not implemented")
	return false
}

func (m *ContainerManager) ContainerHasExclusiveCPUs(pod *v1.Pod, container *v1.Container) bool {
	m.log.Warn().Msg("ContainerHasExclusiveCPUs not implemented")
	return false
}

// podresources.DevicesProvider
func (m *ContainerManager) UpdateAllocatedDevices() {
	m.log.Warn().Msg("UpdateAllocatedDevices not implemented")
}

func (m *ContainerManager) GetDevices(podUID, containerName string) []*podresourcesapi.ContainerDevices {
	m.log.Warn().Msg("GetDevices not implemented")
	return nil
}

func (m *ContainerManager) GetAllocatableDevices() []*podresourcesapi.ContainerDevices {
	m.log.Warn().Msg("GetAllocatableDevices not implemented")
	return nil
}

// podresources.CPUsProvider
func (m *ContainerManager) GetCPUs(podUID, containerName string) []int64 {
	m.log.Warn().Msg("GetCPUs not implemented")
	return nil
}

func (m *ContainerManager) GetAllocatableCPUs() []int64 {
	m.log.Warn().Msg("GetAllocatableCPUs not implemented")
	return nil
}

// podresources.MemoryProvider
func (m *ContainerManager) GetMemory(podUID, containerName string) []*podresourcesapi.ContainerMemory {
	m.log.Warn().Msg("GetMemory not implemented")
	return nil
}

func (m *ContainerManager) GetAllocatableMemory() []*podresourcesapi.ContainerMemory {
	m.log.Warn().Msg("GetAllocatableMemory not implemented")
	return nil
}

// podresources.DynamicResourcesProvider
func (m *ContainerManager) GetDynamicResources(pod *v1.Pod, container *v1.Container) []*podresourcesapi.DynamicResource {
	m.log.Warn().Msg("GetDynamicResources not implemented")
	return nil
}

// internalContainerLifecycle implements cm.InternalContainerLifecycle as a no-op.
type internalContainerLifecycle struct {
	log component.Logger
}

func (i *internalContainerLifecycle) PreCreateContainer(logger klog.Logger, pod *v1.Pod, container *v1.Container, containerConfig *runtimeapi.ContainerConfig) error {
	i.log.Warn().Msg("PreCreateContainer not implemented")
	return nil
}

func (i *internalContainerLifecycle) PreStartContainer(logger klog.Logger, pod *v1.Pod, container *v1.Container, containerID string) error {
	i.log.Warn().Msg("PreStartContainer not implemented")
	return nil
}

func (i *internalContainerLifecycle) PostStopContainer(logger klog.Logger, containerID string) error {
	i.log.Warn().Msg("PostStopContainer not implemented")
	return nil
}

// podContainerManager implements cm.PodContainerManager as a no-op.
type podContainerManager struct {
	log component.Logger
}

func (p *podContainerManager) GetPodContainerName(pod *v1.Pod) (cm.CgroupName, string) {
	p.log.Warn().Msg("GetPodContainerName not implemented")
	return nil, ""
}

func (p *podContainerManager) EnsureExists(logger klog.Logger, pod *v1.Pod) error {
	p.log.Warn().Msg("EnsureExists not implemented")
	return nil
}

func (p *podContainerManager) Exists(pod *v1.Pod) bool {
	p.log.Warn().Msg("Exists not implemented")
	return true
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
