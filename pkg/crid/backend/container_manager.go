package backend

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/server/healthz"
	cri "k8s.io/cri-api/pkg/apis"
	"k8s.io/klog/v2"
	podresourcesv1 "k8s.io/kubelet/pkg/apis/podresources/v1"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	"k8s.io/kubernetes/pkg/kubelet/cm/resourceupdates"
	"k8s.io/kubernetes/pkg/kubelet/config"
	"k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
	"k8s.io/kubernetes/pkg/kubelet/status"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

func NewContainerManager(backend *BackendImpl) cm.ContainerManager {
	containerManager := &ContainerManagerImpl{backend: backend}
	// TODO: start
	return containerManager
}

type ContainerManagerImpl struct {
	backend *BackendImpl
}

// ContainerHasExclusiveCPUs implements [cm.ContainerManager].
func (c *ContainerManagerImpl) ContainerHasExclusiveCPUs(pod *v1.Pod, container *v1.Container) bool {
	panic("unimplemented")
}

// GetAllocatableCPUs implements [cm.ContainerManager].
func (c *ContainerManagerImpl) GetAllocatableCPUs() []int64 {
	panic("unimplemented")
}

// GetAllocatableDevices implements [cm.ContainerManager].
func (c *ContainerManagerImpl) GetAllocatableDevices() []*podresourcesv1.ContainerDevices {
	panic("unimplemented")
}

// GetAllocatableMemory implements [cm.ContainerManager].
func (c *ContainerManagerImpl) GetAllocatableMemory() []*podresourcesv1.ContainerMemory {
	panic("unimplemented")
}

// GetAllocateResourcesPodAdmitHandler implements [cm.ContainerManager].
func (c *ContainerManagerImpl) GetAllocateResourcesPodAdmitHandler() lifecycle.PodAdmitHandler {
	panic("unimplemented")
}

// GetCPUs implements [cm.ContainerManager].
func (c *ContainerManagerImpl) GetCPUs(podUID string, containerName string) []int64 {
	panic("unimplemented")
}

// GetCapacity implements [cm.ContainerManager].
func (c *ContainerManagerImpl) GetCapacity(localStorageCapacityIsolation bool) v1.ResourceList {
	panic("unimplemented")
}

// GetDevicePluginResourceCapacity implements [cm.ContainerManager].
func (c *ContainerManagerImpl) GetDevicePluginResourceCapacity() (v1.ResourceList, v1.ResourceList, []string) {
	panic("unimplemented")
}

// GetDevices implements [cm.ContainerManager].
func (c *ContainerManagerImpl) GetDevices(podUID string, containerName string) []*podresourcesv1.ContainerDevices {
	panic("unimplemented")
}

// GetDynamicResources implements [cm.ContainerManager].
func (c *ContainerManagerImpl) GetDynamicResources(pod *v1.Pod, container *v1.Container) []*podresourcesv1.DynamicResource {
	panic("unimplemented")
}

// GetHealthCheckers implements [cm.ContainerManager].
func (c *ContainerManagerImpl) GetHealthCheckers() []healthz.HealthChecker {
	panic("unimplemented")
}

// GetMemory implements [cm.ContainerManager].
func (c *ContainerManagerImpl) GetMemory(podUID string, containerName string) []*podresourcesv1.ContainerMemory {
	panic("unimplemented")
}

// GetMountedSubsystems implements [cm.ContainerManager].
func (c *ContainerManagerImpl) GetMountedSubsystems() *cm.CgroupSubsystems {
	panic("unimplemented")
}

// GetNodeAllocatableAbsolute implements [cm.ContainerManager].
func (c *ContainerManagerImpl) GetNodeAllocatableAbsolute() v1.ResourceList {
	panic("unimplemented")
}

// GetNodeAllocatableReservation implements [cm.ContainerManager].
func (c *ContainerManagerImpl) GetNodeAllocatableReservation() v1.ResourceList {
	panic("unimplemented")
}

// GetNodeConfig implements [cm.ContainerManager].
func (c *ContainerManagerImpl) GetNodeConfig() cm.NodeConfig {
	panic("unimplemented")
}

// GetPluginRegistrationHandlers implements [cm.ContainerManager].
func (c *ContainerManagerImpl) GetPluginRegistrationHandlers() map[string]cache.PluginHandler {
	panic("unimplemented")
}

// GetPodCgroupRoot implements [cm.ContainerManager].
func (c *ContainerManagerImpl) GetPodCgroupRoot() string {
	panic("unimplemented")
}

// GetQOSContainersInfo implements [cm.ContainerManager].
func (c *ContainerManagerImpl) GetQOSContainersInfo() cm.QOSContainersInfo {
	panic("unimplemented")
}

// GetResources implements [cm.ContainerManager].
func (c *ContainerManagerImpl) GetResources(ctx context.Context, pod *v1.Pod, container *v1.Container) (*container.RunContainerOptions, error) {
	panic("unimplemented")
}

// InternalContainerLifecycle implements [cm.ContainerManager].
func (c *ContainerManagerImpl) InternalContainerLifecycle() cm.InternalContainerLifecycle {
	panic("unimplemented")
}

// NewPodContainerManager implements [cm.ContainerManager].
func (c *ContainerManagerImpl) NewPodContainerManager() cm.PodContainerManager {
	panic("unimplemented")
}

// PodHasExclusiveCPUs implements [cm.ContainerManager].
func (c *ContainerManagerImpl) PodHasExclusiveCPUs(pod *v1.Pod) bool {
	panic("unimplemented")
}

// PodMightNeedToUnprepareResources implements [cm.ContainerManager].
func (c *ContainerManagerImpl) PodMightNeedToUnprepareResources(UID types.UID) bool {
	panic("unimplemented")
}

// PrepareDynamicResources implements [cm.ContainerManager].
func (c *ContainerManagerImpl) PrepareDynamicResources(context.Context, *v1.Pod) error {
	panic("unimplemented")
}

// ShouldResetExtendedResourceCapacity implements [cm.ContainerManager].
func (c *ContainerManagerImpl) ShouldResetExtendedResourceCapacity() bool {
	panic("unimplemented")
}

// Start implements [cm.ContainerManager].
func (c *ContainerManagerImpl) Start(context.Context, *v1.Node, cm.ActivePodsFunc, cm.GetNodeFunc, config.SourcesReady, status.PodStatusProvider, cri.RuntimeService, bool) error {
	panic("unimplemented")
}

// Status implements [cm.ContainerManager].
func (c *ContainerManagerImpl) Status() cm.Status {
	panic("unimplemented")
}

// SystemCgroupsLimit implements [cm.ContainerManager].
func (c *ContainerManagerImpl) SystemCgroupsLimit() v1.ResourceList {
	panic("unimplemented")
}

// UnprepareDynamicResources implements [cm.ContainerManager].
func (c *ContainerManagerImpl) UnprepareDynamicResources(context.Context, *v1.Pod) error {
	panic("unimplemented")
}

// UpdateAllocatedDevices implements [cm.ContainerManager].
func (c *ContainerManagerImpl) UpdateAllocatedDevices() {
	panic("unimplemented")
}

// UpdateAllocatedResourcesStatus implements [cm.ContainerManager].
func (c *ContainerManagerImpl) UpdateAllocatedResourcesStatus(pod *v1.Pod, status *v1.PodStatus) {
	panic("unimplemented")
}

// UpdatePluginResources implements [cm.ContainerManager].
func (c *ContainerManagerImpl) UpdatePluginResources(*framework.NodeInfo, *lifecycle.PodAdmitAttributes) error {
	panic("unimplemented")
}

// UpdateQOSCgroups implements [cm.ContainerManager].
func (c *ContainerManagerImpl) UpdateQOSCgroups(logger klog.Logger) error {
	panic("unimplemented")
}

// Updates implements [cm.ContainerManager].
func (c *ContainerManagerImpl) Updates() <-chan resourceupdates.Update {
	panic("unimplemented")
}

var _ cm.ContainerManager = &ContainerManagerImpl{}
