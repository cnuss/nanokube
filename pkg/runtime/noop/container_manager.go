package noop

import (
	"context"

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
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/config"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
	"k8s.io/kubernetes/pkg/kubelet/status"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework"
)

type ContainerManager struct{}

func NewContainerManager() cm.ContainerManager {
	return &ContainerManager{}
}

func (s *ContainerManager) Start(context.Context, *v1.Node, cm.ActivePodsFunc, cm.GetNodeFunc, config.SourcesReady, status.PodStatusProvider, internalapi.RuntimeService, bool) error {
	return nil
}
func (s *ContainerManager) SystemCgroupsLimit() v1.ResourceList { return v1.ResourceList{} }
func (s *ContainerManager) GetNodeConfig() cm.NodeConfig        { return cm.NodeConfig{} }
func (s *ContainerManager) GetMountedSubsystems() *cm.CgroupSubsystems {
	return &cm.CgroupSubsystems{}
}
func (s *ContainerManager) GetQOSContainersInfo() cm.QOSContainersInfo {
	return cm.QOSContainersInfo{}
}
func (s *ContainerManager) UpdateQOSCgroups(klog.Logger) error             { return nil }
func (s *ContainerManager) Status() cm.Status                              { return cm.Status{} }
func (s *ContainerManager) GetNodeAllocatableReservation() v1.ResourceList { return nil }
func (s *ContainerManager) GetCapacity(localStorageCapacityIsolation bool) v1.ResourceList {
	if !localStorageCapacityIsolation {
		return v1.ResourceList{}
	}
	return v1.ResourceList{
		v1.ResourceEphemeralStorage: *resource.NewQuantity(0, resource.BinarySI),
	}
}
func (s *ContainerManager) GetPluginRegistrationHandlers() map[string]cache.PluginHandler {
	return nil
}
func (s *ContainerManager) GetHealthCheckers() []healthz.HealthChecker {
	return []healthz.HealthChecker{}
}
func (s *ContainerManager) GetDevicePluginResourceCapacity() (v1.ResourceList, v1.ResourceList, []string) {
	return nil, nil, []string{}
}
func (s *ContainerManager) NewPodContainerManager() cm.PodContainerManager {
	return &PodContainerManager{}
}
func (s *ContainerManager) GetResources(context.Context, *v1.Pod, *v1.Container) (*kubecontainer.RunContainerOptions, error) {
	return &kubecontainer.RunContainerOptions{}, nil
}
func (s *ContainerManager) UpdatePluginResources(*schedulerframework.NodeInfo, *lifecycle.PodAdmitAttributes) error {
	return nil
}
func (s *ContainerManager) InternalContainerLifecycle() cm.InternalContainerLifecycle {
	return &InternalContainerLifecycle{}
}
func (s *ContainerManager) GetPodCgroupRoot() string { return "" }
func (s *ContainerManager) GetDevices(_, _ string) []*podresourcesapi.ContainerDevices {
	return nil
}
func (s *ContainerManager) GetAllocatableDevices() []*podresourcesapi.ContainerDevices {
	return nil
}
func (s *ContainerManager) ShouldResetExtendedResourceCapacity() bool { return false }
func (s *ContainerManager) GetAllocateResourcesPodAdmitHandler() lifecycle.PodAdmitHandler {
	return topologymanager.NewFakeManager()
}
func (s *ContainerManager) UpdateAllocatedDevices()     {}
func (s *ContainerManager) GetCPUs(_, _ string) []int64 { return nil }
func (s *ContainerManager) GetAllocatableCPUs() []int64 { return nil }
func (s *ContainerManager) GetMemory(_, _ string) []*podresourcesapi.ContainerMemory {
	return nil
}
func (s *ContainerManager) GetAllocatableMemory() []*podresourcesapi.ContainerMemory {
	return nil
}
func (s *ContainerManager) GetDynamicResources(*v1.Pod, *v1.Container) []*podresourcesapi.DynamicResource {
	return nil
}
func (s *ContainerManager) GetNodeAllocatableAbsolute() v1.ResourceList              { return nil }
func (s *ContainerManager) PrepareDynamicResources(context.Context, *v1.Pod) error   { return nil }
func (s *ContainerManager) UnprepareDynamicResources(context.Context, *v1.Pod) error { return nil }
func (s *ContainerManager) PodMightNeedToUnprepareResources(types.UID) bool          { return false }
func (s *ContainerManager) UpdateAllocatedResourcesStatus(*v1.Pod, *v1.PodStatus)    {}
func (s *ContainerManager) Updates() <-chan resourceupdates.Update                   { return nil }
func (s *ContainerManager) PodHasExclusiveCPUs(*v1.Pod) bool                         { return false }
func (s *ContainerManager) ContainerHasExclusiveCPUs(*v1.Pod, *v1.Container) bool    { return false }

// PodContainerManager implements cm.PodContainerManager as a no-op.
type PodContainerManager struct{}

func (s *PodContainerManager) GetPodContainerName(*v1.Pod) (cm.CgroupName, string) {
	return nil, ""
}
func (s *PodContainerManager) EnsureExists(klog.Logger, *v1.Pod) error          { return nil }
func (s *PodContainerManager) Exists(*v1.Pod) bool                              { return true }
func (s *PodContainerManager) Destroy(klog.Logger, cm.CgroupName) error         { return nil }
func (s *PodContainerManager) ReduceCPULimits(klog.Logger, cm.CgroupName) error { return nil }
func (s *PodContainerManager) GetAllPodsFromCgroups() (map[types.UID]cm.CgroupName, error) {
	return nil, nil
}
func (s *PodContainerManager) IsPodCgroup(string) (bool, types.UID)            { return false, "" }
func (s *PodContainerManager) GetPodCgroupMemoryUsage(*v1.Pod) (uint64, error) { return 0, nil }
func (s *PodContainerManager) GetPodCgroupConfig(*v1.Pod, v1.ResourceName) (*cm.ResourceConfig, error) {
	return nil, nil
}
func (s *PodContainerManager) SetPodCgroupConfig(klog.Logger, *v1.Pod, *cm.ResourceConfig) error {
	return nil
}

// InternalContainerLifecycle implements cm.InternalContainerLifecycle as a no-op.
type InternalContainerLifecycle struct{}

func (s *InternalContainerLifecycle) PreCreateContainer(klog.Logger, *v1.Pod, *v1.Container, *runtimeapi.ContainerConfig) error {
	return nil
}
func (s *InternalContainerLifecycle) PreStartContainer(klog.Logger, *v1.Pod, *v1.Container, string) error {
	return nil
}
func (s *InternalContainerLifecycle) PostStopContainer(klog.Logger, string) error {
	return nil
}
