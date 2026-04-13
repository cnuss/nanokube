package pkg

import (
	"context"
	"fmt"
	"sync"
	"time"

	cadvisorv1 "github.com/google/cadvisor/info/v1"
	cadvisorv2 "github.com/google/cadvisor/info/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/server/healthz"
	cri "k8s.io/cri-api/pkg/apis"
	criv1 "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	podresourcesv1 "k8s.io/kubelet/pkg/apis/podresources/v1"
	v1 "k8s.io/kubelet/pkg/apis/podresources/v1"
	"k8s.io/kubernetes/pkg/kubelet/cadvisor"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	"k8s.io/kubernetes/pkg/kubelet/cm/resourceupdates"
	"k8s.io/kubernetes/pkg/kubelet/config"
	"k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
	"k8s.io/kubernetes/pkg/kubelet/status"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type Driver interface {
	cri.ImageManagerService
	cri.RuntimeService

	Context() context.Context
	Name() string
}

type Backend interface {
	Context() context.Context
	Driver() Driver
	RunOnce(image string, cmd []string, mounts []string) (string, error)

	ImageService() cri.ImageManagerService
	RuntimeService() cri.RuntimeService

	Cadvisor() cadvisor.Interface
	ContainerManager() cm.ContainerManager
}

type BackendImpl struct {
	driver Driver

	cadvisor     cadvisor.Interface
	cadvisorOnce sync.Once

	containerManager     cm.ContainerManager
	containerManagerOnce sync.Once
}

var _ Backend = &BackendImpl{}

func NewBackend(driver Driver) Backend {
	return &BackendImpl{
		driver: driver,
	}
}

func (b *BackendImpl) Context() context.Context {
	return b.driver.Context()
}

func (b *BackendImpl) Driver() Driver {
	return b.driver
}

func (b *BackendImpl) RunOnce(image string, cmd []string, mounts []string) (string, error) {
	_, err := b.ImageService().PullImage(b.Context(), &criv1.ImageSpec{Image: image}, nil, nil)
	if err != nil {
		return "", err
	}

	var sandboxID string
	var containerID string

	defer func() {
		if containerID != "" {
			if err := b.RuntimeService().StopContainer(b.Context(), containerID, 0); err != nil {
				klog.Warningf("Failed to stop container %q: %v", containerID, err)
			}
			if err := b.RuntimeService().RemoveContainer(b.Context(), containerID); err != nil {
				klog.Warningf("Failed to remove container %q: %v", containerID, err)
			}
		}
		if sandboxID != "" {
			if err := b.RuntimeService().StopPodSandbox(b.Context(), sandboxID); err != nil {
				klog.Warningf("Failed to stop pod sandbox %q: %v", sandboxID, err)
			}
			if err := b.RuntimeService().RemovePodSandbox(b.Context(), sandboxID); err != nil {
				klog.Warningf("Failed to remove pod sandbox %q: %v", sandboxID, err)
			}
		}
	}()

	sandboxID, err = b.RuntimeService().RunPodSandbox(b.Context(), &criv1.PodSandboxConfig{
		Linux: &criv1.LinuxPodSandboxConfig{
			SecurityContext: &criv1.LinuxSandboxSecurityContext{
				Privileged: true,
			},
		},
	}, b.Driver().Name())
	if err != nil {
		return "", err
	}

	containerID, err = b.RuntimeService().CreateContainer(b.Context(), sandboxID, &criv1.ContainerConfig{
		Image:   &criv1.ImageSpec{Image: image},
		Command: []string{"tail", "-f", "/dev/null"},
		Mounts: func() []*criv1.Mount {
			var mountsList []*criv1.Mount
			for _, mount := range mounts {
				mountsList = append(mountsList, &criv1.Mount{
					ContainerPath: fmt.Sprintf("/host/%s", mount),
					HostPath:      mount,
				})
			}
			return mountsList
		}(),
	}, nil)
	if err != nil {
		return "", err
	}

	if err := b.RuntimeService().StartContainer(b.Context(), containerID); err != nil {
		return "", err
	}

	stdout, stderr, err := b.RuntimeService().ExecSync(b.Context(), containerID, cmd, 30*time.Second)
	if err != nil {
		return "", err
	}

	if len(stderr) > 0 {
		return "", fmt.Errorf("command failed with stderr: %s", string(stderr))
	}

	return string(stdout), nil
}

func (b *BackendImpl) ImageService() cri.ImageManagerService {
	return b.Driver()
}

func (b *BackendImpl) RuntimeService() cri.RuntimeService {
	return b.Driver()
}

func (b *BackendImpl) Cadvisor() cadvisor.Interface {
	b.cadvisorOnce.Do(func() {
		b.cadvisor = newCadvisor(b)
	})
	return b.cadvisor
}

func (b *BackendImpl) ContainerManager() cm.ContainerManager {
	b.containerManagerOnce.Do(func() {
		b.containerManager = newContainerManager(b)
	})
	return b.containerManager
}

type cadvisorImpl struct {
	backend Backend
}

var _ cadvisor.Interface = &cadvisorImpl{}

func newCadvisor(backend Backend) *cadvisorImpl {
	return &cadvisorImpl{
		backend: backend,
	}
}

func (c *cadvisorImpl) ContainerFsInfo(context.Context) (cadvisorv2.FsInfo, error) {
	panic("unimplemented")
}

func (c *cadvisorImpl) ContainerInfoV2(name string, options cadvisorv2.RequestOptions) (map[string]cadvisorv2.ContainerInfo, error) {
	panic("unimplemented")
}

func (c *cadvisorImpl) GetDirFsInfo(path string) (cadvisorv2.FsInfo, error) {
	panic("unimplemented")
}

func (c *cadvisorImpl) GetRequestedContainersInfo(containerName string, options cadvisorv2.RequestOptions) (map[string]*cadvisorv1.ContainerInfo, error) {
	panic("unimplemented")
}

func (c *cadvisorImpl) ImagesFsInfo(context.Context) (cadvisorv2.FsInfo, error) {
	panic("unimplemented")
}

func (c *cadvisorImpl) MachineInfo() (*cadvisorv1.MachineInfo, error) {
	panic("unimplemented")
}

func (c *cadvisorImpl) RootFsInfo() (cadvisorv2.FsInfo, error) {
	panic("unimplemented")
}

func (c *cadvisorImpl) Start() error {
	return nil
}

func (c *cadvisorImpl) VersionInfo() (*cadvisorv1.VersionInfo, error) {
	panic("unimplemented")
}

type containerManagerImpl struct {
	ctx     context.Context
	backend Backend
}

var _ cm.ContainerManager = &containerManagerImpl{}

func newContainerManager(backend Backend) *containerManagerImpl {
	return &containerManagerImpl{
		ctx:     backend.Context(),
		backend: backend,
	}
}

// ContainerHasExclusiveCPUs implements [cm.ContainerManager].
func (c *containerManagerImpl) ContainerHasExclusiveCPUs(pod *corev1.Pod, container *corev1.Container) bool {
	panic("unimplemented")
}

// GetAllocatableCPUs implements [cm.ContainerManager].
func (c *containerManagerImpl) GetAllocatableCPUs() []int64 {
	panic("unimplemented")
}

// GetAllocatableDevices implements [cm.ContainerManager].
func (c *containerManagerImpl) GetAllocatableDevices() []*podresourcesv1.ContainerDevices {
	panic("unimplemented")
}

// GetAllocatableMemory implements [cm.ContainerManager].
func (c *containerManagerImpl) GetAllocatableMemory() []*podresourcesv1.ContainerMemory {
	panic("unimplemented")
}

// GetAllocateResourcesPodAdmitHandler implements [cm.ContainerManager].
func (c *containerManagerImpl) GetAllocateResourcesPodAdmitHandler() lifecycle.PodAdmitHandler {
	panic("unimplemented")
}

// GetCPUs implements [cm.ContainerManager].
func (c *containerManagerImpl) GetCPUs(podUID string, containerName string) []int64 {
	panic("unimplemented")
}

// GetCapacity implements [cm.ContainerManager].
func (c *containerManagerImpl) GetCapacity(localStorageCapacityIsolation bool) corev1.ResourceList {
	panic("unimplemented")
}

// GetDevicePluginResourceCapacity implements [cm.ContainerManager].
func (c *containerManagerImpl) GetDevicePluginResourceCapacity() (corev1.ResourceList, corev1.ResourceList, []string) {
	panic("unimplemented")
}

// GetDevices implements [cm.ContainerManager].
func (c *containerManagerImpl) GetDevices(podUID string, containerName string) []*podresourcesv1.ContainerDevices {
	panic("unimplemented")
}

// GetDynamicResources implements [cm.ContainerManager].
func (c *containerManagerImpl) GetDynamicResources(pod *corev1.Pod, container *corev1.Container) []*v1.DynamicResource {
	panic("unimplemented")
}

// GetHealthCheckers implements [cm.ContainerManager].
func (c *containerManagerImpl) GetHealthCheckers() []healthz.HealthChecker {
	panic("unimplemented")
}

// GetMemory implements [cm.ContainerManager].
func (c *containerManagerImpl) GetMemory(podUID string, containerName string) []*podresourcesv1.ContainerMemory {
	panic("unimplemented")
}

// GetMountedSubsystems implements [cm.ContainerManager].
func (c *containerManagerImpl) GetMountedSubsystems() *cm.CgroupSubsystems {
	panic("unimplemented")
}

// GetNodeAllocatableAbsolute implements [cm.ContainerManager].
func (c *containerManagerImpl) GetNodeAllocatableAbsolute() corev1.ResourceList {
	panic("unimplemented")
}

// GetNodeAllocatableReservation implements [cm.ContainerManager].
func (c *containerManagerImpl) GetNodeAllocatableReservation() corev1.ResourceList {
	panic("unimplemented")
}

// GetNodeConfig implements [cm.ContainerManager].
func (c *containerManagerImpl) GetNodeConfig() cm.NodeConfig {
	panic("unimplemented")
}

// GetPluginRegistrationHandlers implements [cm.ContainerManager].
func (c *containerManagerImpl) GetPluginRegistrationHandlers() map[string]cache.PluginHandler {
	panic("unimplemented")
}

// GetPodCgroupRoot implements [cm.ContainerManager].
func (c *containerManagerImpl) GetPodCgroupRoot() string {
	return ""
}

// GetQOSContainersInfo implements [cm.ContainerManager].
func (c *containerManagerImpl) GetQOSContainersInfo() cm.QOSContainersInfo {
	panic("unimplemented")
}

// GetResources implements [cm.ContainerManager].
func (c *containerManagerImpl) GetResources(ctx context.Context, pod *corev1.Pod, container *corev1.Container) (*container.RunContainerOptions, error) {
	panic("unimplemented")
}

// InternalContainerLifecycle implements [cm.ContainerManager].
func (c *containerManagerImpl) InternalContainerLifecycle() cm.InternalContainerLifecycle {
	panic("unimplemented")
}

// NewPodContainerManager implements [cm.ContainerManager].
func (c *containerManagerImpl) NewPodContainerManager() cm.PodContainerManager {
	panic("unimplemented")
}

// PodHasExclusiveCPUs implements [cm.ContainerManager].
func (c *containerManagerImpl) PodHasExclusiveCPUs(pod *corev1.Pod) bool {
	panic("unimplemented")
}

// PodMightNeedToUnprepareResources implements [cm.ContainerManager].
func (c *containerManagerImpl) PodMightNeedToUnprepareResources(UID types.UID) bool {
	panic("unimplemented")
}

// PrepareDynamicResources implements [cm.ContainerManager].
func (c *containerManagerImpl) PrepareDynamicResources(context.Context, *corev1.Pod) error {
	panic("unimplemented")
}

// ShouldResetExtendedResourceCapacity implements [cm.ContainerManager].
func (c *containerManagerImpl) ShouldResetExtendedResourceCapacity() bool {
	panic("unimplemented")
}

// Start implements [cm.ContainerManager].
func (c *containerManagerImpl) Start(context.Context, *corev1.Node, cm.ActivePodsFunc, cm.GetNodeFunc, config.SourcesReady, status.PodStatusProvider, cri.RuntimeService, bool) error {
	return nil
}

// Status implements [cm.ContainerManager].
func (c *containerManagerImpl) Status() cm.Status {
	panic("unimplemented")
}

// SystemCgroupsLimit implements [cm.ContainerManager].
func (c *containerManagerImpl) SystemCgroupsLimit() corev1.ResourceList {
	panic("unimplemented")
}

// UnprepareDynamicResources implements [cm.ContainerManager].
func (c *containerManagerImpl) UnprepareDynamicResources(context.Context, *corev1.Pod) error {
	panic("unimplemented")
}

// UpdateAllocatedDevices implements [cm.ContainerManager].
func (c *containerManagerImpl) UpdateAllocatedDevices() {
	panic("unimplemented")
}

// UpdateAllocatedResourcesStatus implements [cm.ContainerManager].
func (c *containerManagerImpl) UpdateAllocatedResourcesStatus(pod *corev1.Pod, status *corev1.PodStatus) {
	panic("unimplemented")
}

// UpdatePluginResources implements [cm.ContainerManager].
func (c *containerManagerImpl) UpdatePluginResources(*framework.NodeInfo, *lifecycle.PodAdmitAttributes) error {
	panic("unimplemented")
}

// UpdateQOSCgroups implements [cm.ContainerManager].
func (c *containerManagerImpl) UpdateQOSCgroups(logger klog.Logger) error {
	panic("unimplemented")
}

// Updates implements [cm.ContainerManager].
func (c *containerManagerImpl) Updates() <-chan resourceupdates.Update {
	panic("unimplemented")
}
