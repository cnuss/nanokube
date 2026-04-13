package pkg

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	cadvisorv1 "github.com/google/cadvisor/info/v1"
	cadvisorv2 "github.com/google/cadvisor/info/v2"
	"github.com/pbnjay/memory"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/server/healthz"
	cri "k8s.io/cri-api/pkg/apis"
	criv1 "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	podresourcesv1 "k8s.io/kubelet/pkg/apis/podresources/v1"
	"k8s.io/kubernetes/pkg/kubelet/cadvisor"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	"k8s.io/kubernetes/pkg/kubelet/cm/resourceupdates"
	"k8s.io/kubernetes/pkg/kubelet/config"
	"k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
	"k8s.io/kubernetes/pkg/kubelet/status"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/kubernetes/pkg/volume/util/subpath"
	"k8s.io/mount-utils"
)

type Driver interface {
	cri.ImageManagerService
	cri.RuntimeService

	Context() context.Context
	Name() string
}

type Manager interface {
	cm.ContainerManager
	lifecycle.PodAdmitHandler
	cm.InternalContainerLifecycle
}

type Backend interface {
	cadvisor.Interface
	container.OSInterface
	mount.Interface
	subpath.Interface
	hostutil.HostUtils

	Context() context.Context
	RunOnce(image string, cmd []string, mounts []string) (string, error)

	Driver() Driver
	Manager() Manager
}

type BackendImpl struct {
	driver Driver

	manager     Manager
	managerOnce sync.Once
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

func (b *BackendImpl) CanSafelySkipMountPointCheck() bool {
	panic("unimplemented")
}

func (b *BackendImpl) Chmod(path string, perm os.FileMode) error {
	panic("unimplemented")
}

func (b *BackendImpl) Chtimes(path string, atime time.Time, mtime time.Time) error {
	panic("unimplemented")
}

func (b *BackendImpl) CleanSubPaths(poodDir string, volumeName string) error {
	panic("unimplemented")
}

func (b *BackendImpl) ContainerFsInfo(context.Context) (cadvisorv2.FsInfo, error) {
	panic("unimplemented")
}

func (b *BackendImpl) ContainerInfoV2(name string, options cadvisorv2.RequestOptions) (map[string]cadvisorv2.ContainerInfo, error) {
	panic("unimplemented")
}

func (b *BackendImpl) Create(path string) (*os.File, error) {
	panic("unimplemented")
}

func (b *BackendImpl) DeviceOpened(pathname string) (bool, error) {
	panic("unimplemented")
}

func (b *BackendImpl) EvalHostSymlinks(pathname string) (string, error) {
	panic("unimplemented")
}

func (b *BackendImpl) GetDirFsInfo(path string) (cadvisorv2.FsInfo, error) {
	panic("unimplemented")
}

func (b *BackendImpl) GetFileType(pathname string) (hostutil.FileType, error) {
	panic("unimplemented")
}

func (b *BackendImpl) GetMode(pathname string) (os.FileMode, error) {
	panic("unimplemented")
}

func (b *BackendImpl) GetMountRefs(pathname string) ([]string, error) {
	panic("unimplemented")
}

func (b *BackendImpl) GetOwner(pathname string) (int64, int64, error) {
	panic("unimplemented")
}

func (b *BackendImpl) GetRequestedContainersInfo(containerName string, options cadvisorv2.RequestOptions) (map[string]*cadvisorv1.ContainerInfo, error) {
	panic("unimplemented")
}

func (b *BackendImpl) GetSELinuxMountContext(pathname string) (string, error) {
	panic("unimplemented")
}

func (b *BackendImpl) GetSELinuxSupport(pathname string) (bool, error) {
	panic("unimplemented")
}

func (b *BackendImpl) Glob(pattern string) ([]string, error) {
	panic("unimplemented")
}

func (b *BackendImpl) Hostname() (name string, err error) {
	panic("unimplemented")
}

func (b *BackendImpl) ImagesFsInfo(context.Context) (cadvisorv2.FsInfo, error) {
	panic("unimplemented")
}

func (b *BackendImpl) IsLikelyNotMountPoint(file string) (bool, error) {
	panic("unimplemented")
}

func (b *BackendImpl) IsMountPoint(file string) (bool, error) {
	panic("unimplemented")
}

func (b *BackendImpl) List() ([]mount.MountPoint, error) {
	panic("unimplemented")
}

func (b *BackendImpl) MachineInfo() (*cadvisorv1.MachineInfo, error) {
	return &cadvisorv1.MachineInfo{
		Timestamp:      time.Now(),
		NumCores:       runtime.NumCPU(),
		MemoryCapacity: memory.TotalMemory(),
		// TODO(partial): fill in more fields as needed
	}, nil
}

func (b *BackendImpl) MakeRShared(path string) error {
	panic("unimplemented")
}

func (b *BackendImpl) MkdirAll(path string, perm os.FileMode) error {
	panic("unimplemented")
}

func (b *BackendImpl) Mount(source string, target string, fstype string, options []string) error {
	panic("unimplemented")
}

func (b *BackendImpl) MountSensitive(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	panic("unimplemented")
}

func (b *BackendImpl) MountSensitiveWithoutSystemd(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	panic("unimplemented")
}

func (b *BackendImpl) MountSensitiveWithoutSystemdWithMountFlags(source string, target string, fstype string, options []string, sensitiveOptions []string, mountFlags []string) error {
	panic("unimplemented")
}

func (b *BackendImpl) Open(name string) (*os.File, error) {
	panic("unimplemented")
}

func (b *BackendImpl) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	panic("unimplemented")
}

func (b *BackendImpl) PathExists(pathname string) (bool, error) {
	panic("unimplemented")
}

func (b *BackendImpl) PathIsDevice(pathname string) (bool, error) {
	panic("unimplemented")
}

func (b *BackendImpl) Pipe() (r *os.File, w *os.File, err error) {
	panic("unimplemented")
}

func (b *BackendImpl) PrepareSafeSubpath(subPath subpath.Subpath) (newHostPath string, cleanupAction func(), err error) {
	panic("unimplemented")
}

func (b *BackendImpl) ReadDir(dirname string) ([]os.DirEntry, error) {
	panic("unimplemented")
}

func (b *BackendImpl) Remove(path string) error {
	panic("unimplemented")
}

func (b *BackendImpl) RemoveAll(path string) error {
	panic("unimplemented")
}

func (b *BackendImpl) Rename(oldpath string, newpath string) error {
	panic("unimplemented")
}

func (b *BackendImpl) RootFsInfo() (cadvisorv2.FsInfo, error) {
	panic("unimplemented")
}

func (b *BackendImpl) SafeMakeDir(subdir string, base string, perm os.FileMode) error {
	panic("unimplemented")
}

func (b *BackendImpl) Start() error {
	panic("unimplemented")
}

func (b *BackendImpl) Stat(path string) (os.FileInfo, error) {
	panic("unimplemented")
}

func (b *BackendImpl) Symlink(oldname string, newname string) error {
	panic("unimplemented")
}

func (b *BackendImpl) Unmount(target string) error {
	panic("unimplemented")
}

func (b *BackendImpl) VersionInfo() (*cadvisorv1.VersionInfo, error) {
	panic("unimplemented")
}

func (b *BackendImpl) Manager() Manager {
	b.managerOnce.Do(func() {
		b.manager = newManager(b)
	})
	return b.manager
}

type managerImpl struct {
	backend Backend
}

var _ Manager = &managerImpl{}

func newManager(backend Backend) Manager {
	return &managerImpl{
		backend: backend,
	}
}

func (c *managerImpl) ContainerHasExclusiveCPUs(pod *corev1.Pod, container *corev1.Container) bool {
	panic("unimplemented")
}

func (c *managerImpl) GetAllocatableCPUs() []int64 {
	panic("unimplemented")
}

func (c *managerImpl) GetAllocatableDevices() []*podresourcesv1.ContainerDevices {
	panic("unimplemented")
}

func (c *managerImpl) GetAllocatableMemory() []*podresourcesv1.ContainerMemory {
	panic("unimplemented")
}

func (c *managerImpl) GetAllocateResourcesPodAdmitHandler() lifecycle.PodAdmitHandler {
	return c
}

func (c *managerImpl) Admit(attributes *lifecycle.PodAdmitAttributes) lifecycle.PodAdmitResult {
	panic("unimplemented")
}

func (c *managerImpl) GetCPUs(podUID string, containerName string) []int64 {
	panic("unimplemented")
}

func (c *managerImpl) GetCapacity(localStorageCapacityIsolation bool) corev1.ResourceList {
	panic("unimplemented")
}

func (c *managerImpl) GetDevicePluginResourceCapacity() (corev1.ResourceList, corev1.ResourceList, []string) {
	panic("unimplemented")
}

func (c *managerImpl) GetDevices(podUID string, containerName string) []*podresourcesv1.ContainerDevices {
	panic("unimplemented")
}

func (c *managerImpl) GetDynamicResources(pod *corev1.Pod, container *corev1.Container) []*podresourcesv1.DynamicResource {
	panic("unimplemented")
}

func (c *managerImpl) GetHealthCheckers() []healthz.HealthChecker {
	// TODO(partial): add health checkers as needed
	return nil
}

func (c *managerImpl) GetMemory(podUID string, containerName string) []*podresourcesv1.ContainerMemory {
	panic("unimplemented")
}

func (c *managerImpl) GetMountedSubsystems() *cm.CgroupSubsystems {
	panic("unimplemented")
}

func (c *managerImpl) GetNodeAllocatableAbsolute() corev1.ResourceList {
	return corev1.ResourceList{
		// TODO(partial): report actual allocatable resources
	}
}

func (c *managerImpl) GetNodeAllocatableReservation() corev1.ResourceList {
	panic("unimplemented")
}

func (c *managerImpl) GetNodeConfig() cm.NodeConfig {
	return cm.NodeConfig{
		// TODO(partial): add fields as needed
	}
}

func (c *managerImpl) GetPluginRegistrationHandlers() map[string]cache.PluginHandler {
	panic("unimplemented")
}

func (c *managerImpl) GetPodCgroupRoot() string {
	// TODO(revisit): Find out if we can inspect the VM for this
	return ""
}

func (c *managerImpl) GetQOSContainersInfo() cm.QOSContainersInfo {
	panic("unimplemented")
}

func (c *managerImpl) GetResources(ctx context.Context, pod *corev1.Pod, container *corev1.Container) (*container.RunContainerOptions, error) {
	panic("unimplemented")
}

func (c *managerImpl) InternalContainerLifecycle() cm.InternalContainerLifecycle {
	return c
}

func (c *managerImpl) PreCreateContainer(_ klog.Logger, _ *corev1.Pod, _ *corev1.Container, _ *criv1.ContainerConfig) error {
	panic("unimplemented")
}

func (c *managerImpl) PreStartContainer(_ klog.Logger, _ *corev1.Pod, _ *corev1.Container, _ string) error {
	panic("unimplemented")
}

func (c *managerImpl) PostStopContainer(_ klog.Logger, _ string) error {
	panic("unimplemented")
}

func (c *managerImpl) NewPodContainerManager() cm.PodContainerManager {
	panic("unimplemented")
}

func (c *managerImpl) PodHasExclusiveCPUs(pod *corev1.Pod) bool {
	panic("unimplemented")
}

func (c *managerImpl) PodMightNeedToUnprepareResources(UID types.UID) bool {
	panic("unimplemented")
}

func (c *managerImpl) PrepareDynamicResources(context.Context, *corev1.Pod) error {
	panic("unimplemented")
}

func (c *managerImpl) ShouldResetExtendedResourceCapacity() bool {
	panic("unimplemented")
}

func (c *managerImpl) Start(context.Context, *corev1.Node, cm.ActivePodsFunc, cm.GetNodeFunc, config.SourcesReady, status.PodStatusProvider, cri.RuntimeService, bool) error {
	panic("unimplemented")
}

func (c *managerImpl) Status() cm.Status {
	panic("unimplemented")
}

func (c *managerImpl) SystemCgroupsLimit() corev1.ResourceList {
	panic("unimplemented")
}

func (c *managerImpl) UnprepareDynamicResources(context.Context, *corev1.Pod) error {
	panic("unimplemented")
}

func (c *managerImpl) UpdateAllocatedDevices() {
	panic("unimplemented")
}

func (c *managerImpl) UpdateAllocatedResourcesStatus(pod *corev1.Pod, status *corev1.PodStatus) {
	panic("unimplemented")
}

func (c *managerImpl) UpdatePluginResources(*framework.NodeInfo, *lifecycle.PodAdmitAttributes) error {
	panic("unimplemented")
}

func (c *managerImpl) UpdateQOSCgroups(logger klog.Logger) error {
	panic("unimplemented")
}

func (c *managerImpl) Updates() <-chan resourceupdates.Update {
	panic("unimplemented")
}
