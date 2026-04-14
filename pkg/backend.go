package pkg

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cnuss/nanokube/pkg/nanokube"
	cadvisorv1 "github.com/google/cadvisor/info/v1"
	cadvisorv2 "github.com/google/cadvisor/info/v2"
	"github.com/pbnjay/memory"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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
	Options() nanokube.Options
	Name() string
	CgroupRoot() string

	ExecHost(image string, cmd []string, mounts []nanokube.Path) (string, error)
}

type Manager interface {
	cm.ContainerManager
	lifecycle.PodAdmitHandler
	cm.InternalContainerLifecycle
	cm.PodContainerManager
	Ready() <-chan struct{}
}

type Backend interface {
	cadvisor.Interface
	container.OSInterface
	mount.Interface
	subpath.Interface
	hostutil.HostUtils

	Context() context.Context

	Driver() Driver
	Manager() Manager
	Ready() <-chan struct{}
}

type fsCacheEntry struct {
	info   cadvisorv2.FsInfo
	expiry time.Time
}

type BackendImpl struct {
	driver  Driver
	options nanokube.Options

	manager     Manager
	managerOnce sync.Once

	startOnce sync.Once
	ready     chan struct{}

	fsCacheMu sync.Mutex
	fsCache   map[string]fsCacheEntry
}

var _ Backend = &BackendImpl{}

func NewBackend(driver Driver) Backend {
	return &BackendImpl{
		driver:  driver,
		options: driver.Options(),
		ready:   make(chan struct{}),
		fsCache: make(map[string]fsCacheEntry),
	}
}

func (b *BackendImpl) Context() context.Context {
	return b.driver.Context()
}

func (b *BackendImpl) Driver() Driver {
	return b.driver
}

func (b *BackendImpl) CanSafelySkipMountPointCheck() bool {
	return false
}

func (b *BackendImpl) Chmod(path string, perm os.FileMode) error {
	return nanokube.Unimplemented()
}

func (b *BackendImpl) Chtimes(path string, atime time.Time, mtime time.Time) error {
	return nanokube.Unimplemented()
}

func (b *BackendImpl) CleanSubPaths(poodDir string, volumeName string) error {
	return nanokube.Unimplemented()
}

func (b *BackendImpl) ContainerFsInfo(context.Context) (cadvisorv2.FsInfo, error) {
	return b.GetDirFsInfo("/")
}

func (b *BackendImpl) ContainerInfoV2(name string, options cadvisorv2.RequestOptions) (map[string]cadvisorv2.ContainerInfo, error) {
	// TODO(partial): port real container metrics from crid/backend/cadvisor.go
	// DEVNOTE: old impl queried ContainerStats+ContainerStatus via CRI for each container.
	// For root "/", synthesized info from HostInfo. Recursive mode listed all containers.
	return map[string]cadvisorv2.ContainerInfo{
		name: {
			Stats: []*cadvisorv2.ContainerStats{
				{Timestamp: time.Now()},
			},
		},
	}, nil
}

func (b *BackendImpl) Create(path string) (*os.File, error) {
	return nil, nanokube.Unimplemented()
}

func (b *BackendImpl) DeviceOpened(pathname string) (bool, error) {
	return false, nanokube.Unimplemented()
}

func (b *BackendImpl) EvalHostSymlinks(pathname string) (string, error) {
	return "", nanokube.Unimplemented()
}

func (b *BackendImpl) GetDirFsInfo(path string) (cadvisorv2.FsInfo, error) {
	b.fsCacheMu.Lock()
	if entry, ok := b.fsCache[path]; ok && time.Now().Before(entry.expiry) {
		b.fsCacheMu.Unlock()
		return entry.info, nil
	}
	b.fsCacheMu.Unlock()

	out, err := b.driver.ExecHost("busybox", []string{"stat", "-f", "-c", "%S %b %a %c %d", "/host" + path}, []nanokube.Path{nanokube.Path(path)})
	if err != nil {
		return cadvisorv2.FsInfo{}, err
	}

	fields := strings.Fields(out)
	if len(fields) != 5 {
		return cadvisorv2.FsInfo{}, fmt.Errorf("GetDirFsInfo: expected 5 fields, got %d: %q", len(fields), out)
	}
	blockSize, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return cadvisorv2.FsInfo{}, fmt.Errorf("GetDirFsInfo: block size: %w", err)
	}
	totalBlocks, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return cadvisorv2.FsInfo{}, fmt.Errorf("GetDirFsInfo: total blocks: %w", err)
	}
	availBlocks, err := strconv.ParseUint(fields[2], 10, 64)
	if err != nil {
		return cadvisorv2.FsInfo{}, fmt.Errorf("GetDirFsInfo: avail blocks: %w", err)
	}
	totalInodes, err := strconv.ParseUint(fields[3], 10, 64)
	if err != nil {
		return cadvisorv2.FsInfo{}, fmt.Errorf("GetDirFsInfo: total inodes: %w", err)
	}
	freeInodes, err := strconv.ParseUint(fields[4], 10, 64)
	if err != nil {
		return cadvisorv2.FsInfo{}, fmt.Errorf("GetDirFsInfo: free inodes: %w", err)
	}

	capacity := blockSize * totalBlocks
	available := blockSize * availBlocks
	info := cadvisorv2.FsInfo{
		Capacity:   capacity,
		Available:  available,
		Usage:      capacity - available,
		Inodes:     &totalInodes,
		InodesFree: &freeInodes,
	}

	b.fsCacheMu.Lock()
	b.fsCache[path] = fsCacheEntry{info: info, expiry: time.Now().Add(60 * time.Second)}
	b.fsCacheMu.Unlock()

	return info, nil
}

func (b *BackendImpl) GetFileType(pathname string) (hostutil.FileType, error) {
	return "", nanokube.Unimplemented()
}

func (b *BackendImpl) GetMode(pathname string) (os.FileMode, error) {
	return 0, nanokube.Unimplemented()
}

func (b *BackendImpl) GetMountRefs(pathname string) ([]string, error) {
	return nil, nanokube.Unimplemented()
}

func (b *BackendImpl) GetOwner(pathname string) (int64, int64, error) {
	return 0, 0, nanokube.Unimplemented()
}

func (b *BackendImpl) GetRequestedContainersInfo(containerName string, options cadvisorv2.RequestOptions) (map[string]*cadvisorv1.ContainerInfo, error) {
	return nil, nanokube.Unimplemented()
}

func (b *BackendImpl) GetSELinuxMountContext(pathname string) (string, error) {
	return "", nanokube.Unimplemented()
}

func (b *BackendImpl) GetSELinuxSupport(pathname string) (bool, error) {
	return false, nanokube.Unimplemented()
}

func (b *BackendImpl) Glob(pattern string) ([]string, error) {
	return nil, fmt.Errorf("unsupported")
}

func (b *BackendImpl) Hostname() (name string, err error) {
	return "", nanokube.Unimplemented()
}

func (b *BackendImpl) ImagesFsInfo(context.Context) (cadvisorv2.FsInfo, error) {
	return cadvisorv2.FsInfo{
		// TODO(partial): report image filesystem info from VM
		// DEVNOTE: old impl delegated to ContainerFsInfo -> GetDirFsInfo("/")
	}, nil
}

func (b *BackendImpl) IsLikelyNotMountPoint(file string) (bool, error) {
	return false, nanokube.Unimplemented()
}

func (b *BackendImpl) IsMountPoint(file string) (bool, error) {
	return false, nanokube.Unimplemented()
}

func (b *BackendImpl) List() ([]mount.MountPoint, error) {
	return nil, nanokube.Unimplemented()
}

func (b *BackendImpl) MachineInfo() (*cadvisorv1.MachineInfo, error) {
	return &cadvisorv1.MachineInfo{
		Timestamp:      time.Now(),
		NumCores:       runtime.NumCPU(),
		MemoryCapacity: memory.TotalMemory(),
		// TODO(partial): fill in more fields as needed
		// DEVNOTE: old impl also set InstanceID, BootID, SystemUUID, MachineID from HostInfo
		// (probed via docker container with host namespaces)
	}, nil
}

func (b *BackendImpl) MakeRShared(path string) error {
	return nil
}

func (b *BackendImpl) MkdirAll(path string, perm os.FileMode) error {
	// TODO(partial): route to host os.MkdirAll or VM depending on path
	// DEVNOTE: kubelet calls this for pod log dirs, plugin dirs, etc.
	return nil
}

func (b *BackendImpl) Mount(source string, target string, fstype string, options []string) error {
	return nanokube.Unimplemented()
}

func (b *BackendImpl) MountSensitive(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	return nanokube.Unimplemented()
}

func (b *BackendImpl) MountSensitiveWithoutSystemd(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	return nanokube.Unimplemented()
}

func (b *BackendImpl) MountSensitiveWithoutSystemdWithMountFlags(source string, target string, fstype string, options []string, sensitiveOptions []string, mountFlags []string) error {
	return nanokube.Unimplemented()
}

func (b *BackendImpl) Open(name string) (*os.File, error) {
	return nil, nanokube.Unimplemented()
}

func (b *BackendImpl) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return nil, nanokube.Unimplemented()
}

func (b *BackendImpl) PathExists(pathname string) (bool, error) {
	return false, nanokube.Unimplemented()
}

func (b *BackendImpl) PathIsDevice(pathname string) (bool, error) {
	return false, nanokube.Unimplemented()
}

func (b *BackendImpl) Pipe() (r *os.File, w *os.File, err error) {
	return nil, nil, nanokube.Unimplemented()
}

func (b *BackendImpl) PrepareSafeSubpath(subPath subpath.Subpath) (newHostPath string, cleanupAction func(), err error) {
	return "", nil, nanokube.Unimplemented()
}

func (b *BackendImpl) ReadDir(dirname string) ([]os.DirEntry, error) {
	return []os.DirEntry{
		// TODO(partial): route to host or VM depending on path
	}, nil
}

func (b *BackendImpl) Remove(path string) error {
	return nanokube.Unimplemented()
}

func (b *BackendImpl) RemoveAll(path string) error {
	return nanokube.Unimplemented()
}

func (b *BackendImpl) Rename(oldpath string, newpath string) error {
	return nanokube.Unimplemented()
}

func (b *BackendImpl) RootFsInfo() (cadvisorv2.FsInfo, error) {
	return b.GetDirFsInfo("/")
}

func (b *BackendImpl) SafeMakeDir(subdir string, base string, perm os.FileMode) error {
	return nanokube.Unimplemented()
}

func (b *BackendImpl) Ready() <-chan struct{} {
	return b.ready
}

func (b *BackendImpl) Start() error {
	b.startOnce.Do(func() {
		// TODO(partial): add any initialization logic as needed
		// DEVNOTE: old cadvisor Start() was a no-op; real init happened in MachineInfo
		close(b.ready)
	})
	return nil
}

func (b *BackendImpl) Stat(path string) (os.FileInfo, error) {
	return nil, nanokube.Unimplemented()
}

func (b *BackendImpl) Symlink(oldname string, newname string) error {
	return nanokube.Unimplemented()
}

func (b *BackendImpl) Unmount(target string) error {
	return nanokube.Unimplemented()
}

func (b *BackendImpl) VersionInfo() (*cadvisorv1.VersionInfo, error) {
	return &cadvisorv1.VersionInfo{
		// TODO(partial): KernelVersion, ContainerOsVersion from VM HostInfo
		// DEVNOTE: old impl got KernelVersion (uname -r) and OSVersion (/etc/os-release)
		// from HostInfo probed inside the VM
	}, nil
}

func (b *BackendImpl) Manager() Manager {
	b.managerOnce.Do(func() {
		b.manager = newManager(b)
	})
	return b.manager
}

type managerImpl struct {
	backend   Backend
	startOnce sync.Once
	ready     chan struct{}
}

var _ Manager = &managerImpl{}

func newManager(backend Backend) Manager {
	return &managerImpl{
		backend: backend,
		ready:   make(chan struct{}),
	}
}

func (c *managerImpl) ContainerHasExclusiveCPUs(pod *corev1.Pod, container *corev1.Container) bool {
	nanokube.Unimplemented()
	return false
}

func (c *managerImpl) GetAllocatableCPUs() []int64 {
	nanokube.Unimplemented()
	return nil
}

func (c *managerImpl) GetAllocatableDevices() []*podresourcesv1.ContainerDevices {
	nanokube.Unimplemented()
	return nil
}

func (c *managerImpl) GetAllocatableMemory() []*podresourcesv1.ContainerMemory {
	nanokube.Unimplemented()
	return nil
}

func (c *managerImpl) GetAllocateResourcesPodAdmitHandler() lifecycle.PodAdmitHandler {
	return c
}

func (c *managerImpl) Admit(attributes *lifecycle.PodAdmitAttributes) lifecycle.PodAdmitResult {
	nanokube.Unimplemented()
	return lifecycle.PodAdmitResult{
		Admit: false,
	}
}

func (c *managerImpl) GetCPUs(podUID string, containerName string) []int64 {
	nanokube.Unimplemented()
	return nil
}

func (c *managerImpl) GetCapacity(localStorageCapacityIsolation bool) corev1.ResourceList {
	info, err := c.backend.MachineInfo()
	if err != nil {
		return corev1.ResourceList{}
	}
	capacity := corev1.ResourceList{
		corev1.ResourceCPU:    *resource.NewQuantity(int64(info.NumCores), resource.DecimalSI),
		corev1.ResourceMemory: *resource.NewQuantity(int64(info.MemoryCapacity), resource.BinarySI),
		corev1.ResourcePods:   *resource.NewQuantity(110, resource.DecimalSI),
	}
	if localStorageCapacityIsolation {
		rootFs, err := c.backend.RootFsInfo()
		if err == nil {
			capacity[corev1.ResourceEphemeralStorage] = *resource.NewQuantity(int64(rootFs.Capacity), resource.BinarySI)
		}
	}
	return capacity
}

func (c *managerImpl) GetDevicePluginResourceCapacity() (corev1.ResourceList, corev1.ResourceList, []string) {
	return corev1.ResourceList{
			// TODO(partial): report device plugin capacity
			// DEVNOTE: old impl delegated to cm.deviceManager.GetCapacity()
		}, corev1.ResourceList{
			// TODO(partial): report device plugin allocatable
			// DEVNOTE: old impl delegated to cm.deviceManager.GetCapacity() (second return value)
		}, nil
}

func (c *managerImpl) GetDevices(podUID string, containerName string) []*podresourcesv1.ContainerDevices {
	nanokube.Unimplemented()
	return nil
}

func (c *managerImpl) GetDynamicResources(pod *corev1.Pod, container *corev1.Container) []*podresourcesv1.DynamicResource {
	nanokube.Unimplemented()
	return nil
}

func (c *managerImpl) GetHealthCheckers() []healthz.HealthChecker {
	// TODO(partial): add health checkers as needed
	// DEVNOTE: old impl returned deviceManager's health checker
	return nil
}

func (c *managerImpl) GetMemory(podUID string, containerName string) []*podresourcesv1.ContainerMemory {
	nanokube.Unimplemented()
	return nil
}

func (c *managerImpl) GetMountedSubsystems() *cm.CgroupSubsystems {
	nanokube.Unimplemented()
	return nil
}

func (c *managerImpl) GetNodeAllocatableAbsolute() corev1.ResourceList {
	return corev1.ResourceList{
		// TODO(partial): report actual allocatable resources
		// DEVNOTE: old impl subtracted SystemReserved and KubeReserved from capacity,
		// clamping negative values to zero
	}
}

func (c *managerImpl) GetNodeAllocatableReservation() corev1.ResourceList {
	return corev1.ResourceList{
		// TODO(partial): report reserved resources
		// DEVNOTE: old impl summed SystemReserved + KubeReserved + hardEvictionReservation
	}
}

func (c *managerImpl) GetNodeConfig() cm.NodeConfig {
	return cm.NodeConfig{
		// TODO(partial): add fields as needed
		// DEVNOTE: old impl returned cm.NodeConfig populated during construction with
		// cgroup settings, CPU/memory/topology manager policies, runtime cgroups, etc.
		CgroupRoot: c.backend.Driver().CgroupRoot(),
	}
}

func (c *managerImpl) GetPluginRegistrationHandlers() map[string]cache.PluginHandler {
	return map[string]cache.PluginHandler{
		// TODO(partial): add device plugin, DRA handlers
		// DEVNOTE: old impl returned map with DevicePlugin and DRA handlers from
		// deviceManager and draManager
	}
}

func (c *managerImpl) GetPodCgroupRoot() string {
	return c.backend.Driver().CgroupRoot()
}

func (c *managerImpl) GetQOSContainersInfo() cm.QOSContainersInfo {
	nanokube.Unimplemented()
	return cm.QOSContainersInfo{}
}

func (c *managerImpl) GetResources(ctx context.Context, pod *corev1.Pod, container *corev1.Container) (*container.RunContainerOptions, error) {
	return nil, nanokube.Unimplemented()
}

func (c *managerImpl) InternalContainerLifecycle() cm.InternalContainerLifecycle {
	return c
}

func (c *managerImpl) PreCreateContainer(_ klog.Logger, _ *corev1.Pod, _ *corev1.Container, _ *criv1.ContainerConfig) error {
	return nanokube.Unimplemented()
}

func (c *managerImpl) PreStartContainer(_ klog.Logger, _ *corev1.Pod, _ *corev1.Container, _ string) error {
	return nanokube.Unimplemented()
}

func (c *managerImpl) PostStopContainer(_ klog.Logger, _ string) error {
	return nanokube.Unimplemented()
}

func (c *managerImpl) NewPodContainerManager() cm.PodContainerManager {
	return c
}

func (c *managerImpl) PodHasExclusiveCPUs(pod *corev1.Pod) bool {
	nanokube.Unimplemented()
	return false
}

func (c *managerImpl) PodMightNeedToUnprepareResources(UID types.UID) bool {
	nanokube.Unimplemented()
	return false
}

func (c *managerImpl) PrepareDynamicResources(context.Context, *corev1.Pod) error {
	return nanokube.Unimplemented()
}

func (c *managerImpl) ShouldResetExtendedResourceCapacity() bool {
	// DEVNOTE: old impl checked device manager checkpoints; returns true after kubelet restart
	// to clear stale extended resources. No device plugins here, so always false.
	return false
}

func (c *managerImpl) Ready() <-chan struct{} {
	return c.ready
}

func (c *managerImpl) Start(context.Context, *corev1.Node, cm.ActivePodsFunc, cm.GetNodeFunc, config.SourcesReady, status.PodStatusProvider, cri.RuntimeService, bool) error {
	c.startOnce.Do(func() {
		// TODO(partial): initialize container manager resources
		// DEVNOTE: old impl initialized DRA manager, CPU manager, memory manager,
		// cached node info, populated ephemeral storage capacity, validated node
		// allocatable, started device manager and periodic cgroup ensure tasks
		close(c.ready)
	})
	return nil
}

func (c *managerImpl) Status() cm.Status {
	return cm.Status{
		// TODO(partial): report soft requirement errors if any
		// DEVNOTE: old impl returned cm.status with SoftRequirements error (read-locked)
	}
}

func (c *managerImpl) SystemCgroupsLimit() corev1.ResourceList {
	nanokube.Unimplemented()
	return nil
}

func (c *managerImpl) UnprepareDynamicResources(context.Context, *corev1.Pod) error {
	return nanokube.Unimplemented()
}

func (c *managerImpl) UpdateAllocatedDevices() {
	nanokube.Unimplemented()
}

func (c *managerImpl) UpdateAllocatedResourcesStatus(pod *corev1.Pod, status *corev1.PodStatus) {
	nanokube.Unimplemented()
}

func (c *managerImpl) UpdatePluginResources(*framework.NodeInfo, *lifecycle.PodAdmitAttributes) error {
	return nanokube.Unimplemented()
}

func (c *managerImpl) UpdateQOSCgroups(logger klog.Logger) error {
	return nanokube.Unimplemented()
}

func (c *managerImpl) Updates() <-chan resourceupdates.Update {
	// TODO(partial): return device plugin / DRA resource update channel
	// DEVNOTE: old impl returned cm.resourceUpdates channel fed by device and DRA managers
	return nil
}

func (c *managerImpl) Destroy(logger klog.Logger, name cm.CgroupName) error {
	return nanokube.Unimplemented()
}

func (c *managerImpl) EnsureExists(logger klog.Logger, pod *corev1.Pod) error {
	return nanokube.Unimplemented()
}

func (c *managerImpl) Exists(*corev1.Pod) bool {
	nanokube.Unimplemented()
	return false
}

func (c *managerImpl) GetAllPodsFromCgroups() (map[types.UID]cm.CgroupName, error) {
	return map[types.UID]cm.CgroupName{
		// TODO(partial): may need to probe VM cgroups via RunOnce
	}, nil
}

func (c *managerImpl) GetPodCgroupConfig(pod *corev1.Pod, resource corev1.ResourceName) (*cm.ResourceConfig, error) {
	return nil, nanokube.Unimplemented()
}

func (c *managerImpl) GetPodCgroupMemoryUsage(pod *corev1.Pod) (uint64, error) {
	return 0, nanokube.Unimplemented()
}

func (c *managerImpl) GetPodContainerName(*corev1.Pod) (cm.CgroupName, string) {
	nanokube.Unimplemented()
	return nil, ""
}

func (c *managerImpl) IsPodCgroup(cgroupfs string) (bool, types.UID) {
	nanokube.Unimplemented()
	return false, ""
}

func (c *managerImpl) ReduceCPULimits(logger klog.Logger, name cm.CgroupName) error {
	return nanokube.Unimplemented()
}

func (c *managerImpl) SetPodCgroupConfig(logger klog.Logger, pod *corev1.Pod, resourceConfig *cm.ResourceConfig) error {
	return nanokube.Unimplemented()
}
