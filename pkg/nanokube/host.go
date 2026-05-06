package nanokube

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	v1 "github.com/cnuss/nanokube/pkg/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/configmap"
	"k8s.io/kubernetes/pkg/volume/csi"
	"k8s.io/kubernetes/pkg/volume/downwardapi"
	"k8s.io/kubernetes/pkg/volume/emptydir"
	"k8s.io/kubernetes/pkg/volume/git_repo"
	"k8s.io/kubernetes/pkg/volume/projected"
	"k8s.io/kubernetes/pkg/volume/secret"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/kubernetes/pkg/volume/util/subpath"
	mount "k8s.io/mount-utils"
)

var PluginName = "nanokube"

type HostImpl struct {
	config v1.Config

	volumeHost         volume.VolumeHost
	volumeHostOnce     sync.Once
	volumeHostProvided chan struct{}

	mounters   sync.Map // map[types.UID]volume.Mounter
	unmounters sync.Map // map[types.UID]volume.Unmounter

	volumePlugins     []volume.VolumePlugin
	volumePluginsOnce sync.Once
}

var _ v1.Host = &HostImpl{}

func NewHost(config v1.Config) v1.Host {
	return &HostImpl{
		config:             config,
		volumeHostProvided: make(chan struct{}),
	}
}

func (h *HostImpl) Config() v1.Config {
	return h.config
}

func (h *HostImpl) CanSafelySkipMountPointCheck() bool {
	Log.Error("CanSafelySkipMountPointCheck")
	panic("unimplemented hostimpl cansafelyskipmountpointcheck")
}

func (h *HostImpl) CleanSubPaths(podDir string, volumeName string) error {
	return nil
}

func (h *HostImpl) GetMountRefs(pathname string) ([]string, error) {
	Log.Error("GetMountRefs", "pathname", pathname)
	panic("unimplemented hostimpl getmountrefs")
}

func (h *HostImpl) IsLikelyNotMountPoint(file string) (bool, error) {
	if h.Config().Options().InDataDir(file) {
		return true, nil
	}
	Log.Error("IsLikelyNotMountPoint", "file", file)
	panic("unimplemented hostimpl islikelynotmountpoint")
}

func (h *HostImpl) IsMountPoint(file string) (bool, error) {
	Log.Error("IsMountPoint", "file", file)
	panic("unimplemented hostimpl ismountpoint")
}

func (h *HostImpl) List() ([]mount.MountPoint, error) {
	Log.Error("List")
	panic("unimplemented hostimpl list")
}

func (h *HostImpl) Mount(source string, target string, fstype string, options []string) error {
	Log.Error("Mount", "source", source, "target", target, "fstype", fstype, "options", options)
	panic("unimplemented hostimpl mount")
}

func (h *HostImpl) MountSensitive(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	Log.Error("MountSensitive", "source", source, "target", target, "fstype", fstype, "options", options, "sensitiveOptions", sensitiveOptions)
	panic("unimplemented hostimpl mountsensitive")
}

func (h *HostImpl) MountSensitiveWithoutSystemd(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	if source == "tmpfs" && fstype == "tmpfs" {
		return nil
	}
	Log.Error("MountSensitiveWithoutSystemd", "source", source, "target", target, "fstype", fstype, "options", options, "sensitiveOptions", sensitiveOptions)
	panic("unimplemented hostimpl mountsensitivewithoutsystemd")
}

func (h *HostImpl) MountSensitiveWithoutSystemdWithMountFlags(source string, target string, fstype string, options []string, sensitiveOptions []string, mountFlags []string) error {
	Log.Error("MountSensitiveWithoutSystemdWithMountFlags", "source", source, "target", target, "fstype", fstype, "options", options, "sensitiveOptions", sensitiveOptions, "mountFlags", mountFlags)
	panic("unimplemented hostimpl mountsensitivewithoutsystemdwithmountflags")
}

func (h *HostImpl) PrepareSafeSubpath(subPath subpath.Subpath) (newHostPath string, cleanupAction func(), err error) {
	Log.Error("PrepareSafeSubpath", "subPath", subPath)
	panic("unimplemented hostimpl preparesafesubpath")
}

func (h *HostImpl) SafeMakeDir(subdir string, base string, perm os.FileMode) error {
	Log.Error("SafeMakeDir", "subdir", subdir, "base", base, "perm", perm)
	panic("unimplemented hostimpl safemakedir")
}

func (h *HostImpl) Unmount(target string) error {
	Log.Error("Unmount", "target", target)
	panic("unimplemented hostimpl unmount")
}

func (h *HostImpl) CanSupport(spec *volume.Spec) bool {
	if spec.Volume != nil && spec.Volume.HostPath != nil {
		return true
	}
	if spec.PersistentVolume != nil && spec.PersistentVolume.Spec.Local != nil {
		return true
	}
	return false
}

func (h *HostImpl) ConstructVolumeSpec(volumeName string, volumePath string) (volume.ReconstructedVolume, error) {
	panic("unimplemented hostimpl constructvolumespec")
}

func (h *HostImpl) GetPluginName() string {
	return PluginName
}

func (h *HostImpl) GetVolumeName(spec *volume.Spec) (string, error) {
	panic("unimplemented hostimpl getvolumename")
}

func (h *HostImpl) Init(host volume.VolumeHost) error {
	h.volumeHostOnce.Do(func() {
		h.volumeHost = host
		close(h.volumeHostProvided)
	})
	return nil
}

func (h *HostImpl) NewMounter(spec *volume.Spec, pod *corev1.Pod) (volume.Mounter, error) {
	return newMounter(h, spec, pod)
}

func (h *HostImpl) NewUnmounter(name string, podUID types.UID) (volume.Unmounter, error) {
	return newUnmounter(h, name, podUID)
}

func (h *HostImpl) RequiresRemount(spec *volume.Spec) bool {
	// TODO(partial): compare specs
	return false
}

func (h *HostImpl) SupportsMountOption() bool {
	panic("unimplemented hostimpl supportsmountoption")
}

func (h *HostImpl) SupportsSELinuxContextMount(spec *volume.Spec) (bool, error) {
	panic("unimplemented hostimpl supportsselinuxcontextmount")
}

func (h *HostImpl) Chmod(path string, perm os.FileMode) error {
	return os.Chmod(path, perm)
}

func (h *HostImpl) Chtimes(path string, atime time.Time, mtime time.Time) error {
	panic("unimplemented hostimpl chtimes")
}

func (h *HostImpl) Create(path string) (*os.File, error) {
	return os.Create(path)
}

func (h *HostImpl) Glob(pattern string) ([]string, error) {
	return filepath.Glob(pattern)
}

func (h *HostImpl) Hostname() (name string, err error) {
	panic("unimplemented hostimpl hostname")
}

func (h *HostImpl) MkdirAll(path string, perm os.FileMode) error {
	if path == "/var/log/containers" {
		return nil
	}
	return os.MkdirAll(path, perm)
}

func (h *HostImpl) Open(name string) (*os.File, error) {
	panic("unimplemented hostimpl open")
}

func (h *HostImpl) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	panic("unimplemented hostimpl openfile")
}

func (h *HostImpl) Pipe() (r *os.File, w *os.File, err error) {
	panic("unimplemented hostimpl pipe")
}

func (h *HostImpl) ReadDir(dirname string) ([]os.DirEntry, error) {
	return os.ReadDir(dirname)
}

func (h *HostImpl) Remove(path string) error {
	return os.Remove(path)
}

func (h *HostImpl) RemoveAll(path string) error {
	return os.RemoveAll(path)
}

func (h *HostImpl) Rename(oldpath string, newpath string) error {
	panic("unimplemented hostimpl rename")
}

func (h *HostImpl) Stat(path string) (os.FileInfo, error) {
	return os.Lstat(path)
}

func (h *HostImpl) Symlink(oldname string, newname string) error {
	return os.Symlink(oldname, newname)
}

func (h *HostImpl) DeviceOpened(pathname string) (bool, error) {
	panic("unimplemented hostimpl deviceopened")
}

func (h *HostImpl) EvalHostSymlinks(pathname string) (string, error) {
	panic("unimplemented hostimpl evalhostsymlinks")
}

func (h *HostImpl) GetFileType(pathname string) (hostutil.FileType, error) {
	panic("unimplemented hostimpl getfiletype")
}

func (h *HostImpl) GetMode(pathname string) (os.FileMode, error) {
	panic("unimplemented hostimpl getmode")
}

func (h *HostImpl) GetOwner(pathname string) (int64, int64, error) {
	panic("unimplemented hostimpl getowner")
}

func (h *HostImpl) GetSELinuxMountContext(pathname string) (string, error) {
	panic("unimplemented hostimpl getselinuxmountcontext")
}

func (h *HostImpl) GetSELinuxSupport(pathname string) (bool, error) {
	panic("unimplemented hostimpl getselinuxsupport")
}

func (h *HostImpl) MakeRShared(path string) error {
	return nil
}

func (h *HostImpl) PathExists(pathname string) (bool, error) {
	panic("unimplemented hostimpl pathexists")
}

func (h *HostImpl) PathIsDevice(pathname string) (bool, error) {
	panic("unimplemented hostimpl pathisdevice")
}

func (h *HostImpl) VolumePlugins() []volume.VolumePlugin {
	h.volumePluginsOnce.Do(func() {
		h.volumePlugins = []volume.VolumePlugin{h}
		h.volumePlugins = append(h.volumePlugins, emptydir.ProbeVolumePlugins()...)
		h.volumePlugins = append(h.volumePlugins, git_repo.ProbeVolumePlugins()...)
		h.volumePlugins = append(h.volumePlugins, secret.ProbeVolumePlugins()...)
		h.volumePlugins = append(h.volumePlugins, downwardapi.ProbeVolumePlugins()...)
		h.volumePlugins = append(h.volumePlugins, configmap.ProbeVolumePlugins()...)
		h.volumePlugins = append(h.volumePlugins, projected.ProbeVolumePlugins()...)
		h.volumePlugins = append(h.volumePlugins, csi.ProbeVolumePlugins()...)
	})
	return h.volumePlugins
}

type mounterImpl struct {
	host *HostImpl
	spec *volume.Spec
	pod  *corev1.Pod
}

func newMounter(host *HostImpl, spec *volume.Spec, pod *corev1.Pod) (volume.Mounter, error) {
	m, _ := host.mounters.LoadOrStore(fmt.Sprintf("%s-%s", spec.Name(), pod.GetUID()), func() volume.Mounter {
		// TODO(partial): move volume plugins here
		return &mounterImpl{
			host: host,
			spec: spec,
			pod:  pod,
		}
	}())
	return m.(volume.Mounter), nil
}

func newUnmounter(host *HostImpl, name string, podUID types.UID) (volume.Unmounter, error) {
	m, _ := host.unmounters.LoadOrStore(fmt.Sprintf("%s-%s", name, podUID), func() volume.Unmounter {
		mounter, _ := host.mounters.Load(fmt.Sprintf("%s-%s", name, podUID))
		return mounter.(volume.Unmounter)
	}())
	return m.(volume.Unmounter), nil
}

func (m *mounterImpl) TearDown() error {
	if m.spec.Volume != nil && m.spec.Volume.VolumeSource.HostPath != nil {
		return nil
	}
	if m.spec.PersistentVolume != nil && m.spec.PersistentVolume.Spec.Local != nil {
		// TODO(incomplete): emit events to args.GetEventRecorder() about volume creation
		backend := m.host.Config().Backend(v1.BackendName(*m.spec.PersistentVolume.Spec.Local.FSType))
		if backend == nil {
			return fmt.Errorf("no backend found for local volume with fstype %s", *m.spec.PersistentVolume.Spec.Local.FSType)
		}
		if m.spec.PersistentVolume.Spec.PersistentVolumeReclaimPolicy == corev1.PersistentVolumeReclaimRetain {
			return nil
		}
		err := backend.Driver().DeleteVolume(m.spec.PersistentVolume.Spec.Local)
		if err != nil {
			return err
		}
		return nil
	}
	Log.Error("TearDown", "spec", m.spec)
	panic("unimplemented mounterimpl teardown")
}

func (m *mounterImpl) TearDownAt(dir string) error {
	Log.Error("TearDownAt", "dir", dir)
	panic("unimplemented mounterimpl teardownat")
}

func (m *mounterImpl) GetAttributes() volume.Attributes {
	if m.spec.Volume != nil && m.spec.Volume.VolumeSource.HostPath != nil {
		return volume.Attributes{
			ReadOnly:       m.spec.ReadOnly,
			Managed:        true,
			SELinuxRelabel: false,
		}
	}
	if m.spec.PersistentVolume != nil && m.spec.PersistentVolume.Spec.Local != nil {
		return volume.Attributes{
			ReadOnly:       m.spec.ReadOnly,
			Managed:        true,
			SELinuxRelabel: false,
		}
	}
	Log.Error("GetAttributes", "spec", m.spec)
	panic("unimplemented mounterimpl getattributes")
}

func (m *mounterImpl) GetMetrics() (*volume.Metrics, error) {
	if m.spec.Volume != nil && m.spec.Volume.VolumeSource.HostPath != nil {
		return &volume.Metrics{
			Time: metav1.Now(),
			// TODO(partial): get real metrics
		}, nil
	}
	if m.spec.PersistentVolume != nil && m.spec.PersistentVolume.Spec.Local != nil {
		return &volume.Metrics{
			Time: metav1.Now(),
			// TODO(partial): get real metrics
		}, nil
	}
	Log.Error("GetMetrics", "spec", m.spec)
	panic("unimplemented mounterimpl getmetrics")
}

func (m *mounterImpl) GetPath() string {
	if m.spec.Volume != nil && m.spec.Volume.VolumeSource.HostPath != nil {
		return m.spec.Volume.VolumeSource.HostPath.Path
	}
	if m.spec.PersistentVolume != nil && m.spec.PersistentVolume.Spec.Local != nil {
		return m.spec.PersistentVolume.Spec.Local.Path
	}
	Log.Error("GetPath", "spec", m.spec)
	panic("unimplemented mounterimpl getpath")
}

func (m *mounterImpl) SetUp(args volume.MounterArgs) error {
	if m.spec.Volume != nil && m.spec.Volume.VolumeSource.HostPath != nil {
		return nil
	}
	if m.spec.PersistentVolume != nil && m.spec.PersistentVolume.Spec.Local != nil {
		// TODO(incomplete): emit events to args.GetEventRecorder() about volume creation
		backend := m.host.Config().Backend(v1.BackendName(*m.spec.PersistentVolume.Spec.Local.FSType))
		if backend == nil {
			return fmt.Errorf("no backend found for local volume with fstype %s", *m.spec.PersistentVolume.Spec.Local.FSType)
		}
		err := backend.Driver().CreateVolume(m.spec.PersistentVolume.Spec.Local)
		if err != nil {
			return err
		}
		return nil
	}
	Log.Error("SetUp", "spec", m.spec, "args", args)
	panic("unimplemented mounterimpl setup")
}

func (m *mounterImpl) SetUpAt(dir string, args volume.MounterArgs) error {
	Log.Error("SetupAt", "dir", dir, "args", args)
	panic("unimplemented mounterimpl setupat")
}

var (
	_ volume.Mounter   = &mounterImpl{}
	_ volume.Unmounter = &mounterImpl{}
)
