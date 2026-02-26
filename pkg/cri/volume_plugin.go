package cri

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/cnuss/nanokube/pkg/cri/docker"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/mount-utils"
)

const PluginName = "nanokube.io/volume"

var errNotImplemented = fmt.Errorf("mounter: not yet implemented")

type mounter struct {
	mount.Interface
	dataDir string
	ctx     context.Context
	mounts  sync.Map // target path → mount.MountPoint
	volumes sync.Map // target path → Docker volume name
	// per-volume fields (set on instances returned by NewMounter/NewUnmounter)
	backend    Backend
	volumeName string
	path       string
	readOnly   bool
}

type volumePlugin struct {
	volume.VolumePlugin
	backend Backend
	ctx     context.Context
	host    volume.VolumeHost

	Mounter *mounter
}

func NewVolumePlugin(ctx context.Context, backend Backend, dataDir string) *volumePlugin {
	logger.Trace().Msg("NewVolumePlugin")
	m := newMounter(ctx, dataDir)
	if db, ok := backend.(*docker.Backend); ok {
		db.Mounts = m
	}
	return &volumePlugin{
		backend: backend,
		ctx:     ctx,
		Mounter: m,
	}
}

func (p *volumePlugin) Init(host volume.VolumeHost) error {
	p.host = host

	if client := host.GetKubeClient(); client != nil {
		sc := &storagev1.StorageClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: "nanokube",
				Annotations: map[string]string{
					"storageclass.kubernetes.io/is-default-class": "true",
				},
			},
			Provisioner: PluginName,
		}
		if _, err := client.StorageV1().StorageClasses().Create(p.ctx, sc, metav1.CreateOptions{}); err != nil && !errors.IsAlreadyExists(err) {
			logger.Warn().Err(err).Msg("failed to create default StorageClass")
		}
	}
	return nil
}

func (p *volumePlugin) GetPluginName() string {
	return PluginName
}

func (p *volumePlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	if spec == nil {
		return "", fmt.Errorf("nil volume spec")
	}
	return spec.Name(), nil
}

func (p *volumePlugin) CanSupport(spec *volume.Spec) bool {
	return false // provisioning is handled by runProvisioner; mount is handled by built-in plugins
}

func (p *volumePlugin) RequiresRemount(spec *volume.Spec) bool {
	return false
}

func (p *volumePlugin) NewMounter(spec *volume.Spec, pod *v1.Pod) (volume.Mounter, error) {
	if spec == nil {
		return nil, fmt.Errorf("nil volume spec")
	}
	name := spec.Name()
	path := filepath.Join(p.Mounter.dataDir, "volumes", name)
	logger.Trace().Str("name", name).Str("path", path).Msg("NewMounter")
	return &mounter{
		backend:    p.backend,
		volumeName: name,
		path:       path,
		readOnly:   spec.ReadOnly,
		ctx:        p.ctx,
		dataDir:    p.Mounter.dataDir,
	}, nil
}

func (p *volumePlugin) NewUnmounter(name string, uid types.UID) (volume.Unmounter, error) {
	path := filepath.Join(p.Mounter.dataDir, "volumes", name)
	logger.Trace().Str("name", name).Str("path", path).Msg("NewUnmounter")
	return &mounter{
		backend:    p.backend,
		volumeName: name,
		path:       path,
		ctx:        p.ctx,
		dataDir:    p.Mounter.dataDir,
	}, nil
}

func (p *volumePlugin) ConstructVolumeSpec(volumeName, volumePath string) (volume.ReconstructedVolume, error) {
	pv := &v1.PersistentVolume{}
	pv.Name = volumeName
	pv.Spec.PersistentVolumeSource.Local = &v1.LocalVolumeSource{Path: volumePath}
	return volume.ReconstructedVolume{
		Spec: volume.NewSpecFromPersistentVolume(pv, false),
	}, nil
}

func (p *volumePlugin) SupportsMountOption() bool {
	return false
}

func (p *volumePlugin) SupportsSELinuxContextMount(spec *volume.Spec) (bool, error) {
	return false, nil
}

// --- ProvisionableVolumePlugin / DeletableVolumePlugin ---

func (p *volumePlugin) NewProvisioner(_ klog.Logger, options volume.VolumeOptions) (volume.Provisioner, error) {
	return &provisioner{
		backend: p.backend,
		ctx:     p.ctx,
		dataDir: p.Mounter.dataDir,
		options: options,
	}, nil
}

func (p *volumePlugin) NewDeleter(_ klog.Logger, spec *volume.Spec) (volume.Deleter, error) {
	name := spec.Name()
	return &mounter{
		backend:    p.backend,
		volumeName: name,
		path:       filepath.Join(p.Mounter.dataDir, "volumes", name),
		ctx:        p.ctx,
		dataDir:    p.Mounter.dataDir,
	}, nil
}

// Delete implements volume.Deleter on *mounter.
func (m *mounter) Delete() error {
	return m.TearDownAt(m.path)
}

type provisioner struct {
	backend Backend
	ctx     context.Context
	dataDir string
	options volume.VolumeOptions
}

func (p *provisioner) Provision(_ *v1.Node, _ []v1.TopologySelectorTerm) (*v1.PersistentVolume, error) {
	name := p.options.PVName
	volPath := filepath.Join(p.dataDir, "volumes", name)

	if p.backend != nil {
		if _, err := p.backend.CreateVolume(p.ctx, name); err != nil {
			return nil, fmt.Errorf("create volume %s: %w", name, err)
		}
	}
	if err := os.MkdirAll(volPath, 0755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", volPath, err)
	}

	pv := &v1.PersistentVolume{}
	pv.Name = name
	pv.Spec.Capacity = v1.ResourceList{
		v1.ResourceStorage: p.options.PVC.Spec.Resources.Requests[v1.ResourceStorage],
	}
	pv.Spec.AccessModes = p.options.PVC.Spec.AccessModes
	pv.Spec.PersistentVolumeReclaimPolicy = p.options.PersistentVolumeReclaimPolicy
	pv.Spec.PersistentVolumeSource.Local = &v1.LocalVolumeSource{Path: volPath}
	return pv, nil
}

func newMounter(ctx context.Context, dataDir string) *mounter {
	logger.Trace().Msg("newMounter")
	return &mounter{
		dataDir: dataDir,
		ctx:     ctx,
	}
}

// --- volume.Mounter / volume.Unmounter (per-volume instances) ---

func (m *mounter) GetPath() string { return m.path }

func (m *mounter) GetMetrics() (*volume.Metrics, error) { return &volume.Metrics{}, nil }

func (m *mounter) SetUp(args volume.MounterArgs) error { return m.SetUpAt(m.path, args) }

func (m *mounter) SetUpAt(dir string, _ volume.MounterArgs) error {
	if m.backend != nil {
		if _, err := m.backend.CreateVolume(m.ctx, m.volumeName); err != nil {
			return err
		}
	}
	return os.MkdirAll(dir, 0755)
}

func (m *mounter) GetAttributes() volume.Attributes {
	return volume.Attributes{ReadOnly: m.readOnly, Managed: true}
}

func (m *mounter) TearDown() error { return m.TearDownAt(m.path) }

func (m *mounter) TearDownAt(_ string) error {
	if m.backend != nil {
		return m.backend.RemoveVolume(m.ctx, m.volumeName)
	}
	return nil
}

// --- mount.Interface routing ---

func (m *mounter) Mount(source string, target string, fstype string, options []string) error {
	switch {
	case fstype == "tmpfs":
		return m.mountTmpfs(target, "Mount", options)
	case fstype == "bind" || (fstype == "" && slices.Contains(options, "bind")):
		return m.mountBind(source, target, "Mount", options)
	default:
		logger.Trace().Str("source", source).Str("target", target).Str("fstype", fstype).Strs("options", options).Msg("Mount not implemented")
		return errNotImplemented
	}
}

func (m *mounter) MountSensitive(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	switch {
	case fstype == "tmpfs":
		return m.mountTmpfs(target, "MountSensitive", options)
	case fstype == "bind" || (fstype == "" && slices.Contains(options, "bind")):
		return m.mountBind(source, target, "MountSensitive", options)
	default:
		logger.Trace().Str("source", source).Str("target", target).Str("fstype", fstype).Strs("options", options).Msg("MountSensitive not implemented")
		return errNotImplemented
	}
}

func (m *mounter) MountSensitiveWithoutSystemd(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	switch {
	case fstype == "tmpfs":
		return m.mountTmpfs(target, "MountSensitiveWithoutSystemd", options)
	case fstype == "bind" || (fstype == "" && slices.Contains(options, "bind")):
		return m.mountBind(source, target, "MountSensitiveWithoutSystemd", options)
	default:
		logger.Trace().Str("source", source).Str("target", target).Str("fstype", fstype).Strs("options", options).Msg("MountSensitiveWithoutSystemd not implemented")
		return errNotImplemented
	}
}

func (m *mounter) MountSensitiveWithoutSystemdWithMountFlags(source string, target string, fstype string, options []string, sensitiveOptions []string, mountFlags []string) error {
	switch {
	case fstype == "tmpfs":
		return m.mountTmpfs(target, "MountSensitiveWithoutSystemdWithMountFlags", options)
	case fstype == "bind" || (fstype == "" && slices.Contains(options, "bind")):
		return m.mountBind(source, target, "MountSensitiveWithoutSystemdWithMountFlags", options)
	default:
		logger.Trace().Str("source", source).Str("target", target).Str("fstype", fstype).Strs("options", options).Msg("MountSensitiveWithoutSystemdWithMountFlags not implemented")
		return errNotImplemented
	}
}

func (m *mounter) Unmount(target string) error {
	logger.Info().Str("target", target).Msg("Unmount")
	m.mounts.Delete(target)
	m.volumes.Delete(target)
	return nil
}

func (m *mounter) List() ([]mount.MountPoint, error) {
	var pts []mount.MountPoint
	m.mounts.Range(func(_, v any) bool {
		pts = append(pts, v.(mount.MountPoint))
		return true
	})
	return pts, nil
}

func (m *mounter) IsLikelyNotMountPoint(file string) (bool, error) {
	_, mounted := m.mounts.Load(file)
	return !mounted, nil
}

func (m *mounter) CanSafelySkipMountPointCheck() bool {
	return false
}

func (m *mounter) IsMountPoint(file string) (bool, error) {
	_, mounted := m.mounts.Load(file)
	return mounted, nil
}

func (m *mounter) GetMountRefs(pathname string) ([]string, error) {
	return nil, nil
}

// --- helpers ---

// mountTmpfs records a tmpfs mount in the tracking map.
func (m *mounter) mountTmpfs(target string, method string, options []string) error {
	logger.Trace().Str("target", target).Str("method", method).Strs("options", options).Msg("mountTmpfs")
	m.mounts.Store(target, mount.MountPoint{
		Device: "tmpfs",
		Path:   target,
		Type:   "tmpfs",
		Opts:   options,
	})
	return nil
}

// mountBind records a bind mount and propagates Docker volume names.
func (m *mounter) mountBind(source, target, method string, options []string) error {
	logger.Trace().Str("source", source).Str("target", target).Str("method", method).Strs("options", options).Msg("mountBind")

	m.mounts.Store(target, mount.MountPoint{
		Device: source,
		Path:   target,
		Type:   "bind",
		Opts:   options,
	})

	// Propagate Docker volume name: if the source is already tracked as a
	// volume (from a prior MountDevice call), carry the name to the new target.
	// Otherwise, extract from the $DataDir/volumes/<name> path convention.
	if volName, ok := m.GetVolume(source); ok {
		m.volumes.Store(target, volName)
	} else if name := m.extractVolumeName(source); name != "" {
		m.volumes.Store(target, name)
		m.volumes.Store(source, name)
	}

	return nil
}

// extractVolumeName returns the Docker volume name if source is under
// $DataDir/volumes/<name>. Returns empty string otherwise.
func (m *mounter) extractVolumeName(source string) string {
	prefix := filepath.Join(m.dataDir, "volumes") + string(filepath.Separator)
	if !strings.HasPrefix(source, prefix) {
		return ""
	}
	rel := strings.TrimPrefix(source, prefix)
	name := strings.SplitN(rel, string(filepath.Separator), 2)[0]
	return name
}

// --- MountLookup (satisfies critypes.MountLookup) ---

// GetTmpfs returns the comma-joined options for a tracked tmpfs mount.
// If the directory already contains files (e.g. written by AtomicWriter for
// Projected/Secret/DownwardAPI volumes), we return false so the CRI falls
// back to a bind mount, making those files visible inside the container.
func (m *mounter) GetTmpfs(path string) (string, bool) {
	v, ok := m.mounts.Load(path)
	if !ok {
		return "", false
	}
	pt := v.(mount.MountPoint)
	if pt.Type != "tmpfs" {
		return "", false
	}
	entries, err := os.ReadDir(path)
	if err == nil && len(entries) > 0 {
		logger.Debug().Str("path", path).Int("entries", len(entries)).Msg("tmpfs has pre-written content, falling back to bind mount")
		return "", false
	}
	return strings.Join(pt.Opts, ","), true
}

// GetVolume returns the Docker volume name for a tracked bind mount path.
func (m *mounter) GetVolume(path string) (string, bool) {
	v, ok := m.volumes.Load(path)
	if !ok {
		return "", false
	}
	return v.(string), true
}
