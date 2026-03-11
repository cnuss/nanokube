package backend

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/cnuss/nanokube/pkg/component"
	"k8s.io/mount-utils"
)

func NewMounter(backend *BackendImpl) *MounterImpl {
	return &MounterImpl{
		backend: backend,
		log:     component.NewLogger("mounter"),
	}
}

type MounterImpl struct {
	backend *BackendImpl
	log     component.Logger
	mounts  sync.Map // target path → mount.MountPoint
	volumes sync.Map // target path → Docker volume name
}

// CanSafelySkipMountPointCheck implements [mount.Interface].
func (m *MounterImpl) CanSafelySkipMountPointCheck() bool {
	return false
}

// GetMountRefs implements [mount.Interface].
func (m *MounterImpl) GetMountRefs(pathname string) ([]string, error) {
	return nil, nil
}

// IsLikelyNotMountPoint implements [mount.Interface].
func (m *MounterImpl) IsLikelyNotMountPoint(file string) (bool, error) {
	_, mounted := m.mounts.Load(file)
	return !mounted, nil
}

// IsMountPoint implements [mount.Interface].
func (m *MounterImpl) IsMountPoint(file string) (bool, error) {
	_, mounted := m.mounts.Load(file)
	return mounted, nil
}

// List implements [mount.Interface].
func (m *MounterImpl) List() ([]mount.MountPoint, error) {
	var pts []mount.MountPoint
	m.mounts.Range(func(_, v any) bool {
		pts = append(pts, v.(mount.MountPoint))
		return true
	})
	return pts, nil
}

// Mount implements [mount.Interface].
func (m *MounterImpl) Mount(source string, target string, fstype string, options []string) error {
	return m.doMount(source, target, fstype, options)
}

// MountSensitive implements [mount.Interface].
func (m *MounterImpl) MountSensitive(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	return m.doMount(source, target, fstype, options)
}

// MountSensitiveWithoutSystemd implements [mount.Interface].
func (m *MounterImpl) MountSensitiveWithoutSystemd(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	return m.doMount(source, target, fstype, options)
}

// MountSensitiveWithoutSystemdWithMountFlags implements [mount.Interface].
func (m *MounterImpl) MountSensitiveWithoutSystemdWithMountFlags(source string, target string, fstype string, options []string, sensitiveOptions []string, mountFlags []string) error {
	return m.doMount(source, target, fstype, options)
}

// Unmount implements [mount.Interface].
func (m *MounterImpl) Unmount(target string) error {
	m.mounts.Delete(target)
	m.volumes.Delete(target)
	return nil
}

func (m *MounterImpl) doMount(source, target, fstype string, options []string) error {
	switch {
	case fstype == "tmpfs":
		m.mounts.Store(target, mount.MountPoint{
			Device: "tmpfs",
			Path:   target,
			Type:   "tmpfs",
			Opts:   options,
		})
		return nil
	case fstype == "bind" || (fstype == "" && slices.Contains(options, "bind")):
		m.mounts.Store(target, mount.MountPoint{
			Device: source,
			Path:   target,
			Type:   "bind",
			Opts:   options,
		})
		if volName, ok := m.GetVolume(source); ok {
			m.volumes.Store(target, volName)
		} else if name := m.extractVolumeName(source); name != "" {
			m.volumes.Store(target, name)
			m.volumes.Store(source, name)
		}
		return nil
	default:
		m.log.Warn().Str("source", source).Str("target", target).Str("fstype", fstype).Msg("unsupported mount type")
		return nil
	}
}

func (m *MounterImpl) extractVolumeName(source string) string {
	prefix := filepath.Join(m.backend.DataDir(), "volumes") + string(filepath.Separator)
	if !strings.HasPrefix(source, prefix) {
		return ""
	}
	rel := strings.TrimPrefix(source, prefix)
	return strings.SplitN(rel, string(filepath.Separator), 2)[0]
}

// GetTmpfs implements [MountLookup]. Returns tmpfs options for the path,
// unless the directory has pre-written content (projected/secret/downwardAPI),
// in which case it returns false so the backend uses a bind mount instead.
func (m *MounterImpl) GetTmpfs(path string) (string, bool) {
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
		m.log.Debug().Str("path", path).Int("entries", len(entries)).Msg("tmpfs has pre-written content, falling back to bind mount")
		return "", false
	}
	return strings.Join(pt.Opts, ","), true
}

// GetVolume implements [MountLookup]. Returns the Docker volume name for a tracked path.
func (m *MounterImpl) GetVolume(path string) (string, bool) {
	v, ok := m.volumes.Load(path)
	if !ok {
		return "", false
	}
	return v.(string), true
}

var _ mount.Interface = &MounterImpl{}
var _ MountLookup = &MounterImpl{}
