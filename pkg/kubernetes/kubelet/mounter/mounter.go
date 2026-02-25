package mounter

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/cnuss/nanokube/pkg/component"
	"k8s.io/mount-utils"
)

var (
	errNotImplemented = fmt.Errorf("scoped mounter not yet implemented")
	logger            = component.NewLogger("mounter")
)

// ScopedMounter implements mount.Interface for unprivileged operation.
// Each fstype is handled by a dedicated implementation file (e.g. tmpfs.go).
// Unrecognized fstypes return errNotImplemented.
type ScopedMounter struct {
	DataDir string
	mounts  sync.Map // target path → mount.MountPoint
}

func (m *ScopedMounter) Mount(source string, target string, fstype string, options []string) error {
	switch fstype {
	case "tmpfs":
		return m.mountTmpfs(target, "Mount", options)
	default:
		logger.Trace().Str("source", source).Str("target", target).Str("fstype", fstype).Strs("options", options).Msg("Mount not implemented")
		return errNotImplemented
	}
}

func (m *ScopedMounter) MountSensitive(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	switch fstype {
	case "tmpfs":
		return m.mountTmpfs(target, "MountSensitive", options)
	default:
		logger.Trace().Str("source", source).Str("target", target).Str("fstype", fstype).Strs("options", options).Msg("MountSensitive not implemented")
		return errNotImplemented
	}
}

func (m *ScopedMounter) MountSensitiveWithoutSystemd(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	switch fstype {
	case "tmpfs":
		return m.mountTmpfs(target, "MountSensitiveWithoutSystemd", options)
	default:
		logger.Trace().Str("source", source).Str("target", target).Str("fstype", fstype).Strs("options", options).Msg("MountSensitiveWithoutSystemd not implemented")
		return errNotImplemented
	}
}

func (m *ScopedMounter) MountSensitiveWithoutSystemdWithMountFlags(source string, target string, fstype string, options []string, sensitiveOptions []string, mountFlags []string) error {
	switch fstype {
	case "tmpfs":
		return m.mountTmpfs(target, "MountSensitiveWithoutSystemdWithMountFlags", options)
	default:
		logger.Trace().Str("source", source).Str("target", target).Str("fstype", fstype).Strs("options", options).Msg("MountSensitiveWithoutSystemdWithMountFlags not implemented")
		return errNotImplemented
	}
}

func (m *ScopedMounter) Unmount(target string) error {
	logger.Info().Str("target", target).Msg("Unmount")
	m.mounts.Delete(target)
	return nil
}

func (m *ScopedMounter) List() ([]mount.MountPoint, error) {
	var pts []mount.MountPoint
	m.mounts.Range(func(_, v any) bool {
		pts = append(pts, v.(mount.MountPoint))
		return true
	})
	return pts, nil
}

func (m *ScopedMounter) IsLikelyNotMountPoint(file string) (bool, error) {
	_, mounted := m.mounts.Load(file)
	return !mounted, nil
}

func (m *ScopedMounter) CanSafelySkipMountPointCheck() bool {
	return false
}

func (m *ScopedMounter) IsMountPoint(file string) (bool, error) {
	_, mounted := m.mounts.Load(file)
	return mounted, nil
}

func (m *ScopedMounter) GetMountRefs(pathname string) ([]string, error) {
	return nil, nil
}

// GetTmpfs returns the comma-joined options for a tracked tmpfs mount.
// Satisfies docker.MountLookup so the Docker backend can use native tmpfs.
//
// If the directory already contains files (e.g. written by AtomicWriter for
// Projected/Secret/DownwardAPI volumes), we return false so the CRI falls
// back to a bind mount, making those files visible inside the container.
func (m *ScopedMounter) GetTmpfs(path string) (string, bool) {
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
