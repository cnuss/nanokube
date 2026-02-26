package mounter

import (
	"path/filepath"
	"strings"

	"k8s.io/mount-utils"
)

// mountBind records a bind mount in the tracking map and propagates
// Docker volume names through the mount chain.
func (m *ScopedMounter) mountBind(source, target, method string, options []string) error {
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
func (m *ScopedMounter) extractVolumeName(source string) string {
	prefix := filepath.Join(m.DataDir, "volumes") + string(filepath.Separator)
	if !strings.HasPrefix(source, prefix) {
		return ""
	}
	rel := strings.TrimPrefix(source, prefix)
	name := strings.SplitN(rel, string(filepath.Separator), 2)[0]
	return name
}

// GetVolume returns the Docker volume name for a tracked bind mount path.
func (m *ScopedMounter) GetVolume(path string) (string, bool) {
	v, ok := m.volumes.Load(path)
	if !ok {
		return "", false
	}
	return v.(string), true
}
