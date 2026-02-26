package mounter

import "k8s.io/mount-utils"

// mountTmpfs records a tmpfs mount in the tracking map.
// The directory is already created by the volume plugin (os.MkdirAll);
// we just track it so IsLikelyNotMountPoint and List return correct results.
func (m *ScopedMounter) mountTmpfs(target string, method string, options []string) error {
	logger.Trace().Str("target", target).Str("method", method).Strs("options", options).Msg("mountTmpfs")
	m.mounts.Store(target, mount.MountPoint{
		Device: "tmpfs",
		Path:   target,
		Type:   "tmpfs",
		Opts:   options,
	})
	return nil
}
