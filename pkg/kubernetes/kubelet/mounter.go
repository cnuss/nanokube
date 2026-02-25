package kubelet

import (
	"fmt"

	"k8s.io/mount-utils"
)

var (
	errMounterNotImplemented = fmt.Errorf("scoped mounter not yet implemented")
	mounterLog               = newLogger("mounter")
)

// ScopedMounter is a stub mount.Interface that returns not-implemented errors.
type ScopedMounter struct {
	DataDir string
}

func (m *ScopedMounter) Mount(source string, target string, fstype string, options []string) error {
	switch fstype {
	case "tmpfs":
		mounterLog.Trace().Str("source", source).Str("target", target).Strs("options", options).Msg("Mount tmpfs (todo: emptydir)")
	default:
		mounterLog.Trace().Str("source", source).Str("target", target).Str("fstype", fstype).Strs("options", options).Msg("Mount not implemented")
	}
	return errMounterNotImplemented
}

func (m *ScopedMounter) MountSensitive(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	switch fstype {
	case "tmpfs":
		mounterLog.Trace().Str("source", source).Str("target", target).Strs("options", options).Int("sensitiveCount", len(sensitiveOptions)).Msg("MountSensitive tmpfs (todo: emptydir)")
	default:
		mounterLog.Trace().Str("source", source).Str("target", target).Str("fstype", fstype).Strs("options", options).Int("sensitiveCount", len(sensitiveOptions)).Msg("MountSensitive not implemented")
	}
	return errMounterNotImplemented
}

func (m *ScopedMounter) MountSensitiveWithoutSystemd(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	switch fstype {
	case "tmpfs":
		mounterLog.Trace().Str("source", source).Str("target", target).Strs("options", options).Int("sensitiveCount", len(sensitiveOptions)).Msg("MountSensitiveWithoutSystemd tmpfs (todo: emptydir)")
	default:
		mounterLog.Trace().Str("source", source).Str("target", target).Str("fstype", fstype).Strs("options", options).Int("sensitiveCount", len(sensitiveOptions)).Msg("MountSensitiveWithoutSystemd not implemented")
	}
	return errMounterNotImplemented
}

func (m *ScopedMounter) MountSensitiveWithoutSystemdWithMountFlags(source string, target string, fstype string, options []string, sensitiveOptions []string, mountFlags []string) error {
	switch fstype {
	case "tmpfs":
		mounterLog.Trace().Str("source", source).Str("target", target).Strs("options", options).Int("sensitiveCount", len(sensitiveOptions)).Strs("mountFlags", mountFlags).Msg("MountSensitiveWithoutSystemdWithMountFlags tmpfs (todo: emptydir)")
	default:
		mounterLog.Trace().Str("source", source).Str("target", target).Str("fstype", fstype).Strs("options", options).Int("sensitiveCount", len(sensitiveOptions)).Strs("mountFlags", mountFlags).Msg("MountSensitiveWithoutSystemdWithMountFlags not implemented")
	}
	return errMounterNotImplemented
}

func (m *ScopedMounter) Unmount(target string) error {
	mounterLog.Trace().Str("target", target).Msg("Unmount")
	return errMounterNotImplemented
}

func (m *ScopedMounter) List() ([]mount.MountPoint, error) {
	mounterLog.Trace().Msg("List")
	return nil, errMounterNotImplemented
}

func (m *ScopedMounter) IsLikelyNotMountPoint(file string) (bool, error) {
	mounterLog.Trace().Str("file", file).Msg("IsLikelyNotMountPoint")
	return true, errMounterNotImplemented
}

func (m *ScopedMounter) CanSafelySkipMountPointCheck() bool {
	mounterLog.Trace().Msg("CanSafelySkipMountPointCheck")
	return false
}

func (m *ScopedMounter) IsMountPoint(file string) (bool, error) {
	mounterLog.Trace().Str("file", file).Msg("IsMountPoint")
	return false, errMounterNotImplemented
}

func (m *ScopedMounter) GetMountRefs(pathname string) ([]string, error) {
	mounterLog.Trace().Str("path", pathname).Msg("GetMountRefs")
	return nil, errMounterNotImplemented
}
