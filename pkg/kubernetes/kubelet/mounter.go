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
	mounterLog.Debug().Str("source", source).Str("target", target).Msg("Mount not implemented")
	return errMounterNotImplemented
}

func (m *ScopedMounter) MountSensitive(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	mounterLog.Debug().Str("source", source).Str("target", target).Msg("MountSensitive not implemented")
	return errMounterNotImplemented
}

func (m *ScopedMounter) MountSensitiveWithoutSystemd(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	mounterLog.Debug().Str("source", source).Str("target", target).Msg("MountSensitiveWithoutSystemd not implemented")
	return errMounterNotImplemented
}

func (m *ScopedMounter) MountSensitiveWithoutSystemdWithMountFlags(source string, target string, fstype string, options []string, sensitiveOptions []string, mountFlags []string) error {
	mounterLog.Debug().Str("source", source).Str("target", target).Msg("MountSensitiveWithoutSystemdWithMountFlags not implemented")
	return errMounterNotImplemented
}

func (m *ScopedMounter) Unmount(target string) error {
	mounterLog.Debug().Str("target", target).Msg("Unmount not implemented")
	return errMounterNotImplemented
}

func (m *ScopedMounter) List() ([]mount.MountPoint, error) {
	mounterLog.Debug().Msg("List not implemented")
	return nil, errMounterNotImplemented
}

func (m *ScopedMounter) IsLikelyNotMountPoint(file string) (bool, error) {
	mounterLog.Debug().Str("file", file).Msg("IsLikelyNotMountPoint not implemented")
	return true, errMounterNotImplemented
}

func (m *ScopedMounter) CanSafelySkipMountPointCheck() bool {
	mounterLog.Debug().Msg("CanSafelySkipMountPointCheck not implemented")
	return false
}

func (m *ScopedMounter) IsMountPoint(file string) (bool, error) {
	mounterLog.Debug().Str("file", file).Msg("IsMountPoint not implemented")
	return false, errMounterNotImplemented
}

func (m *ScopedMounter) GetMountRefs(pathname string) ([]string, error) {
	mounterLog.Debug().Str("path", pathname).Msg("GetMountRefs not implemented")
	return nil, errMounterNotImplemented
}
