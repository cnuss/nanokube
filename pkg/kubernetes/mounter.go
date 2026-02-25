package kubernetes

import (
	"fmt"

	"github.com/rs/zerolog/log"
	"k8s.io/mount-utils"
)

var errMounterNotImplemented = fmt.Errorf("scoped mounter not yet implemented")

// ScopedMounter is a stub mount.Interface that returns not-implemented errors.
type ScopedMounter struct {
	DataDir string
}

func (m *ScopedMounter) Mount(source string, target string, fstype string, options []string) error {
	log.Warn().Str("source", source).Str("target", target).Msg("Mount not implemented")
	return errMounterNotImplemented
}

func (m *ScopedMounter) MountSensitive(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	log.Warn().Str("source", source).Str("target", target).Msg("MountSensitive not implemented")
	return errMounterNotImplemented
}

func (m *ScopedMounter) MountSensitiveWithoutSystemd(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	log.Warn().Str("source", source).Str("target", target).Msg("MountSensitiveWithoutSystemd not implemented")
	return errMounterNotImplemented
}

func (m *ScopedMounter) MountSensitiveWithoutSystemdWithMountFlags(source string, target string, fstype string, options []string, sensitiveOptions []string, mountFlags []string) error {
	log.Warn().Str("source", source).Str("target", target).Msg("MountSensitiveWithoutSystemdWithMountFlags not implemented")
	return errMounterNotImplemented
}

func (m *ScopedMounter) Unmount(target string) error {
	log.Warn().Str("target", target).Msg("Unmount not implemented")
	return errMounterNotImplemented
}

func (m *ScopedMounter) List() ([]mount.MountPoint, error) {
	log.Warn().Msg("List not implemented")
	return nil, errMounterNotImplemented
}

func (m *ScopedMounter) IsLikelyNotMountPoint(file string) (bool, error) {
	log.Warn().Str("file", file).Msg("IsLikelyNotMountPoint not implemented")
	return true, errMounterNotImplemented
}

func (m *ScopedMounter) CanSafelySkipMountPointCheck() bool {
	log.Warn().Msg("CanSafelySkipMountPointCheck not implemented")
	return false
}

func (m *ScopedMounter) IsMountPoint(file string) (bool, error) {
	log.Warn().Str("file", file).Msg("IsMountPoint not implemented")
	return false, errMounterNotImplemented
}

func (m *ScopedMounter) GetMountRefs(pathname string) ([]string, error) {
	log.Warn().Str("path", pathname).Msg("GetMountRefs not implemented")
	return nil, errMounterNotImplemented
}
