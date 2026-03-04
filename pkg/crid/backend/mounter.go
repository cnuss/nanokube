package backend

import "k8s.io/mount-utils"

func NewMounter(backend *BackendImpl) mount.Interface {
	mounter := &MounterImpl{backend: backend}
	// TODO: start
	return mounter
}

type MounterImpl struct {
	backend *BackendImpl
}

// CanSafelySkipMountPointCheck implements [mount.Interface].
func (m *MounterImpl) CanSafelySkipMountPointCheck() bool {
	panic("unimplemented")
}

// GetMountRefs implements [mount.Interface].
func (m *MounterImpl) GetMountRefs(pathname string) ([]string, error) {
	panic("unimplemented")
}

// IsLikelyNotMountPoint implements [mount.Interface].
func (m *MounterImpl) IsLikelyNotMountPoint(file string) (bool, error) {
	panic("unimplemented")
}

// IsMountPoint implements [mount.Interface].
func (m *MounterImpl) IsMountPoint(file string) (bool, error) {
	panic("unimplemented")
}

// List implements [mount.Interface].
func (m *MounterImpl) List() ([]mount.MountPoint, error) {
	panic("unimplemented")
}

// Mount implements [mount.Interface].
func (m *MounterImpl) Mount(source string, target string, fstype string, options []string) error {
	panic("unimplemented")
}

// MountSensitive implements [mount.Interface].
func (m *MounterImpl) MountSensitive(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	panic("unimplemented")
}

// MountSensitiveWithoutSystemd implements [mount.Interface].
func (m *MounterImpl) MountSensitiveWithoutSystemd(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	panic("unimplemented")
}

// MountSensitiveWithoutSystemdWithMountFlags implements [mount.Interface].
func (m *MounterImpl) MountSensitiveWithoutSystemdWithMountFlags(source string, target string, fstype string, options []string, sensitiveOptions []string, mountFlags []string) error {
	panic("unimplemented")
}

// Unmount implements [mount.Interface].
func (m *MounterImpl) Unmount(target string) error {
	panic("unimplemented")
}

var _ mount.Interface = &MounterImpl{}
