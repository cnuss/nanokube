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
	panic("CanSafelySkipMountPointCheck: unimplemented")
}

// GetMountRefs implements [mount.Interface].
func (m *MounterImpl) GetMountRefs(pathname string) ([]string, error) {
	panic("GetMountRefs: unimplemented")
}

// IsLikelyNotMountPoint implements [mount.Interface].
func (m *MounterImpl) IsLikelyNotMountPoint(file string) (bool, error) {
	panic("IsLikelyNotMountPoint: unimplemented")
}

// IsMountPoint implements [mount.Interface].
func (m *MounterImpl) IsMountPoint(file string) (bool, error) {
	panic("IsMountPoint: unimplemented")
}

// List implements [mount.Interface].
func (m *MounterImpl) List() ([]mount.MountPoint, error) {
	panic("List: unimplemented")
}

// Mount implements [mount.Interface].
func (m *MounterImpl) Mount(source string, target string, fstype string, options []string) error {
	panic("Mount: unimplemented")
}

// MountSensitive implements [mount.Interface].
func (m *MounterImpl) MountSensitive(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	panic("MountSensitive: unimplemented")
}

// MountSensitiveWithoutSystemd implements [mount.Interface].
func (m *MounterImpl) MountSensitiveWithoutSystemd(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	panic("MountSensitiveWithoutSystemd: unimplemented")
}

// MountSensitiveWithoutSystemdWithMountFlags implements [mount.Interface].
func (m *MounterImpl) MountSensitiveWithoutSystemdWithMountFlags(source string, target string, fstype string, options []string, sensitiveOptions []string, mountFlags []string) error {
	panic("MountSensitiveWithoutSystemdWithMountFlags: unimplemented")
}

// Unmount implements [mount.Interface].
func (m *MounterImpl) Unmount(target string) error {
	panic("Unmount: unimplemented")
}

var _ mount.Interface = &MounterImpl{}
