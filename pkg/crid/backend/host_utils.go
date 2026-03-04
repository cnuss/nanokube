package backend

import (
	"os"

	"k8s.io/kubernetes/pkg/volume/util/hostutil"
)

func NewHostUtils(backend *BackendImpl) hostutil.HostUtils {
	hostUtils := &HostUtilsImpl{backend: backend}
	// TODO: start
	return hostUtils
}

type HostUtilsImpl struct {
	backend *BackendImpl
}

// DeviceOpened implements [hostutil.HostUtils].
func (h *HostUtilsImpl) DeviceOpened(pathname string) (bool, error) {
	panic("unimplemented")
}

// EvalHostSymlinks implements [hostutil.HostUtils].
func (h *HostUtilsImpl) EvalHostSymlinks(pathname string) (string, error) {
	panic("unimplemented")
}

// GetFileType implements [hostutil.HostUtils].
func (h *HostUtilsImpl) GetFileType(pathname string) (hostutil.FileType, error) {
	panic("unimplemented")
}

// GetMode implements [hostutil.HostUtils].
func (h *HostUtilsImpl) GetMode(pathname string) (os.FileMode, error) {
	panic("unimplemented")
}

// GetOwner implements [hostutil.HostUtils].
func (h *HostUtilsImpl) GetOwner(pathname string) (int64, int64, error) {
	panic("unimplemented")
}

// GetSELinuxMountContext implements [hostutil.HostUtils].
func (h *HostUtilsImpl) GetSELinuxMountContext(pathname string) (string, error) {
	panic("unimplemented")
}

// GetSELinuxSupport implements [hostutil.HostUtils].
func (h *HostUtilsImpl) GetSELinuxSupport(pathname string) (bool, error) {
	panic("unimplemented")
}

// MakeRShared implements [hostutil.HostUtils].
func (h *HostUtilsImpl) MakeRShared(path string) error {
	panic("unimplemented")
}

// PathExists implements [hostutil.HostUtils].
func (h *HostUtilsImpl) PathExists(pathname string) (bool, error) {
	panic("unimplemented")
}

// PathIsDevice implements [hostutil.HostUtils].
func (h *HostUtilsImpl) PathIsDevice(pathname string) (bool, error) {
	panic("unimplemented")
}

var _ hostutil.HostUtils = &HostUtilsImpl{}
