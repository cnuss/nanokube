package backend

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/cnuss/nanokube/_old/component"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
)

func NewHostUtils(backend *BackendImpl) hostutil.HostUtils {
	hostUtils := &HostUtilsImpl{backend: backend, log: component.NewLogger("host-utils")}
	// TODO: start
	return hostUtils
}

type HostUtilsImpl struct {
	backend *BackendImpl
	log     component.Logger
}

func (h *HostUtilsImpl) DeviceOpened(pathname string) (bool, error) {
	return false, nil
}

func (h *HostUtilsImpl) PathIsDevice(pathname string) (bool, error) {
	return false, nil
}

func (h *HostUtilsImpl) MakeRShared(path string) error {
	return nil
}

func (h *HostUtilsImpl) GetFileType(pathname string) (hostutil.FileType, error) {
	info, err := os.Stat(pathname)
	if err != nil {
		if os.IsNotExist(err) {
			return hostutil.FileTypeUnknown, fmt.Errorf("path %q does not exist", pathname)
		}
		return hostutil.FileTypeUnknown, err
	}
	mode := info.Mode()
	if mode.IsDir() {
		return hostutil.FileTypeDirectory, nil
	} else if mode.IsRegular() {
		return hostutil.FileTypeFile, nil
	} else if mode&os.ModeSocket == os.ModeSocket {
		return hostutil.FileTypeSocket, nil
	} else if mode&os.ModeDevice == os.ModeDevice {
		if mode&os.ModeCharDevice == os.ModeCharDevice {
			return hostutil.FileTypeCharDev, nil
		}
		return hostutil.FileTypeBlockDev, nil
	}
	return hostutil.FileTypeUnknown, fmt.Errorf("unknown file type for %q", pathname)
}

func (h *HostUtilsImpl) PathExists(pathname string) (bool, error) {
	_, err := os.Stat(pathname)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (h *HostUtilsImpl) EvalHostSymlinks(pathname string) (string, error) {
	return filepath.EvalSymlinks(pathname)
}

func (h *HostUtilsImpl) GetOwner(pathname string) (int64, int64, error) {
	return 0, 0, component.WrapErr(h.log, fmt.Errorf("not implemented"), pathname)
}

func (h *HostUtilsImpl) GetSELinuxSupport(pathname string) (bool, error) {
	return false, nil
}

func (h *HostUtilsImpl) GetMode(pathname string) (os.FileMode, error) {
	return 0, component.WrapErr(h.log, fmt.Errorf("not implemented"), pathname)
}

func (h *HostUtilsImpl) GetSELinuxMountContext(pathname string) (string, error) {
	return "", nil
}

var _ hostutil.HostUtils = &HostUtilsImpl{}
