package kubelet

import (
	"fmt"
	"os"
	"path/filepath"

	"k8s.io/kubernetes/pkg/volume/util/hostutil"
)

var (
	errHostUtilNotImplemented = fmt.Errorf("scoped host util not yet implemented")
	hostutilLog               = newLogger("hostutil")
)

// ScopedHostUtil is a stub hostutil.HostUtils that returns not-implemented errors.
type ScopedHostUtil struct {
	DataDir string
}

func (h *ScopedHostUtil) DeviceOpened(pathname string) (bool, error) {
	hostutilLog.Warn().Str("path", pathname).Msg("DeviceOpened not implemented")
	return false, nil
}

func (h *ScopedHostUtil) PathIsDevice(pathname string) (bool, error) {
	hostutilLog.Warn().Str("path", pathname).Msg("PathIsDevice not implemented")
	return false, nil
}

func (h *ScopedHostUtil) MakeRShared(path string) error {
	hostutilLog.Warn().Str("path", path).Msg("MakeRShared not implemented")
	return nil
}

func (h *ScopedHostUtil) GetFileType(pathname string) (hostutil.FileType, error) {
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

func (h *ScopedHostUtil) PathExists(pathname string) (bool, error) {
	_, err := os.Stat(pathname)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (h *ScopedHostUtil) EvalHostSymlinks(pathname string) (string, error) {
	return filepath.EvalSymlinks(pathname)
}

func (h *ScopedHostUtil) GetOwner(pathname string) (int64, int64, error) {
	hostutilLog.Warn().Str("path", pathname).Msg("GetOwner not implemented")
	return 0, 0, errHostUtilNotImplemented
}

func (h *ScopedHostUtil) GetSELinuxSupport(pathname string) (bool, error) {
	hostutilLog.Warn().Str("path", pathname).Msg("GetSELinuxSupport not implemented")
	return false, nil
}

func (h *ScopedHostUtil) GetMode(pathname string) (os.FileMode, error) {
	hostutilLog.Warn().Str("path", pathname).Msg("GetMode not implemented")
	return 0, errHostUtilNotImplemented
}

func (h *ScopedHostUtil) GetSELinuxMountContext(pathname string) (string, error) {
	hostutilLog.Warn().Str("path", pathname).Msg("GetSELinuxMountContext not implemented")
	return "", nil
}
