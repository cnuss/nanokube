package kubernetes

import (
	"fmt"
	"os"

	"k8s.io/kubernetes/pkg/volume/util/hostutil"
)

var (
	errHostUtilNotImplemented = fmt.Errorf("scoped host util not yet implemented")
	hostutilLog               = component("hostutil")
)

// ScopedHostUtil is a stub hostutil.HostUtils that returns not-implemented errors.
type ScopedHostUtil struct {
	DataDir string
}

func (h *ScopedHostUtil) DeviceOpened(pathname string) (bool, error) {
	hostutilLog.Debug().Str("path", pathname).Msg("DeviceOpened not implemented")
	return false, nil
}

func (h *ScopedHostUtil) PathIsDevice(pathname string) (bool, error) {
	hostutilLog.Debug().Str("path", pathname).Msg("PathIsDevice not implemented")
	return false, nil
}

func (h *ScopedHostUtil) MakeRShared(path string) error {
	hostutilLog.Debug().Str("path", path).Msg("MakeRShared not implemented")
	return nil
}

func (h *ScopedHostUtil) GetFileType(pathname string) (hostutil.FileType, error) {
	hostutilLog.Debug().Str("path", pathname).Msg("GetFileType not implemented")
	return hostutil.FileTypeUnknown, errHostUtilNotImplemented
}

func (h *ScopedHostUtil) PathExists(pathname string) (bool, error) {
	hostutilLog.Debug().Str("path", pathname).Msg("PathExists not implemented")
	return false, errHostUtilNotImplemented
}

func (h *ScopedHostUtil) EvalHostSymlinks(pathname string) (string, error) {
	hostutilLog.Debug().Str("path", pathname).Msg("EvalHostSymlinks not implemented")
	return "", errHostUtilNotImplemented
}

func (h *ScopedHostUtil) GetOwner(pathname string) (int64, int64, error) {
	hostutilLog.Debug().Str("path", pathname).Msg("GetOwner not implemented")
	return 0, 0, errHostUtilNotImplemented
}

func (h *ScopedHostUtil) GetSELinuxSupport(pathname string) (bool, error) {
	hostutilLog.Debug().Str("path", pathname).Msg("GetSELinuxSupport not implemented")
	return false, nil
}

func (h *ScopedHostUtil) GetMode(pathname string) (os.FileMode, error) {
	hostutilLog.Debug().Str("path", pathname).Msg("GetMode not implemented")
	return 0, errHostUtilNotImplemented
}

func (h *ScopedHostUtil) GetSELinuxMountContext(pathname string) (string, error) {
	hostutilLog.Debug().Str("path", pathname).Msg("GetSELinuxMountContext not implemented")
	return "", nil
}
