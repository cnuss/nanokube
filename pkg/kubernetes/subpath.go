package kubernetes

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"k8s.io/kubernetes/pkg/volume/util/subpath"
)

var subpathLog = newLogger("subpath")

// ScopedSubpath implements subpath.Interface with real os.MkdirAll calls,
// remapping absolute paths outside DataDir into it.
type ScopedSubpath struct {
	DataDir string
}

func (s *ScopedSubpath) remap(path string) string {
	if filepath.IsAbs(path) && !strings.HasPrefix(path, s.DataDir) {
		return filepath.Join(s.DataDir, path)
	}
	return path
}

func (s *ScopedSubpath) CleanSubPaths(podDir string, volumeName string) error {
	subpathLog.Debug().Str("podDir", podDir).Str("volume", volumeName).Msg("CleanSubPaths not implemented")
	return nil
}

func (s *ScopedSubpath) PrepareSafeSubpath(sub subpath.Subpath) (string, func(), error) {
	subpathLog.Debug().Str("path", sub.Path).Msg("PrepareSafeSubpath not implemented")
	return sub.Path, nil, nil
}

func (s *ScopedSubpath) SafeMakeDir(subdir string, base string, perm os.FileMode) error {
	remappedBase := s.remap(base)
	target := filepath.Join(remappedBase, subdir)

	// Verify target stays under base to prevent path traversal
	resolved, err := filepath.Abs(target)
	if err != nil {
		return fmt.Errorf("resolve path: %w", err)
	}
	absBase, err := filepath.Abs(remappedBase)
	if err != nil {
		return fmt.Errorf("resolve base: %w", err)
	}
	if !strings.HasPrefix(resolved, absBase+string(filepath.Separator)) && resolved != absBase {
		return fmt.Errorf("path %q escapes base %q", subdir, base)
	}

	return os.MkdirAll(target, perm)
}
