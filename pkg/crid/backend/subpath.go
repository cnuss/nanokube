package backend

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cnuss/nanokube/pkg/component"
	"k8s.io/kubernetes/pkg/volume/util/subpath"
)

func NewSubpath(backend *BackendImpl) subpath.Interface {
	subpath := &SubpathImpl{backend: backend, log: component.NewLogger("subpath")}
	// TODO: start
	return subpath
}

type SubpathImpl struct {
	backend *BackendImpl
	log     component.Logger
}

func (s *SubpathImpl) remap(path string) string {
	if filepath.IsAbs(path) && !strings.HasPrefix(path, s.backend.DataDir()) {
		return filepath.Join(s.backend.DataDir(), path)
	}
	return path
}

func (s *SubpathImpl) CleanSubPaths(podDir string, volumeName string) error {
	s.log.Warn().Str("podDir", podDir).Str("volume", volumeName).Msg("CleanSubPaths not implemented")
	return nil
}

func (s *SubpathImpl) PrepareSafeSubpath(sub subpath.Subpath) (string, func(), error) {
	s.log.Warn().Str("path", sub.Path).Msg("PrepareSafeSubpath not implemented")
	return sub.Path, nil, nil
}

func (s *SubpathImpl) SafeMakeDir(subdir string, base string, perm os.FileMode) error {
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

var _ subpath.Interface = &SubpathImpl{}
