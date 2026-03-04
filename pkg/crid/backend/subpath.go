package backend

import (
	"os"

	"k8s.io/kubernetes/pkg/volume/util/subpath"
)

func NewSubpath(backend *BackendImpl) subpath.Interface {
	subpath := &SubpathImpl{backend: backend}
	// TODO: start
	return subpath
}

type SubpathImpl struct {
	backend *BackendImpl
}

// CleanSubPaths implements [subpath.Interface].
func (s *SubpathImpl) CleanSubPaths(poodDir string, volumeName string) error {
	panic("unimplemented")
}

// PrepareSafeSubpath implements [subpath.Interface].
func (s *SubpathImpl) PrepareSafeSubpath(subPath subpath.Subpath) (newHostPath string, cleanupAction func(), err error) {
	panic("unimplemented")
}

// SafeMakeDir implements [subpath.Interface].
func (s *SubpathImpl) SafeMakeDir(subdir string, base string, perm os.FileMode) error {
	panic("unimplemented")
}

var _ subpath.Interface = &SubpathImpl{}
