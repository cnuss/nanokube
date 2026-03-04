package backend

import (
	"os"
	"time"

	"k8s.io/kubernetes/pkg/kubelet/container"
)

func NewOS(backend *BackendImpl) container.OSInterface {
	os := &OSImpl{backend: backend}
	// TODO: start
	return os
}

type OSImpl struct {
	backend *BackendImpl
}

// Chmod implements [container.OSInterface].
func (o *OSImpl) Chmod(path string, perm os.FileMode) error {
	panic("unimplemented")
}

// Chtimes implements [container.OSInterface].
func (o *OSImpl) Chtimes(path string, atime time.Time, mtime time.Time) error {
	panic("unimplemented")
}

// Create implements [container.OSInterface].
func (o *OSImpl) Create(path string) (*os.File, error) {
	panic("unimplemented")
}

// Glob implements [container.OSInterface].
func (o *OSImpl) Glob(pattern string) ([]string, error) {
	panic("unimplemented")
}

// Hostname implements [container.OSInterface].
func (o *OSImpl) Hostname() (name string, err error) {
	panic("unimplemented")
}

// MkdirAll implements [container.OSInterface].
func (o *OSImpl) MkdirAll(path string, perm os.FileMode) error {
	panic("unimplemented")
}

// Open implements [container.OSInterface].
func (o *OSImpl) Open(name string) (*os.File, error) {
	panic("unimplemented")
}

// OpenFile implements [container.OSInterface].
func (o *OSImpl) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	panic("unimplemented")
}

// Pipe implements [container.OSInterface].
func (o *OSImpl) Pipe() (r *os.File, w *os.File, err error) {
	panic("unimplemented")
}

// ReadDir implements [container.OSInterface].
func (o *OSImpl) ReadDir(dirname string) ([]os.DirEntry, error) {
	panic("unimplemented")
}

// Remove implements [container.OSInterface].
func (o *OSImpl) Remove(path string) error {
	panic("unimplemented")
}

// RemoveAll implements [container.OSInterface].
func (o *OSImpl) RemoveAll(path string) error {
	panic("unimplemented")
}

// Rename implements [container.OSInterface].
func (o *OSImpl) Rename(oldpath string, newpath string) error {
	panic("unimplemented")
}

// Stat implements [container.OSInterface].
func (o *OSImpl) Stat(path string) (os.FileInfo, error) {
	panic("unimplemented")
}

// Symlink implements [container.OSInterface].
func (o *OSImpl) Symlink(oldname string, newname string) error {
	panic("unimplemented")
}

var _ container.OSInterface = &OSImpl{}
