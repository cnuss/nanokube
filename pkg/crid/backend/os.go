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
	panic("Chmod: unimplemented")
}

// Chtimes implements [container.OSInterface].
func (o *OSImpl) Chtimes(path string, atime time.Time, mtime time.Time) error {
	panic("Chtimes: unimplemented")
}

// Create implements [container.OSInterface].
func (o *OSImpl) Create(path string) (*os.File, error) {
	panic("Create: unimplemented")
}

// Glob implements [container.OSInterface].
func (o *OSImpl) Glob(pattern string) ([]string, error) {
	panic("Glob: unimplemented")
}

// Hostname implements [container.OSInterface].
func (o *OSImpl) Hostname() (name string, err error) {
	panic("Hostname: unimplemented")
}

// MkdirAll implements [container.OSInterface].
func (o *OSImpl) MkdirAll(path string, perm os.FileMode) error {
	panic("MkdirAll: unimplemented")
}

// Open implements [container.OSInterface].
func (o *OSImpl) Open(name string) (*os.File, error) {
	panic("Open: unimplemented")
}

// OpenFile implements [container.OSInterface].
func (o *OSImpl) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	panic("OpenFile: unimplemented")
}

// Pipe implements [container.OSInterface].
func (o *OSImpl) Pipe() (r *os.File, w *os.File, err error) {
	panic("Pipe: unimplemented")
}

// ReadDir implements [container.OSInterface].
func (o *OSImpl) ReadDir(dirname string) ([]os.DirEntry, error) {
	panic("ReadDir: unimplemented")
}

// Remove implements [container.OSInterface].
func (o *OSImpl) Remove(path string) error {
	panic("Remove: unimplemented")
}

// RemoveAll implements [container.OSInterface].
func (o *OSImpl) RemoveAll(path string) error {
	panic("RemoveAll: unimplemented")
}

// Rename implements [container.OSInterface].
func (o *OSImpl) Rename(oldpath string, newpath string) error {
	panic("Rename: unimplemented")
}

// Stat implements [container.OSInterface].
func (o *OSImpl) Stat(path string) (os.FileInfo, error) {
	panic("Stat: unimplemented")
}

// Symlink implements [container.OSInterface].
func (o *OSImpl) Symlink(oldname string, newname string) error {
	panic("Symlink: unimplemented")
}

var _ container.OSInterface = &OSImpl{}
