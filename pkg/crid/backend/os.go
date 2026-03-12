package backend

import (
	"os"
	"path/filepath"
	"strings"
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

func (o *OSImpl) remap(path string) string {
	if filepath.IsAbs(path) && !strings.HasPrefix(path, o.backend.DataDir()) && !strings.HasPrefix(path, filepath.Dir(o.backend.DataDir())) {
		return filepath.Join(o.backend.DataDir(), path)
	}
	return path
}

func (o *OSImpl) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(o.remap(path), perm)
}

func (o *OSImpl) Symlink(oldname string, newname string) error {
	return os.Symlink(o.remap(oldname), o.remap(newname))
}

func (o *OSImpl) Stat(path string) (os.FileInfo, error) {
	return os.Stat(o.remap(path))
}

func (o *OSImpl) Remove(path string) error {
	return os.Remove(o.remap(path))
}

func (o *OSImpl) RemoveAll(path string) error {
	return os.RemoveAll(o.remap(path))
}

func (o *OSImpl) Create(path string) (*os.File, error) {
	return os.Create(o.remap(path))
}

func (o *OSImpl) Chmod(path string, perm os.FileMode) error {
	return os.Chmod(o.remap(path), perm)
}

func (o *OSImpl) Hostname() (string, error) {
	host, err := o.backend.HostInfo()
	if err != nil {
		return "", err
	}
	return host.Hostname, nil
}

func (o *OSImpl) Chtimes(path string, atime time.Time, mtime time.Time) error {
	return os.Chtimes(o.remap(path), atime, mtime)
}

func (o *OSImpl) Pipe() (r *os.File, w *os.File, err error) {
	return os.Pipe()
}

func (o *OSImpl) ReadDir(dirname string) ([]os.DirEntry, error) {
	return os.ReadDir(o.remap(dirname))
}

func (o *OSImpl) Glob(pattern string) ([]string, error) {
	return filepath.Glob(o.remap(pattern))
}

func (o *OSImpl) Open(name string) (*os.File, error) {
	return os.Open(o.remap(name))
}

func (o *OSImpl) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(o.remap(name), flag, perm)
}

func (o *OSImpl) Rename(oldpath, newpath string) error {
	return os.Rename(o.remap(oldpath), o.remap(newpath))
}

var _ container.OSInterface = &OSImpl{}
