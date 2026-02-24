package kubernetes

import (
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ScopedOS implements kubecontainer.OSInterface and redirects absolute paths
// outside the data directory into it. This prevents the kubelet from writing
// to system paths like /var/log/containers when running unprivileged.
type ScopedOS struct {
	DataDir string
}

func (s *ScopedOS) remap(path string) string {
	if filepath.IsAbs(path) && !strings.HasPrefix(path, s.DataDir) {
		return filepath.Join(s.DataDir, path)
	}
	return path
}

func (s *ScopedOS) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(s.remap(path), perm)
}

func (s *ScopedOS) Symlink(oldname string, newname string) error {
	return os.Symlink(s.remap(oldname), s.remap(newname))
}

func (s *ScopedOS) Stat(path string) (os.FileInfo, error) {
	return os.Stat(s.remap(path))
}

func (s *ScopedOS) Remove(path string) error {
	return os.Remove(s.remap(path))
}

func (s *ScopedOS) RemoveAll(path string) error {
	return os.RemoveAll(s.remap(path))
}

func (s *ScopedOS) Create(path string) (*os.File, error) {
	return os.Create(s.remap(path))
}

func (s *ScopedOS) Chmod(path string, perm os.FileMode) error {
	return os.Chmod(s.remap(path), perm)
}

func (s *ScopedOS) Hostname() (string, error) {
	return os.Hostname()
}

func (s *ScopedOS) Chtimes(path string, atime time.Time, mtime time.Time) error {
	return os.Chtimes(s.remap(path), atime, mtime)
}

func (s *ScopedOS) Pipe() (r *os.File, w *os.File, err error) {
	return os.Pipe()
}

func (s *ScopedOS) ReadDir(dirname string) ([]os.DirEntry, error) {
	return os.ReadDir(s.remap(dirname))
}

func (s *ScopedOS) Glob(pattern string) ([]string, error) {
	return filepath.Glob(s.remap(pattern))
}

func (s *ScopedOS) Open(name string) (*os.File, error) {
	return os.Open(s.remap(name))
}

func (s *ScopedOS) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(s.remap(name), flag, perm)
}

func (s *ScopedOS) Rename(oldpath, newpath string) error {
	return os.Rename(s.remap(oldpath), s.remap(newpath))
}
