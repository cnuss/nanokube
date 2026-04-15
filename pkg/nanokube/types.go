package nanokube

import (
	"path/filepath"
)

type (
	DataDir     string
	FileName    string
	Path        string
	NetworkType string
)

var (
	DataDirLock       DataDir  = "lock"
	DataDirKubelet    DataDir  = "kubelet"
	DataDirCerts      DataDir  = "certs"
	DataDirLogs       DataDir  = "logs"
	DataDirEtcd       DataDir  = "etcd"
	DataDirStaticPods DataDir  = DataDir(filepath.Join(string(DataDirKubelet), "static-pods"))
	CertFile          FileName = "apiserver.crt"
	KeyFile           FileName = "apiserver.key"

	NetworkHost       NetworkType = "host"
	NetworkBridge     NetworkType = "bridge"
	NetworkSubnetSize             = 28
)

type Options interface {
	Name() string
	Verbosity() int
	Clean() bool
	DataDir() string
	Standalone() bool

	DataDirAt(name DataDir) string
	FilePathAt(dir DataDir, path FileName) string

	InDataDir(path string) bool
}
