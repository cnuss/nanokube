package nanokube

import (
	"context"
	"path/filepath"

	cri "k8s.io/cri-api/pkg/apis"
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
	DataDirKube       DataDir  = ".kube"
	DataDirStaticPods DataDir  = DataDir(filepath.Join(string(DataDirKubelet), "static-pods"))
	KeyFile           FileName = FileName(filepath.Join(string(DataDirCerts), "apiserver.key"))
	KubeconfigFile    FileName = FileName(filepath.Join(string(DataDirKube), "config"))

	NetworkHost       NetworkType = "host"
	NetworkBridge     NetworkType = "bridge"
	NetworkSubnetSize             = 28
)

type Driver interface {
	cri.ImageManagerService
	cri.RuntimeService
	NetworkService

	Context() context.Context
	Options() Options
	Name() string

	CgroupRoot() string
	ExecHost(image string, cmd []string, mounts []Path) (string, error)
	LogStream(containerID string) *LogStream
}

type Options interface {
	Name() string
	Verbosity() int
	Clean() bool
	DataDir() DataDir
	Standalone() bool

	DataDirAt(name DataDir) string
	InDataDir(path string) bool
}
