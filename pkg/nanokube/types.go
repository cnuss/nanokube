package nanokube

import (
	"context"
	"io"
	"net"
	"path/filepath"

	cri "k8s.io/cri-api/pkg/apis"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"
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
	CertFile          FileName = FileName(filepath.Join(string(DataDirCerts), "apiserver.crt"))
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
	LogStream(containerID string, status *runtime.ContainerStatus) LogStream
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

type Tunnel interface {
	Context() context.Context
	LocalPort() int32
	LocalHost() net.IP
	FQDN() string
	Hostname() string
	Domain() string
	Ready() <-chan struct{}
}

// LogStream represents a container's log pump. Runtimes construct one via
// NewLogStream, providing a LogSource that knows how to open stdout/stderr
// readers. The impl owns pipes, CRI-format formatting, and log-file I/O.
type LogStream interface {
	Start()
	Stop()
	Destroy()
	Stdout() io.ReadCloser
	Stderr() io.ReadCloser
}

// LogSource opens the runtime's raw stdout/stderr streams for a container.
// close is called to release the underlying resource (e.g. Docker ContainerLogs reader).
type LogSource func(ctx context.Context) (stdout io.Reader, stderr io.Reader, close func(), err error)
