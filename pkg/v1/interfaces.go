package v1

import (
	"context"
	"io"
	"net"
	"net/url"
	"time"

	"github.com/emicklei/go-restful/v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	storage "k8s.io/apiserver/pkg/server/storage"
	client "k8s.io/client-go/kubernetes"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/tools/record"
	cri "k8s.io/cri-api/pkg/apis"
	criv1 "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/kubernetes/cmd/kube-apiserver/app"
	apiserveroptions "k8s.io/kubernetes/cmd/kube-apiserver/app/options"
	kubeletoptions "k8s.io/kubernetes/cmd/kubelet/app/options"
	"k8s.io/kubernetes/pkg/kubelet"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/cadvisor"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	"k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/kubernetes/pkg/volume/util/subpath"
	mount "k8s.io/mount-utils"
	"k8s.io/utils/exec"
)

type Ready interface {
	Ready() <-chan struct{}
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
	Ready

	Context() context.Context
	LocalPort() int32
	LocalIP() net.IP
	LocalHostname() string
	LocalDomain() string
	LocalFQDN() string
	Domain() string
	FQDN() string
	Hostname() string
	URL() *url.URL
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

type AllocatedNetwork interface {
	ID() string
	Type() NetworkType
	Gateway() net.IP
	Network() net.IPNet

	Deallocate() error

	WithSandboxUID(sandboxUID *types.UID) AllocatedNetwork
	SandboxUID() *types.UID
}

type NetworkService interface {
	Context() context.Context
	GetNetwork(ctx context.Context, id string) (*NetworkType, *net.IP, *net.IPNet, error)
	CreateNetwork(ctx context.Context, networkType NetworkType, net *net.IPNet, gateway *net.IP) (string, error)
	RemoveNetwork(ctx context.Context, id string) error
}

type Network interface {
	Networks() []AllocatedNetwork
	FromID(ctx context.Context, id string) (AllocatedNetwork, error)
	FromIP(ctx context.Context, ip net.IP) (AllocatedNetwork, error)
	FromUID(ctx context.Context, sandboxUID types.UID) (AllocatedNetwork, error)
	FromStatus(ctx context.Context, status *criv1.PodSandboxStatus) (AllocatedNetwork, error)
	FromConfig(ctx context.Context, config *criv1.PodSandboxConfig) (AllocatedNetwork, error)
	Default(ctx context.Context) AllocatedNetwork
}

type Driver interface {
	cri.ImageManagerService
	cri.RuntimeService
	NetworkService

	Context() context.Context
	Options() Options
	Name() string

	ExecOnHost(ctx context.Context, image string, cmd []string, mounts []Path) (string, error)
	ExecOnNetwork(ctx context.Context, network AllocatedNetwork, image string, cmd []string, portMap []PortMap) (string, error)

	CgroupRoot() string
	LogStream(containerID string, status *criv1.ContainerStatus) LogStream
	Service() *restful.WebService

	WithBaseURL(baseURL *url.URL) Driver
	BaseURL() *url.URL

	WithNetwork(network Network) Driver
	Network() Network
}

type Client interface {
	client.Interface
	Clientset() *client.Clientset
	WithHeartbeat(interval time.Duration) Client
	WithQps(qps float32) Client
	WithTimeout(timeout time.Duration) Client
	WithTunnel(tunnel Tunnel, local bool) Client

	Kubeconfig(name string) *clientcmdapi.Config
	WriteKubeconfig(path string) error
}

type ApiServer interface {
	Ready

	Client(ctx context.Context) Client
	Config() *app.Config
	Tunnel() Tunnel
}

type Kubelet interface {
	Ready

	Run(ctx context.Context)
	Flags() *kubeletoptions.KubeletFlags
	Configuration() *kubeletconfig.KubeletConfiguration
	Dependencies() *kubelet.Dependencies
	Tunnel() Tunnel
}

type StorageFactory interface {
	Ready

	storage.StorageFactory

	ServerList() []string
	Port() int
	WithDefault(factory *storage.DefaultStorageFactory) StorageFactory
	Default() *storage.DefaultStorageFactory
}

type Manager interface {
	Ready

	cm.ContainerManager
	lifecycle.PodAdmitHandler
	cm.InternalContainerLifecycle
	cm.PodContainerManager
}

type Backend interface {
	Ready

	cadvisor.Interface
	container.OSInterface
	mount.Interface
	subpath.Interface
	hostutil.HostUtils

	Context() context.Context
	Name() BackendName

	Driver() Driver
	Network() Network
	Manager() Manager
	Services() []*restful.WebService

	WithBaseURL(baseURL *url.URL) Backend
}

type Kube interface {
	record.EventRecorder
	Config() Config

	ApiServerOptions() *apiserveroptions.CompletedOptions

	Client() Client

	WithKubelet(kubelet Kubelet) Kube
	Kubelet() Kubelet

	WithApiServer(apiserver ApiServer) Kube
	ApiServer() ApiServer

	WithStorageFactory(storagefactory StorageFactory) Kube
	StorageFactory() StorageFactory

	NodeReady() chan *corev1.ObjectReference
}

type Config interface {
	Context() context.Context
	Cancel(reason Error)
	Done() <-chan struct{}
	OnShutdown(fns ...func(ctx context.Context)) Config

	Options() Options
	Version() string

	Tunnel(name ServiceName) Tunnel

	WithKubelet(kubelet Kubelet) Config
	WithApiServer(apiserver ApiServer) Config
	WithStorageFactory(storagefactory StorageFactory) Config
	WithBackend(name BackendName, backend Backend) Config

	Kube() Kube
	Kubelet() Kubelet
	ApiServer() ApiServer
	StorageFactory() StorageFactory

	Backends() map[BackendName]Backend
	Services(baseURL *url.URL) []*restful.WebService
	DefaultBackend() Backend
	Backend(name BackendName) Backend
}

type PortMap interface {
	Local() int32
	Remote() int32
	Protocol() Protocol
}

type Error interface {
	error
	exec.ExitError
	WithCommand(cmd []string) Error
	WithCode(code int) Error
	WithError(err error) Error
	WithErrors(errs ...error) Error
}
