package v1

import (
	"context"
	"io"
	"net"
	"time"

	corev1 "k8s.io/api/core/v1"
	storage "k8s.io/apiserver/pkg/server/storage"
	client "k8s.io/client-go/kubernetes"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/tools/record"
	cri "k8s.io/cri-api/pkg/apis"
	criv1 "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/kubelet/pkg/cri/streaming"
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
	LocalHost() net.IP
	FQDN() string
	Hostname() string
	Domain() string
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
	Name() string
	Type() NetworkType
	Gateway() net.IP
	Network() net.IPNet

	Deallocate(ctx context.Context) error
}

type NetworkService interface {
	Context() context.Context
	DefaultNetwork(ctx context.Context) string
	GetNetwork(ctx context.Context, name string) (*string, *NetworkType, *net.IP, *net.IPNet, error)
	CreateNetwork(ctx context.Context, name string, networkType NetworkType, net *net.IPNet, gateway *net.IP) error
	RemoveNetwork(ctx context.Context, name string) error
}

type Network interface {
	Allocate(config *criv1.PodSandboxConfig) (AllocatedNetwork, error)
	Default() AllocatedNetwork
}

type Driver interface {
	cri.ImageManagerService
	cri.RuntimeService
	NetworkService

	Context() context.Context
	Options() Options
	Name() string

	CgroupRoot() string
	ExecHost(image string, cmd []string, mounts []Path) (string, error)
	LogStream(containerID string, status *criv1.ContainerStatus) LogStream
	StreamRuntime() streaming.Runtime
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
}

type Kubelet interface {
	Ready

	Run(ctx context.Context)
	Flags() *kubeletoptions.KubeletFlags
	Configuration() *kubeletconfig.KubeletConfiguration
	Deps() *kubelet.Dependencies
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

	Driver() Driver
	Network() Network
	StreamServer() streaming.Server
	Manager() Manager
}

type Crid interface {
	Backends() map[string]Backend
	DefaultBackend() Backend
}

type Kube interface {
	record.EventRecorder

	ApiServerOptions() *apiserveroptions.CompletedOptions
	ApiServerTunnel() Tunnel
	ApiServerFQDN() string

	KubeletFlags() *kubeletoptions.KubeletFlags
	KubeletConfiguration() *kubeletconfig.KubeletConfiguration
	KubeletTunnel() Tunnel
	KubeletFQDN() string

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
	Cancel(reason error)

	Options() Options
	Version() string

	Kube() Kube
	Crid() Crid

	NewTunnel() Tunnel

	WithKubelet(kubelet Kubelet) Config
	WithApiServer(apiserver ApiServer) Config
	WithStorageFactory(storagefactory StorageFactory) Config
}
