package v1

import (
	"context"
	"crypto/x509"
	"io"
	"net"
	"net/url"
	"time"

	"github.com/emicklei/go-restful/v3"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	storage "k8s.io/apiserver/pkg/server/storage"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/informers"
	client "k8s.io/client-go/kubernetes"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/tools/record"
	cri "k8s.io/cri-api/pkg/apis"
	criv1 "k8s.io/cri-api/pkg/apis/runtime/v1"
	apiserveroptions "k8s.io/kubernetes/cmd/kube-apiserver/app/options"
	kubeletoptions "k8s.io/kubernetes/cmd/kubelet/app/options"
	"k8s.io/kubernetes/pkg/kubelet"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/cadvisor"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	"k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/kubernetes/pkg/volume/util/subpath"
	"k8s.io/mount-utils"
	"k8s.io/utils/exec"
)

type Ready interface {
	Ready() <-chan struct{}
}

type Options interface {
	Name() string
	Verbosity() int
	DataDir() DataDir

	DataDirAt(name DataDir) string
	FilePathAt(file FileName) string
	InDataDir(path string) bool

	Args() []string
	RunFlags(cmd *cobra.Command) Options
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
	CACerts() []*x509.Certificate
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

type VolumeService interface {
	ClaimVolume(backend Backend, client Client, pvc *corev1.PersistentVolumeClaim) *corev1ac.PersistentVolumeClaimApplyConfiguration
	CreateVolume(pv *corev1.LocalVolumeSource) error
	DeleteVolume(pv *corev1.LocalVolumeSource) error
	ReleaseVolume(backend Backend, client Client, pvc *corev1.PersistentVolumeClaim) error
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
	VolumeService

	Context() context.Context
	Kubelet() Kubelet
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

	Context() context.Context
	Client() Client
	Done() <-chan struct{}
	CACerts() []*x509.Certificate
}

type Storage interface {
	storage.StorageFactory
	Ready

	Context() context.Context
	Done() <-chan struct{}
	ServerList() []string
	Port() int
	WithFactory(factory *storage.DefaultStorageFactory) Storage
	Factory() *storage.DefaultStorageFactory
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

	Context() context.Context
	Kubelet() Kubelet
	Name() BackendName

	Driver() Driver
	Network() Network
	Manager() Manager
	Services() []*restful.WebService

	WithBaseURL(baseURL *url.URL) Backend

	Reconcile(obj interface{}, deleted bool)
}

type Kube interface {
	record.EventRecorder
	Kubelet() Kubelet

	ApiServerOptions() *apiserveroptions.CompletedOptions
	Args(service ServiceName, mountPath string) []string

	Environ() []string
	Broadcaster() record.EventBroadcaster
	InformerFactory() informers.SharedInformerFactory

	KubeletFlags() *kubeletoptions.KubeletFlags
	KubeletConfiguration() *kubeletconfig.KubeletConfiguration
	KubeletDependencies() *kubelet.Dependencies
	KubeletHostname() string // TODO: Remove

	NodeReady() chan struct{}
}

type Kubelet interface {
	Context() context.Context
	Options() Options
	Version() string

	Cancel(reason Error)
	Detach()
	OnCancel(fns ...func(ctx context.Context)) Kubelet
	OnReady(service ServiceName, fns ...func(ctx context.Context)) Kubelet

	Canceled() <-chan struct{}
	Done() <-chan int

	WithApiServer(apiserver ApiServer) Kubelet
	ApiServer() ApiServer

	WithStorage(storage Storage) Kubelet
	Storage() Storage

	WithBackend(name BackendName, backend Backend) Kubelet
	Backends() map[BackendName]Backend
	Backend(name BackendName) Backend
	DefaultBackend() Backend

	Host() Host
	Kube() Kube
	Client() Client
	Tunnel(name TunnelName) Tunnel
	StaticPods() []*corev1.Pod

	Services(baseURL *url.URL) []*restful.WebService

	Run() Kubelet
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

type Host interface {
	hostutil.HostUtils
	container.OSInterface
	volume.VolumePlugin
	subpath.Interface
	mount.Interface

	Kubelet() Kubelet
	VolumePlugins() []volume.VolumePlugin
}
