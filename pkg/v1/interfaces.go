package v1

import (
	"context"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/cnuss/nanokube/pkg/tunnel"
	"github.com/emicklei/go-restful/v3"
	"go.etcd.io/etcd/client/v3/kubernetes"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/informers"
	client "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/tools/record"
	internalapi "k8s.io/cri-api/pkg/apis"
	criv1 "k8s.io/cri-api/pkg/apis/runtime/v1"
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

type Nanokube interface {
	context.Context
	record.EventRecorder
	record.EventRecorderLogger

	Options() Options

	WithCancel() (context.Context, context.CancelFunc)

	Cancel(reason Error)
	CancelErr(reason error)
	Errors() []error

	Backend(name BackendName) Backend
	DefaultBackend() Backend

	Host() Host
	Client() Client
	Broadcaster() record.EventBroadcaster
	Tunnel() tunnel.Tunnel
	StaticPods() []*corev1.Pod

	Services(baseURL *url.URL) []*restful.WebService

	CertFilePath() string
	KeyFilePath() string
	RootCaFilePath() string
	WithLoopback(loopback *rest.Config) Nanokube
	KubeconfigPath() string
	NodeReady() <-chan struct{}
	NodeRef() *corev1.ObjectReference
	MachineID() string

	Storage() Storage
	SetSharedInformerFactory(factory informers.SharedInformerFactory) informers.SharedInformerFactory
	SharedInformerFactory() informers.SharedInformerFactory
}

type Ready interface {
	Ready() <-chan struct{}
}

// TODO(deprecate)
type Options interface {
	Name() string
	Verbosity() int
	DataDir() DataDir

	DataDirAt(name DataDir) string
	FilePathAt(file FileName) string
	InDataDir(path string) bool

	Args() []string
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
	internalapi.ImageManagerService
	internalapi.RuntimeService
	NetworkService
	VolumeService

	Context() context.Context
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
	Sink() record.EventSink

	WithHeartbeat(interval time.Duration) Client
	WithQps(qps float32) Client
	WithTimeout(timeout time.Duration) Client
	WithTunnel(tunnel tunnel.Tunnel, local bool) Client

	Kubeconfig(name string) *clientcmdapi.Config
	WriteKubeconfig(path string) error
}

type ApiServer interface {
	Ready

	Context() context.Context
	Client() Client
	Done() <-chan struct{}
	CACerts() []*x509.Certificate

	Handler() http.Handler
	Server() *server.GenericAPIServer
	Destroy() error
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

	Nanokube() Nanokube
	Name() BackendName

	Driver() Driver
	Network() Network
	Manager() Manager
	Services() []*restful.WebService

	WithBaseURL(baseURL *url.URL) Backend

	Reconcile(obj interface{}, deleted bool)
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

	VolumePlugins() []volume.VolumePlugin
}

type Storage interface {
	Cancel(reason error)

	WithTransportConfig(cfg storagebackend.TransportConfig) Storage
	Client() *kubernetes.Client

	SetConfig(config *server.Config) *server.Config

	Port() int
	Endpoints() []string
	ClientURLs() []url.URL
}

// type Storage interface {
// 	generic.RESTOptionsGetter

// 	SetConfig(config *server.Config) *server.Config
// 	WithResource(inner kubestorage.Interface, resource schema.GroupResource) StorageClient

// 	Servers() []string
// 	Shutdown()
// }

// type StorageClient interface {
// 	kubestorage.Interface
// }
