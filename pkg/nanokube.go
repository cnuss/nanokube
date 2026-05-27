package pkg

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"time"
	_ "unsafe"

	"github.com/cnuss/nanokube/pkg/nanokube"
	"github.com/cnuss/nanokube/pkg/storage"
	v1 "github.com/cnuss/nanokube/pkg/v1"
	"github.com/emicklei/go-restful/v3"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	apifeatures "k8s.io/apiserver/pkg/features"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/cert"
	"k8s.io/klog/v2"
	kubeletoptions "k8s.io/kubernetes/cmd/kubelet/app/options"
	"k8s.io/kubernetes/pkg/features"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
)

func NewNanokube(ctx context.Context) v1.Nanokube {
	ctx, cancel := context.WithCancelCause(ctx)

	nano := &nanokubeImpl{
		ctx:                           ctx,
		cancel:                        cancel,
		options:                       nanokube.NewOptions(),
		exitCode:                      make(chan int, 1),
		storageProvided:               make(chan struct{}),
		sharedInformerFactoryProvided: make(chan struct{}),
		loopbackProvided:              make(chan struct{}),
		nodeReady:                     make(chan struct{}),
	}

	// go func() {
	// 	<-nano.ctx.Done()
	// 	cause := context.Cause(nano.ctx)
	// 	code := 0
	// 	var err v1.Error
	// 	if errors.As(cause, &err) {
	// 		fmt.Fprintf(os.Stderr, "nanokube exiting: %v\n", err)
	// 		code = err.ExitStatus()
	// 	}
	// 	select {
	// 	case <-nano.apiserverProvided:
	// 		<-nano.apiserver.Done()
	// 	default:
	// 	}
	// 	select {
	// 	case <-nano.storageProvided:
	// 		<-nano.storage.Done()
	// 	default:
	// 	}
	// 	nano.exitCodeOnce.Do(func() {
	// 		nano.exitCode <- code
	// 	})
	// }()

	return nano
}

type nanokubeImpl struct {
	ctx     context.Context
	cancel  context.CancelCauseFunc
	cmd     *cobra.Command
	options v1.Options

	version     string
	versionOnce sync.Once

	cancelErrs   []error
	cancelErrsMu sync.Mutex

	exitCode     chan int
	exitCodeOnce sync.Once

	host     v1.Host
	hostOnce sync.Once

	storage         v1.Storage
	storageOnce     sync.Once
	storageProvided chan struct{}

	backends     sync.Map
	backendsOnce sync.Once

	services     []*restful.WebService
	servicesOnce sync.Once

	staticPods     []*corev1.Pod
	staticPodsOnce sync.Once

	dirs    sync.Map
	files   sync.Map
	tunnels sync.Map

	pidfileWritten bool

	kubeletFlags     *kubeletoptions.KubeletFlags
	kubeletFlagsOnce sync.Once

	kubeletConfiguration     *kubeletconfig.KubeletConfiguration
	kubeletConfigurationOnce sync.Once

	sharedInformerFactory         informers.SharedInformerFactory
	sharedInformerFactoryOnce     sync.Once
	sharedInformerFactoryProvided chan struct{}

	loopback         *rest.Config
	loopbackOnce     sync.Once
	loopbackProvided chan struct{}

	client     v1.Client
	clientOnce sync.Once

	broadcaster     record.EventBroadcaster
	broadcasterOnce sync.Once

	eventsOnce sync.Once

	nodeReady     chan struct{}
	nodeRef       *corev1.ObjectReference
	nodeReadyOnce sync.Once

	proxiedRecorder     record.EventRecorder
	proxiedRecorderOnce sync.Once

	certKeyOnce sync.Once

	rootCaOnce sync.Once
}

var _ v1.Nanokube = &nanokubeImpl{}

func (n *nanokubeImpl) Deadline() (time.Time, bool) {
	return n.ctx.Deadline()
}

func (n *nanokubeImpl) Done() <-chan struct{} {
	return n.ctx.Done()
}

func (n *nanokubeImpl) Err() error {
	return n.ctx.Err()
}

func (n *nanokubeImpl) Value(key any) any {
	return n.ctx.Value(key)
}

func (n *nanokubeImpl) Options() v1.Options {
	return n.options
}

func (n *nanokubeImpl) WithCancel() (context.Context, context.CancelFunc) {
	return context.WithCancel(n)
}

//go:linkname nodeReadyGracePeriod k8s.io/kubernetes/pkg/kubelet.nodeReadyGracePeriod
var nodeReadyGracePeriod time.Duration

func init() {
	nodeReadyGracePeriod = 30 * time.Second // TODO(remove): temporary override for debugging
}

var FeatureGates = map[string]bool{
	string(features.KubeletInUserNamespace): true,
	// Cloudflare-specific features:
	// - SSE not supported, so disable features that rely on SSE
	string(apifeatures.WatchList):                          false,
	string(features.TranslateStreamCloseWebsocketRequests): false,
	string(features.PortForwardWebsockets):                 false,
	// kubelet wraps the kubelet->streaming hop with a V5 WebSocket -> SPDY
	// translator; cloudflared quick tunnels strip the SPDY upgrade headers
	// and the second hop dies. Keep V5 end-to-end (server side accepts V5
	// via the cri-streaming patch).
	string(features.ExtendWebSocketsToKubelet): false,
}

func (k *nanokubeImpl) Storage() v1.Storage {
	k.storageOnce.Do(func() {
		k.storage = storage.NewStorage(k)
		close(k.storageProvided)
	})
	return k.storage
}

func (k *nanokubeImpl) SetSharedInformerFactory(factory informers.SharedInformerFactory) informers.SharedInformerFactory {
	k.sharedInformerFactoryOnce.Do(func() {
		k.sharedInformerFactory = factory
		close(k.sharedInformerFactoryProvided)
	})
	return k.sharedInformerFactory
}

func (k *nanokubeImpl) SharedInformerFactory() informers.SharedInformerFactory {
	<-nanokube.Await(k.ctx, k.sharedInformerFactoryProvided)
	return k.sharedInformerFactory
}

func (k *nanokubeImpl) Broadcaster() record.EventBroadcaster {
	k.broadcasterOnce.Do(func() {
		k.broadcaster = record.NewBroadcaster(record.WithContext(k))
	})
	return k.broadcaster
}

func (k *nanokubeImpl) KubeletHostname() string {
	tunnel := k.Tunnel()
	return tunnel.Hostname()
}

func (k *nanokubeImpl) KubeletFlags() *kubeletoptions.KubeletFlags {
	k.kubeletFlagsOnce.Do(func() {
		tunnel := k.Tunnel()
		k.kubeletFlags = kubeletoptions.NewKubeletFlags()
		k.kubeletFlags.CloudProvider = "external"
		k.kubeletFlags.HostnameOverride = k.KubeletHostname()
		k.kubeletFlags.NodeLabels = make(map[string]string) // TODO(incomplete): add labels
		k.kubeletFlags.NodeIP = tunnel.LocalIP().String()
		k.kubeletFlags.RootDirectory = k.Options().DataDirAt(v1.DataDirKubelet)
	})
	return k.kubeletFlags
}

func (k *nanokubeImpl) Environ() []string {
	return []string{
		"KUBERNETES_SERVICE_HOST=" + k.Tunnel().FQDN(),
		"KUBERNETES_SERVICE_PORT=443",
	}
}

func (k *nanokubeImpl) recorder() record.EventRecorder {
	k.proxiedRecorderOnce.Do(func() {
		tunnel := k.Tunnel()
		k.proxiedRecorder = k.Broadcaster().NewRecorder(scheme.Scheme, corev1.EventSource{Component: k.Options().Name(), Host: tunnel.FQDN()})
	})
	return k.proxiedRecorder
}

func (k *nanokubeImpl) AnnotatedEventf(object k8sruntime.Object, annotations map[string]string, eventtype string, reason string, messageFmt string, args ...interface{}) {
	k.Logf(object, eventtype, reason, messageFmt, args...)
	k.recorder().AnnotatedEventf(object, annotations, eventtype, reason, messageFmt, args...)
}

func (k *nanokubeImpl) Event(object k8sruntime.Object, eventtype string, reason string, message string) {
	k.Logf(object, eventtype, reason, "%s", message)
	k.recorder().Event(object, eventtype, reason, message)
}

func (k *nanokubeImpl) WithLogger(klog.Logger) record.EventRecorderLogger {
	return k
}

func (k *nanokubeImpl) Eventf(object k8sruntime.Object, eventtype string, reason string, messageFmt string, args ...interface{}) {
	k.Logf(object, eventtype, reason, messageFmt, args...)
	k.recorder().Eventf(object, eventtype, reason, messageFmt, args...)

	// 14:01:42 INF Eventf group="" version=v1 kind=Node type=Normal reason=NodeReady message="Node depends-location-assessments-silence.trycloudflare.com status is now: NodeReady"
	if object.GetObjectKind().GroupVersionKind().Group == "" &&
		object.GetObjectKind().GroupVersionKind().Version == "v1" &&
		object.GetObjectKind().GroupVersionKind().Kind == "Node" &&
		eventtype == corev1.EventTypeNormal &&
		reason == "NodeReady" {
		if ref, ok := object.(*corev1.ObjectReference); ok {
			k.nodeReadyOnce.Do(func() {
				k.nodeRef = ref
				close(k.nodeReady)
			})
		}
	}
}

func (k *nanokubeImpl) Logf(object k8sruntime.Object, eventtype string, reason string, messageFmt string, args ...interface{}) {
	kv := []interface{}{"gvk", object.GetObjectKind().GroupVersionKind().String(), "reason", reason}
	if ref, ok := object.(*corev1.ObjectReference); ok {
		kv = append(kv, "namespace", ref.Namespace, "name", ref.Name)
	}
	if eventtype == corev1.EventTypeWarning && k.Options().Verbosity() >= 2 {
		kv = append(kv, "stack", string(debug.Stack()))
	}
	msg := fmt.Sprintf(messageFmt, args...)
	switch eventtype {
	case corev1.EventTypeNormal, corev1.EventTypeWarning:
		klog.InfoS(msg, kv...)
	default:
		klog.V(2).InfoS(msg, kv...)
	}
}

func (k *nanokubeImpl) NodeReady() <-chan struct{} {
	return k.nodeReady
}

func (k *nanokubeImpl) NodeRef() *corev1.ObjectReference {
	<-nanokube.Await(k.ctx, k.nodeReady)
	return k.nodeRef
}

func (k *nanokubeImpl) CertFilePath() string {
	certPath, _ := k.ensureCertKey()
	return certPath
}

func (k *nanokubeImpl) KeyFilePath() string {
	_, keyPath := k.ensureCertKey()
	return keyPath
}

// RootCaFilePath returns the path to a CA bundle containing the self-signed
// apiserver cert plus the tunnel's CA roots (Mozilla bundle + Cloudflare
// root). The apiserver cert is its own CA (self-signed); the tunnel roots
// are needed so clients also trust the cloudflared edge cert when verifying
// the chain over the tunnel.
func (k *nanokubeImpl) RootCaFilePath() string {
	path := k.Options().FilePathAt(v1.CAFile)
	k.rootCaOnce.Do(func() {
		var buf bytes.Buffer

		certPath, _ := k.ensureCertKey()
		certPEM, err := os.ReadFile(certPath)
		if err != nil {
			k.Cancel(nanokube.NewError(fmt.Errorf("read apiserver cert %s: %w", certPath, err)))
			return
		}
		buf.Write(certPEM)

		for _, c := range k.Tunnel().CACerts() {
			if err := pem.Encode(&buf, &pem.Block{Type: "CERTIFICATE", Bytes: c.Raw}); err != nil {
				k.Cancel(nanokube.NewError(fmt.Errorf("PEM-encode tunnel CA cert: %w", err)))
				return
			}
		}

		if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
			k.Cancel(nanokube.NewError(fmt.Errorf("write CA bundle %s: %w", path, err)))
			return
		}
	})
	return path
}

func (k *nanokubeImpl) WithLoopback(loopback *rest.Config) v1.Nanokube {
	k.loopbackOnce.Do(func() {
		k.loopback = loopback
		close(k.loopbackProvided)
	})
	return k
}

// KubeconfigPath materializes a kubeconfig file pointing at the apiserver via
// the loopback rest.Config registered with WithLoopback (typically inside an
// apiserver PostStartHook from ctx.LoopbackClientConfig) and returns its disk
// path. Blocks until WithLoopback has been called.
func (k *nanokubeImpl) KubeconfigPath() string {
	<-nanokube.Await(k.ctx, k.loopbackProvided)
	loopback := k.loopback
	ctxName := loopback.Host
	cfg := clientcmdapi.NewConfig()
	cfg.Clusters[ctxName] = &clientcmdapi.Cluster{
		Server:                   loopback.Host,
		CertificateAuthorityData: loopback.CAData,
		InsecureSkipTLSVerify:    loopback.Insecure,
		TLSServerName:            loopback.TLSClientConfig.ServerName,
	}
	cfg.AuthInfos[ctxName] = &clientcmdapi.AuthInfo{
		ClientCertificateData: loopback.CertData,
		ClientKeyData:         loopback.KeyData,
		Token:                 loopback.BearerToken,
	}
	cfg.Contexts[ctxName] = &clientcmdapi.Context{
		Cluster:  ctxName,
		AuthInfo: ctxName,
	}
	cfg.CurrentContext = ctxName

	path := k.Options().FilePathAt(v1.KubeconfigFile)
	if err := clientcmd.WriteToFile(*cfg, path); err != nil {
		k.Cancel(nanokube.NewError(fmt.Errorf("write kubeconfig %s: %w", path, err)))
		return ""
	}
	return path
}

func (k *nanokubeImpl) ensureCertKey() (string, string) {
	certPath := k.Options().FilePathAt(v1.CertFile)
	keyPath := k.Options().FilePathAt(v1.KeyFile)
	k.certKeyOnce.Do(func() {
		// Reuse an existing pair if both files are present (restart case).
		if _, err := os.Stat(certPath); err == nil {
			if _, err := os.Stat(keyPath); err == nil {
				return
			}
		}

		tunnel := k.Tunnel()

		certPEM, keyPEM, err := cert.GenerateSelfSignedCertKey(
			"localhost",
			[]net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback, tunnel.LocalIP()},
			[]string{tunnel.FQDN(), tunnel.Hostname()},
		)
		if err != nil {
			k.Cancel(nanokube.NewError(fmt.Errorf("generate self-signed cert: %w", err)))
			return
		}
		if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
			k.Cancel(nanokube.NewError(fmt.Errorf("write cert %s: %w", certPath, err)))
			return
		}
		if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
			k.Cancel(nanokube.NewError(fmt.Errorf("write key %s: %w", keyPath, err)))
			return
		}
	})
	return certPath, keyPath
}

func (k *nanokubeImpl) CancelErr(reason error) {
	k.Cancel(nanokube.NewError(reason))
}

func (k *nanokubeImpl) Cancel(reason v1.Error) {
	k.cancelErrsMu.Lock()
	k.cancelErrs = append(k.cancelErrs, reason)
	k.cancelErrsMu.Unlock()
	if k.pidfileWritten {
		pidPath := k.Options().FilePathAt(v1.PidFile(k.Options()))
		if err := os.Remove(pidPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			klog.ErrorS(err, "failed to remove pidfile", "path", pidPath)
		}
	}
	k.cancel(reason)
	var fatal v1.Error
	if errors.As(reason, &fatal) {
		runtime.Goexit()
	}
}

func (k *nanokubeImpl) Errors() []error {
	k.cancelErrsMu.Lock()
	defer k.cancelErrsMu.Unlock()
	if len(k.cancelErrs) == 0 {
		return nil
	}
	out := make([]error, len(k.cancelErrs))
	copy(out, k.cancelErrs)
	return out
}

func (k *nanokubeImpl) Version() string {
	k.versionOnce.Do(func() {
		k.version = k.cmd.Version
	})
	return k.version
}

func (k *nanokubeImpl) Tunnel() v1.Tunnel {
	// DEVNOTE: Formerly had separate tunnels for the API server and kubelet
	//          Leaving scaffolding in place to allow for multiple tunnels in the future, but for now just return a single shared tunnel.
	tunnelName := v1.SharedTunnel

	tunnel, _ := k.tunnels.LoadOrStore(tunnelName, func() v1.Tunnel {
		return NewTunnel(tunnelName)
	}())
	return tunnel.(v1.Tunnel)
}

func (k *nanokubeImpl) Host() v1.Host {
	k.hostOnce.Do(func() {
		k.host = nanokube.NewHost(k)
	})
	return k.host
}

func (k *nanokubeImpl) Client() v1.Client {
	k.clientOnce.Do(func() {
		<-nanokube.Await(k.ctx, k.loopbackProvided)
		k.client = nanokube.NewClient(k, k.loopback)
	})
	return k.client
}

func (k *nanokubeImpl) Backend(name v1.BackendName) v1.Backend {
	if backend, ok := k.backends.Load(name); ok {
		return backend.(v1.Backend)
	}
	return nil
}

func (k *nanokubeImpl) Backends() map[v1.BackendName]v1.Backend {
	k.backendsOnce.Do(func() {
		for _, detect := range v1.Backends {
			backend := detect(k)
			if backend != nil {
				klog.InfoS("backend detected", "backend", backend.Name())
				k.WithBackend(backend.Name(), backend)
			}
		}
	})

	backends := make(map[v1.BackendName]v1.Backend)
	k.backends.Range(func(key, value any) bool {
		name := key.(v1.BackendName)
		backend := value.(v1.Backend)
		backends[name] = backend
		return true
	})
	return backends
}

func (k *nanokubeImpl) Services(baseURL *url.URL) []*restful.WebService {
	k.servicesOnce.Do(func() {
		services := []*restful.WebService{}
		for _, backend := range k.Backends() {
			services = append(services, backend.WithBaseURL(baseURL.JoinPath(string(backend.Name()))).Services()...)
		}
		k.services = services
	})
	return k.services
}

func (k *nanokubeImpl) DefaultBackend() v1.Backend {
	for _, backend := range k.Backends() {
		return backend
	}
	// TODO(incomplete): better info on backends searched
	// TODO(partial): better CTA on how to add backends
	k.Cancel(nanokube.NewError(fmt.Errorf("no backends detected")))
	return nil
}

func (k *nanokubeImpl) WithBackend(name v1.BackendName, backend v1.Backend) v1.Nanokube {
	k.backends.Store(name, backend)
	return k
}

func (k *nanokubeImpl) StaticPods() []*corev1.Pod {
	k.staticPodsOnce.Do(func() {
		k.staticPods = []*corev1.Pod{}
		mountPath := string(k.Options().DataDir())
		volumes := []corev1.Volume{
			{Name: "nanokube-home", VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: string(mountPath),
				},
			}},
		}
		volumeMounts := []corev1.VolumeMount{
			{Name: "nanokube-home", MountPath: string(mountPath)},
		}

		k.staticPods = append(k.staticPods, &corev1.Pod{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Pod",
				APIVersion: "v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      k.Options().Name(),
				Namespace: "kube-system",
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:         "some-name",
						Image:        "some-image:latest",
						Command:      []string{"some-command"},
						Args:         []string{"some-args"},
						VolumeMounts: volumeMounts,
					},
				},
				Volumes: volumes,
			},
		})
	})
	return k.staticPods
}
