package pkg

import (
	"bytes"
	"context"
	"crypto/tls"
	_ "embed"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	_ "unsafe"

	"github.com/cnuss/nanokube/pkg/nanokube"
	"github.com/cnuss/nanokube/pkg/storage"
	v1 "github.com/cnuss/nanokube/pkg/v1"
	"github.com/emicklei/go-restful/v3"
	"github.com/spf13/cobra"
	noopoteltrace "go.opentelemetry.io/otel/trace/noop"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	apifeatures "k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	"k8s.io/apiserver/pkg/server/options"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/cert"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/klog/v2"
	apiserveroptions "k8s.io/kubernetes/cmd/kube-apiserver/app/options"
	kubeletoptions "k8s.io/kubernetes/cmd/kubelet/app/options"
	"k8s.io/kubernetes/pkg/capabilities"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	containertest "k8s.io/kubernetes/pkg/kubelet/container/testing"
	probetest "k8s.io/kubernetes/pkg/kubelet/prober/testing"
	kubeletserver "k8s.io/kubernetes/pkg/kubelet/server"
	kubeletutil "k8s.io/kubernetes/pkg/kubelet/util"
	"k8s.io/kubernetes/pkg/util/oom"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/kubernetes/pkg/volume/util/subpath"
	"k8s.io/mount-utils"
)

func NewNanokube(ctx context.Context) v1.Nanokube {
	ctx, cancel := context.WithCancelCause(ctx)

	nano := &nanokubeImpl{
		ctx:                           ctx,
		cancel:                        cancel,
		options:                       nanokube.NewOptions(),
		canceled:                      make(chan struct{}),
		exitCode:                      make(chan int, 1),
		apiserverProvided:             make(chan struct{}),
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

	runOnce sync.Once

	canceled     chan struct{}
	canceledOnce sync.Once
	cancelMu     sync.Mutex
	cancelHooks  [][]func(context.Context)
	cancelOnce   sync.Once
	cancelErrs   []error
	cancelErrsMu sync.Mutex

	exitCode     chan int
	exitCodeOnce sync.Once

	host     v1.Host
	hostOnce sync.Once

	apiserver         v1.ApiServer
	apiserverProvided chan struct{}

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
	detachOnce     sync.Once

	httpServer     *http.Server
	httpServerOnce sync.Once

	listener     net.Listener
	listenerOnce sync.Once

	server     *kubeletserver.Server
	serverOnce sync.Once

	bootstrap     kubelet.Bootstrap
	bootstrapOnce sync.Once

	// folded from former KubeImpl
	kubeletFlags     *kubeletoptions.KubeletFlags
	kubeletFlagsOnce sync.Once

	kubeletConfiguration     *kubeletconfig.KubeletConfiguration
	kubeletConfigurationOnce sync.Once

	kubeletDependencies     *kubelet.Dependencies
	kubeletDependenciesOnce sync.Once

	apiServerOptions     *apiserveroptions.CompletedOptions
	apiServerOptionsOnce sync.Once

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

	tlsConfig     *tls.Config
	tlsConfigOnce sync.Once

	tlsOptions     *kubeletserver.TLSOptions
	tlsOptionsOnce sync.Once

	secureServing     *options.SecureServingOptionsWithLoopback
	secureServingOnce sync.Once

	certKeyOnce sync.Once
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

func (k *nanokubeImpl) ApiServerOptions() *apiserveroptions.CompletedOptions {
	k.apiServerOptionsOnce.Do(func() {
		tunnel := k.Tunnel()
		opts := apiserveroptions.NewServerRunOptions()
		opts.Authentication.ServiceAccounts.Issuers = []string{fmt.Sprintf("https://%s", tunnel.FQDN())}
		opts.Authentication.ServiceAccounts.KeyFiles = []string{k.KeyFilePath()}
		opts.Authorization.Modes = []string{"Node", "RBAC"}
		opts.EndpointReconcilerType = "none" // TODO(partial): manage kubernetes service
		// opts.Etcd.StorageConfig.Transport.ServerList = k.Storage().Servers()
		opts.GenericServerRunOptions.ExternalHost = tunnel.FQDN()
		// TODO(incomplete): fiddling with shutdown
		opts.GenericServerRunOptions.ShutdownDelayDuration = 5 * time.Second
		opts.GenericServerRunOptions.ShutdownWatchTerminationGracePeriod = 10 * time.Second
		opts.GenericServerRunOptions.ShutdownSendRetryAfter = true
		opts.KubeletConfig.PreferredAddressTypes = []string{
			string(corev1.NodeExternalDNS),
			// string(corev1.NodeInternalIP),
			// string(corev1.NodeInternalDNS),
			// string(corev1.NodeHostName),
		}
		opts.SecureServing = k.SecureServing()
		opts.ServiceAccountSigningKeyFile = k.KeyFilePath()

		complete, err := opts.Complete(k)
		if err != nil {
			k.Cancel(nanokube.NewError(fmt.Errorf("failed to complete apiserver options: %w", err)))
			return
		}

		errs := complete.Validate()
		if len(errs) > 0 {
			k.Cancel(nanokube.NewError(fmt.Errorf("failed to validate apiserver options: %v", errs)))
			return
		}

		klog.InfoS("apiserver configured", "fqdn", opts.GenericServerRunOptions.ExternalHost)
		k.apiServerOptions = &complete
	})
	return k.apiServerOptions
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

func (k *nanokubeImpl) KubeletConfiguration() *kubeletconfig.KubeletConfiguration {
	k.kubeletConfigurationOnce.Do(func() {
		tunnel := k.Tunnel()
		if cfg, err := kubeletoptions.NewKubeletConfiguration(); err == nil {
			k.kubeletConfiguration = cfg
		} else {
			k.kubeletConfiguration = &kubeletconfig.KubeletConfiguration{}
		}
		k.kubeletConfiguration.Address = tunnel.LocalIP().String()
		k.kubeletConfiguration.ClusterDomain = tunnel.Domain()
		// TODO(incomplete): probe a container to get resolv.conf
		k.kubeletConfiguration.ClusterDNS = []string{"1.1.1.1"}
		k.kubeletConfiguration.FileCheckFrequency = metav1.Duration{Duration: 1 * time.Second}
		k.kubeletConfiguration.PodLogsDir = k.Options().DataDirAt(v1.DataDirLogs)
		k.kubeletConfiguration.Port = tunnel.LocalPort()
		k.kubeletConfiguration.ReadOnlyPort = 0
		k.kubeletConfiguration.RegisterNode = true
		k.kubeletConfiguration.StaticPodPath = k.Options().DataDirAt(v1.DataDirStaticPods)
	})
	return k.kubeletConfiguration
}

func (k *nanokubeImpl) KubeletDependencies() *kubelet.Dependencies {
	k.kubeletDependenciesOnce.Do(func() {
		k.kubeletDependencies = &kubelet.Dependencies{
			// These dependencies are required by the kubelet
			RemoteRuntimeService: nil,
			RemoteImageService:   nil,
			CAdvisorInterface:    nil,
			ContainerManager:     nil,
			TLSOptions:           nil,
			// These are needed for non-standalone mode
			KubeClient:      nil,
			HeartbeatClient: nil,
			// Use fake implementations for the rest of the dependencies
			ProbeManager:              probetest.FakeManager{},
			OSInterface:               &containertest.FakeOS{},
			VolumePlugins:             []volume.VolumePlugin{},
			OOMAdjuster:               oom.NewFakeOOMAdjuster(),
			Mounter:                   &mount.FakeMounter{},
			Subpather:                 &subpath.FakeSubpath{},
			HostUtil:                  hostutil.NewFakeHostUtil(nil),
			PodStartupLatencyTracker:  kubeletutil.NewPodStartupLatencyTracker(),
			NodeStartupLatencyTracker: kubeletutil.NewNodeStartupLatencyTracker(),
			TracerProvider:            noopoteltrace.NewTracerProvider(),
			Recorder:                  &record.FakeRecorder{},
		}
		k.kubeletDependencies.KubeClient = k.Client()
		k.kubeletDependencies.HeartbeatClient = k.Client()
		k.kubeletDependencies.RemoteRuntimeService = k.DefaultBackend().Driver()
		k.kubeletDependencies.RemoteImageService = k.DefaultBackend().Driver()
		k.kubeletDependencies.CAdvisorInterface = k.DefaultBackend()
		k.kubeletDependencies.ContainerManager = k.DefaultBackend().Manager()
		k.kubeletDependencies.TLSOptions = k.TLSOptions()
		k.kubeletDependencies.ProbeManager = nil
		// TODO(1.36): Dependencies.Services field was removed; backend WebServices need a new mount point
		k.kubeletDependencies.VolumePlugins = k.Host().VolumePlugins()
		k.kubeletDependencies.OSInterface = k.Host()
		k.kubeletDependencies.Mounter = k.Host()
		k.kubeletDependencies.Subpather = k.Host()
		k.kubeletDependencies.HostUtil = k.Host()
		k.kubeletDependencies.Recorder = k
	})
	return k.kubeletDependencies
}

func (k *nanokubeImpl) Args(service v1.ServiceName, mountPath string) []string {
	rootCaFile := func() string {
		var buf bytes.Buffer
		for _, c := range k.ApiServer().CACerts() {
			if err := pem.Encode(&buf, &pem.Block{Type: "CERTIFICATE", Bytes: c.Raw}); err != nil {
				k.Cancel(nanokube.NewError(fmt.Errorf("failed to PEM-encode CA cert: %w", err)))
				return ""
			}
		}
		path := k.Options().FilePathAt(v1.CAFile)
		if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
			k.Cancel(nanokube.NewError(fmt.Errorf("failed to write CA file %s: %w", path, err)))
			return ""
		}
		return string(v1.CAFile)
	}

	gateKeys := make([]string, 0, len(FeatureGates))
	for k := range FeatureGates {
		gateKeys = append(gateKeys, k)
	}
	sort.Strings(gateKeys)
	gateParts := make([]string, 0, len(gateKeys))
	for _, k := range gateKeys {
		gateParts = append(gateParts, fmt.Sprintf("%s=%t", k, FeatureGates[k]))
	}
	featureGates := "--feature-gates=" + strings.Join(gateParts, ",")

	switch service {
	case v1.ControllerManagerService:
		return []string{
			"--kubeconfig=" + mountPath + "/.kube/config",
			"--authentication-kubeconfig=" + mountPath + "/.kube/config",
			"--authorization-kubeconfig=" + mountPath + "/.kube/config",
			"--leader-elect=true",
			"--controller-shutdown-timeout=0",
			"--use-service-account-credentials=false",
			"--tls-cert-file=" + mountPath + "/" + string(v1.CertFile),
			"--tls-private-key-file=" + mountPath + "/" + string(v1.KeyFile),
			"--service-account-private-key-file=" + mountPath + "/" + string(v1.KeyFile),
			"--root-ca-file=" + mountPath + "/" + rootCaFile(),
			featureGates,
		}
	case v1.SchedulerService:
		return []string{
			"--kubeconfig=" + mountPath + "/.kube/config",
			"--authentication-kubeconfig=" + mountPath + "/.kube/config",
			"--authorization-kubeconfig=" + mountPath + "/.kube/config",
			"--authentication-skip-lookup=true",
			"--leader-elect=true",
			"--tls-cert-file=" + mountPath + "/" + string(v1.CertFile),
			"--tls-private-key-file=" + mountPath + "/" + string(v1.KeyFile),
			featureGates,
		}
	}
	return []string{}
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

func (k *nanokubeImpl) SecureServing() *options.SecureServingOptionsWithLoopback {
	k.secureServingOnce.Do(func() {
		tunnel := k.Tunnel()
		listener, err := net.Listen("tcp", net.JoinHostPort(tunnel.LocalIP().String(), strconv.FormatUint(uint64(tunnel.LocalPort()), 10)))
		if err != nil {
			k.Cancel(nanokube.NewError(fmt.Errorf("failed to create listener: %w", err)).WithCode(1))
			return
		}

		ss := options.NewSecureServingOptions().WithLoopback()
		ss.BindAddress = tunnel.LocalIP()
		ss.BindPort = int(tunnel.LocalPort())
		ss.Listener = listener
		ss.DisableHTTP2Serving = !v1.HTTP2
		ss.ServerCert = options.GeneratableKeyCert{
			CertKey: options.CertKey{
				CertFile: k.CertFilePath(),
				KeyFile:  k.KeyFilePath(),
			},
		}
		k.secureServing = ss
	})
	return k.secureServing
}

func (k *nanokubeImpl) TLSConfig() *tls.Config {
	k.tlsConfigOnce.Do(func() {
		nextProtos := []string{"h2", "http/1.1"}
		if !v1.HTTP2 {
			nextProtos = []string{"http/1.1"}
		}
		opts := k.TLSOptions()

		tlsConfig := &tls.Config{
			MinVersion:       opts.MinVersion,
			CipherSuites:     opts.CipherSuites,
			CurvePreferences: opts.CurvePreferences,
			NextProtos:       nextProtos,
		}

		// TODO(partial): get rid of this. this makes the LookbackClient work
		si := k.ApiServer().Server().SecureServingInfo
		dyn := dynamiccertificates.NewDynamicServingCertificateController(tlsConfig, nil, si.Cert, si.SNICerts, nil)
		si.Cert.AddListener(dyn)
		dyn.RunOnce()

		tlsConfig.GetConfigForClient = dyn.GetConfigForClient
		k.tlsConfig = tlsConfig
	})
	return k.tlsConfig
}

func (k *nanokubeImpl) TLSOptions() *kubeletserver.TLSOptions {
	k.tlsOptionsOnce.Do(func() {
		k.tlsOptions = &kubeletserver.TLSOptions{
			MinVersion: func() uint16 {
				if v, err := cliflag.TLSVersion(k.KubeletConfiguration().TLSMinVersion); err == nil {
					return v
				}
				return cliflag.DefaultTLSVersion()
			}(),
			CipherSuites: func() []uint16 {
				if v, err := cliflag.TLSCipherSuites(k.KubeletConfiguration().TLSCipherSuites); err == nil {
					return v
				}
				return nil
			}(),
			CertFile: k.CertFilePath(),
			KeyFile:  k.KeyFilePath(),
		}
	})
	return k.tlsOptions
}

func (k *nanokubeImpl) CertFilePath() string {
	certPath, _ := k.ensureCertKey()
	return certPath
}

func (k *nanokubeImpl) KeyFilePath() string {
	_, keyPath := k.ensureCertKey()
	return keyPath
}

// RootCaFilePath returns the path to the CA bundle that signs the apiserver's
// serving cert. Today the apiserver cert is self-signed, so the cert file is
// its own CA bundle — same file as CertFilePath. If we ever generate a
// separate CA, this is the seam to change.
func (k *nanokubeImpl) RootCaFilePath() string {
	return k.CertFilePath()
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

const shutdownTimeout = 30 * time.Second

// keepAliveListener mirrors the private tcpKeepAliveListener in
// k8s.io/apiserver/pkg/server: enables TCP keep-alive on accepted
// connections so half-dead clients get cleaned up.
type keepAliveListener struct {
	net.Listener
	period time.Duration
}

func (ln keepAliveListener) Accept() (net.Conn, error) {
	c, err := ln.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(ln.period)
	}
	return c, nil
}

func (k *nanokubeImpl) CancelErr(reason error) {
	k.Cancel(nanokube.NewError(reason))
}

func (k *nanokubeImpl) Cancel(reason v1.Error) {
	k.cancelErrsMu.Lock()
	k.cancelErrs = append(k.cancelErrs, reason)
	k.cancelErrsMu.Unlock()
	k.canceledOnce.Do(func() { close(k.canceled) })
	k.runCancelHooks()
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

func (k *nanokubeImpl) Detach() {
	k.detachOnce.Do(func() {
		pidStr := os.Getenv("NANOKUBE_LAUNCHER_PID")
		if pidStr == "" {
			return
		}
		ppid, err := strconv.Atoi(pidStr)
		if err != nil {
			klog.ErrorS(err, "invalid NANOKUBE_LAUNCHER_PID", "value", pidStr)
			return
		}
		if err := syscall.Kill(ppid, syscall.SIGUSR1); err != nil {
			klog.ErrorS(err, "failed to signal launcher", "pid", ppid)
		}
	})
}

func (k *nanokubeImpl) Canceled() <-chan struct{} {
	return k.canceled
}

func (k *nanokubeImpl) OnCancel(fns ...func(ctx context.Context)) v1.Nanokube {
	k.cancelMu.Lock()
	defer k.cancelMu.Unlock()
	k.cancelHooks = append(k.cancelHooks, fns)
	return k
}

func (k *nanokubeImpl) OnReady(service v1.ServiceName, fns ...func(ctx context.Context)) v1.Nanokube {
	for _, fn := range fns {
		var ch <-chan struct{}

		switch service {
		case v1.APIServerService:
			ch = k.ApiServer().Ready()
		case v1.Node:
			ch = k.NodeReady()
		default:
			k.Cancel(nanokube.NewError(fmt.Errorf("unknown service %s", service)))
			return k
		}

		go func(fn func(ctx context.Context), ch <-chan struct{}) {
			for {
				select {
				case <-ch:
					fn(k)
					return
				case <-k.Canceled():
					return
				}
			}
		}(fn, ch)
	}
	return k
}

func (k *nanokubeImpl) runCancelHooks() {
	k.cancelOnce.Do(func() {
		k.cancelMu.Lock()
		groups := append([][]func(context.Context){}, k.cancelHooks...)
		k.cancelMu.Unlock()

		hookCtx, hookCancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer hookCancel()

		for groupIdx, group := range groups {
			var wg sync.WaitGroup
			for _, hook := range group {
				wg.Add(1)
				go func(fn func(context.Context)) {
					defer wg.Done()
					defer func() {
						if r := recover(); r != nil {
							klog.ErrorS(nil, "cancel hook panicked", "group", groupIdx, "recover", r, "stack", string(debug.Stack()))
						}
					}()
					fn(hookCtx)
				}(hook)
			}

			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()

			select {
			case <-done:
			case <-hookCtx.Done():
				klog.InfoS("cancel hook group timed out", "group", groupIdx, "timeout", shutdownTimeout)
				return
			}
		}
	})
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
		return NewTunnel(k, tunnelName)
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

func (k *nanokubeImpl) WithApiServer(apiserver v1.ApiServer) v1.Nanokube {
	k.apiserver = apiserver
	close(k.apiserverProvided)
	return k
}

func (k *nanokubeImpl) ApiServer() v1.ApiServer {
	<-k.apiserverProvided
	return k.apiserver
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
						Name:         "controller-manager",
						Image:        "registry.k8s.io/kube-controller-manager:" + k.Version(),
						Command:      []string{"kube-controller-manager"},
						Args:         k.Args(v1.ControllerManagerService, mountPath),
						VolumeMounts: volumeMounts,
					},
					{
						Name:         "scheduler",
						Image:        "registry.k8s.io/kube-scheduler:" + k.Version(),
						Command:      []string{"kube-scheduler"},
						Args:         k.Args(v1.SchedulerService, mountPath),
						VolumeMounts: volumeMounts,
					},
				},
				Volumes: volumes,
			},
		})
	})
	return k.staticPods
}

func (k *nanokubeImpl) Bootstrap() kubelet.Bootstrap {
	k.bootstrapOnce.Do(func() {
		// kubeDeps := k.KubeletDependencies()

		// server := &kubeletoptions.KubeletServer{
		// 	KubeletFlags:         *k.KubeletFlags(),
		// 	KubeletConfiguration: *k.KubeletConfiguration(),
		// }

		// hostname, _ := nodeutil.GetHostname(server.HostnameOverride)
		// nodeName := types.NodeName(hostname)
		// nodeIPs, _, _ := nodeutil.ParseNodeIPArgument(server.NodeIP, server.CloudProvider)

		// klet, err := kubeletapp.CreateAndInitKubelet(k.ctx, server, kubeDeps, hostname, nodeName, nodeIPs)
		// if err != nil {
		// 	k.Cancel(nanokube.NewError(fmt.Errorf("create kubelet: %w", err)).WithCode(1))
		// 	return
		// }

		// k.bootstrap = klet
	})
	return k.bootstrap
}

func (k *nanokubeImpl) Run() v1.Nanokube {
	k.runOnce.Do(func() {
		pidPath := k.Options().FilePathAt(v1.PidFile(k.Options()))
		if err := os.WriteFile(pidPath, []byte(strconv.Itoa(os.Getpid())), 0o644); err != nil {
			klog.ErrorS(err, "failed to write pidfile", "path", pidPath)
		} else {
			k.pidfileWritten = true
		}

		for _, pod := range k.StaticPods() {
			data, err := json.Marshal(pod)
			if err != nil {
				k.Cancel(nanokube.NewError(fmt.Errorf("Failed to marshal static pod %s: %v", pod.Name, err)))
				return
			}
			if err := os.WriteFile(filepath.Join(k.KubeletConfiguration().StaticPodPath, pod.Name+".json"), data, 0o644); err != nil {
				k.Cancel(nanokube.NewError(fmt.Errorf("Failed to write static pod manifest %s: %v", pod.Name, err)))
				return
			}
		}

		capabilities.Initialize(capabilities.Capabilities{AllowPrivileged: true})

		go func() {
			ln := keepAliveListener{Listener: k.SecureServing().Listener, period: 1 * time.Minute}
			if err := k.HTTPServer().ServeTLS(ln, k.CertFilePath(), k.KeyFilePath()); err != nil && !errors.Is(err, http.ErrServerClosed) {
				k.Cancel(nanokube.NewError(fmt.Errorf("kubelet HTTP server error: %w", err)).WithCode(1))
			}
		}()
		go k.Bootstrap().Run(k.ctx, k.KubeletDependencies().PodConfig.Updates())
		go k.Bootstrap().ListenAndServePodResources(k.ctx)
		go k.Bootstrap().ListenAndServePods(k.ctx)
	})
	return k
}

func (k *nanokubeImpl) Server() *kubeletserver.Server {
	k.serverOnce.Do(func() {
		// kl := k.Bootstrap().(*kubelet.Kubelet)
		// ksrv := kubeletserver.NewServer(k.ctx, kl, kl.ResourceAnalyzer(), kl.HealthCheckers(), kl.Flagz(), k.KubeletDependencies().Auth, k.KubeletConfiguration())
		// ksrv.InstallTracingFilter(k.KubeletDependencies().TracerProvider)

		// //
		// // DEVNOTE: add streaming handlers
		// //          TODO(partial): consider providing these directly and disable API Server management of /exec /attach /portforward
		// //
		// for _, ws := range k.Services(k.Tunnel().URL()) {
		// 	ksrv.Restful().Add(ws)
		// 	nanokube.Log.Info("registered service", "service", ws.RootPath())
		// }

		// //
		// // DEVNOTE: we combine the kubelet server and the kubernetes apiserver together
		// //          TODO(partial): add kubelet auth
		// //
		// ksrv.Restful().Handle("/", k.ApiServer().Handler())
		// nanokube.Log.Info("registered API server")

		// k.server = &ksrv
	})
	return k.server
}

func (k *nanokubeImpl) HTTPServer() *http.Server {
	k.httpServerOnce.Do(func() {
		tunnel := k.Tunnel()
		k.httpServer = &http.Server{
			Addr:      net.JoinHostPort(tunnel.LocalIP().String(), strconv.FormatUint(uint64(tunnel.LocalPort()), 10)),
			TLSConfig: k.TLSConfig(),
			Handler:   k.Server(),
		}
		k.httpServer.RegisterOnShutdown(func() {
			if err := k.ApiServer().Destroy(); err != nil {
				klog.ErrorS(err, "failed to destroy API server")
			}
		})
	})
	return k.httpServer
}
