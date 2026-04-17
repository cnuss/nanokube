package pkg

import (
	"context"
	_ "embed"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
	_ "unsafe"

	"github.com/cnuss/nanokube/pkg/nanokube"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	storage "k8s.io/apiserver/pkg/server/storage"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	apiserveroptions "k8s.io/kubernetes/cmd/kube-apiserver/app/options"
	kubeletoptions "k8s.io/kubernetes/cmd/kubelet/app/options"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/server"
	"k8s.io/kubernetes/pkg/kubemark"
)

//go:embed kube-system.yaml
var kubeSystemManifest string

//go:linkname nodeReadyGracePeriod k8s.io/kubernetes/pkg/kubelet.nodeReadyGracePeriod
var nodeReadyGracePeriod time.Duration

func init() {
	nodeReadyGracePeriod = 30 * time.Second // TODO(remove): temporary override for debugging
}

var FeatureGates = map[string]bool{
	"KubeletInUserNamespace": true,
}

// HTTP2 toggles HTTP/2 end-to-end: apiserver secure serving AND cloudflared tunnel origin dialing.
// Must stay 1-1 — enabling Http2Origin on the tunnel without HTTP/2 serving on apiserver (or vice versa) breaks exec/port-forward stream multiplexing.
var HTTP2 = true

type Kube interface {
	record.EventRecorder

	ApiServerOptions() *apiserveroptions.CompletedOptions
	ApiServerTunnel() nanokube.Tunnel
	ApiServerHostname() string

	KubeletFlags() *kubeletoptions.KubeletFlags
	KubeletConfiguration() *kubeletconfig.KubeletConfiguration
	KubeletTunnel() nanokube.Tunnel
	KubeletHostname() string

	Client() nanokube.Client

	WithKubelet(kubelet *kubemark.HollowKubelet) Kube
	Kubelet() *kubemark.HollowKubelet
	WithApiServer(apiserver nanokube.ApiServer) Kube
	ApiServer() nanokube.ApiServer
	WithStorageFactory(storagefactory nanokube.StorageFactory) Kube
	StorageFactory() nanokube.StorageFactory

	NodeReady() chan struct{}

	writeStaticPods() error
}

type KubeImpl struct {
	ctx    context.Context
	config Config

	apiServerOptions     *apiserveroptions.CompletedOptions
	apiServerOptionsOnce sync.Once

	apiServerTunnel     nanokube.Tunnel
	apiServerTunnelOnce sync.Once

	kubeletTunnel     nanokube.Tunnel
	kubeletTunnelOnce sync.Once

	kubeletFlags     *kubeletoptions.KubeletFlags
	kubeletFlagsOnce sync.Once

	kubeletConfig     *kubeletconfig.KubeletConfiguration
	kubeletConfigOnce sync.Once

	tlsOptions     *server.TLSOptions
	tlsOptionsOnce sync.Once

	defaultStorageFactory     *storage.DefaultStorageFactory
	defaultStorageFactoryOnce sync.Once

	kubelet         *kubemark.HollowKubelet
	kubeletProvided chan struct{}

	apiserver         nanokube.ApiServer
	apiserverProvided chan struct{}

	storagefactory         nanokube.StorageFactory
	storagefactoryProvided chan struct{}

	broadcaster      record.EventBroadcaster
	recorder         record.EventRecorder
	recorderProvided chan struct{}
	eventsOnce       sync.Once

	nodeReady     chan struct{}
	nodeReadyOnce sync.Once
}

var _ Kube = &KubeImpl{}

func newKube(config Config) Kube {
	kube := &KubeImpl{
		ctx:                    config.Context(),
		config:                 config,
		kubeletProvided:        make(chan struct{}),
		apiserverProvided:      make(chan struct{}),
		storagefactoryProvided: make(chan struct{}),
		recorderProvided:       make(chan struct{}),
		broadcaster:            record.NewBroadcaster(record.WithContext(config.Context())),
		nodeReady:              make(chan struct{}),
	}

	return kube
}

func (k *KubeImpl) ApiServerOptions() *apiserveroptions.CompletedOptions {
	k.apiServerOptionsOnce.Do(func() {
		opts := apiserveroptions.NewServerRunOptions()
		opts.Authentication.ServiceAccounts.Issuers = []string{fmt.Sprintf("https://%s:%d", k.ApiServerTunnel().Hostname(), k.ApiServerTunnel().Port())}
		opts.Authentication.ServiceAccounts.KeyFiles = []string{filepath.Join(string(k.config.Options().DataDir()), string(nanokube.KeyFile))}
		opts.Authorization.Modes = []string{"Node", "RBAC"}
		opts.EndpointReconcilerType = "none" // TODO(partial): manage kubernetes service
		opts.Etcd.StorageConfig.Transport.ServerList = k.StorageFactory().ServerList()
		opts.GenericServerRunOptions.ExternalHost = k.ApiServerHostname()
		opts.GenericServerRunOptions.ShutdownDelayDuration = 0
		opts.SecureServing.BindAddress = net.ParseIP("0.0.0.0")
		opts.SecureServing.BindPort = k.ApiServerTunnel().Port()
		opts.SecureServing.DisableHTTP2Serving = !HTTP2
		opts.SecureServing.ServerCert.CertDirectory = k.config.Options().DataDirAt(nanokube.DataDirCerts)
		opts.ServiceAccountSigningKeyFile = filepath.Join(string(k.config.Options().DataDir()), string(nanokube.KeyFile))
		opts.ServiceClusterIPRanges = "10.0.0.0/16" // TODO

		complete, err := opts.Complete(k.config.Context())
		if err != nil {
			klog.Fatalf("Failed to complete apiserver options: %v", err)
		}

		errs := complete.Validate()
		if len(errs) > 0 {
			klog.Fatalf("Failed to validate apiserver options: %v", errs)
		}

		nanokube.Log.Info("apiserver configured", "hostname", opts.GenericServerRunOptions.ExternalHost)
		k.apiServerOptions = &complete
	})
	return k.apiServerOptions
}

func (k *KubeImpl) ApiServerTunnel() nanokube.Tunnel {
	k.apiServerTunnelOnce.Do(func() {
		k.apiServerTunnel = k.config.NewTunnel()
	})
	return k.apiServerTunnel
}

func (k *KubeImpl) ApiServerHostname() string {
	return k.ApiServerTunnel().Hostname()
}

func (k *KubeImpl) KubeletTunnel() nanokube.Tunnel {
	k.kubeletTunnelOnce.Do(func() {
		k.kubeletTunnel = k.config.NewTunnel()
	})
	return k.kubeletTunnel
}

func (k *KubeImpl) KubeletHostname() string {
	return k.KubeletTunnel().Hostname()
}

func (k *KubeImpl) KubeletFlags() *kubeletoptions.KubeletFlags {
	k.kubeletFlagsOnce.Do(func() {
		k.kubeletFlags = kubeletoptions.NewKubeletFlags()
		k.kubeletFlags.RootDirectory = k.config.Options().DataDirAt(nanokube.DataDirKubelet)
		k.kubeletFlags.CertDirectory = k.config.Options().DataDirAt(nanokube.DataDirCerts)
		k.kubeletFlags.HostnameOverride = k.KubeletHostname()
		k.kubeletFlags.NodeIP = "127.0.0.1"
		k.kubeletFlags.NodeLabels = make(map[string]string) // TODO(incomplete): add labels
	})
	return k.kubeletFlags
}

func (k *KubeImpl) KubeletConfiguration() *kubeletconfig.KubeletConfiguration {
	k.kubeletConfigOnce.Do(func() {
		config, err := kubeletoptions.NewKubeletConfiguration()
		if err != nil {
			klog.Fatalf("Failed to create kubelet configuration: %v", err)
		}
		if err := k.writeStaticPods(); err != nil {
			klog.Fatalf("Failed to write static pods: %v", err)
		}
		config.PodLogsDir = k.config.Options().DataDirAt(nanokube.DataDirLogs)
		config.StaticPodPath = k.config.Options().DataDirAt(nanokube.DataDirStaticPods)
		config.ReadOnlyPort = 0

		k.recorder = k.broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: k.config.Options().Name(), Host: k.KubeletHostname()})
		close(k.recorderProvided)

		nanokube.Log.Info("kubelet configured", "hostname", k.KubeletHostname())
		k.kubeletConfig = config
	})
	return k.kubeletConfig
}

func (k *KubeImpl) Client() nanokube.Client {
	return k.ApiServer().Client(k.ctx)
}

func (k *KubeImpl) WithKubelet(kubelet *kubemark.HollowKubelet) Kube {
	if k.config.Options().Standalone() {
		kubelet.KubeletConfiguration.RegisterNode = false
		kubelet.KubeletDeps.KubeClient = nil
		kubelet.KubeletDeps.EventClient = nil
		kubelet.KubeletDeps.HeartbeatClient = nil
	}
	kubelet.KubeletDeps.ProbeManager = nil
	kubelet.KubeletDeps.Recorder = k
	kubelet.KubeletDeps.OSInterface = k.config.Crid().DefaultBackend()
	kubelet.KubeletDeps.Mounter = k.config.Crid().DefaultBackend()
	kubelet.KubeletDeps.Subpather = k.config.Crid().DefaultBackend()
	kubelet.KubeletDeps.HostUtil = k.config.Crid().DefaultBackend()
	k.kubelet = kubelet
	close(k.kubeletProvided)
	return k
}

func (k *KubeImpl) Kubelet() *kubemark.HollowKubelet {
	<-k.kubeletProvided
	return k.kubelet
}

func (k *KubeImpl) WithApiServer(apiserver nanokube.ApiServer) Kube {
	apiserver.Config().KubeAPIs.ControlPlane.StorageFactory = k.StorageFactory()
	// TODO: set apiserver config on kubelet
	k.apiserver = apiserver
	close(k.apiserverProvided)
	return k
}

func (k *KubeImpl) ApiServer() nanokube.ApiServer {
	<-k.apiserverProvided
	return k.apiserver
}

func (k *KubeImpl) WithStorageFactory(storagefactory nanokube.StorageFactory) Kube {
	k.storagefactory = storagefactory
	close(k.storagefactoryProvided)
	return k
}

func (k *KubeImpl) StorageFactory() nanokube.StorageFactory {
	<-k.storagefactoryProvided
	return k.storagefactory
}

func (k *KubeImpl) writeStaticPods() error {
	path := k.config.Options().DataDirAt(nanokube.DataDirStaticPods)
	if err := os.WriteFile(filepath.Join(path, "static-pods.yaml"), []byte(kubeSystemManifest), 0o644); err != nil {
		return fmt.Errorf("write static pod manifest: %w", err)
	}
	return nil
}

func (k *KubeImpl) AnnotatedEventf(object runtime.Object, annotations map[string]string, eventtype string, reason string, messageFmt string, args ...interface{}) {
	<-k.recorderProvided
	k.Logf(object, eventtype, reason, messageFmt, args...)
	k.recorder.AnnotatedEventf(object, annotations, eventtype, reason, messageFmt, args...)
}

func (k *KubeImpl) Event(object runtime.Object, eventtype string, reason string, message string) {
	<-k.recorderProvided
	k.Logf(object, eventtype, reason, "%s", message)
	k.recorder.Event(object, eventtype, reason, message)
}

func (k *KubeImpl) Eventf(object runtime.Object, eventtype string, reason string, messageFmt string, args ...interface{}) {
	<-k.recorderProvided
	k.Logf(object, eventtype, reason, messageFmt, args...)
	k.recorder.Eventf(object, eventtype, reason, messageFmt, args...)

	// 14:01:42 INF Eventf group="" version=v1 kind=Node type=Normal reason=NodeReady message="Node depends-location-assessments-silence.trycloudflare.com status is now: NodeReady"
	if object.GetObjectKind().GroupVersionKind().Group == "" &&
		object.GetObjectKind().GroupVersionKind().Version == "v1" &&
		object.GetObjectKind().GroupVersionKind().Kind == "Node" {
		if eventtype == v1.EventTypeNormal && reason == "NodeReady" {
			k.nodeReadyOnce.Do(func() {
				nanokube.Log.Info("node is ready", "hostname", k.KubeletHostname())
				close(k.nodeReady)
			})
		}
	}
}

func (k *KubeImpl) Logf(object runtime.Object, eventtype string, reason string, messageFmt string, args ...interface{}) {
	logFunc := nanokube.Log.Debug
	switch eventtype {
	case v1.EventTypeNormal:
		logFunc = nanokube.Log.Info
	case v1.EventTypeWarning:
		logFunc = nanokube.Log.Warn
	}
	logFunc(fmt.Sprintf(messageFmt, args...), "gvk", object.GetObjectKind().GroupVersionKind().String(), "reason", reason)
}

func (k *KubeImpl) NodeReady() chan struct{} {
	return k.nodeReady
}
