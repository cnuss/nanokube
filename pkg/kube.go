package pkg

import (
	"context"
	"crypto/tls"
	_ "embed"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
	_ "unsafe"

	"github.com/cnuss/nanokube/pkg/nanokube"
	v1 "github.com/cnuss/nanokube/pkg/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	storage "k8s.io/apiserver/pkg/server/storage"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/klog/v2"
	apiserveroptions "k8s.io/kubernetes/cmd/kube-apiserver/app/options"
	kubeletoptions "k8s.io/kubernetes/cmd/kubelet/app/options"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/server"
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

type KubeImpl struct {
	ctx    context.Context
	config v1.Config

	apiServerOptions     *apiserveroptions.CompletedOptions
	apiServerOptionsOnce sync.Once

	apiServerTunnel     v1.Tunnel
	apiServerTunnelOnce sync.Once

	kubeletTunnel     v1.Tunnel
	kubeletTunnelOnce sync.Once

	kubeletFlags     *kubeletoptions.KubeletFlags
	kubeletFlagsOnce sync.Once

	kubeletConfig     *kubeletconfig.KubeletConfiguration
	kubeletConfigOnce sync.Once

	tlsOptions     *server.TLSOptions
	tlsOptionsOnce sync.Once

	defaultStorageFactory     *storage.DefaultStorageFactory
	defaultStorageFactoryOnce sync.Once

	kubelet         v1.Kubelet
	kubeletProvided chan struct{}

	apiserver         v1.ApiServer
	apiserverProvided chan struct{}

	storagefactory         v1.StorageFactory
	storagefactoryProvided chan struct{}

	broadcaster      record.EventBroadcaster
	recorder         record.EventRecorder
	recorderProvided chan struct{}
	eventsOnce       sync.Once

	nodeReady     chan *corev1.ObjectReference
	nodeReadyOnce sync.Once
}

var _ v1.Kube = &KubeImpl{}

func newKube(config v1.Config) v1.Kube {
	kube := &KubeImpl{
		ctx:                    config.Context(),
		config:                 config,
		kubeletProvided:        make(chan struct{}),
		apiserverProvided:      make(chan struct{}),
		storagefactoryProvided: make(chan struct{}),
		recorderProvided:       make(chan struct{}),
		broadcaster:            record.NewBroadcaster(record.WithContext(config.Context())),
		nodeReady:              make(chan *corev1.ObjectReference, 1),
	}

	return kube
}

func (k *KubeImpl) ApiServerOptions() *apiserveroptions.CompletedOptions {
	k.apiServerOptionsOnce.Do(func() {
		opts := apiserveroptions.NewServerRunOptions()
		opts.Authentication.ServiceAccounts.Issuers = []string{fmt.Sprintf("https://%s", k.ApiServerTunnel().FQDN())}
		opts.Authentication.ServiceAccounts.KeyFiles = []string{filepath.Join(string(k.config.Options().DataDir()), string(v1.KeyFile))}
		opts.Authorization.Modes = []string{"Node", "RBAC"}
		opts.EndpointReconcilerType = "none" // TODO(partial): manage kubernetes service
		opts.Etcd.StorageConfig.Transport.ServerList = k.StorageFactory().ServerList()
		opts.GenericServerRunOptions.ExternalHost = k.ApiServerFQDN()
		opts.GenericServerRunOptions.ShutdownDelayDuration = 0
		opts.KubeletConfig.PreferredAddressTypes = []string{
			string(corev1.NodeExternalDNS),
			// string(corev1.NodeInternalIP),
			// string(corev1.NodeInternalDNS),
			// string(corev1.NodeHostName),
		}
		opts.SecureServing.BindAddress = k.ApiServerTunnel().LocalHost()
		opts.SecureServing.BindPort = int(k.ApiServerTunnel().LocalPort())
		opts.SecureServing.DisableHTTP2Serving = !v1.HTTP2
		opts.SecureServing.ServerCert.CertDirectory = k.config.Options().DataDirAt(v1.DataDirCerts)
		opts.ServiceAccountSigningKeyFile = filepath.Join(string(k.config.Options().DataDir()), string(v1.KeyFile))

		complete, err := opts.Complete(k.config.Context())
		if err != nil {
			klog.Fatalf("Failed to complete apiserver options: %v", err)
		}

		errs := complete.Validate()
		if len(errs) > 0 {
			klog.Fatalf("Failed to validate apiserver options: %v", errs)
		}

		nanokube.Log.Info("apiserver configured", "fqdn", opts.GenericServerRunOptions.ExternalHost)
		k.apiServerOptions = &complete
	})
	return k.apiServerOptions
}

func (k *KubeImpl) ApiServerTunnel() v1.Tunnel {
	k.apiServerTunnelOnce.Do(func() {
		k.apiServerTunnel = k.config.NewTunnel()
	})
	return k.apiServerTunnel
}

func (k *KubeImpl) ApiServerFQDN() string {
	return k.ApiServerTunnel().FQDN()
}

func (k *KubeImpl) KubeletTunnel() v1.Tunnel {
	k.kubeletTunnelOnce.Do(func() {
		k.kubeletTunnel = k.config.NewTunnel()
	})
	return k.kubeletTunnel
}

func (k *KubeImpl) KubeletFQDN() string {
	return k.KubeletTunnel().FQDN()
}

func (k *KubeImpl) KubeletFlags() *kubeletoptions.KubeletFlags {
	k.kubeletFlagsOnce.Do(func() {
		k.kubeletFlags = kubeletoptions.NewKubeletFlags()
		k.kubeletFlags.CloudProvider = "external"
		k.kubeletFlags.HostnameOverride = k.KubeletTunnel().Hostname()
		k.kubeletFlags.NodeLabels = make(map[string]string) // TODO(incomplete): add labels
		k.kubeletFlags.NodeIP = k.KubeletTunnel().LocalHost().String()
		k.kubeletFlags.RootDirectory = k.config.Options().DataDirAt(v1.DataDirKubelet)
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
		config.Address = k.KubeletTunnel().LocalHost().String()
		config.ClusterDomain = k.KubeletTunnel().Domain()
		// TODO(incomplete): probe a container to get resolv.conf
		config.ClusterDNS = []string{"1.1.1.1"}
		config.PodLogsDir = k.config.Options().DataDirAt(v1.DataDirLogs)
		config.Port = k.KubeletTunnel().LocalPort()
		config.ReadOnlyPort = 0
		config.StaticPodPath = k.config.Options().DataDirAt(v1.DataDirStaticPods)

		k.recorder = k.broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: k.config.Options().Name(), Host: k.KubeletFQDN()})
		close(k.recorderProvided)

		nanokube.Log.Info("kubelet configured", "fqdn", k.KubeletFQDN())
		k.kubeletConfig = config
	})
	return k.kubeletConfig
}

func (k *KubeImpl) Client() v1.Client {
	return k.ApiServer().Client(k.ctx)
}

func (k *KubeImpl) WithKubelet(kubelet v1.Kubelet) v1.Kube {
	if k.config.Options().Standalone() {
		kubelet.Configuration().RegisterNode = false
		kubelet.Deps().KubeClient = nil
		kubelet.Deps().EventClient = nil
		kubelet.Deps().HeartbeatClient = nil
	}
	kubelet.Deps().ProbeManager = nil
	kubelet.Deps().Recorder = k
	kubelet.Deps().OSInterface = k.config.Crid().DefaultBackend()
	kubelet.Deps().Mounter = k.config.Crid().DefaultBackend()
	kubelet.Deps().Subpather = k.config.Crid().DefaultBackend()
	kubelet.Deps().HostUtil = k.config.Crid().DefaultBackend()
	kubelet.Deps().TLSOptions = &server.TLSOptions{
		Config: &tls.Config{
			NextProtos: func() []string {
				if !v1.HTTP2 {
					return []string{"http/1.1"}
				}
				return []string{"h2", "http/1.1"}
			}(),
			MinVersion: func() uint16 {
				if v, err := cliflag.TLSVersion(kubelet.Configuration().TLSMinVersion); err == nil {
					return v
				}
				return cliflag.DefaultTLSVersion()
			}(),
			CipherSuites: func() []uint16 {
				if v, err := cliflag.TLSCipherSuites(kubelet.Configuration().TLSCipherSuites); err == nil {
					return v
				}
				return nil
			}(),
		},
		CertFile: filepath.Join(string(k.config.Options().DataDir()), string(v1.CertFile)),
		KeyFile:  filepath.Join(string(k.config.Options().DataDir()), string(v1.KeyFile)),
	}
	k.kubelet = kubelet
	close(k.kubeletProvided)
	return k
}

func (k *KubeImpl) Kubelet() v1.Kubelet {
	<-k.kubeletProvided
	return k.kubelet
}

func (k *KubeImpl) WithApiServer(apiserver v1.ApiServer) v1.Kube {
	apiserver.Config().KubeAPIs.ControlPlane.StorageFactory = k.StorageFactory()
	// TODO: set apiserver config on kubelet
	k.apiserver = apiserver
	close(k.apiserverProvided)
	return k
}

func (k *KubeImpl) ApiServer() v1.ApiServer {
	<-k.apiserverProvided
	return k.apiserver
}

func (k *KubeImpl) WithStorageFactory(storagefactory v1.StorageFactory) v1.Kube {
	k.storagefactory = storagefactory
	close(k.storagefactoryProvided)
	return k
}

func (k *KubeImpl) StorageFactory() v1.StorageFactory {
	<-k.storagefactoryProvided
	return k.storagefactory
}

func (k *KubeImpl) writeStaticPods() error {
	path := k.config.Options().DataDirAt(v1.DataDirStaticPods)
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
		object.GetObjectKind().GroupVersionKind().Kind == "Node" &&
		eventtype == corev1.EventTypeNormal &&
		reason == "NodeReady" {
		if ref, ok := object.(*corev1.ObjectReference); ok {
			if ref.Name == k.KubeletTunnel().Hostname() {
				k.nodeReadyOnce.Do(func() {
					k.nodeReady <- ref
					close(k.nodeReady)
				})
			}
		}
	}
}

func (k *KubeImpl) Logf(object runtime.Object, eventtype string, reason string, messageFmt string, args ...interface{}) {
	logFunc := nanokube.Log.Debug
	switch eventtype {
	case corev1.EventTypeNormal:
		logFunc = nanokube.Log.Info
	case corev1.EventTypeWarning:
		logFunc = nanokube.Log.Warn
	}
	kv := []interface{}{"gvk", object.GetObjectKind().GroupVersionKind().String(), "reason", reason}
	if ref, ok := object.(*corev1.ObjectReference); ok {
		kv = append(kv, "namespace", ref.Namespace, "name", ref.Name)
	}
	logFunc(fmt.Sprintf(messageFmt, args...), kv...)
}

func (k *KubeImpl) NodeReady() chan *corev1.ObjectReference {
	return k.nodeReady
}
