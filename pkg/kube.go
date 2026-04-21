package pkg

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	_ "unsafe"

	"github.com/cnuss/nanokube/pkg/nanokube"
	v1 "github.com/cnuss/nanokube/pkg/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	storage "k8s.io/apiserver/pkg/server/storage"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	apiserveroptions "k8s.io/kubernetes/cmd/kube-apiserver/app/options"
)

//go:embed kube-system.yaml
var kubeSystemManifest string

//go:linkname nodeReadyGracePeriod k8s.io/kubernetes/pkg/kubelet.nodeReadyGracePeriod
var nodeReadyGracePeriod time.Duration

func init() {
	nodeReadyGracePeriod = 30 * time.Second // TODO(remove): temporary override for debugging
}

var FeatureGates = map[string]bool{
	// Disable apiserver's WebSocket→SPDY translator for exec/attach so the
	// client's WebSocket upgrade is forwarded end-to-end to the kubelet.
	// Cloudflare tunnels pass Upgrade: websocket but strip Upgrade: SPDY/3.1.
	"TranslateStreamCloseWebsocketRequests": false,
	"KubeletInUserNamespace":                true,
}

type KubeImpl struct {
	ctx    context.Context
	config v1.Config

	apiServerOptions     *apiserveroptions.CompletedOptions
	apiServerOptionsOnce sync.Once

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
	if err := utilfeature.DefaultMutableFeatureGate.SetFromMap(FeatureGates); err != nil {
		klog.Fatalf("Failed to set feature gates: %v", err)
	}
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

func (k *KubeImpl) Config() v1.Config {
	return k.config
}

func (k *KubeImpl) ApiServerOptions() *apiserveroptions.CompletedOptions {
	k.apiServerOptionsOnce.Do(func() {
		tunnel := k.Config().Tunnel(v1.APIServerService)
		opts := apiserveroptions.NewServerRunOptions()
		opts.Authentication.ServiceAccounts.Issuers = []string{fmt.Sprintf("https://%s", tunnel.FQDN())}
		opts.Authentication.ServiceAccounts.KeyFiles = []string{filepath.Join(string(k.Config().Options().DataDir()), string(v1.KeyFile))}
		opts.Authorization.Modes = []string{"Node", "RBAC"}
		opts.EndpointReconcilerType = "none" // TODO(partial): manage kubernetes service
		opts.Etcd.StorageConfig.Transport.ServerList = k.StorageFactory().ServerList()
		opts.GenericServerRunOptions.ExternalHost = tunnel.FQDN()
		opts.GenericServerRunOptions.ShutdownDelayDuration = 0
		opts.KubeletConfig.PreferredAddressTypes = []string{
			string(corev1.NodeExternalDNS),
			// string(corev1.NodeInternalIP),
			// string(corev1.NodeInternalDNS),
			// string(corev1.NodeHostName),
		}
		opts.SecureServing.BindAddress = tunnel.LocalIP()
		opts.SecureServing.BindPort = int(tunnel.LocalPort())
		opts.SecureServing.DisableHTTP2Serving = !v1.HTTP2
		opts.SecureServing.ServerCert.CertDirectory = k.Config().Options().DataDirAt(v1.DataDirCerts)
		opts.ServiceAccountSigningKeyFile = filepath.Join(string(k.Config().Options().DataDir()), string(v1.KeyFile))

		complete, err := opts.Complete(k.Config().Context())
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

func (k *KubeImpl) WithKubelet(kubelet v1.Kubelet) v1.Kube {
	if err := os.WriteFile(filepath.Join(kubelet.Configuration().StaticPodPath, "static-pods.yaml"), []byte(kubeSystemManifest), 0o644); err != nil {
		klog.Fatalf("Failed to write static pod manifest: %v", err)
	}

	k.kubelet = kubelet
	close(k.kubeletProvided)

	k.recorder = k.broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: k.Config().Options().Name(), Host: kubelet.Tunnel().FQDN()})
	close(k.recorderProvided)

	kubelet.Dependencies().Recorder = k
	return k
}

func (k *KubeImpl) Kubelet() v1.Kubelet {
	<-k.kubeletProvided
	return k.kubelet
}

func (k *KubeImpl) WithApiServer(apiserver v1.ApiServer) v1.Kube {
	apiserver.Config().KubeAPIs.ControlPlane.StorageFactory = k.StorageFactory()
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

func (k *KubeImpl) Client() v1.Client {
	return k.ApiServer().Client(k.ctx)
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
			filterNames := []string{
				k.Kubelet().Tunnel().Hostname(),
				k.Kubelet().Tunnel().FQDN(),
				k.Kubelet().Tunnel().LocalHostname(),
				k.Kubelet().Tunnel().LocalFQDN(),
			}
			for _, name := range filterNames {
				if strings.ToLower(ref.Name) == strings.ToLower(name) {
					k.nodeReadyOnce.Do(func() {
						k.nodeReady <- ref
						close(k.nodeReady)
					})
					break
				}
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
