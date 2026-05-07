package pkg

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"time"
	_ "unsafe"

	"github.com/cnuss/nanokube/pkg/nanokube"
	v1 "github.com/cnuss/nanokube/pkg/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	storage "k8s.io/apiserver/pkg/server/storage"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	apiserveroptions "k8s.io/kubernetes/cmd/kube-apiserver/app/options"
	"k8s.io/kubernetes/pkg/features"
)

//go:linkname nodeReadyGracePeriod k8s.io/kubernetes/pkg/kubelet.nodeReadyGracePeriod
var nodeReadyGracePeriod time.Duration

func init() {
	nodeReadyGracePeriod = 30 * time.Second // TODO(remove): temporary override for debugging
}

var FeatureGates = map[string]bool{
	string(features.TranslateStreamCloseWebsocketRequests): false,
	string(features.PortForwardWebsockets):                 false,
	string(features.KubeletInUserNamespace):                true,
}

type KubeImpl struct {
	ctx    context.Context
	config v1.Config

	apiServerOptions     *apiserveroptions.CompletedOptions
	apiServerOptionsOnce sync.Once

	defaultStorageFactory     *storage.DefaultStorageFactory
	defaultStorageFactoryOnce sync.Once

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

	informerFactory     informers.SharedInformerFactory
	informerFactoryOnce sync.Once
}

var _ v1.Kube = &KubeImpl{}

func newKube(config v1.Config) *KubeImpl {
	if err := utilfeature.DefaultMutableFeatureGate.SetFromMap(FeatureGates); err != nil {
		klog.Fatalf("Failed to set feature gates: %v", err)
	}

	stopCh := make(chan struct{})
	wait.NeverStop = stopCh
	go func() {
		<-config.Canceled()
		close(stopCh)
	}()

	klog.OsExit = func(code int) {
		config.Cancel(nanokube.NewError(fmt.Errorf("klog exit")).WithCode(code))
	}

	kube := &KubeImpl{
		ctx:                    config.Context(),
		config:                 config,
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

func (k *KubeImpl) InformerFactory() informers.SharedInformerFactory {
	k.informerFactoryOnce.Do(func() {
		k.informerFactory = informers.NewSharedInformerFactory(k.Client(), 0)
	})
	return k.informerFactory
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

//go:embed kube-system.yaml
var kubeSystem string

func (k *KubeImpl) bindKubelet(c *ConfigImpl) {
	// convert kubeSystemManifest into v1.Pod
	pod := &corev1.Pod{}
	if err := runtime.DecodeInto(scheme.Codecs.UniversalDecoder(), []byte(kubeSystem), pod); err != nil {
		klog.Fatalf("Failed to decode kube-system manifest: %v", err)
	}

	for i := range pod.Spec.Containers {
		container := &pod.Spec.Containers[i]
		if container.Args == nil {
			container.Args = []string{}
		}
		container.Args = append(container.Args, k.Args(v1.ServiceName(container.Name))...)
	}

	json, err := json.Marshal(pod)
	if err != nil {
		klog.Fatalf("Failed to marshal kube-system pod: %v", err)
	}
	if err := os.WriteFile(filepath.Join(c.kubeletConfiguration.StaticPodPath, "kube-system.yaml"), json, 0o644); err != nil {
		klog.Fatalf("Failed to write static pod manifest: %v", err)
	}

	k.recorder = k.broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: k.Config().Options().Name(), Host: c.Tunnel(v1.KubeletService).FQDN()})
	close(k.recorderProvided)

	c.kubeletDependencies.Recorder = k
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

func (k *KubeImpl) Args(service v1.ServiceName) []string {
	rootCaFile := func() string {
		var buf bytes.Buffer
		for _, cert := range k.ApiServer().CACerts() {
			if err := pem.Encode(&buf, &pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}); err != nil {
				klog.Fatalf("Failed to PEM-encode CA cert: %v", err)
			}
		}
		path := filepath.Join(string(k.Config().Options().DataDir()), string(v1.CAFile))
		if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
			klog.Fatalf("Failed to write CA file %s: %v", path, err)
		}
		return string(v1.CAFile)
	}

	switch service {
	case v1.ControllerManagerService:
		return []string{
			"--root-ca-file=/home/nanokube/" + rootCaFile(),
		}
	}
	return []string{}
}

func (k *KubeImpl) Client() v1.Client {
	return k.ApiServer().Client(k.ctx)
}

func (k *KubeImpl) Environ() []string {
	return []string{
		"KUBERNETES_SERVICE_HOST=" + k.ApiServer().Tunnel().FQDN(),
		"KUBERNETES_SERVICE_PORT=443",
	}
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
			tunnel := k.config.Tunnel(v1.KubeletService)
			filterNames := []string{
				tunnel.Hostname(),
				tunnel.FQDN(),
				tunnel.LocalHostname(),
				tunnel.LocalFQDN(),
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
	if eventtype == corev1.EventTypeWarning && k.config.Options().Verbosity() >= 2 {
		kv = append(kv, "stack", string(debug.Stack()))
	}
	logFunc(fmt.Sprintf(messageFmt, args...), kv...)
}

func (k *KubeImpl) NodeReady() chan *corev1.ObjectReference {
	return k.nodeReady
}
