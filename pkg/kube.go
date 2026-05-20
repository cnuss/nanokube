package pkg

import (
	"bytes"
	"context"
	"crypto/tls"
	_ "embed"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	_ "unsafe"

	"github.com/cnuss/nanokube/pkg/nanokube"
	v1 "github.com/cnuss/nanokube/pkg/v1"
	noopoteltrace "go.opentelemetry.io/otel/trace/noop"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	apifeatures "k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	"k8s.io/apiserver/pkg/server/options"
	storage "k8s.io/apiserver/pkg/server/storage"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/cert"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/klog/v2"
	apiserveroptions "k8s.io/kubernetes/cmd/kube-apiserver/app/options"
	kubeletoptions "k8s.io/kubernetes/cmd/kubelet/app/options"
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
}

type KubeImpl struct {
	ctx     context.Context
	kubelet v1.Kubelet

	kubeletFlags     *kubeletoptions.KubeletFlags
	kubeletFlagsOnce sync.Once

	kubeletConfiguration     *kubeletconfig.KubeletConfiguration
	kubeletConfigurationOnce sync.Once

	kubeletDependencies     *kubelet.Dependencies
	kubeletDependenciesOnce sync.Once

	apiServerOptions     *apiserveroptions.CompletedOptions
	apiServerOptionsOnce sync.Once

	defaultStorageFactory     *storage.DefaultStorageFactory
	defaultStorageFactoryOnce sync.Once

	broadcaster     record.EventBroadcaster
	broadcasterOnce sync.Once

	eventsOnce sync.Once

	nodeReady     chan struct{}
	nodeReadyOnce sync.Once

	informerFactory     informers.SharedInformerFactory
	informerFactoryOnce sync.Once

	staticPods     []*corev1.Pod
	staticPodsOnce sync.Once

	proxiedRecorder     record.EventRecorder
	proxiedRecorderOnce sync.Once

	tlsConfig     *tls.Config
	tlsConfigOnce sync.Once

	tlsOptions     *kubeletserver.TLSOptions
	tlsOptionsOnce sync.Once

	secureServing     *options.SecureServingOptionsWithLoopback
	secureServingOnce sync.Once

	listener     net.Listener
	listenerOnce sync.Once

	certKeyOnce sync.Once
}

var _ v1.Kube = &KubeImpl{}

func newKube(kubelet v1.Kubelet) *KubeImpl {
	if err := utilfeature.DefaultMutableFeatureGate.SetFromMap(FeatureGates); err != nil {
		kubelet.Cancel(nanokube.NewError(fmt.Errorf("failed to set feature gates: %w", err)))
		return nil
	}

	stopCh := make(chan struct{})
	wait.NeverStop = stopCh
	go func() {
		<-kubelet.Canceled()
		close(stopCh)
	}()

	klog.OsExit = func(code int) {
		kubelet.Cancel(nanokube.NewError(fmt.Errorf("klog exit")).WithCode(code))
	}

	kube := &KubeImpl{
		ctx:       kubelet.Context(),
		kubelet:   kubelet,
		nodeReady: make(chan struct{}),
	}

	return kube
}

func (k *KubeImpl) Kubelet() v1.Kubelet {
	return k.kubelet
}

func (k *KubeImpl) Broadcaster() record.EventBroadcaster {
	k.broadcasterOnce.Do(func() {
		k.broadcaster = record.NewBroadcaster(record.WithContext(k.Kubelet().Context()))
	})
	return k.broadcaster
}

func (k *KubeImpl) InformerFactory() informers.SharedInformerFactory {
	k.informerFactoryOnce.Do(func() {
		k.informerFactory = informers.NewSharedInformerFactory(k.Kubelet().Client(), 0)
	})
	return k.informerFactory
}

func (k *KubeImpl) ApiServerOptions() *apiserveroptions.CompletedOptions {
	k.apiServerOptionsOnce.Do(func() {
		tunnel := k.Kubelet().Tunnel(v1.APIServerTunnel)
		opts := apiserveroptions.NewServerRunOptions()
		opts.Authentication.ServiceAccounts.Issuers = []string{fmt.Sprintf("https://%s", tunnel.FQDN())}
		opts.Authentication.ServiceAccounts.KeyFiles = []string{k.KeyFilePath()}
		opts.Authorization.Modes = []string{"Node", "RBAC"}
		opts.EndpointReconcilerType = "none" // TODO(partial): manage kubernetes service
		opts.Etcd.StorageConfig.Transport.ServerList = k.Kubelet().Storage().ServerList()
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

		complete, err := opts.Complete(k.Kubelet().Context())
		if err != nil {
			k.Kubelet().Cancel(nanokube.NewError(fmt.Errorf("failed to complete apiserver options: %w", err)))
			return
		}

		errs := complete.Validate()
		if len(errs) > 0 {
			k.Kubelet().Cancel(nanokube.NewError(fmt.Errorf("failed to validate apiserver options: %v", errs)))
			return
		}

		nanokube.Log.Info("apiserver configured", "fqdn", opts.GenericServerRunOptions.ExternalHost)
		k.apiServerOptions = &complete
	})
	return k.apiServerOptions
}

func (k *KubeImpl) KubeletHostname() string {
	tunnel := k.Kubelet().Tunnel(v1.KubeletTunnel)
	return tunnel.Hostname()
}

func (k *KubeImpl) KubeletFlags() *kubeletoptions.KubeletFlags {
	k.kubeletFlagsOnce.Do(func() {
		tunnel := k.Kubelet().Tunnel(v1.KubeletTunnel)
		k.kubeletFlags = kubeletoptions.NewKubeletFlags()
		k.kubeletFlags.CloudProvider = "external"
		k.kubeletFlags.HostnameOverride = k.KubeletHostname()
		k.kubeletFlags.NodeLabels = make(map[string]string) // TODO(incomplete): add labels
		k.kubeletFlags.NodeIP = tunnel.LocalIP().String()
		k.kubeletFlags.RootDirectory = k.Kubelet().Options().DataDirAt(v1.DataDirKubelet)
	})
	return k.kubeletFlags
}

func (k *KubeImpl) KubeletConfiguration() *kubeletconfig.KubeletConfiguration {
	k.kubeletConfigurationOnce.Do(func() {
		tunnel := k.Kubelet().Tunnel(v1.KubeletTunnel)
		if cfg, err := kubeletoptions.NewKubeletConfiguration(); err == nil {
			k.kubeletConfiguration = cfg
		} else {
			k.kubeletConfiguration = &kubeletconfig.KubeletConfiguration{}
		}
		k.kubeletConfiguration.Address = tunnel.LocalIP().String()
		k.kubeletConfiguration.ClusterDomain = tunnel.Domain()
		// TODO(incomplete): probe a container to get resolv.conf
		k.kubeletConfiguration.ClusterDNS = []string{"1.1.1.1"}
		k.kubeletConfiguration.PodLogsDir = k.Kubelet().Options().DataDirAt(v1.DataDirLogs)
		k.kubeletConfiguration.Port = tunnel.LocalPort()
		k.kubeletConfiguration.ReadOnlyPort = 0
		k.kubeletConfiguration.RegisterNode = true
		k.kubeletConfiguration.StaticPodPath = k.Kubelet().Options().DataDirAt(v1.DataDirStaticPods)
	})
	return k.kubeletConfiguration
}

func (k *KubeImpl) KubeletDependencies() *kubelet.Dependencies {
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
		k.kubeletDependencies.KubeClient = k.Kubelet().Client()
		k.kubeletDependencies.HeartbeatClient = k.Kubelet().Client()
		k.kubeletDependencies.RemoteRuntimeService = k.Kubelet().DefaultBackend().Driver()
		k.kubeletDependencies.RemoteImageService = k.Kubelet().DefaultBackend().Driver()
		k.kubeletDependencies.CAdvisorInterface = k.Kubelet().DefaultBackend()
		k.kubeletDependencies.ContainerManager = k.Kubelet().DefaultBackend().Manager()
		k.kubeletDependencies.TLSOptions = k.TLSOptions()
		k.kubeletDependencies.ProbeManager = nil
		// TODO(1.36): Dependencies.Services field was removed; backend WebServices need a new mount point
		k.kubeletDependencies.VolumePlugins = k.Kubelet().Host().VolumePlugins()
		k.kubeletDependencies.OSInterface = k.Kubelet().Host()
		k.kubeletDependencies.Mounter = k.Kubelet().Host()
		k.kubeletDependencies.Subpather = k.Kubelet().Host()
		k.kubeletDependencies.HostUtil = k.Kubelet().Host()
		k.kubeletDependencies.Recorder = k
	})
	return k.kubeletDependencies
}

func (k *KubeImpl) Args(service v1.ServiceName, mountPath string) []string {
	rootCaFile := func() string {
		var buf bytes.Buffer
		for _, c := range k.Kubelet().ApiServer().CACerts() {
			if err := pem.Encode(&buf, &pem.Block{Type: "CERTIFICATE", Bytes: c.Raw}); err != nil {
				k.Kubelet().Cancel(nanokube.NewError(fmt.Errorf("failed to PEM-encode CA cert: %w", err)))
				return ""
			}
		}
		path := k.Kubelet().Options().FilePathAt(v1.CAFile)
		if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
			k.Kubelet().Cancel(nanokube.NewError(fmt.Errorf("failed to write CA file %s: %w", path, err)))
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

func (k *KubeImpl) Environ() []string {
	return []string{
		"KUBERNETES_SERVICE_HOST=" + k.Kubelet().Tunnel(v1.APIServerTunnel).FQDN(),
		"KUBERNETES_SERVICE_PORT=443",
	}
}

func (k *KubeImpl) recorder() record.EventRecorder {
	k.proxiedRecorderOnce.Do(func() {
		tunnel := k.Kubelet().Tunnel(v1.KubeletTunnel)
		k.proxiedRecorder = k.Broadcaster().NewRecorder(scheme.Scheme, corev1.EventSource{Component: k.Kubelet().Options().Name(), Host: tunnel.FQDN()})
	})
	return k.proxiedRecorder
}

func (k *KubeImpl) AnnotatedEventf(object runtime.Object, annotations map[string]string, eventtype string, reason string, messageFmt string, args ...interface{}) {
	k.Logf(object, eventtype, reason, messageFmt, args...)
	k.recorder().AnnotatedEventf(object, annotations, eventtype, reason, messageFmt, args...)
}

func (k *KubeImpl) Event(object runtime.Object, eventtype string, reason string, message string) {
	k.Logf(object, eventtype, reason, "%s", message)
	k.recorder().Event(object, eventtype, reason, message)
}

func (k *KubeImpl) WithLogger(klog.Logger) record.EventRecorderLogger {
	return k
}

func (k *KubeImpl) Eventf(object runtime.Object, eventtype string, reason string, messageFmt string, args ...interface{}) {
	k.Logf(object, eventtype, reason, messageFmt, args...)
	k.recorder().Eventf(object, eventtype, reason, messageFmt, args...)

	// 14:01:42 INF Eventf group="" version=v1 kind=Node type=Normal reason=NodeReady message="Node depends-location-assessments-silence.trycloudflare.com status is now: NodeReady"
	if object.GetObjectKind().GroupVersionKind().Group == "" &&
		object.GetObjectKind().GroupVersionKind().Version == "v1" &&
		object.GetObjectKind().GroupVersionKind().Kind == "Node" &&
		eventtype == corev1.EventTypeNormal &&
		reason == "NodeReady" {
		if ref, ok := object.(*corev1.ObjectReference); ok {
			if strings.EqualFold(k.KubeletHostname(), ref.Name) {
				k.nodeReadyOnce.Do(func() {
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
	if eventtype == corev1.EventTypeWarning && k.Kubelet().Options().Verbosity() >= 2 {
		kv = append(kv, "stack", string(debug.Stack()))
	}
	logFunc(fmt.Sprintf(messageFmt, args...), kv...)
}

func (k *KubeImpl) NodeReady() chan struct{} {
	return k.nodeReady
}

func (k *KubeImpl) SecureServing() *options.SecureServingOptionsWithLoopback {
	k.secureServingOnce.Do(func() {
		tunnel := k.Kubelet().Tunnel(v1.APIServerTunnel)
		listener, err := net.Listen("tcp", net.JoinHostPort(tunnel.LocalIP().String(), strconv.FormatUint(uint64(tunnel.LocalPort()), 10)))
		if err != nil {
			k.Kubelet().Cancel(nanokube.NewError(fmt.Errorf("failed to create listener: %w", err)).WithCode(1))
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

func (k *KubeImpl) TLSConfig() *tls.Config {
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
		si := k.Kubelet().ApiServer().Server().SecureServingInfo
		dyn := dynamiccertificates.NewDynamicServingCertificateController(tlsConfig, nil, si.Cert, si.SNICerts, nil)
		si.Cert.AddListener(dyn)
		dyn.RunOnce()

		tlsConfig.GetConfigForClient = dyn.GetConfigForClient
		k.tlsConfig = tlsConfig
	})
	return k.tlsConfig
}

func (k *KubeImpl) TLSOptions() *kubeletserver.TLSOptions {
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

func (k *KubeImpl) CertFilePath() string {
	certPath, _ := k.ensureCertKey()
	return certPath
}

func (k *KubeImpl) KeyFilePath() string {
	_, keyPath := k.ensureCertKey()
	return keyPath
}

func (k *KubeImpl) ensureCertKey() (string, string) {
	certPath := k.Kubelet().Options().FilePathAt(v1.CertFile)
	keyPath := k.Kubelet().Options().FilePathAt(v1.KeyFile)
	k.certKeyOnce.Do(func() {
		// Reuse an existing pair if both files are present (restart case).
		if _, err := os.Stat(certPath); err == nil {
			if _, err := os.Stat(keyPath); err == nil {
				return
			}
		}

		apiserverTunnel := k.Kubelet().Tunnel(v1.APIServerTunnel)
		kubeletTunnel := k.Kubelet().Tunnel(v1.KubeletTunnel)

		certPEM, keyPEM, err := cert.GenerateSelfSignedCertKey(
			"localhost",
			[]net.IP{net.IPv4(127, 0, 0, 1), apiserverTunnel.LocalIP(), kubeletTunnel.LocalIP()},
			[]string{apiserverTunnel.FQDN(), apiserverTunnel.Hostname(), kubeletTunnel.FQDN(), kubeletTunnel.Hostname()},
		)
		if err != nil {
			k.Kubelet().Cancel(nanokube.NewError(fmt.Errorf("generate self-signed cert: %w", err)))
			return
		}
		if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
			k.Kubelet().Cancel(nanokube.NewError(fmt.Errorf("write cert %s: %w", certPath, err)))
			return
		}
		if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
			k.Kubelet().Cancel(nanokube.NewError(fmt.Errorf("write key %s: %w", keyPath, err)))
			return
		}
	})
	return certPath, keyPath
}
