package pkg

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/cnuss/nanokube/pkg/nanokube"
	storage "k8s.io/apiserver/pkg/server/storage"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	apiserveroptions "k8s.io/kubernetes/cmd/kube-apiserver/app/options"
	kubeletoptions "k8s.io/kubernetes/cmd/kubelet/app/options"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/server"
	"k8s.io/kubernetes/pkg/kubemark"
)

var FeatureGates = map[string]bool{
	"KubeletInUserNamespace": true,
}

type Kube interface {
	ApiServerOptions() *apiserveroptions.CompletedOptions
	Client() nanokube.Client

	KubeletFlags() *kubeletoptions.KubeletFlags
	KubeletConfiguration() *kubeletconfig.KubeletConfiguration

	TLSOptions() *server.TLSOptions
	Recorder() record.EventRecorder

	WithKubelet(kubelet *kubemark.HollowKubelet) Kube
	Kubelet() *kubemark.HollowKubelet
	WithApiServer(apiserver nanokube.ApiServer) Kube
	ApiServer() nanokube.ApiServer
	WithStorageFactory(storagefactory nanokube.StorageFactory) Kube
	StorageFactory() nanokube.StorageFactory
}

type KubeImpl struct {
	ctx    context.Context
	config Config

	apiServerOptions     *apiserveroptions.CompletedOptions
	apiServerOptionsOnce sync.Once

	apiServerTunnel     Tunnel
	apiServerTunnelOnce sync.Once

	kubeletTunnel     Tunnel
	kubeletTunnelOnce sync.Once

	kubeletFlags     *kubeletoptions.KubeletFlags
	kubeletFlagsOnce sync.Once

	kubeletConfig     *kubeletconfig.KubeletConfiguration
	kubeletConfigOnce sync.Once

	tlsOptions     *server.TLSOptions
	tlsOptionsOnce sync.Once

	recorder     record.EventRecorder
	recorderOnce sync.Once

	defaultStorageFactory     *storage.DefaultStorageFactory
	defaultStorageFactoryOnce sync.Once

	kubelet         *kubemark.HollowKubelet
	kubeletProvided chan struct{}

	apiserver         nanokube.ApiServer
	apiserverProvided chan struct{}

	storagefactory         nanokube.StorageFactory
	storagefactoryProvided chan struct{}
}

var _ Kube = &KubeImpl{}

func newKube(config Config) Kube {
	kube := &KubeImpl{
		ctx:                    config.Context(),
		config:                 config,
		kubeletProvided:        make(chan struct{}),
		apiserverProvided:      make(chan struct{}),
		storagefactoryProvided: make(chan struct{}),
	}

	return kube
}

func (k *KubeImpl) ApiServerOptions() *apiserveroptions.CompletedOptions {
	k.apiServerOptionsOnce.Do(func() {
		opts := apiserveroptions.NewServerRunOptions()
		opts.Authentication.ServiceAccounts.Issuers = []string{fmt.Sprintf("https://%s:%d", k.ApiServerTunnel().Hostname(), k.ApiServerTunnel().Port())}
		opts.Authentication.ServiceAccounts.KeyFiles = []string{k.config.Options().FilePathAt(nanokube.DataDirCerts, nanokube.KeyFile)}
		opts.Authorization.Modes = []string{"Node", "RBAC"}
		opts.EndpointReconcilerType = "none" // TODO(partial): manage kubernetes service
		opts.Etcd.StorageConfig.Transport.ServerList = k.StorageFactory().ServerList()
		opts.GenericServerRunOptions.ExternalHost = k.ApiServerTunnel().Hostname()
		opts.GenericServerRunOptions.ShutdownDelayDuration = 0
		opts.SecureServing.BindAddress = net.ParseIP("0.0.0.0")
		opts.SecureServing.BindPort = k.ApiServerTunnel().Port()
		opts.SecureServing.DisableHTTP2Serving = true
		opts.SecureServing.ServerCert.CertDirectory = k.config.Options().DataDirAt(nanokube.DataDirCerts)
		opts.ServiceAccountSigningKeyFile = k.config.Options().FilePathAt(nanokube.DataDirCerts, nanokube.KeyFile)
		opts.ServiceClusterIPRanges = "10.0.0.0/16" // TODO

		complete, err := opts.Complete(k.config.Context())
		if err != nil {
			klog.Fatalf("Failed to complete apiserver options: %v", err)
		}

		errs := complete.Validate()
		if len(errs) > 0 {
			klog.Fatalf("Failed to validate apiserver options: %v", errs)
		}
		k.apiServerOptions = &complete
	})
	return k.apiServerOptions
}

func (k *KubeImpl) ApiServerTunnel() Tunnel {
	k.apiServerTunnelOnce.Do(func() {
		k.apiServerTunnel = k.config.Tunnel()
	})
	return k.apiServerTunnel
}

func (k *KubeImpl) KubeletTunnel() Tunnel {
	k.kubeletTunnelOnce.Do(func() {
		k.kubeletTunnel = k.config.Tunnel()
	})
	return k.kubeletTunnel
}

func (k *KubeImpl) KubeletFlags() *kubeletoptions.KubeletFlags {
	k.kubeletFlagsOnce.Do(func() {
		k.kubeletFlags = kubeletoptions.NewKubeletFlags()
		k.kubeletFlags.RootDirectory = k.config.Options().DataDirAt(nanokube.DataDirKubelet)
		k.kubeletFlags.CertDirectory = k.config.Options().DataDirAt(nanokube.DataDirCerts)
		k.kubeletFlags.LockFilePath = k.config.Options().FilePathAt(nanokube.DataDirLock, nanokube.KubeletLock)
		k.kubeletFlags.HostnameOverride = k.KubeletTunnel().Hostname()
	})
	return k.kubeletFlags
}

func (k *KubeImpl) KubeletConfiguration() *kubeletconfig.KubeletConfiguration {
	k.kubeletConfigOnce.Do(func() {
		config, err := kubeletoptions.NewKubeletConfiguration()
		if err != nil {
			klog.Fatalf("Failed to create kubelet configuration: %v", err)
		}
		config.PodLogsDir = k.config.Options().DataDirAt(nanokube.DataDirLogs)
		config.ReadOnlyPort = 0
		k.kubeletConfig = config
	})
	return k.kubeletConfig
}

func (k *KubeImpl) Client() nanokube.Client {
	return k.ApiServer().Client(k.ctx)
}

func (k *KubeImpl) TLSOptions() *server.TLSOptions {
	k.tlsOptionsOnce.Do(func() {
		k.tlsOptions = nil
	})
	return k.tlsOptions
}

func (k *KubeImpl) Recorder() record.EventRecorder {
	k.recorderOnce.Do(func() {
		k.recorder = nil
	})
	return k.recorder
}

func (k *KubeImpl) WithKubelet(kubelet *kubemark.HollowKubelet) Kube {
	kubelet.KubeletDeps.ProbeManager = nil
	kubelet.KubeletDeps.TLSOptions = k.TLSOptions()
	kubelet.KubeletDeps.Recorder = k.Recorder()
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
