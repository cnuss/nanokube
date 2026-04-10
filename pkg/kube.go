package pkg

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/cnuss/nanokube/pkg/nanokube"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	client "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	kubeletoptions "k8s.io/kubernetes/cmd/kubelet/app/options"
	apiserveroptions "k8s.io/kubernetes/pkg/controlplane/apiserver/options"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/server"
	"k8s.io/kubernetes/pkg/kubemark"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/kubernetes/pkg/volume/util/subpath"
	"k8s.io/mount-utils"
)

var FeatureGates = map[string]bool{
	"KubeletInUserNamespace": true,
}

type Kube interface {
	KubeletFlags() *kubeletoptions.KubeletFlags
	KubeletConfiguration() *kubeletconfig.KubeletConfiguration
	ApiServerOptions() *apiserveroptions.Options

	Client() *client.Clientset
	HeartbeatClient() *client.Clientset

	TLSOptions() *server.TLSOptions
	Recorder() record.EventRecorder
	OSInterface() container.OSInterface
	Mounter() mount.Interface
	Subpather() subpath.Interface
	HostUtil() hostutil.HostUtils

	WithKubelet(kubelet *kubemark.HollowKubelet) Kube
	Kubelet() *kubemark.HollowKubelet
	WithApiServer(apiserver *nanokube.HollowApiServer) Kube
	ApiServer() *nanokube.HollowApiServer
	WithStorageFactory(storageFactory nanokube.StorageFactory) Kube
	StorageFactory() nanokube.StorageFactory
}

type KubeImpl struct {
	ctx    context.Context
	config Config

	kubeletTunnel     Tunnel
	kubeletTunnelOnce sync.Once

	kubeletFlags     *kubeletoptions.KubeletFlags
	kubeletFlagsOnce sync.Once

	kubeletConfig     *kubeletconfig.KubeletConfiguration
	kubeletConfigOnce sync.Once

	apiserverOptions     *apiserveroptions.Options
	apiserverOptionsOnce sync.Once

	client     *client.Clientset
	clientOnce sync.Once

	heartbeatClient     *client.Clientset
	heartbeatClientOnce sync.Once

	tlsOptions     *server.TLSOptions
	tlsOptionsOnce sync.Once

	recorder     record.EventRecorder
	recorderOnce sync.Once

	osInterface     container.OSInterface
	osInterfaceOnce sync.Once

	mounter     mount.Interface
	mounterOnce sync.Once

	subpather     subpath.Interface
	subpatherOnce sync.Once

	hostUtil     hostutil.HostUtils
	hostUtilOnce sync.Once

	kubelet         *kubemark.HollowKubelet
	kubeletProvided chan struct{}

	apiserver         *nanokube.HollowApiServer
	apiserverProvided chan struct{}

	storageFactory         nanokube.StorageFactory
	storageFactoryProvided chan struct{}
}

var _ Kube = &KubeImpl{}

func newKube(config Config) Kube {
	// Override wait.NeverStop
	stopCh := make(chan struct{})
	wait.NeverStop = stopCh
	go func() {
		<-config.Context().Done()
		close(stopCh)
	}()

	// Override klog logging
	// klog.SetLogger(config.Logger("klog"))
	klog.OsExit = func(code int) {
		config.Cancel(NewFatalError(fmt.Errorf("klog exited with code %d", code)).WithCode(code))
		runtime.Goexit()
	}

	kube := &KubeImpl{
		ctx:                    config.Context(),
		config:                 config,
		kubeletProvided:        make(chan struct{}),
		apiserverProvided:      make(chan struct{}),
		storageFactoryProvided: make(chan struct{}),
	}

	return kube
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
		// k.kubeletFlags.HostnameOverride = k.KubeletTunnel().Hostname()
	})
	return k.kubeletFlags
}

func (k *KubeImpl) KubeletConfiguration() *kubeletconfig.KubeletConfiguration {
	k.kubeletConfigOnce.Do(func() {
		k.kubeletConfig = &kubeletconfig.KubeletConfiguration{}
		k.kubeletConfig.PodLogsDir = k.config.Options().DataDirAt(nanokube.DataDirLogs)
		k.kubeletConfig.SyncFrequency = v1.Duration{Duration: 10 * time.Second}
	})
	return k.kubeletConfig
}

func (k *KubeImpl) ApiServerOptions() *apiserveroptions.Options {
	k.apiserverOptionsOnce.Do(func() {
		k.apiserverOptions = &apiserveroptions.Options{}
	})
	return k.apiserverOptions
}

func (k *KubeImpl) Client() *client.Clientset {
	k.clientOnce.Do(func() {
		k.client = nil
	})
	return k.client
}

func (k *KubeImpl) HeartbeatClient() *client.Clientset {
	k.heartbeatClientOnce.Do(func() {
		k.heartbeatClient = nil
	})
	return k.heartbeatClient
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

func (k *KubeImpl) OSInterface() container.OSInterface {
	k.osInterfaceOnce.Do(func() {
		k.osInterface = nil
	})
	return k.osInterface
}

func (k *KubeImpl) Mounter() mount.Interface {
	k.mounterOnce.Do(func() {
		k.mounter = nil
	})
	return k.mounter
}

func (k *KubeImpl) Subpather() subpath.Interface {
	k.subpatherOnce.Do(func() {
		k.subpather = nil
	})
	return k.subpather
}

func (k *KubeImpl) HostUtil() hostutil.HostUtils {
	k.hostUtilOnce.Do(func() {
		k.hostUtil = nil
	})
	return k.hostUtil
}

func (k *KubeImpl) WithKubelet(kubelet *kubemark.HollowKubelet) Kube {
	kubelet.KubeletDeps.ProbeManager = nil
	kubelet.KubeletDeps.TLSOptions = k.TLSOptions()
	kubelet.KubeletDeps.Recorder = k.Recorder()
	kubelet.KubeletDeps.OSInterface = k.OSInterface()
	kubelet.KubeletDeps.Mounter = k.Mounter()
	kubelet.KubeletDeps.Subpather = k.Subpather()
	kubelet.KubeletDeps.HostUtil = k.HostUtil()
	k.kubelet = kubelet
	close(k.kubeletProvided)
	return k
}

func (k *KubeImpl) Kubelet() *kubemark.HollowKubelet {
	<-k.kubeletProvided
	return k.kubelet
}

func (k *KubeImpl) WithApiServer(apiserver *nanokube.HollowApiServer) Kube {
	// TODO Build configs
	k.apiserver = apiserver
	close(k.apiserverProvided)
	return k
}

func (k *KubeImpl) ApiServer() *nanokube.HollowApiServer {
	<-k.apiserverProvided
	return k.apiserver
}

func (k *KubeImpl) WithStorageFactory(storageFactory nanokube.StorageFactory) Kube {
	k.storageFactory = storageFactory
	close(k.storageFactoryProvided)
	return k
}

func (k *KubeImpl) StorageFactory() nanokube.StorageFactory {
	<-k.storageFactoryProvided
	return k.storageFactory
}
