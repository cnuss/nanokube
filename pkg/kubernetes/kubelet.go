package kubernetes

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/cnuss/nanokube/pkg/crid"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubernetes/cmd/kubelet/app/options"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubemark"
)

type Kubelet struct {
	ctx          context.Context
	crid         *crid.CRID
	log          component.Logger
	featureGates map[string]bool

	flags     *options.KubeletFlags
	flagsOnce sync.Once

	config     *kubeletconfig.KubeletConfiguration
	configOnce sync.Once

	client        *clientset.Clientset
	clientOnce    sync.Once
	heartbeat     *clientset.Clientset
	heartbeatOnce sync.Once
}

func NewKubelet(crid *crid.CRID, featureGates map[string]bool) *Kubelet {
	k := &Kubelet{
		ctx:          crid.Context(),
		crid:         crid,
		featureGates: featureGates,
		log:          component.NewLogger("kubelet"),
	}
	go k.KubeClient()
	return k
}

func (k *Kubelet) Flags() *options.KubeletFlags {
	k.flagsOnce.Do(func() {
		k.log.Info().Msg("initializing flags")
		hostInfo, _ := k.crid.DefaultBackend().HostInfo()
		dataDirs := k.crid.DataDirs()

		k.flags = options.NewKubeletFlags()
		k.flags.RootDirectory = dataDirs.Root
		k.flags.CertDirectory = dataDirs.PKI
		k.flags.HostnameOverride = hostInfo.Hostname
		k.flags.MaxContainerCount = 100
		k.flags.MaxPerPodContainerCount = 2
		k.flags.NodeLabels = map[string]string{} // TODO: Probe
	})
	return k.flags
}

func (k *Kubelet) Config() *kubeletconfig.KubeletConfiguration {
	k.configOnce.Do(func() {
		k.log.Info().Msg("initializing config")
		hostInfo, _ := k.crid.DefaultBackend().HostInfo()
		dataDirs := k.crid.DataDirs()

		config, err := options.NewKubeletConfiguration()
		if err != nil {
			k.log.Error().Err(err).Msg("failed to create kubelet config")
			return
		}
		config.ImageMinimumGCAge = metav1.Duration{Duration: 1 * time.Minute}
		config.StaticPodPath = dataDirs.Manifests
		config.PodLogsDir = dataDirs.Logs
		config.ClusterDomain = "cluster.local" // TODO: Probe
		config.ClusterDNS = hostInfo.Nameservers
		config.Authentication = kubeletconfig.KubeletAuthentication{
			Anonymous: kubeletconfig.KubeletAnonymousAuthentication{Enabled: true},
			Webhook:   kubeletconfig.KubeletWebhookAuthentication{Enabled: false},
		}
		config.Authorization = kubeletconfig.KubeletAuthorization{
			Mode: kubeletconfig.KubeletAuthorizationModeAlwaysAllow,
		}
		config.TLSCertFile = k.crid.Certs().CertPath()
		config.TLSPrivateKeyFile = k.crid.Certs().KeyPath()
		config.EnableServer = true
		config.Port = 10250
		config.ReadOnlyPort = 0
		config.EnableControllerAttachDetach = true
		config.HairpinMode = kubeletconfig.HairpinVeth
		config.CgroupsPerQOS = false
		config.CgroupDriver = "cgroupfs"
		config.EnforceNodeAllocatable = []string{}
		config.EvictionHard = map[string]string{}
		config.ImageGCHighThresholdPercent = 100
		config.FailSwapOn = false
		config.LocalStorageCapacityIsolation = false
		config.RotateCertificates = false
		config.ServerTLSBootstrap = false
		config.RegisterNode = true
		config.CPUCFSQuota = false
		config.CPUCFSQuotaPeriod = metav1.Duration{Duration: 100 * time.Millisecond}
		config.ContainerLogMaxFiles = 5
		config.ContainerLogMaxWorkers = 1
		config.ContainerLogMonitorInterval = metav1.Duration{Duration: 10 * time.Second}
		config.ProtectKernelDefaults = false
		config.FeatureGates = k.featureGates
		k.config = config
	})
	return k.config
}

// KubeClient blocks until the API server is reachable, then connects
// CRID (event sink, CSI provisioner). Safe to call from a goroutine.
// Returns the client for convenience.
func (k *Kubelet) KubeClient() *clientset.Clientset {
	k.clientOnce.Do(func() {
		k.log.Info().Msg("initializing kube client")
		for {
			restConfig, err := clientcmd.BuildConfigFromFlags("", k.crid.Files().Kubeconfig)
			if err != nil {
				k.log.Debug().Err(err).Msg("kubeconfig not ready")
			} else if client, err := clientset.NewForConfig(restConfig); err != nil {
				k.log.Debug().Err(err).Msg("kube client not ready")
			} else {
				k.log.Info().Msg("kube client ready")
				k.client = client
				k.crid.WithKubeClient(client)
				return
			}
			select {
			case <-k.ctx.Done():
				return
			case <-time.After(1 * time.Second):
			}
		}
	})
	return k.client
}

func (k *Kubelet) HeartbeatClient() *clientset.Clientset {
	k.heartbeatOnce.Do(func() {
		k.log.Info().Msg("initializing heartbeat client")
		config := k.Config()
		restConfig, err := clientcmd.BuildConfigFromFlags("", k.crid.Files().Kubeconfig)
		if err != nil {
			k.log.Error().Err(err).Msg("heartbeat kubeconfig failed")
			return
		}
		restConfig.Timeout = config.NodeStatusUpdateFrequency.Duration
		leaseTimeout := time.Duration(config.NodeLeaseDurationSeconds) * time.Second
		if restConfig.Timeout > leaseTimeout {
			restConfig.Timeout = leaseTimeout
		}
		restConfig.QPS = float32(-1)
		client, err := clientset.NewForConfig(restConfig)
		if err != nil {
			k.log.Error().Err(err).Msg("heartbeat client failed")
			return
		}
		k.heartbeat = client
	})
	return k.heartbeat
}

func (k *Kubelet) Stop() component.Stopped {
	return component.Closed("tcp", "127.0.0.1:10250", nil)
}

func (k *Kubelet) Start(ctx context.Context) (component.Started, error) {
	k.log.Info().Msg("starting kubelet")

	// Build and run HollowKubelet
	hk := kubemark.NewHollowKubelet(
		k.Flags(),
		k.Config(),
		k.KubeClient(),
		k.HeartbeatClient(),
		k.crid.DefaultBackend().Cadvisor(),
		k.crid.DefaultBackend().Images(),
		k.crid.DefaultBackend().Containers(),
		k.crid.DefaultBackend().ContainerManager(),
	)
	hk.KubeletDeps.OSInterface = k.crid.DefaultBackend().OS()
	hk.KubeletDeps.Mounter = k.crid.DefaultBackend().Mounter()
	hk.KubeletDeps.Subpather = k.crid.DefaultBackend().Subpath()
	hk.KubeletDeps.HostUtil = k.crid.DefaultBackend().HostUtils()
	hk.KubeletDeps.Recorder = k.crid.DefaultBackend().EventRecorder()
	hk.KubeletDeps.ProbeManager = k.crid.DefaultBackend().Prober()
	hk.KubeletDeps.TLSOptions = k.crid.TLSOptions()
	exited := make(chan error, 1)
	go func() {
		k.log.Info().Msg("hollow kubelet goroutine starting")
		hk.Run(ctx)
		k.log.Warn().Msg("hollow kubelet goroutine exited")
		exited <- fmt.Errorf("kubelet exited unexpectedly")
	}()

	// Wait for kubelet to be healthy
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err := <-exited:
			return nil, err
		case <-ticker.C:
			reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			req, _ := http.NewRequestWithContext(reqCtx, "GET", "https://127.0.0.1:10250/healthz", nil)
			resp, err := httpClient.Do(req)
			cancel()
			if err != nil {
				k.log.Debug().Err(err).Msg("healthz probe failed")
				continue
			}
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				k.log.Info().Msg("kubelet is ready")
				return component.Ready(), nil
			}
			k.log.Debug().Int("status", resp.StatusCode).Msg("healthz returned non-200")
		}
	}
}
