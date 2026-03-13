package kubernetes

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"time"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/cnuss/nanokube/pkg/config"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubernetes/cmd/kubelet/app/options"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/server"
	"k8s.io/kubernetes/pkg/kubemark"
)

var kubeletLog = newLogger("kubelet")

type Kubelet struct {
	config *config.Config
}

func NewKubelet(config *config.Config) *Kubelet {
	return &Kubelet{
		config: config,
	}
}

func (k *Kubelet) Stop() component.Stopped {
	return component.Closed("tcp", "127.0.0.1:10250", nil)
}

func (k *Kubelet) Start(ctx context.Context) (component.Started, error) {
	kubeletLog.Info().Msg("starting kubelet")
	hostInfo, err := k.config.CRID.DefaultBackend().HostInfo()
	if err != nil {
		return nil, fmt.Errorf("failed to get host info from CRID: %w", err)
	}

	flags := options.NewKubeletFlags()
	flags.RootDirectory = k.config.DataDirs.Root
	flags.CertDirectory = k.config.DataDirs.PKI
	flags.HostnameOverride = hostInfo.Hostname
	flags.MaxContainerCount = 100
	flags.MaxPerPodContainerCount = 2
	flags.NodeLabels = map[string]string{} // TODO: Probe

	// Build KubeletConfiguration with upstream defaults, then apply our overrides
	config, err := options.NewKubeletConfiguration()
	if err != nil {
		return nil, fmt.Errorf("kubelet config: %w", err)
	}
	config.ImageMinimumGCAge = metav1.Duration{Duration: 1 * time.Minute}
	config.StaticPodPath = k.config.DataDirs.Manifests
	config.PodLogsDir = k.config.DataDirs.Logs
	config.ClusterDomain = "cluster.local" // TODO: Probe
	config.ClusterDNS = hostInfo.Nameservers
	config.Authentication = kubeletconfig.KubeletAuthentication{
		Anonymous: kubeletconfig.KubeletAnonymousAuthentication{Enabled: true},
		Webhook:   kubeletconfig.KubeletWebhookAuthentication{Enabled: false},
	}
	config.Authorization = kubeletconfig.KubeletAuthorization{
		Mode: kubeletconfig.KubeletAuthorizationModeAlwaysAllow,
	}
	config.TLSCertFile = k.config.Certs().CertPath()
	config.TLSPrivateKeyFile = k.config.Certs().KeyPath()
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
	config.FeatureGates = k.config.FeatureGates

	// Create k8s clients from kubeconfig
	kubeconfigPath := k.config.Files().Kubeconfig
	restConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return nil, fmt.Errorf("kubelet kubeconfig: %w", err)
	}
	client, err := clientset.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("kubelet client: %w", err)
	}

	heartbeatConfig := *restConfig
	heartbeatConfig.Timeout = config.NodeStatusUpdateFrequency.Duration
	leaseTimeout := time.Duration(config.NodeLeaseDurationSeconds) * time.Second
	if heartbeatConfig.Timeout > leaseTimeout {
		heartbeatConfig.Timeout = leaseTimeout
	}
	heartbeatConfig.QPS = float32(-1)
	heartbeatClient, err := clientset.NewForConfig(&heartbeatConfig)
	if err != nil {
		return nil, fmt.Errorf("kubelet heartbeat client: %w", err)
	}

	// Connect the CRID to the kubernetes API server
	k.config.CRID.WithClient(client)

	// Build and run HollowKubelet
	hk := kubemark.NewHollowKubelet(
		flags,
		config,
		client,
		heartbeatClient,
		k.config.CRID.DefaultBackend().Cadvisor(),
		k.config.CRID.DefaultBackend().Images(),
		k.config.CRID.DefaultBackend().Containers(),
		k.config.CRID.DefaultBackend().ContainerManager(),
	)
	hk.KubeletDeps.OSInterface = k.config.CRID.DefaultBackend().OS()
	hk.KubeletDeps.Mounter = k.config.CRID.DefaultBackend().Mounter()
	hk.KubeletDeps.Subpather = k.config.CRID.DefaultBackend().Subpath()
	hk.KubeletDeps.HostUtil = k.config.CRID.DefaultBackend().HostUtils()
	hk.KubeletDeps.Recorder = k.config.CRID.DefaultBackend().EventRecorder()
	hk.KubeletDeps.ProbeManager = k.config.CRID.DefaultBackend().Prober()
	hk.KubeletDeps.TLSOptions = &server.TLSOptions{
		Config:   &tls.Config{MinVersion: tls.VersionTLS12},
		CertFile: config.TLSCertFile,
		KeyFile:  config.TLSPrivateKeyFile,
	}
	exited := make(chan error, 1)
	go func() {
		kubeletLog.Info().Msg("hollow kubelet goroutine starting")
		hk.Run(ctx)
		kubeletLog.Warn().Msg("hollow kubelet goroutine exited")
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
				kubeletLog.Debug().Err(err).Msg("healthz probe failed")
				continue
			}
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				kubeletLog.Info().Msg("kubelet is ready")
				return component.Ready(), nil
			}
			kubeletLog.Debug().Int("status", resp.StatusCode).Msg("healthz returned non-200")
		}
	}
}
