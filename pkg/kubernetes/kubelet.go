package kubernetes

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/cnuss/nanokube/pkg/config"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubernetes/cmd/kubelet/app/options"
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

	// Build KubeletFlags — use a dedicated subdirectory so the kubelet's
	// internal cleanup doesn't interfere with certs, etcd data, etc.
	kubeletRoot := filepath.Join(k.config.DataDir, "kubelet")
	os.MkdirAll(kubeletRoot, 0o755)
	os.MkdirAll(filepath.Join(k.config.DataDir, "manifests"), 0o755)
	os.MkdirAll(filepath.Join(k.config.DataDir, "volumes"), 0o755)

	f := options.NewKubeletFlags()
	f.RootDirectory = kubeletRoot
	f.CertDirectory = filepath.Join(kubeletRoot, "pki")
	if host, err := k.config.CRID.DefaultBackend().HostInfo(); err == nil {
		f.HostnameOverride = host.Hostname
	}
	f.MinimumGCAge = metav1.Duration{Duration: 1 * time.Minute}
	f.MaxContainerCount = 100
	f.MaxPerPodContainerCount = 2

	// Build KubeletConfiguration with upstream defaults, then apply our overrides
	c, err := options.NewKubeletConfiguration()
	if err != nil {
		return nil, fmt.Errorf("kubelet config: %w", err)
	}
	// c.ContainerRuntimeEndpoint = k.config.CRID.Endpoint()
	// c.StaticPodPath = filepath.Join(k.config.DataDir, "manifests")
	// c.PodLogsDir = filepath.Join(k.config.DataDir, "logs")
	// c.ClusterDomain = k.config.CRID.Domain()
	// c.ClusterDNS = k.config.CRID.Nameservers()
	// c.Authentication = kubeletconfig.KubeletAuthentication{
	// 	Anonymous: kubeletconfig.KubeletAnonymousAuthentication{Enabled: true},
	// 	Webhook:   kubeletconfig.KubeletWebhookAuthentication{Enabled: false},
	// }
	// c.Authorization = kubeletconfig.KubeletAuthorization{
	// 	Mode: kubeletconfig.KubeletAuthorizationModeAlwaysAllow,
	// }
	// c.TLSCertFile = k.config.Certs.CertPath()
	// c.TLSPrivateKeyFile = k.config.Certs.KeyPath()
	// c.EnableServer = true
	// c.Port = 10250
	// c.ReadOnlyPort = 0
	// c.EnableControllerAttachDetach = false
	// c.HairpinMode = kubeletconfig.HairpinVeth
	c.CgroupsPerQOS = false
	c.CgroupDriver = "cgroupfs"
	c.EnforceNodeAllocatable = []string{}
	c.EvictionHard = map[string]string{}
	c.ImageGCHighThresholdPercent = 100
	c.FailSwapOn = false
	c.LocalStorageCapacityIsolation = false
	c.RotateCertificates = false
	c.ServerTLSBootstrap = false
	c.RegisterNode = true
	c.CPUCFSQuota = false
	// c.CPUCFSQuotaPeriod = metav1.Duration{Duration: 100 * time.Millisecond}
	c.ContainerLogMaxFiles = 5
	c.ContainerLogMaxWorkers = 1
	// c.ContainerLogMonitorInterval = metav1.Duration{Duration: 10 * time.Second}
	c.ProtectKernelDefaults = false
	// c.FeatureGates = k.config.FeatureGates

	// Create k8s clients from kubeconfig
	kubeconfigPath := k.config.KubeconfigPath()
	restConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return nil, fmt.Errorf("kubelet kubeconfig: %w", err)
	}
	client, err := clientset.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("kubelet client: %w", err)
	}

	heartbeatConfig := *restConfig
	heartbeatConfig.Timeout = c.NodeStatusUpdateFrequency.Duration
	leaseTimeout := time.Duration(c.NodeLeaseDurationSeconds) * time.Second
	if heartbeatConfig.Timeout > leaseTimeout {
		heartbeatConfig.Timeout = leaseTimeout
	}
	heartbeatConfig.QPS = float32(-1)
	heartbeatClient, err := clientset.NewForConfig(&heartbeatConfig)
	if err != nil {
		return nil, fmt.Errorf("kubelet heartbeat client: %w", err)
	}

	// Connect the CRID to the kubernetes API server
	volumePlugin := k.config.CRID.WithClient(client).DefaultBackend().VolumePlugin()

	// Build and run HollowKubelet
	hk := kubemark.NewHollowKubelet(
		f, c,
		client,
		heartbeatClient,
		k.config.CRID.DefaultBackend().Cadvisor(),
		k.config.CRID.DefaultBackend().Images(),
		k.config.CRID.DefaultBackend().Containers(),
		k.config.CRID.DefaultBackend().ContainerManager(),
	)
	hk.KubeletDeps.VolumePlugins = append(hk.KubeletDeps.VolumePlugins, volumePlugin)
	hk.KubeletDeps.OSInterface = k.config.CRID.DefaultBackend().OS()
	hk.KubeletDeps.Mounter = k.config.CRID.DefaultBackend().Mounter()
	hk.KubeletDeps.Subpather = k.config.CRID.DefaultBackend().Subpath()
	hk.KubeletDeps.HostUtil = k.config.CRID.DefaultBackend().HostUtils()
	hk.KubeletDeps.Recorder = k.config.CRID.DefaultBackend().EventRecorder()
	hk.KubeletDeps.ProbeManager = k.config.CRID.DefaultBackend().Prober()
	hk.KubeletDeps.TLSOptions = &server.TLSOptions{
		Config:   &tls.Config{MinVersion: tls.VersionTLS12},
		CertFile: c.TLSCertFile,
		KeyFile:  c.TLSPrivateKeyFile,
	}
	go hk.Run(ctx)

	// Wait for kubelet to be healthy
	httpClient := &http.Client{
		Timeout: 2 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			resp, err := httpClient.Get("https://127.0.0.1:10250/healthz")
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					kubeletLog.Info().Msg("kubelet is ready")
					return component.Ready(), nil
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}
