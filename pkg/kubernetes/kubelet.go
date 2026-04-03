package kubernetes

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"sync"
	"time"
	"unsafe"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/cnuss/nanokube/pkg/crid"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubernetes/cmd/kubelet/app/options"
	"k8s.io/kubernetes/pkg/kubelet"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubemark"
)

type Clients struct {
	Standard  *clientset.Clientset
	Heartbeat *clientset.Clientset
}

type Kubelet struct {
	ctx          context.Context
	crid         *crid.CRID
	log          component.Logger
	featureGates map[string]bool

	name     string
	nameOnce sync.Once

	flags     *options.KubeletFlags
	flagsOnce sync.Once

	config     *kubeletconfig.KubeletConfiguration
	configOnce sync.Once

	clients     Clients
	clientsOnce sync.Once

	deps *kubelet.Dependencies
}

func NewKubelet(crid *crid.CRID, featureGates map[string]bool) *Kubelet {
	return &Kubelet{
		ctx:          crid.Context(),
		crid:         crid,
		featureGates: featureGates,
		log:          component.NewLogger("kubelet"),
	}
}

func (k *Kubelet) Name() string {
	k.nameOnce.Do(func() {
		k.log.Info().Msg("initializing name")
		k.name = k.crid.DefaultBackend().HostnameOverride()
	})
	return k.name
}

func (k *Kubelet) Flags() *options.KubeletFlags {
	k.flagsOnce.Do(func() {
		k.log.Info().Msg("initializing flags")
		dataDirs := k.crid.DataDirs()

		k.flags = options.NewKubeletFlags()
		k.flags.RootDirectory = dataDirs.Kubelet
		k.flags.CertDirectory = dataDirs.PKI
		k.flags.HostnameOverride = k.Name()
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

// Clients blocks until the kubeconfig is written, then builds
// both the standard and heartbeat clients. Safe to call from a goroutine.
func (k *Kubelet) Clients() Clients {
	k.clientsOnce.Do(func() {
		k.log.Info().Msg("initializing kube clients")
		kubeconfigPath := k.crid.Kubeconfig()

		// Standard client
		restConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			k.log.Error().Err(err).Msg("kubeconfig failed")
			return
		}
		client, err := clientset.NewForConfig(restConfig)
		if err != nil {
			k.log.Error().Err(err).Msg("kube client failed")
			return
		}
		k.log.Info().Msg("kube client ready")
		k.clients.Standard = client
		k.crid.WithKubeClient(client)

		// Heartbeat client — lower timeout, unlimited QPS
		cfg := k.Config()
		hbConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			k.log.Error().Err(err).Msg("heartbeat kubeconfig failed")
			return
		}
		hbConfig.Timeout = cfg.NodeStatusUpdateFrequency.Duration
		leaseTimeout := time.Duration(cfg.NodeLeaseDurationSeconds) * time.Second
		if hbConfig.Timeout > leaseTimeout {
			hbConfig.Timeout = leaseTimeout
		}
		hbConfig.QPS = float32(-1)
		hbClient, err := clientset.NewForConfig(hbConfig)
		if err != nil {
			k.log.Error().Err(err).Msg("heartbeat client failed")
			return
		}
		k.clients.Heartbeat = hbClient
		k.log.Info().Msg("heartbeat client ready")
	})
	return k.clients
}

func (k *Kubelet) KubeClient() *clientset.Clientset {
	return k.Clients().Standard
}

func (k *Kubelet) HeartbeatClient() *clientset.Clientset {
	return k.Clients().Heartbeat
}

func (k *Kubelet) SetNotReady(ctx context.Context, reason, message string) {
	k.log.Info().Str("node", k.Name()).Msg("marking node not ready")

	node := &v1.Node{
		Spec: v1.NodeSpec{
			Taints: []v1.Taint{{
				Key:    "node.kubernetes.io/not-ready",
				Effect: v1.TaintEffectNoSchedule,
			}},
		},
		Status: v1.NodeStatus{
			Conditions: []v1.NodeCondition{{
				Type:    v1.NodeReady,
				Status:  v1.ConditionFalse,
				Reason:  reason,
				Message: message,
			}},
		},
	}
	if data, err := json.Marshal(node); err == nil {
		if _, err := k.KubeClient().CoreV1().Nodes().Patch(ctx, k.Name(), types.MergePatchType, data, metav1.PatchOptions{}, "status"); err != nil {
			k.log.Warn().Err(err).Msg("failed to patch node status")
		}
		if _, err := k.KubeClient().CoreV1().Nodes().Patch(ctx, k.Name(), types.MergePatchType, data, metav1.PatchOptions{}); err != nil {
			k.log.Warn().Err(err).Msg("failed to taint node")
		}
	}
}

func (k *Kubelet) Stop(ctx context.Context) component.Stopped {
	k.log.Info().Msg("stopping kubelet")
	k.SetNotReady(ctx, "Stopping", "Kubelet is stopping")

	if k.deps != nil && k.deps.PodConfig != nil {
		k.log.Info().Msg("closing pod config updates channel")
		func() {
			defer func() {
				if r := recover(); r != nil {
					k.log.Warn().Interface("recover", r).Msg("failed to close pod config updates channel")
					return
				}
				k.log.Info().Msg("pod config updates channel closed")
			}()
			field := reflect.ValueOf(k.deps.PodConfig).Elem().FieldByName("updates")
			ch := reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem()
			ch.Close()
		}()
	}
	return component.Closed("tcp", "127.0.0.1:10250", nil)
}

func (k *Kubelet) Start(ctx context.Context) (component.Started, error) {
	k.log.Info().Msg("starting kubelet")
	go k.Clients()

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
	hk.KubeletDeps.ProbeManager = nil
	hk.KubeletDeps.OSInterface = k.crid.DefaultBackend().OS()
	hk.KubeletDeps.Mounter = k.crid.DefaultBackend().Mounter()
	hk.KubeletDeps.Subpather = k.crid.DefaultBackend().Subpath()
	hk.KubeletDeps.HostUtil = k.crid.DefaultBackend().HostUtils()
	hk.KubeletDeps.Recorder = k.crid.DefaultBackend().EventRecorder()
	hk.KubeletDeps.TLSOptions = k.crid.TLSOptions()
	k.deps = hk.KubeletDeps
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
