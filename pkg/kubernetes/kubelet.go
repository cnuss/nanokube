package kubernetes

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"time"

	"github.com/cnuss/nanokube/pkg/config"
	"github.com/cnuss/nanokube/pkg/cri"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/trace/noop"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	remote "k8s.io/cri-client/pkg"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/cmd/kubelet/app/options"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	"k8s.io/kubernetes/pkg/kubemark"
)

type Kubelet struct {
	config *config.Config
}

func NewKubelet(config *config.Config) *Kubelet {
	return &Kubelet{
		config: config,
	}
}

func (k *Kubelet) Start(ctx context.Context) error {
	log.Info().Msg("starting kubelet")

	// Build KubeletFlags
	f := options.NewKubeletFlags()
	f.RootDirectory = k.config.DataDir
	f.HostnameOverride = k.config.CRI.Hostname()
	f.MinimumGCAge = metav1.Duration{Duration: 1 * time.Minute}
	f.MaxContainerCount = 100
	f.MaxPerPodContainerCount = 2

	// Build KubeletConfiguration with upstream defaults, then apply our overrides
	c, err := options.NewKubeletConfiguration()
	if err != nil {
		return fmt.Errorf("kubelet config: %w", err)
	}
	k.config.ApplyKubeletConfig(c)

	// Create k8s clients from kubeconfig
	kubeconfigPath := k.config.KubeconfigPath()
	restConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return fmt.Errorf("kubelet kubeconfig: %w", err)
	}
	client, err := clientset.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("kubelet client: %w", err)
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
		return fmt.Errorf("kubelet heartbeat client: %w", err)
	}

	// Connect to CRI socket via remote clients
	endpoint := k.config.CRI.Endpoint()
	logger := klog.Background()
	tp := noop.NewTracerProvider()
	runtimeService, err := remote.NewRemoteRuntimeService(endpoint, 30*time.Second, tp, &logger)
	if err != nil {
		return fmt.Errorf("kubelet runtime service: %w", err)
	}
	imageService, err := remote.NewRemoteImageService(endpoint, 30*time.Second, tp, &logger)
	if err != nil {
		return fmt.Errorf("kubelet image service: %w", err)
	}

	// Create cadvisor + container manager
	cadvisorInterface := cri.NewCadvisor(k.config.CRI.Hostname())
	containerManager := cm.NewStubContainerManager()

	// Build and run HollowKubelet
	hk := kubemark.NewHollowKubelet(
		f, c,
		client,
		heartbeatClient,
		cadvisorInterface,
		imageService,
		runtimeService,
		containerManager,
	)
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
			return ctx.Err()
		default:
			resp, err := httpClient.Get("https://127.0.0.1:10250/healthz")
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					log.Info().Msg("kubelet is ready")
					return nil
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}
