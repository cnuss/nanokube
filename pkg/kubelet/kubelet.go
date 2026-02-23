package kubelet

import (
	"context"
	"crypto/tls"
	"net/http"
	"path/filepath"
	"time"

	"github.com/cnuss/nanokube/pkg/config"
	"github.com/rs/zerolog/log"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/kubernetes/cmd/kubelet/app/options"
	"k8s.io/kubernetes/pkg/kubelet"
	"k8s.io/kubernetes/pkg/kubemark"
)

type NanoKubelet struct {
	config *config.Config
	hk     *kubemark.HollowKubelet
}

func New(cfg *config.Config) *NanoKubelet {
	kubeconfigPath := cfg.KubeconfigPath()

	restConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to build kubeconfig")
	}

	kubeClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create kube client")
	}

	flags := &options.KubeletFlags{
		RootDirectory:    cfg.DataDir,
		CertDirectory:    cfg.DataDir,
		HostnameOverride: cfg.Runtime.Hostname(),
		KubeConfig:       kubeconfigPath,
	}

	hk := kubemark.NewHollowKubelet(
		flags,
		&cfg.Kubelet,
		kubeClient,
		kubeClient,
		cfg.Runtime.Cadvisor(),
		cfg.Runtime.ImageService(),
		cfg.Runtime.RuntimeService(),
		cfg.Runtime.ContainerManager(),
	)

	// Use real event recording instead of FakeRecorder
	hk.KubeletDeps.EventClient = kubeClient.CoreV1()
	hk.KubeletDeps.Recorder = nil

	return &NanoKubelet{
		config: cfg,
		hk:     hk,
	}
}

func (nk *NanoKubelet) Start(ctx context.Context) error {
	log.Info().Msg("starting kubelet")

	logsapi.ReapplyHandling = logsapi.ReapplyHandlingIgnoreUnchanged
	kubelet.ContainerLogsDir = filepath.Join(nk.config.DataDir, "containers")

	// Set lazy fields
	nk.hk.KubeletConfiguration.TLSCertFile = nk.config.Certs.CertPath()
	nk.hk.KubeletConfiguration.TLSPrivateKeyFile = nk.config.Certs.KeyPath()
	nk.hk.KubeletConfiguration.FeatureGates = nk.config.FeatureGates
	nk.hk.KubeletConfiguration.EnableServer = true

	go nk.hk.Run(ctx)

	// Wait for kubelet to be healthy
	client := &http.Client{
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
			resp, err := client.Get("https://127.0.0.1:10250/healthz")
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
