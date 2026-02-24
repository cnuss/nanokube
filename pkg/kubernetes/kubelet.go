package kubernetes

import (
	"context"
	"crypto/tls"
	"net/http"
	"time"

	"github.com/cnuss/nanokube/pkg/config"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/kubernetes/cmd/kubelet/app"
)

type Kubelet struct {
	config *config.Config
	cmd    *cobra.Command
}

func NewKubelet(config *config.Config) *Kubelet {
	return &Kubelet{
		config: config,
	}
}

func (k *Kubelet) Start(ctx context.Context) error {
	log.Info().Msg("starting kubelet")

	logsapi.ReapplyHandling = logsapi.ReapplyHandlingIgnoreUnchanged

	k.cmd = app.NewKubeletCommand(ctx)
	k.cmd.SilenceUsage = true
	k.cmd.SilenceErrors = true
	k.cmd.SetContext(ctx)

	args := append(k.config.KubeArgs(),
		"--kubeconfig="+k.config.KubeconfigPath(),
		"--config="+k.config.KubeletConfigPath(),
		"--root-dir="+k.config.DataDir,
		"--cert-dir="+k.config.DataDir,
	)

	k.cmd.SetArgs(args)
	go k.cmd.ExecuteContext(ctx)

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
