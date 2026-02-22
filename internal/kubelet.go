package internal

import (
	"crypto/tls"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/kubernetes/cmd/kubelet/app"
)

type Kubelet struct {
	config *Config
	cmd    *cobra.Command
}

func NewKubelet(config *Config) *Kubelet {
	return &Kubelet{
		config: config,
	}
}

func (k *Kubelet) Start() error {
	ctx := k.config.ctx
	log.Info().Msg("starting kubelet")

	logsapi.ReapplyHandling = logsapi.ReapplyHandlingIgnoreUnchanged

	k.cmd = app.NewKubeletCommand(ctx)

	args := append(k.config.KubeArgs(),
		"--config="+k.config.KubeletConfigPath(),
		"--kubeconfig="+k.config.KubeconfigPath(),
		"--root-dir="+k.config.DataDir,
		"--cert-dir="+k.config.DataDir,
		"--hostname-override="+k.config.Runtime.Hostname(),
		"--cluster-domain="+k.config.Runtime.Domain(),
		"--cluster-dns="+strings.Join(k.config.Runtime.Nameservers(), ","),
		// TLS
		"--tls-cert-file="+k.config.Certs.CertPath(),
		"--tls-private-key-file="+k.config.Certs.KeyPath(),
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
