package internal

import (
	"crypto/tls"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"
)

type Scheduler struct {
	config *Config
	cmd    *cobra.Command
}

func NewScheduler(config *Config) *Scheduler {
	return &Scheduler{
		config: config,
	}
}

func (s *Scheduler) Start() error {
	ctx := s.config.ctx
	log.Info().Msg("starting scheduler")

	logsapi.ReapplyHandling = logsapi.ReapplyHandlingIgnoreUnchanged

	s.cmd = app.NewSchedulerCommand()
	s.cmd.SetContext(ctx)

	args := append(s.config.KubeArgs(),
		"--kubeconfig="+s.config.KubeconfigPath(),
		"--authentication-kubeconfig="+s.config.KubeconfigPath(),
		"--authorization-kubeconfig="+s.config.KubeconfigPath(),
		"--authentication-skip-lookup=true",
		"--bind-address=127.0.0.1",
		"--leader-elect=false",
		// TLS
		"--tls-cert-file="+s.config.Certs.CertPath(),
		"--tls-private-key-file="+s.config.Certs.KeyPath(),
		"--client-ca-file="+s.config.Certs.CertPath(),
	)

	s.cmd.SetArgs(args)

	go s.cmd.ExecuteContext(ctx)

	// Wait for scheduler to be healthy
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
			resp, err := client.Get("https://127.0.0.1:10259/healthz")
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					log.Info().Msg("scheduler is ready")
					return nil
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}
