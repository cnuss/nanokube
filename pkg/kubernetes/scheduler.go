package kubernetes

import (
	"context"
	"crypto/tls"
	"net/http"
	"time"

	pkgconfig "github.com/cnuss/nanokube/pkg/config"
	"github.com/spf13/cobra"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"
)

var schedulerLog = newLogger("scheduler")

type Scheduler struct {
	config *pkgconfig.Config
	cmd    *cobra.Command
}

func NewScheduler(config *pkgconfig.Config) *Scheduler {
	return &Scheduler{
		config: config,
	}
}

func (s *Scheduler) Start(ctx context.Context) error {
	schedulerLog.Info().Msg("starting scheduler")

	logsapi.ReapplyHandling = logsapi.ReapplyHandlingIgnoreUnchanged

	s.cmd = app.NewSchedulerCommand()
	s.cmd.SilenceUsage = true
	s.cmd.SilenceErrors = true
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
					schedulerLog.Info().Msg("scheduler is ready")
					return nil
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (s *Scheduler) Stop() {}
