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
	"k8s.io/kubernetes/cmd/kube-controller-manager/app"
)

type ControllerManager struct {
	config *config.Config
	cmd    *cobra.Command
}

func NewControllerManager(config *config.Config) *ControllerManager {
	return &ControllerManager{
		config: config,
	}
}

func (c *ControllerManager) Start(ctx context.Context) error {
	log.Info().Msg("starting controller-manager")

	// Allow logging reconfiguration since apiserver already configured it
	logsapi.ReapplyHandling = logsapi.ReapplyHandlingIgnoreUnchanged

	c.cmd = app.NewControllerManagerCommand()
	c.cmd.SilenceUsage = true
	c.cmd.SilenceErrors = true
	c.cmd.SetContext(ctx)

	args := append(c.config.KubeArgs(),
		"--kubeconfig="+c.config.KubeconfigPath(),
		"--authentication-kubeconfig="+c.config.KubeconfigPath(),
		"--authorization-kubeconfig="+c.config.KubeconfigPath(),
		"--authentication-skip-lookup=true",
		"--bind-address=127.0.0.1",
		"--leader-elect=false",
		"--use-service-account-credentials=false",
		// TLS
		"--tls-cert-file="+c.config.Certs.CertPath(),
		"--tls-private-key-file="+c.config.Certs.KeyPath(),
		"--client-ca-file="+c.config.Certs.CertPath(),
		// Service account
		"--service-account-private-key-file="+c.config.Certs.KeyPath(),
		"--root-ca-file="+c.config.Certs.CertPath(),
		// Cluster
		"--cluster-signing-cert-file="+c.config.Certs.CertPath(),
		"--cluster-signing-key-file="+c.config.Certs.KeyPath(),
	)

	c.cmd.SetArgs(args)
	go c.cmd.ExecuteContext(ctx)

	// Wait for controller-manager to be healthy
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
			resp, err := client.Get("https://127.0.0.1:10257/healthz")
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					log.Info().Msg("controller-manager is ready")
					return nil
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}
