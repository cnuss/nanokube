package internal

import (
	"context"
	"crypto/tls"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/kubernetes/cmd/kube-controller-manager/app"
)

type ControllerManager struct {
	config *Config
	cmd    *cobra.Command
}

func NewControllerManager(config *Config) *ControllerManager {
	return &ControllerManager{
		config: config,
	}
}

func (c *ControllerManager) Start(ctx context.Context) error {
	log.Info().Msg("starting controller-manager")

	// Allow logging reconfiguration since apiserver already configured it
	logsapi.ReapplyHandling = logsapi.ReapplyHandlingIgnoreUnchanged

	c.cmd = app.NewControllerManagerCommand()
	c.cmd.SetContext(ctx)

	// controllers := []string{
	// 	"*",
	// }

	// Note: Don't use KubeArgs() here - logging is already configured by apiserver
	args := []string{
		"--feature-gates=" + c.config.FeatureGatesString(),
		"--kubeconfig=" + c.config.Kubeconfig,
		"--authentication-kubeconfig=" + c.config.Kubeconfig,
		"--authorization-kubeconfig=" + c.config.Kubeconfig,
		"--authentication-skip-lookup=true",
		"--bind-address=127.0.0.1",
		"--leader-elect=false",
		// "--controllers=" + strings.Join(controllers, ","),
		"--use-service-account-credentials=false",
		// TLS
		"--tls-cert-file=" + c.config.CertPath,
		"--tls-private-key-file=" + c.config.KeyPath,
		"--client-ca-file=" + c.config.CertPath,
		// Service account
		"--service-account-private-key-file=" + c.config.KeyPath,
		"--root-ca-file=" + c.config.CertPath,
		// Cluster
		"--cluster-signing-cert-file=" + c.config.CertPath,
		"--cluster-signing-key-file=" + c.config.KeyPath,
	}

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
