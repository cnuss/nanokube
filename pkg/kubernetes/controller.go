package kubernetes

import (
	"context"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/cnuss/nanokube/pkg/config"
	"github.com/spf13/cobra"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/kubernetes/cmd/kube-controller-manager/app"
)

var controllerLog = newLogger("controller")

type ControllerManager struct {
	config *config.Config
	cmd    *cobra.Command
}

func NewControllerManager(config *config.Config) *ControllerManager {
	return &ControllerManager{
		config: config,
	}
}

func (c *ControllerManager) Start(ctx context.Context) (component.Started, error) {
	controllerLog.Info().Msg("starting controller-manager")

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
		"--controller-shutdown-timeout=0",
		"--use-service-account-credentials=false",
		// TLS
		"--tls-cert-file="+c.config.Certs().CertPath(),
		"--tls-private-key-file="+c.config.Certs().KeyPath(),
		"--client-ca-file="+c.config.Certs().CertPath(),
		// Service account
		"--service-account-private-key-file="+c.config.Certs().KeyPath(),
		"--root-ca-file="+c.config.Certs().CertPath(),
		// Cluster
		"--cluster-signing-cert-file="+c.config.Certs().CertPath(),
		"--cluster-signing-key-file="+c.config.Certs().KeyPath(),
	)

	c.cmd.SetArgs(args)

	return component.Opened("tcp", "127.0.0.1:10257", func() {
		go c.cmd.ExecuteContext(ctx)
	}), nil
}

func (c *ControllerManager) Stop() component.Stopped {
	return component.Closed("tcp", "127.0.0.1:10257", nil)
}
