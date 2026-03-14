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
	certs        *config.Certs
	kubeconfig   string
	featureGates map[string]bool
	verbosity    int
	cmd          *cobra.Command
}

func NewControllerManager(certs *config.Certs, kubeconfig string, featureGates map[string]bool, verbosity int) *ControllerManager {
	return &ControllerManager{
		certs:        certs,
		kubeconfig:   kubeconfig,
		featureGates: featureGates,
		verbosity:    verbosity,
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

	args := append(kubeArgs(c.featureGates, c.verbosity),
		"--kubeconfig="+c.kubeconfig,
		"--authentication-kubeconfig="+c.kubeconfig,
		"--authorization-kubeconfig="+c.kubeconfig,
		"--authentication-skip-lookup=true",
		"--bind-address=127.0.0.1",
		"--leader-elect=false",
		"--controller-shutdown-timeout=0",
		"--use-service-account-credentials=false",
		// TLS
		"--tls-cert-file="+c.certs.CertPath(),
		"--tls-private-key-file="+c.certs.KeyPath(),
		"--client-ca-file="+c.certs.CertPath(),
		// Service account
		"--service-account-private-key-file="+c.certs.KeyPath(),
		"--root-ca-file="+c.certs.CertPath(),
		// Cluster
		"--cluster-signing-cert-file="+c.certs.CertPath(),
		"--cluster-signing-key-file="+c.certs.KeyPath(),
	)

	c.cmd.SetArgs(args)

	return component.Opened("tcp", "127.0.0.1:10257", func() {
		go c.cmd.ExecuteContext(ctx)
	}), nil
}

func (c *ControllerManager) Stop() component.Stopped {
	return component.Closed("tcp", "127.0.0.1:10257", nil)
}
