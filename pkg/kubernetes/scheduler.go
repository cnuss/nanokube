package kubernetes

import (
	"context"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/cnuss/nanokube/pkg/config"
	"github.com/spf13/cobra"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"
)

var schedulerLog = newLogger("scheduler")

type Scheduler struct {
	certs        *config.Certs
	kubeconfig   string
	featureGates map[string]bool
	verbosity    int
	cmd          *cobra.Command
}

func NewScheduler(certs *config.Certs, kubeconfig string, featureGates map[string]bool, verbosity int) *Scheduler {
	return &Scheduler{
		certs:        certs,
		kubeconfig:   kubeconfig,
		featureGates: featureGates,
		verbosity:    verbosity,
	}
}

func (s *Scheduler) Start(ctx context.Context) (component.Started, error) {
	schedulerLog.Info().Msg("starting scheduler")

	logsapi.ReapplyHandling = logsapi.ReapplyHandlingIgnoreUnchanged

	s.cmd = app.NewSchedulerCommand()
	s.cmd.SilenceUsage = true
	s.cmd.SilenceErrors = true
	s.cmd.SetContext(ctx)

	args := append(kubeArgs(s.featureGates, s.verbosity),
		"--kubeconfig="+s.kubeconfig,
		"--authentication-kubeconfig="+s.kubeconfig,
		"--authorization-kubeconfig="+s.kubeconfig,
		"--authentication-skip-lookup=true",
		"--bind-address=127.0.0.1",
		"--leader-elect=false",
		// TLS
		"--tls-cert-file="+s.certs.CertPath(),
		"--tls-private-key-file="+s.certs.KeyPath(),
		"--client-ca-file="+s.certs.CertPath(),
	)

	s.cmd.SetArgs(args)

	return component.Opened("tcp", "127.0.0.1:10259", func() {
		go s.cmd.ExecuteContext(ctx)
	}), nil
}

func (s *Scheduler) Stop() component.Stopped {
	return component.Closed("tcp", "127.0.0.1:10259", nil)
}
