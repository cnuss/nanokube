package kubernetes

import (
	"context"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/cnuss/nanokube/pkg/config"
	"github.com/spf13/cobra"
	"k8s.io/kubernetes/cmd/kube-apiserver/app"
)

var apiserverLog = newLogger("apiserver")

type APIServer struct {
	certs        *config.Certs
	featureGates map[string]bool
	verbosity    int
	cmd          *cobra.Command
}

func NewAPIServer(certs *config.Certs, featureGates map[string]bool, verbosity int) *APIServer {
	return &APIServer{
		certs:        certs,
		featureGates: featureGates,
		verbosity:    verbosity,
	}
}

func (a *APIServer) Start(ctx context.Context) (component.Started, error) {
	apiserverLog.Info().Msg("starting apiserver")

	/*
		TODO:
		--oidc-issuer-url="${OIDC_ISS}" \
		--oidc-client-id="${OIDC_AUD}" \
		--oidc-required-claim="azp=${OIDC_AZP}" \
		--oidc-groups-claim=permissions \
		--kubelet-preferred-address-types=ExternalDNS \
		--kubelet-port=443 \
	*/

	a.cmd = app.NewAPIServerCommand()
	a.cmd.SilenceUsage = true
	a.cmd.SilenceErrors = true
	a.cmd.SetContext(ctx)

	args := append(kubeArgs(a.featureGates, a.verbosity),
		"--bind-address=0.0.0.0",
		"--advertise-address=127.0.0.1",
		"--external-hostname=localhost",
		"--etcd-servers=https://127.0.0.1:2379",
		"--etcd-cafile="+a.certs.CertPath(),
		"--etcd-certfile="+a.certs.CertPath(),
		"--etcd-keyfile="+a.certs.KeyPath(),
		"--service-account-issuer=https://kubernetes.default.svc.cluster.local",
		"--allow-privileged=true",
		"--authorization-mode=Node,RBAC",
		"--enable-bootstrap-token-auth",
		"--audit-log-path=-",
		"--audit-log-format=legacy",
		"--disable-http2-serving",
		"--endpoint-reconciler-type=none",
		"--watch-cache=false",
		"--shutdown-delay-duration=0s",
		// TLS
		"--tls-cert-file="+a.certs.CertPath(),
		"--tls-private-key-file="+a.certs.KeyPath(),
		"--client-ca-file="+a.certs.CertPath(),
		// Service account
		"--service-account-key-file="+a.certs.CertPath(),
		"--service-account-signing-key-file="+a.certs.KeyPath(),
		// Kubelet
		"--kubelet-client-certificate="+a.certs.CertPath(),
		"--kubelet-client-key="+a.certs.KeyPath(),
	)

	a.cmd.SetArgs(args)

	return component.Opened("tcp", "127.0.0.1:6443", func() {
		go a.cmd.ExecuteContext(ctx)
	}), nil
}

func (a *APIServer) Stop() component.Stopped {
	return component.Closed("tcp", "127.0.0.1:6443", nil)
}
