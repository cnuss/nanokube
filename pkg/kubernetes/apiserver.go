package kubernetes

import (
	"context"
	"crypto/tls"
	"net/http"
	"time"

	"github.com/cnuss/nanokube/pkg/config"
	"github.com/spf13/cobra"
	"k8s.io/kubernetes/cmd/kube-apiserver/app"
)

var apiserverLog = newLogger("apiserver")

type APIServer struct {
	config *config.Config
	cmd    *cobra.Command
}

func NewAPIServer(config *config.Config) *APIServer {
	return &APIServer{
		config: config,
	}
}

func (a *APIServer) Start(ctx context.Context) error {
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

	args := append(a.config.KubeArgs(),
		"--bind-address=0.0.0.0",
		"--advertise-address=127.0.0.1",
		"--external-hostname=localhost",
		"--etcd-servers=https://127.0.0.1:2379",
		"--etcd-cafile="+a.config.Certs.CertPath(),
		"--etcd-certfile="+a.config.Certs.CertPath(),
		"--etcd-keyfile="+a.config.Certs.KeyPath(),
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
		"--tls-cert-file="+a.config.Certs.CertPath(),
		"--tls-private-key-file="+a.config.Certs.KeyPath(),
		"--client-ca-file="+a.config.Certs.CertPath(),
		// Service account
		"--service-account-key-file="+a.config.Certs.CertPath(),
		"--service-account-signing-key-file="+a.config.Certs.KeyPath(),
		// Kubelet
		"--kubelet-client-certificate="+a.config.Certs.CertPath(),
		"--kubelet-client-key="+a.config.Certs.KeyPath(),
	)

	a.cmd.SetArgs(args)
	go a.cmd.ExecuteContext(ctx)

	// Wait for API server to be healthy
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
			resp, err := client.Get("https://127.0.0.1:6443/healthz")
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					apiserverLog.Info().Msg("apiserver is ready")
					return nil
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (a *APIServer) Stop() {}
