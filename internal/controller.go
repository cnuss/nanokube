package internal

import (
	"context"
	"crypto/tls"
	"net/http"
	"strings"
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

	controllers := []string{
		// Garbage collection
		"garbage-collector-controller",
		"pod-garbage-collector-controller",
		"storageversion-garbage-collector-controller",
		// Cleanup / TTL
		"ttl-controller",
		"ttl-after-finished-controller",
		"legacy-serviceaccount-token-cleaner-controller",
		// Namespace lifecycle
		"namespace-controller",
		// RBAC
		"clusterrole-aggregation-controller",
		// Service accounts
		"serviceaccount-controller",
		// Certificates
		"certificatesigningrequest-signing-controller",
		"certificatesigningrequest-approving-controller",
		"certificatesigningrequest-cleaner-controller",
		// Node management
		"node-lifecycle-controller",
		"taint-eviction-controller",
		// Core workloads
		"deployment-controller",
		"replicaset-controller",
		"job-controller",
		"cronjob-controller",
		"daemonset-controller",
		"statefulset-controller",
		// Service & endpoint management
		"root-ca-certificate-publisher-controller",
		"serviceaccount-token-controller",
	}

	// Note: Don't use KubeArgs() here - logging is already configured by apiserver
	args := []string{
		"--feature-gates=" + c.config.FeatureGatesString(),
		"--kubeconfig=" + c.config.Certs.Kubeconfig,
		"--authentication-kubeconfig=" + c.config.Certs.Kubeconfig,
		"--authorization-kubeconfig=" + c.config.Certs.Kubeconfig,
		"--authentication-skip-lookup=true",
		"--bind-address=127.0.0.1",
		"--leader-elect=false",
		"--controllers=" + strings.Join(controllers, ","),
		"--use-service-account-credentials=false",
		// TLS
		"--tls-cert-file=" + c.config.Certs.ServerCert,
		"--tls-private-key-file=" + c.config.Certs.ServerKey,
		"--client-ca-file=" + c.config.Certs.CACert,
		// Service account
		"--service-account-private-key-file=" + c.config.Certs.ClientKey,
		"--root-ca-file=" + c.config.Certs.CACert,
		// Cluster
		"--cluster-signing-cert-file=" + c.config.Certs.CACert,
		"--cluster-signing-key-file=" + c.config.Certs.CAKey,
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
