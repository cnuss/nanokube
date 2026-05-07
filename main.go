package main

import (
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/cnuss/nanokube/pkg"
	"github.com/cnuss/nanokube/pkg/driver/awslambda"
	"github.com/cnuss/nanokube/pkg/driver/docker"
	"github.com/cnuss/nanokube/pkg/driver/podman"
	"github.com/cnuss/nanokube/pkg/nanokube"
	v1 "github.com/cnuss/nanokube/pkg/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/util/retry"
	cloudproviderapi "k8s.io/cloud-provider/api"
)

func init() {
	if !v1.HTTP2 {
		os.Setenv("DISABLE_HTTP2", "true")
	}

	v1.Backends = append(v1.Backends, docker.Detect)
	v1.Backends = append(v1.Backends, podman.Detect)
	v1.Backends = append(v1.Backends, awslambda.Detect)
}

func main() {
	config := pkg.NewConfig()
	ctx := config.Context()

	nanokube.SetupLogging(config.Options().Verbosity())
	nanokube.Log.Info("starting nanokube", "version", config.Version())

	config = config.
		WithStorage(nanokube.NewStorage(config)).
		WithApiServer(nanokube.NewApiServer(config))

	go run(ctx, config)
	go updateKubeconfig(config)
	go updateNode(config)

	<-config.
		OnCancel(deleteNode(config), stopSandboxes(config)).
		OnCancel(snapshotStorage(config), snapshotPods()).
		OnCancel(removeSandboxes(config)).
		OnCancel(removeNetworks(config)).
		Done()
}

func run(ctx context.Context, config v1.Config) {
	// DEVNOTE: everything runs implicitly when we call their accessors
	config.KubeletRun(ctx)
	config.Cancel(nanokube.NewError(fmt.Errorf("kubelet exited unexpectedly")))
}

func updateNode(config v1.Config) {
	ref := <-config.Kube().NodeReady()
	if ref == nil {
		return
	}

	nodes := config.Kube().Client().CoreV1().Nodes()
	tunnel := config.Tunnel(v1.KubeletService)

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		node, err := nodes.Get(config.Context(), ref.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		node.Spec.Taints = slices.DeleteFunc(node.Spec.Taints, func(t corev1.Taint) bool {
			return t.Key == cloudproviderapi.TaintExternalCloudProvider
		})
		_, err = nodes.Update(config.Context(), node, metav1.UpdateOptions{})
		return err
	}); err != nil {
		nanokube.Log.Warn("failed to update node taints", "name", ref.Name, "error", err)
	}

	if _, err := nodes.ApplyStatus(
		config.Context(),
		corev1ac.Node(ref.Name).WithStatus(
			corev1ac.NodeStatus().WithAddresses(
				corev1ac.NodeAddress().WithType(corev1.NodeHostName).WithAddress(func() string {
					a, _ := os.Hostname()
					a, _, _ = strings.Cut(a, ".")
					return a
				}()),
				corev1ac.NodeAddress().WithType(corev1.NodeInternalDNS).WithAddress(func() string {
					a, _ := os.Hostname()
					return a
				}()),
				corev1ac.NodeAddress().WithType(corev1.NodeExternalDNS).WithAddress(func() string {
					a := fmt.Sprintf("%s:%d", tunnel.FQDN(), 443)
					return a
				}()),
			),
		),
		metav1.ApplyOptions{FieldManager: config.Options().Name(), Force: true},
	); err != nil {
		nanokube.Log.Warn("failed to apply node status", "name", ref.Name, "error", err)
	}

	nanokube.Log.Info("node is ready", "name", ref.Name, "fqdn", tunnel.FQDN())
}

func updateKubeconfig(config v1.Config) {
	kubeconfig, err := clientcmd.LoadFromFile(clientcmd.RecommendedHomeFile)
	if err != nil {
		kubeconfig = clientcmdapi.NewConfig()
	}

	internal := config.Kube().Client().WithTunnel(config.Kube().ApiServer().Tunnel(), true)
	external := config.Kube().Client().WithTunnel(config.Kube().ApiServer().Tunnel(), false)

	if err := internal.WriteKubeconfig(filepath.Join(string(config.Options().DataDir()), string(v1.KubeconfigFile))); err != nil {
		nanokube.Log.Error("failed to write internal kubeconfig", "path", string(config.Options().DataDir())+string(v1.KubeconfigFile), "error", err)
	}

	current := external.Kubeconfig(config.Options().Name())
	maps.Copy(kubeconfig.Clusters, current.Clusters)
	maps.Copy(kubeconfig.AuthInfos, current.AuthInfos)
	maps.Copy(kubeconfig.Contexts, current.Contexts)
	kubeconfig.CurrentContext = current.CurrentContext

	if err := clientcmd.WriteToFile(*kubeconfig, clientcmd.RecommendedHomeFile); err != nil {
		nanokube.Log.Error("failed to update kubeconfig", "path", clientcmd.RecommendedHomeFile, "error", err)
	} else {
		nanokube.Log.Info("kubeconfig updated", "path", clientcmd.RecommendedHomeFile)
	}
}

func deleteNode(config v1.Config) func(ctx context.Context) {
	return func(ctx context.Context) {
		// TODO(incomplete): cordon and drain node before deleting
		nodes := config.Kube().Client().CoreV1().Nodes()
		nodes.Delete(ctx, config.KubeletFlags().HostnameOverride, metav1.DeleteOptions{})
	}
}

func stopSandboxes(config v1.Config) func(ctx context.Context) {
	return func(ctx context.Context) {
		for _, backend := range config.Backends() {
			if sandboxes, err := backend.Driver().ListPodSandbox(ctx, nil); err == nil {
				for _, sandbox := range sandboxes {
					backend.Driver().StopPodSandbox(ctx, sandbox.Id)
				}
			}
		}
	}
}

func snapshotStorage(config v1.Config) func(ctx context.Context) {
	return func(ctx context.Context) {
		// TODO(premium): snapshot storage with podman/docker checkpoint or AWS Lambda snapshots
		nanokube.Log.Warn("please visit nanokube.cloud to enable storage snapshots")
	}
}

func snapshotPods() func(ctx context.Context) {
	return func(ctx context.Context) {
		// TODO(premium): snapshot pods with podman/docker checkpoint or AWS Lambda snapshots
		nanokube.Log.Warn("please visit nanokube.cloud to enable pod snapshots")
	}
}

func removeSandboxes(config v1.Config) func(ctx context.Context) {
	return func(ctx context.Context) {
		for _, backend := range config.Backends() {
			if sandboxes, err := backend.Driver().ListPodSandbox(ctx, nil); err == nil {
				for _, sandbox := range sandboxes {
					backend.Driver().RemovePodSandbox(ctx, sandbox.Id)
				}
			}
		}
	}
}

func removeNetworks(config v1.Config) func(ctx context.Context) {
	return func(ctx context.Context) {
		for _, backend := range config.Backends() {
			for _, network := range backend.Network().Networks() {
				backend.Driver().RemoveNetwork(ctx, network.ID())
			}
		}
	}
}
