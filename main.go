package main

import (
	"context"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/cnuss/nanokube/pkg"
	"github.com/cnuss/nanokube/pkg/driver/awslambda"
	"github.com/cnuss/nanokube/pkg/driver/docker"
	"github.com/cnuss/nanokube/pkg/driver/podman"
	"github.com/cnuss/nanokube/pkg/nanokube"
	v1 "github.com/cnuss/nanokube/pkg/v1"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/util/retry"
	cloudproviderapi "k8s.io/cloud-provider/api"
	"k8s.io/component-base/version"
	"k8s.io/kubectl/pkg/drain"
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
	pflag.CommandLine = pflag.NewFlagSet(os.Args[0], pflag.ContinueOnError)
	cmd := &cobra.Command{
		Use:           "nanokube [flags]",
		Short:         "nanokube is a fully functional Kubernetes cluster that runs natively on your machine",
		Version:       version.Get().GitVersion,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	kubelet := pkg.NewKubelet(cmd)

	<-kubelet.
		WithStorage(nanokube.NewStorage(kubelet)).
		WithApiServer(nanokube.NewApiServer(kubelet)).
		OnReady(v1.APIServerService, updateKubeconfig(kubelet)).
		OnReady(v1.Node, updateNode(kubelet), detach(kubelet)).
		OnCancel(cordonAndDrain(kubelet), stopSandboxes(kubelet)).
		OnCancel(snapshotStorage(kubelet), snapshotPods()).
		OnCancel(removeSandboxes(kubelet)).
		OnCancel(removeNetworks(kubelet)).
		Run().Done()
}

func updateNode(kubelet v1.Kubelet) func(ctx context.Context) {
	return func(ctx context.Context) {
		nodes := kubelet.Kube().Client().CoreV1().Nodes()
		tunnel := kubelet.Tunnel(v1.KubeletService)

		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			node, err := nodes.Get(kubelet.Context(), kubelet.Kube().KubeletHostname(), metav1.GetOptions{})
			if err != nil {
				return err
			}
			node.Spec.Taints = slices.DeleteFunc(node.Spec.Taints, func(t corev1.Taint) bool {
				return t.Key == cloudproviderapi.TaintExternalCloudProvider
			})
			_, err = nodes.Update(kubelet.Context(), node, metav1.UpdateOptions{})
			return err
		}); err != nil {
			nanokube.Log.Warn("failed to update node taints", "name", kubelet.Kube().KubeletHostname(), "error", err)
		}

		if _, err := nodes.ApplyStatus(
			kubelet.Context(),
			corev1ac.Node(kubelet.Kube().KubeletHostname()).WithStatus(
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
			metav1.ApplyOptions{FieldManager: kubelet.Options().Name(), Force: true},
		); err != nil {
			nanokube.Log.Warn("failed to apply node status", "name", kubelet.Kube().KubeletHostname(), "error", err)
		}

		nanokube.Log.Info("node is ready", "name", kubelet.Kube().KubeletHostname(), "fqdn", tunnel.FQDN())
	}
}

func detach(kubelet v1.Kubelet) func(ctx context.Context) {
	return func(ctx context.Context) {
		kubelet.Detach()
	}
}

func updateKubeconfig(kubelet v1.Kubelet) func(ctx context.Context) {
	return func(ctx context.Context) {
		kubeconfig, err := clientcmd.LoadFromFile(clientcmd.RecommendedHomeFile)
		if err != nil {
			kubeconfig = clientcmdapi.NewConfig()
		}

		internal := kubelet.Kube().Client().WithTunnel(kubelet.Kube().ApiServer().Tunnel(), true)
		external := kubelet.Kube().Client().WithTunnel(kubelet.Kube().ApiServer().Tunnel(), false)

		if err := internal.WriteKubeconfig(filepath.Join(string(kubelet.Options().DataDir()), string(v1.KubeconfigFile))); err != nil {
			nanokube.Log.Error("failed to write internal kubeconfig", "path", string(kubelet.Options().DataDir())+string(v1.KubeconfigFile), "error", err)
		}

		current := external.Kubeconfig(kubelet.Options().Name())
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
}

func cordonAndDrain(kubelet v1.Kubelet) func(ctx context.Context) {
	return func(ctx context.Context) {
		nanokube.Log.Info("cordoning and draining node", "name", kubelet.Kube().KubeletHostname())
		helper := &drain.Helper{
			Ctx:                 ctx,
			Client:              kubelet.Kube().Client().Clientset(),
			Force:               true,
			GracePeriodSeconds:  30,
			IgnoreAllDaemonSets: true,
			DeleteEmptyDirData:  true,
			Timeout:             60 * time.Second,
			Out:                 io.Discard,
			ErrOut:              io.Discard,
		}
		node, err := kubelet.Kube().Client().CoreV1().Nodes().Get(ctx, kubelet.Kube().KubeletHostname(), metav1.GetOptions{})
		if err != nil {
			nanokube.Log.Warn("failed to get node for draining", "name", kubelet.Kube().KubeletHostname(), "error", err)
			return
		}
		drain.RunCordonOrUncordon(helper, node, true)
		nanokube.Log.Info("node cordoned", "name", kubelet.Kube().KubeletHostname())
		drain.RunNodeDrain(helper, kubelet.Kube().KubeletHostname())
		nanokube.Log.Info("node drained", "name", kubelet.Kube().KubeletHostname())
	}
}

func stopSandboxes(kubelet v1.Kubelet) func(ctx context.Context) {
	return func(ctx context.Context) {
		for _, backend := range kubelet.Backends() {
			if sandboxes, err := backend.Driver().ListPodSandbox(ctx, nil); err == nil {
				for _, sandbox := range sandboxes {
					backend.Driver().StopPodSandbox(ctx, sandbox.Id)
				}
			}
		}
	}
}

func snapshotStorage(kubelet v1.Kubelet) func(ctx context.Context) {
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

func removeSandboxes(kubelet v1.Kubelet) func(ctx context.Context) {
	return func(ctx context.Context) {
		for _, backend := range kubelet.Backends() {
			if sandboxes, err := backend.Driver().ListPodSandbox(ctx, nil); err == nil {
				for _, sandbox := range sandboxes {
					backend.Driver().RemovePodSandbox(ctx, sandbox.Id)
				}
			}
		}
	}
}

func removeNetworks(kubelet v1.Kubelet) func(ctx context.Context) {
	return func(ctx context.Context) {
		for _, backend := range kubelet.Backends() {
			for _, network := range backend.Network().Networks() {
				backend.Driver().RemoveNetwork(ctx, network.ID())
			}
		}
	}
}
