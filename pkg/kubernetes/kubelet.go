package kubernetes

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/cnuss/nanokube/pkg/config"
	"github.com/cnuss/nanokube/pkg/cri"
	deps "github.com/cnuss/nanokube/pkg/kubernetes/kubelet"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	remote "k8s.io/cri-client/pkg"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/cmd/kubelet/app/options"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	"k8s.io/kubernetes/pkg/kubemark"
)

var kubeletLog = newLogger("kubelet")

type Kubelet struct {
	config *config.Config
}

func NewKubelet(config *config.Config) *Kubelet {
	return &Kubelet{
		config: config,
	}
}

func (k *Kubelet) Start(ctx context.Context) error {
	kubeletLog.Info().Msg("starting kubelet")

	// Build KubeletFlags — use a dedicated subdirectory so the kubelet's
	// internal cleanup doesn't interfere with certs, etcd data, etc.
	kubeletRoot := filepath.Join(k.config.DataDir, "kubelet")
	os.MkdirAll(kubeletRoot, 0755)
	os.MkdirAll(filepath.Join(k.config.DataDir, "manifests"), 0755)
	os.MkdirAll(filepath.Join(k.config.DataDir, "volumes"), 0755)

	f := options.NewKubeletFlags()
	f.RootDirectory = kubeletRoot
	f.CertDirectory = filepath.Join(kubeletRoot, "pki")
	f.HostnameOverride = k.config.CRI.Hostname()
	f.MinimumGCAge = metav1.Duration{Duration: 1 * time.Minute}
	f.MaxContainerCount = 100
	f.MaxPerPodContainerCount = 2

	// Build KubeletConfiguration with upstream defaults, then apply our overrides
	c, err := options.NewKubeletConfiguration()
	if err != nil {
		return fmt.Errorf("kubelet config: %w", err)
	}
	k.config.ApplyKubeletConfig(c)

	// Create k8s clients from kubeconfig
	kubeconfigPath := k.config.KubeconfigPath()
	restConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return fmt.Errorf("kubelet kubeconfig: %w", err)
	}
	client, err := clientset.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("kubelet client: %w", err)
	}

	heartbeatConfig := *restConfig
	heartbeatConfig.Timeout = c.NodeStatusUpdateFrequency.Duration
	leaseTimeout := time.Duration(c.NodeLeaseDurationSeconds) * time.Second
	if heartbeatConfig.Timeout > leaseTimeout {
		heartbeatConfig.Timeout = leaseTimeout
	}
	heartbeatConfig.QPS = float32(-1)
	heartbeatClient, err := clientset.NewForConfig(&heartbeatConfig)
	if err != nil {
		return fmt.Errorf("kubelet heartbeat client: %w", err)
	}

	// Connect to CRI socket via remote clients
	endpoint := k.config.CRI.Endpoint()
	logger := klog.Background()
	tp := deps.TracerProvider{}
	runtimeService, err := remote.NewRemoteRuntimeService(endpoint, 30*time.Second, tp, &logger)
	if err != nil {
		return fmt.Errorf("kubelet runtime service: %w", err)
	}
	imageService, err := remote.NewRemoteImageService(endpoint, 30*time.Second, tp, &logger)
	if err != nil {
		return fmt.Errorf("kubelet image service: %w", err)
	}

	// Get the runtime backend from CRI for cadvisor + capacity
	var backend cri.Backend
	criImpl, _ := k.config.CRI.(*cri.CRI)
	if criImpl != nil {
		backend = criImpl.RuntimeBackend()
	}

	// Create cadvisor + container manager
	cadvisorInterface := cri.NewCadvisor(ctx, k.config.CRI.Hostname(), backend)
	containerManager := buildContainerManager(cadvisorInterface)
	volumePlugin := cri.NewVolumePlugin(ctx, backend, k.config.DataDir)

	// Build and run HollowKubelet
	hk := kubemark.NewHollowKubelet(
		f, c,
		client,
		heartbeatClient,
		cadvisorInterface,
		imageService,
		runtimeService,
		containerManager,
	)
	hk.KubeletDeps.OSInterface = &deps.ScopedOS{DataDir: k.config.DataDir}
	hk.KubeletDeps.VolumePlugins = append(hk.KubeletDeps.VolumePlugins, volumePlugin)
	hk.KubeletDeps.Mounter = volumePlugin.Mounter
	hk.KubeletDeps.Subpather = &deps.ScopedSubpath{DataDir: k.config.DataDir}
	hk.KubeletDeps.HostUtil = &deps.ScopedHostUtil{DataDir: k.config.DataDir}
	hk.KubeletDeps.Recorder = deps.EventRecorder{}
	hk.KubeletDeps.ProbeManager = deps.ProbeManager{}
	go hk.Run(ctx)

	// Wait for kubelet to be healthy
	httpClient := &http.Client{
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
			resp, err := httpClient.Get("http://127.0.0.1:10250/healthz")
			if err != nil {
				resp, err = httpClient.Get("https://127.0.0.1:10250/healthz")
			}
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					kubeletLog.Info().Msg("kubelet is ready")
					go runProvisioner(ctx, client, k.config.DataDir, backend, volumePlugin.GetPluginName())
					return nil
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// runProvisioner watches for PVCs annotated with our provisioner and creates PVs.
func runProvisioner(ctx context.Context, client clientset.Interface, dataDir string, backend cri.Backend, pluginName string) {
	for {
		watcher, err := client.CoreV1().PersistentVolumeClaims("").Watch(ctx, metav1.ListOptions{})
		if err != nil {
			kubeletLog.Warn().Err(err).Msg("provisioner: watch failed, retrying")
			time.Sleep(5 * time.Second)
			continue
		}
		for event := range watcher.ResultChan() {
			if event.Type != watch.Added && event.Type != watch.Modified {
				continue
			}
			pvc, ok := event.Object.(*v1.PersistentVolumeClaim)
			if !ok || pvc.Status.Phase != v1.ClaimPending || pvc.Spec.VolumeName != "" {
				continue
			}
			ann := pvc.Annotations
			if ann["volume.kubernetes.io/storage-provisioner"] != pluginName &&
				ann["volume.beta.kubernetes.io/storage-provisioner"] != pluginName {
				continue
			}

			pvName := "pvc-" + string(pvc.UID)
			volPath := filepath.Join(dataDir, "volumes", pvName)

			if backend != nil {
				if _, err := backend.CreateVolume(ctx, pvName); err != nil {
					kubeletLog.Warn().Err(err).Str("pvc", pvc.Name).Msg("provisioner: create volume failed")
					continue
				}
			}
			os.MkdirAll(volPath, 0755)

			capacity := pvc.Spec.Resources.Requests[v1.ResourceStorage]
			pv := &v1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: pvName,
				},
				Spec: v1.PersistentVolumeSpec{
					Capacity:                      v1.ResourceList{v1.ResourceStorage: capacity},
					AccessModes:                   pvc.Spec.AccessModes,
					PersistentVolumeReclaimPolicy: v1.PersistentVolumeReclaimRetain,
					ClaimRef: &v1.ObjectReference{
						Namespace: pvc.Namespace,
						Name:      pvc.Name,
						UID:       pvc.UID,
					},
					PersistentVolumeSource: v1.PersistentVolumeSource{
						HostPath: &v1.HostPathVolumeSource{Path: volPath},
					},
				},
			}
			if _, err := client.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{}); err != nil {
				if !errors.IsAlreadyExists(err) {
					kubeletLog.Warn().Err(err).Str("pv", pvName).Msg("provisioner: create PV failed")
				}
				continue
			}
			kubeletLog.Info().Str("pv", pvName).Str("pvc", pvc.Name).Msg("provisioner: created PV")
		}
		if ctx.Err() != nil {
			return
		}
	}
}

// capacityContainerManager embeds the stub and overrides GetCapacity with real values.
type capacityContainerManager struct {
	cm.ContainerManager
	capacity v1.ResourceList
}

func (m *capacityContainerManager) GetCapacity(localStorageCapacityIsolation bool) v1.ResourceList {
	return m.capacity
}

func (m *capacityContainerManager) GetNodeAllocatableAbsolute() v1.ResourceList {
	return m.capacity
}

// buildContainerManager creates a container manager that reports real CPU/memory
// capacity from cadvisor's MachineInfo, falling back to a plain stub.
func buildContainerManager(cadvisorInterface *cri.Cadvisor) cm.ContainerManager {
	stub := cm.NewStubContainerManager()

	info, err := cadvisorInterface.MachineInfo()
	if err != nil || info.NumCores == 0 {
		kubeletLog.Warn().Err(err).Msg("kubelet: MachineInfo unavailable, using stub container manager")
		return stub
	}

	capacity := v1.ResourceList{
		v1.ResourceCPU:    *resource.NewQuantity(int64(info.NumCores), resource.DecimalSI),
		v1.ResourceMemory: *resource.NewQuantity(int64(info.MemoryCapacity), resource.BinarySI),
	}
	kubeletLog.Info().Int("cpus", info.NumCores).Uint64("memory", info.MemoryCapacity).Msg("kubelet: runtime-backed capacity")

	return &capacityContainerManager{
		ContainerManager: stub,
		capacity:         capacity,
	}
}
