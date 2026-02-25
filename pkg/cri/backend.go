package cri

import (
	"context"
	"errors"
	"time"

	critypes "github.com/cnuss/nanokube/pkg/cri/types"
	"github.com/rs/zerolog/log"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// Backend abstracts a container engine (Docker, Podman) behind the CRI interface.
type Backend interface {
	// Lifecycle
	Init(ctx context.Context) error
	Close() error

	// Version / Status
	Version(ctx context.Context) (*runtimeapi.VersionResponse, error)
	Status(ctx context.Context, verbose bool) (*runtimeapi.StatusResponse, error)
	UpdateRuntimeConfig(ctx context.Context, config *runtimeapi.RuntimeConfig) error
	RuntimeConfig(ctx context.Context) (*runtimeapi.RuntimeConfigResponse, error)

	// Pod Sandbox
	RunPodSandbox(ctx context.Context, config *runtimeapi.PodSandboxConfig, runtimeHandler string) (string, error)
	StopPodSandbox(ctx context.Context, podSandboxID string) error
	RemovePodSandbox(ctx context.Context, podSandboxID string) error
	RemovePodSandboxes(ctx context.Context) ([]string, error)
	PodSandboxStatus(ctx context.Context, podSandboxID string, verbose bool) (*runtimeapi.PodSandboxStatusResponse, error)
	ListPodSandbox(ctx context.Context, filter *runtimeapi.PodSandboxFilter) ([]*runtimeapi.PodSandbox, error)

	// Container
	CreateContainer(ctx context.Context, podSandboxID string, config *runtimeapi.ContainerConfig, sandboxConfig *runtimeapi.PodSandboxConfig) (string, error)
	StartContainer(ctx context.Context, containerID string) error
	StopContainer(ctx context.Context, containerID string, timeout int64) error
	RemoveContainer(ctx context.Context, containerID string) error
	RemoveContainers(ctx context.Context) ([]string, error)
	ListContainers(ctx context.Context, filter *runtimeapi.ContainerFilter) ([]*runtimeapi.Container, error)
	ContainerStatus(ctx context.Context, containerID string, verbose bool) (*runtimeapi.ContainerStatusResponse, error)
	UpdateContainerResources(ctx context.Context, containerID string, resources *runtimeapi.ContainerResources) error
	ReopenContainerLog(ctx context.Context, containerID string) error
	ExecSync(ctx context.Context, containerID string, cmd []string, timeout time.Duration) ([]byte, []byte, error)

	// Stats
	ContainerStats(ctx context.Context, containerID string) (*runtimeapi.ContainerStats, error)
	ListContainerStats(ctx context.Context, filter *runtimeapi.ContainerStatsFilter) ([]*runtimeapi.ContainerStats, error)
	PodSandboxStats(ctx context.Context, podSandboxID string) (*runtimeapi.PodSandboxStats, error)
	ListPodSandboxStats(ctx context.Context, filter *runtimeapi.PodSandboxStatsFilter) ([]*runtimeapi.PodSandboxStats, error)

	// Metrics
	ListMetricDescriptors(ctx context.Context) ([]*runtimeapi.MetricDescriptor, error)
	ListPodSandboxMetrics(ctx context.Context) ([]*runtimeapi.PodSandboxMetrics, error)

	// Checkpoint
	CheckpointContainer(ctx context.Context, containerID string, location string, timeout int64) error

	// Container Events
	GetContainerEvents(ctx context.Context, eventsCh chan *runtimeapi.ContainerEventResponse) error

	// Resource updates
	UpdatePodSandboxResources(ctx context.Context, req *runtimeapi.UpdatePodSandboxResourcesRequest) (*runtimeapi.UpdatePodSandboxResourcesResponse, error)

	// Image
	ListImages(ctx context.Context, filter *runtimeapi.ImageFilter) ([]*runtimeapi.Image, error)
	ImageStatus(ctx context.Context, image *runtimeapi.ImageSpec, verbose bool) (*runtimeapi.ImageStatusResponse, error)
	PullImage(ctx context.Context, image *runtimeapi.ImageSpec, auth *runtimeapi.AuthConfig, sandbox *runtimeapi.PodSandboxConfig) (string, error)
	RemoveImage(ctx context.Context, image *runtimeapi.ImageSpec) error
	ImageFsInfo(ctx context.Context) (*runtimeapi.ImageFsInfoResponse, error)

	// Probe runs a short-lived container from image with cmd, optional bind
	// mounts (host:container:mode), and returns stdout. The container is
	// removed after execution.
	RunProbe(ctx context.Context, image string, cmd []string, binds []string) ([]byte, error)

	// HostInfo returns CPU count, total memory (bytes), kernel version, and OS
	// version from the container runtime's view of the host.
	HostInfo(ctx context.Context) (cpus int, memoryBytes int64, kernelVersion string, osVersion string, err error)

	// HostIDs returns the host's boot ID, system UUID, and machine ID by
	// probing the host's /proc, /sys, and /etc via namespace sharing.
	HostIDs(ctx context.Context) (bootID string, systemUUID string, machineID string, err error)

	// Volume lifecycle
	CreateVolume(ctx context.Context, name string) (string, error)
	RemoveVolume(ctx context.Context, name string) error
	RemoveVolumes(ctx context.Context) ([]string, error)
	ListVolumes(ctx context.Context, labelFilter map[string]string) ([]string, error)

	// Network lifecycle
	EnsureNetwork(ctx context.Context, networkType critypes.NetworkType) (string, error)
	RemoveNetworks(ctx context.Context) ([]string, error)
}

// Cleanup performs centralized teardown by calling the backend's Remove
// functions. It is invoked automatically via ctx.Done, not called directly.
// Order: containers → sandboxes → volumes → networks.
func Cleanup(backend Backend) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var errs []error

	containers, err := backend.RemoveContainers(ctx)
	log.Info().Strs("ids", containers).Msg("cleanup: removed containers")
	errs = append(errs, err)

	sandboxes, err := backend.RemovePodSandboxes(ctx)
	log.Info().Strs("ids", sandboxes).Msg("cleanup: removed sandboxes")
	errs = append(errs, err)

	volumes, err := backend.RemoveVolumes(ctx)
	log.Info().Strs("ids", volumes).Msg("cleanup: removed volumes")
	errs = append(errs, err)

	networks, err := backend.RemoveNetworks(ctx)
	log.Info().Strs("ids", networks).Msg("cleanup: removed networks")
	errs = append(errs, err)

	return errors.Join(errs...)
}
