package podman

import (
	"context"
	"fmt"
	"time"

	critypes "github.com/cnuss/nanokube/pkg/cri/types"
	"github.com/cnuss/nanokube/pkg/logging"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

var (
	errNotImplemented = fmt.Errorf("podman backend not yet implemented")
	logger            = logging.Component("podman")
)

// Backend implements cri.Backend using the Podman API.
type Backend struct {
	podmanSocket string
}

func New(podmanSocket string) *Backend {
	return &Backend{podmanSocket: podmanSocket}
}

func (b *Backend) Init(ctx context.Context) error {
	logger.Debug().Msg("Init not implemented")
	return errNotImplemented
}

func (b *Backend) Close() error {
	logger.Debug().Msg("Close not implemented")
	return nil
}

// Version / Status

func (b *Backend) Version(ctx context.Context) (*runtimeapi.VersionResponse, error) {
	logger.Debug().Msg("Version not implemented")
	return nil, errNotImplemented
}

func (b *Backend) Status(ctx context.Context, verbose bool) (*runtimeapi.StatusResponse, error) {
	logger.Debug().Msg("Status not implemented")
	return nil, errNotImplemented
}

func (b *Backend) UpdateRuntimeConfig(ctx context.Context, config *runtimeapi.RuntimeConfig) error {
	logger.Debug().Msg("UpdateRuntimeConfig not implemented")
	return nil
}

func (b *Backend) RuntimeConfig(ctx context.Context) (*runtimeapi.RuntimeConfigResponse, error) {
	logger.Debug().Msg("RuntimeConfig not implemented")
	return &runtimeapi.RuntimeConfigResponse{}, nil
}

// Pod Sandbox

func (b *Backend) RunPodSandbox(ctx context.Context, config *runtimeapi.PodSandboxConfig, runtimeHandler string) (string, error) {
	logger.Debug().Msg("RunPodSandbox not implemented")
	return "", errNotImplemented
}

func (b *Backend) StopPodSandbox(ctx context.Context, podSandboxID string) error {
	logger.Debug().Msg("StopPodSandbox not implemented")
	return errNotImplemented
}

func (b *Backend) RemovePodSandbox(ctx context.Context, podSandboxID string) error {
	logger.Debug().Msg("RemovePodSandbox not implemented")
	return errNotImplemented
}

func (b *Backend) RemovePodSandboxes(ctx context.Context) ([]string, error) {
	logger.Debug().Msg("RemovePodSandboxes not implemented")
	return nil, errNotImplemented
}

func (b *Backend) PodSandboxStatus(ctx context.Context, podSandboxID string, verbose bool) (*runtimeapi.PodSandboxStatusResponse, error) {
	logger.Debug().Msg("PodSandboxStatus not implemented")
	return nil, errNotImplemented
}

func (b *Backend) ListPodSandbox(ctx context.Context, filter *runtimeapi.PodSandboxFilter) ([]*runtimeapi.PodSandbox, error) {
	logger.Debug().Msg("ListPodSandbox not implemented")
	return nil, errNotImplemented
}

// Container

func (b *Backend) CreateContainer(ctx context.Context, podSandboxID string, config *runtimeapi.ContainerConfig, sandboxConfig *runtimeapi.PodSandboxConfig) (string, error) {
	logger.Debug().Msg("CreateContainer not implemented")
	return "", errNotImplemented
}

func (b *Backend) StartContainer(ctx context.Context, containerID string) error {
	logger.Debug().Msg("StartContainer not implemented")
	return errNotImplemented
}

func (b *Backend) StopContainer(ctx context.Context, containerID string, timeout int64) error {
	logger.Debug().Msg("StopContainer not implemented")
	return errNotImplemented
}

func (b *Backend) RemoveContainer(ctx context.Context, containerID string) error {
	logger.Debug().Msg("RemoveContainer not implemented")
	return errNotImplemented
}

func (b *Backend) RemoveContainers(ctx context.Context) ([]string, error) {
	logger.Debug().Msg("RemoveContainers not implemented")
	return nil, errNotImplemented
}

func (b *Backend) ListContainers(ctx context.Context, filter *runtimeapi.ContainerFilter) ([]*runtimeapi.Container, error) {
	logger.Debug().Msg("ListContainers not implemented")
	return nil, errNotImplemented
}

func (b *Backend) ContainerStatus(ctx context.Context, containerID string, verbose bool) (*runtimeapi.ContainerStatusResponse, error) {
	logger.Debug().Msg("ContainerStatus not implemented")
	return nil, errNotImplemented
}

func (b *Backend) UpdateContainerResources(ctx context.Context, containerID string, resources *runtimeapi.ContainerResources) error {
	logger.Debug().Msg("UpdateContainerResources not implemented")
	return errNotImplemented
}

func (b *Backend) ReopenContainerLog(ctx context.Context, containerID string) error {
	logger.Debug().Msg("ReopenContainerLog not implemented")
	return errNotImplemented
}

func (b *Backend) ExecSync(ctx context.Context, containerID string, cmd []string, timeout time.Duration) ([]byte, []byte, error) {
	logger.Debug().Msg("ExecSync not implemented")
	return nil, nil, errNotImplemented
}

// Stats

func (b *Backend) ContainerStats(ctx context.Context, containerID string) (*runtimeapi.ContainerStats, error) {
	logger.Debug().Msg("ContainerStats not implemented")
	return nil, errNotImplemented
}

func (b *Backend) ListContainerStats(ctx context.Context, filter *runtimeapi.ContainerStatsFilter) ([]*runtimeapi.ContainerStats, error) {
	logger.Debug().Msg("ListContainerStats not implemented")
	return nil, errNotImplemented
}

func (b *Backend) PodSandboxStats(ctx context.Context, podSandboxID string) (*runtimeapi.PodSandboxStats, error) {
	logger.Debug().Msg("PodSandboxStats not implemented")
	return nil, errNotImplemented
}

func (b *Backend) ListPodSandboxStats(ctx context.Context, filter *runtimeapi.PodSandboxStatsFilter) ([]*runtimeapi.PodSandboxStats, error) {
	logger.Debug().Msg("ListPodSandboxStats not implemented")
	return nil, errNotImplemented
}

// Metrics

func (b *Backend) ListMetricDescriptors(ctx context.Context) ([]*runtimeapi.MetricDescriptor, error) {
	logger.Debug().Msg("ListMetricDescriptors not implemented")
	return nil, nil
}

func (b *Backend) ListPodSandboxMetrics(ctx context.Context) ([]*runtimeapi.PodSandboxMetrics, error) {
	logger.Debug().Msg("ListPodSandboxMetrics not implemented")
	return nil, nil
}

// Checkpoint

func (b *Backend) CheckpointContainer(ctx context.Context, containerID, location string, timeout int64) error {
	logger.Debug().Msg("CheckpointContainer not implemented")
	return errNotImplemented
}

// Container Events

func (b *Backend) GetContainerEvents(ctx context.Context, eventsCh chan *runtimeapi.ContainerEventResponse) error {
	logger.Debug().Msg("GetContainerEvents not implemented")
	<-ctx.Done()
	close(eventsCh)
	return nil
}

// Resource updates

func (b *Backend) UpdatePodSandboxResources(ctx context.Context, req *runtimeapi.UpdatePodSandboxResourcesRequest) (*runtimeapi.UpdatePodSandboxResourcesResponse, error) {
	logger.Debug().Msg("UpdatePodSandboxResources not implemented")
	return &runtimeapi.UpdatePodSandboxResourcesResponse{}, nil
}

// Image

func (b *Backend) ListImages(ctx context.Context, filter *runtimeapi.ImageFilter) ([]*runtimeapi.Image, error) {
	logger.Debug().Msg("ListImages not implemented")
	return nil, errNotImplemented
}

func (b *Backend) ImageStatus(ctx context.Context, image *runtimeapi.ImageSpec, verbose bool) (*runtimeapi.ImageStatusResponse, error) {
	logger.Debug().Msg("ImageStatus not implemented")
	return nil, errNotImplemented
}

func (b *Backend) PullImage(ctx context.Context, image *runtimeapi.ImageSpec, auth *runtimeapi.AuthConfig, sandbox *runtimeapi.PodSandboxConfig) (string, error) {
	logger.Debug().Msg("PullImage not implemented")
	return "", errNotImplemented
}

func (b *Backend) RemoveImage(ctx context.Context, image *runtimeapi.ImageSpec) error {
	logger.Debug().Msg("RemoveImage not implemented")
	return errNotImplemented
}

func (b *Backend) ImageFsInfo(ctx context.Context) (*runtimeapi.ImageFsInfoResponse, error) {
	logger.Debug().Msg("ImageFsInfo not implemented")
	return nil, errNotImplemented
}

// Probe / Host

func (b *Backend) RunProbe(ctx context.Context, image string, cmd []string, binds []string) ([]byte, error) {
	logger.Debug().Msg("RunProbe not implemented")
	return nil, errNotImplemented
}

func (b *Backend) HostInfo(ctx context.Context) (int, int64, string, string, error) {
	logger.Debug().Msg("HostInfo not implemented")
	return 0, 0, "", "", errNotImplemented
}

func (b *Backend) HostIDs(ctx context.Context) (string, string, string, error) {
	logger.Debug().Msg("HostIDs not implemented")
	return "", "", "", errNotImplemented
}

// Volume lifecycle

func (b *Backend) CreateVolume(ctx context.Context, name string) (string, error) {
	logger.Debug().Msg("CreateVolume not implemented")
	return "", errNotImplemented
}

func (b *Backend) RemoveVolume(ctx context.Context, name string) error {
	logger.Debug().Msg("RemoveVolume not implemented")
	return errNotImplemented
}

func (b *Backend) RemoveVolumes(ctx context.Context) ([]string, error) {
	logger.Debug().Msg("RemoveVolumes not implemented")
	return nil, errNotImplemented
}

func (b *Backend) ListVolumes(ctx context.Context, labelFilter map[string]string) ([]string, error) {
	logger.Debug().Msg("ListVolumes not implemented")
	return nil, errNotImplemented
}

// Network lifecycle

func (b *Backend) EnsureNetwork(ctx context.Context, networkType critypes.NetworkType) (string, error) {
	logger.Debug().Msg("EnsureNetwork not implemented")
	return "", errNotImplemented
}

func (b *Backend) RemoveNetworks(ctx context.Context) ([]string, error) {
	logger.Debug().Msg("RemoveNetworks not implemented")
	return nil, errNotImplemented
}
