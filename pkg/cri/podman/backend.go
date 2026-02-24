package podman

import (
	"context"
	"fmt"
	"time"

	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

var errNotImplemented = fmt.Errorf("podman backend not yet implemented")

// Backend implements cri.Backend using the Podman API.
type Backend struct {
	podmanSocket string
}

func New(podmanSocket string) *Backend {
	return &Backend{podmanSocket: podmanSocket}
}

func (b *Backend) Init(ctx context.Context) error {
	return errNotImplemented
}

func (b *Backend) Close() error {
	return nil
}

// Version / Status

func (b *Backend) Version(ctx context.Context) (*runtimeapi.VersionResponse, error) {
	return nil, errNotImplemented
}

func (b *Backend) Status(ctx context.Context, verbose bool) (*runtimeapi.StatusResponse, error) {
	return nil, errNotImplemented
}

func (b *Backend) UpdateRuntimeConfig(ctx context.Context, config *runtimeapi.RuntimeConfig) error {
	return nil
}

func (b *Backend) RuntimeConfig(ctx context.Context) (*runtimeapi.RuntimeConfigResponse, error) {
	return &runtimeapi.RuntimeConfigResponse{}, nil
}

// Pod Sandbox

func (b *Backend) RunPodSandbox(ctx context.Context, config *runtimeapi.PodSandboxConfig, runtimeHandler string) (string, error) {
	return "", errNotImplemented
}

func (b *Backend) StopPodSandbox(ctx context.Context, podSandboxID string) error {
	return errNotImplemented
}

func (b *Backend) RemovePodSandbox(ctx context.Context, podSandboxID string) error {
	return errNotImplemented
}

func (b *Backend) PodSandboxStatus(ctx context.Context, podSandboxID string, verbose bool) (*runtimeapi.PodSandboxStatusResponse, error) {
	return nil, errNotImplemented
}

func (b *Backend) ListPodSandbox(ctx context.Context, filter *runtimeapi.PodSandboxFilter) ([]*runtimeapi.PodSandbox, error) {
	return nil, errNotImplemented
}

// Container

func (b *Backend) CreateContainer(ctx context.Context, podSandboxID string, config *runtimeapi.ContainerConfig, sandboxConfig *runtimeapi.PodSandboxConfig) (string, error) {
	return "", errNotImplemented
}

func (b *Backend) StartContainer(ctx context.Context, containerID string) error {
	return errNotImplemented
}

func (b *Backend) StopContainer(ctx context.Context, containerID string, timeout int64) error {
	return errNotImplemented
}

func (b *Backend) RemoveContainer(ctx context.Context, containerID string) error {
	return errNotImplemented
}

func (b *Backend) ListContainers(ctx context.Context, filter *runtimeapi.ContainerFilter) ([]*runtimeapi.Container, error) {
	return nil, errNotImplemented
}

func (b *Backend) ContainerStatus(ctx context.Context, containerID string, verbose bool) (*runtimeapi.ContainerStatusResponse, error) {
	return nil, errNotImplemented
}

func (b *Backend) UpdateContainerResources(ctx context.Context, containerID string, resources *runtimeapi.ContainerResources) error {
	return errNotImplemented
}

func (b *Backend) ReopenContainerLog(ctx context.Context, containerID string) error {
	return errNotImplemented
}

func (b *Backend) ExecSync(ctx context.Context, containerID string, cmd []string, timeout time.Duration) ([]byte, []byte, error) {
	return nil, nil, errNotImplemented
}

// Stats

func (b *Backend) ContainerStats(ctx context.Context, containerID string) (*runtimeapi.ContainerStats, error) {
	return nil, errNotImplemented
}

func (b *Backend) ListContainerStats(ctx context.Context, filter *runtimeapi.ContainerStatsFilter) ([]*runtimeapi.ContainerStats, error) {
	return nil, errNotImplemented
}

func (b *Backend) PodSandboxStats(ctx context.Context, podSandboxID string) (*runtimeapi.PodSandboxStats, error) {
	return nil, errNotImplemented
}

func (b *Backend) ListPodSandboxStats(ctx context.Context, filter *runtimeapi.PodSandboxStatsFilter) ([]*runtimeapi.PodSandboxStats, error) {
	return nil, errNotImplemented
}

// Metrics

func (b *Backend) ListMetricDescriptors(ctx context.Context) ([]*runtimeapi.MetricDescriptor, error) {
	return nil, nil
}

func (b *Backend) ListPodSandboxMetrics(ctx context.Context) ([]*runtimeapi.PodSandboxMetrics, error) {
	return nil, nil
}

// Checkpoint

func (b *Backend) CheckpointContainer(ctx context.Context, containerID, location string, timeout int64) error {
	return errNotImplemented
}

// Container Events

func (b *Backend) GetContainerEvents(ctx context.Context, eventsCh chan *runtimeapi.ContainerEventResponse) error {
	<-ctx.Done()
	close(eventsCh)
	return nil
}

// Resource updates

func (b *Backend) UpdatePodSandboxResources(ctx context.Context, req *runtimeapi.UpdatePodSandboxResourcesRequest) (*runtimeapi.UpdatePodSandboxResourcesResponse, error) {
	return &runtimeapi.UpdatePodSandboxResourcesResponse{}, nil
}

// Image

func (b *Backend) ListImages(ctx context.Context, filter *runtimeapi.ImageFilter) ([]*runtimeapi.Image, error) {
	return nil, errNotImplemented
}

func (b *Backend) ImageStatus(ctx context.Context, image *runtimeapi.ImageSpec, verbose bool) (*runtimeapi.ImageStatusResponse, error) {
	return nil, errNotImplemented
}

func (b *Backend) PullImage(ctx context.Context, image *runtimeapi.ImageSpec, auth *runtimeapi.AuthConfig, sandbox *runtimeapi.PodSandboxConfig) (string, error) {
	return "", errNotImplemented
}

func (b *Backend) RemoveImage(ctx context.Context, image *runtimeapi.ImageSpec) error {
	return errNotImplemented
}

func (b *Backend) ImageFsInfo(ctx context.Context) (*runtimeapi.ImageFsInfoResponse, error) {
	return nil, errNotImplemented
}
