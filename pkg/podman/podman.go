package podman

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/cnuss/nanokube/pkg"
	v1 "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func Detect(config pkg.Config) pkg.Backend {
	sockets := []string{}

	for _, socket := range sockets {
		if _, err := os.Stat(socket); err == nil {
			backend, err := newBackend(config)
			if err != nil {
				continue
			}
			return backend
		}
	}

	return nil
}

func newBackend(config pkg.Config) (pkg.Backend, error) {
	_, cancel := context.WithTimeout(config.Context(), 5*time.Second)
	defer cancel()

	// TODO: Setup client and ping

	driver := &driver{config: config}

	return pkg.NewBackend(driver), nil
}

type driver struct {
	config pkg.Config
}

var _ pkg.Driver = &driver{}

func (d *driver) Name() string {
	return "podman"
}

func (d *driver) Attach(ctx context.Context, req *v1.AttachRequest) (*v1.AttachResponse, error) {
	return nil, fmt.Errorf("unimplemented")
}

func (d *driver) CheckpointContainer(ctx context.Context, options *v1.CheckpointContainerRequest) error {
	return fmt.Errorf("unimplemented")
}

func (d *driver) Close() error {
	return fmt.Errorf("unimplemented")
}

func (d *driver) ContainerStats(ctx context.Context, containerID string) (*v1.ContainerStats, error) {
	return nil, fmt.Errorf("unimplemented")
}

func (d *driver) ContainerStatus(ctx context.Context, containerID string, verbose bool) (*v1.ContainerStatusResponse, error) {
	return nil, fmt.Errorf("unimplemented")
}

func (d *driver) CreateContainer(ctx context.Context, podSandboxID string, config *v1.ContainerConfig, sandboxConfig *v1.PodSandboxConfig) (string, error) {
	return "", fmt.Errorf("unimplemented")
}

func (d *driver) Exec(ctx context.Context, request *v1.ExecRequest) (*v1.ExecResponse, error) {
	return nil, fmt.Errorf("unimplemented")
}

func (d *driver) ExecSync(ctx context.Context, containerID string, cmd []string, timeout time.Duration) (stdout []byte, stderr []byte, err error) {
	return nil, nil, fmt.Errorf("unimplemented")
}

func (d *driver) GetContainerEvents(ctx context.Context, containerEventsCh chan *v1.ContainerEventResponse, connectionEstablishedCallback func(v1.RuntimeService_GetContainerEventsClient)) error {
	return fmt.Errorf("unimplemented")
}

func (d *driver) ImageFsInfo(ctx context.Context) (*v1.ImageFsInfoResponse, error) {
	return nil, fmt.Errorf("unimplemented")
}

func (d *driver) ImageStatus(ctx context.Context, image *v1.ImageSpec, verbose bool) (*v1.ImageStatusResponse, error) {
	return nil, fmt.Errorf("unimplemented")
}

func (d *driver) ListContainerStats(ctx context.Context, filter *v1.ContainerStatsFilter) ([]*v1.ContainerStats, error) {
	return []*v1.ContainerStats{}, fmt.Errorf("unimplemented")
}

func (d *driver) ListContainers(ctx context.Context, filter *v1.ContainerFilter) ([]*v1.Container, error) {
	return []*v1.Container{}, fmt.Errorf("unimplemented")
}

func (d *driver) ListImages(ctx context.Context, filter *v1.ImageFilter) ([]*v1.Image, error) {
	return []*v1.Image{}, fmt.Errorf("unimplemented")
}

func (d *driver) ListMetricDescriptors(ctx context.Context) ([]*v1.MetricDescriptor, error) {
	return []*v1.MetricDescriptor{}, fmt.Errorf("unimplemented")
}

func (d *driver) ListPodSandbox(ctx context.Context, filter *v1.PodSandboxFilter) ([]*v1.PodSandbox, error) {
	return []*v1.PodSandbox{}, fmt.Errorf("unimplemented")
}

func (d *driver) ListPodSandboxMetrics(ctx context.Context) ([]*v1.PodSandboxMetrics, error) {
	return []*v1.PodSandboxMetrics{}, fmt.Errorf("unimplemented")
}

func (d *driver) ListPodSandboxStats(ctx context.Context, filter *v1.PodSandboxStatsFilter) ([]*v1.PodSandboxStats, error) {
	return []*v1.PodSandboxStats{}, fmt.Errorf("unimplemented")
}

func (d *driver) PodSandboxStats(ctx context.Context, podSandboxID string) (*v1.PodSandboxStats, error) {
	return nil, fmt.Errorf("unimplemented")
}

func (d *driver) PodSandboxStatus(ctx context.Context, podSandboxID string, verbose bool) (*v1.PodSandboxStatusResponse, error) {
	return nil, fmt.Errorf("unimplemented")
}

func (d *driver) PortForward(ctx context.Context, request *v1.PortForwardRequest) (*v1.PortForwardResponse, error) {
	return nil, fmt.Errorf("unimplemented")
}

func (d *driver) PullImage(ctx context.Context, image *v1.ImageSpec, auth *v1.AuthConfig, podSandboxConfig *v1.PodSandboxConfig) (string, error) {
	return "", fmt.Errorf("unimplemented")
}

func (d *driver) RemoveContainer(ctx context.Context, containerID string) error {
	return fmt.Errorf("unimplemented")
}

func (d *driver) RemoveImage(ctx context.Context, image *v1.ImageSpec) error {
	return fmt.Errorf("unimplemented")
}

func (d *driver) RemovePodSandbox(ctx context.Context, podSandboxID string) error {
	return fmt.Errorf("unimplemented")
}

func (d *driver) ReopenContainerLog(ctx context.Context, ContainerID string) error {
	return fmt.Errorf("unimplemented")
}

func (d *driver) RunPodSandbox(ctx context.Context, config *v1.PodSandboxConfig, runtimeHandler string) (string, error) {
	return "", fmt.Errorf("unimplemented")
}

func (d *driver) RuntimeConfig(ctx context.Context) (*v1.RuntimeConfigResponse, error) {
	return nil, fmt.Errorf("unimplemented")
}

func (d *driver) StartContainer(ctx context.Context, containerID string) error {
	return fmt.Errorf("unimplemented")
}

func (d *driver) Status(ctx context.Context, verbose bool) (*v1.StatusResponse, error) {
	return nil, fmt.Errorf("unimplemented")
}

func (d *driver) StopContainer(ctx context.Context, containerID string, timeout int64) error {
	return fmt.Errorf("unimplemented")
}

func (d *driver) StopPodSandbox(ctx context.Context, podSandboxID string) error {
	return fmt.Errorf("unimplemented")
}

func (d *driver) UpdateContainerResources(ctx context.Context, containerID string, resources *v1.ContainerResources) error {
	return fmt.Errorf("unimplemented")
}

func (d *driver) UpdatePodSandboxResources(ctx context.Context, request *v1.UpdatePodSandboxResourcesRequest) (*v1.UpdatePodSandboxResourcesResponse, error) {
	return nil, fmt.Errorf("unimplemented")
}

func (d *driver) UpdateRuntimeConfig(ctx context.Context, runtimeConfig *v1.RuntimeConfig) error {
	return fmt.Errorf("unimplemented")
}

func (d *driver) Version(ctx context.Context, apiVersion string) (*v1.VersionResponse, error) {
	return &v1.VersionResponse{
		Version:           d.config.Version(),
		RuntimeName:       "podman",
		RuntimeVersion:    "0.0.0", // TODO from podman
		RuntimeApiVersion: apiVersion,
	}, nil
}
