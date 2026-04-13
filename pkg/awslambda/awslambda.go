package awslambda

import (
	"context"
	"os"
	"time"

	"github.com/cnuss/nanokube/pkg"
	"github.com/cnuss/nanokube/pkg/nanokube"
	v1 "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func Detect(config pkg.Config) pkg.Backend {
	temp := os.Getenv("AWS_TODO")

	if temp == "" {
		return nil
	}

	driver := &driver{config: config}
	return pkg.NewBackend(driver)
}

type driver struct {
	config pkg.Config
}

var _ pkg.Driver = &driver{}

func (d *driver) Name() string {
	return "awslambda"
}

func (d *driver) Context() context.Context {
	return d.config.Context()
}

func (d *driver) Options() nanokube.Options {
	return d.config.Options()
}

func (d *driver) CgroupRoot() string {
	nanokube.Unimplemented()
	return "/"
}

func (d *driver) Attach(ctx context.Context, req *v1.AttachRequest) (*v1.AttachResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) CheckpointContainer(ctx context.Context, options *v1.CheckpointContainerRequest) error {
	return nanokube.Unimplemented()
}

func (d *driver) Close() error {
	return nanokube.Unimplemented()
}

func (d *driver) ContainerStats(ctx context.Context, containerID string) (*v1.ContainerStats, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) ContainerStatus(ctx context.Context, containerID string, verbose bool) (*v1.ContainerStatusResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) CreateContainer(ctx context.Context, podSandboxID string, config *v1.ContainerConfig, sandboxConfig *v1.PodSandboxConfig) (string, error) {
	return "", nanokube.Unimplemented()
}

func (d *driver) Exec(ctx context.Context, request *v1.ExecRequest) (*v1.ExecResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) ExecSync(ctx context.Context, containerID string, cmd []string, timeout time.Duration) (stdout []byte, stderr []byte, err error) {
	return nil, nil, nanokube.Unimplemented()
}

func (d *driver) GetContainerEvents(ctx context.Context, containerEventsCh chan *v1.ContainerEventResponse, connectionEstablishedCallback func(v1.RuntimeService_GetContainerEventsClient)) error {
	return nanokube.Unimplemented()
}

func (d *driver) ImageFsInfo(ctx context.Context) (*v1.ImageFsInfoResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) ImageStatus(ctx context.Context, image *v1.ImageSpec, verbose bool) (*v1.ImageStatusResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) ListContainerStats(ctx context.Context, filter *v1.ContainerStatsFilter) ([]*v1.ContainerStats, error) {
	return []*v1.ContainerStats{}, nanokube.Unimplemented()
}

func (d *driver) ListContainers(ctx context.Context, filter *v1.ContainerFilter) ([]*v1.Container, error) {
	return []*v1.Container{}, nanokube.Unimplemented()
}

func (d *driver) ListImages(ctx context.Context, filter *v1.ImageFilter) ([]*v1.Image, error) {
	return []*v1.Image{}, nanokube.Unimplemented()
}

func (d *driver) ListMetricDescriptors(ctx context.Context) ([]*v1.MetricDescriptor, error) {
	return []*v1.MetricDescriptor{}, nanokube.Unimplemented()
}

func (d *driver) ListPodSandbox(ctx context.Context, filter *v1.PodSandboxFilter) ([]*v1.PodSandbox, error) {
	return []*v1.PodSandbox{}, nanokube.Unimplemented()
}

func (d *driver) ListPodSandboxMetrics(ctx context.Context) ([]*v1.PodSandboxMetrics, error) {
	return []*v1.PodSandboxMetrics{}, nanokube.Unimplemented()
}

func (d *driver) ListPodSandboxStats(ctx context.Context, filter *v1.PodSandboxStatsFilter) ([]*v1.PodSandboxStats, error) {
	return []*v1.PodSandboxStats{}, nanokube.Unimplemented()
}

func (d *driver) PodSandboxStats(ctx context.Context, podSandboxID string) (*v1.PodSandboxStats, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) PodSandboxStatus(ctx context.Context, podSandboxID string, verbose bool) (*v1.PodSandboxStatusResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) PortForward(ctx context.Context, request *v1.PortForwardRequest) (*v1.PortForwardResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) PullImage(ctx context.Context, image *v1.ImageSpec, auth *v1.AuthConfig, podSandboxConfig *v1.PodSandboxConfig) (string, error) {
	return "", nanokube.Unimplemented()
}

func (d *driver) RemoveContainer(ctx context.Context, containerID string) error {
	return nanokube.Unimplemented()
}

func (d *driver) RemoveImage(ctx context.Context, image *v1.ImageSpec) error {
	return nanokube.Unimplemented()
}

func (d *driver) RemovePodSandbox(ctx context.Context, podSandboxID string) error {
	return nanokube.Unimplemented()
}

func (d *driver) ReopenContainerLog(ctx context.Context, ContainerID string) error {
	return nanokube.Unimplemented()
}

func (d *driver) RunPodSandbox(ctx context.Context, config *v1.PodSandboxConfig, runtimeHandler string) (string, error) {
	return "", nanokube.Unimplemented()
}

func (d *driver) RuntimeConfig(ctx context.Context) (*v1.RuntimeConfigResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) StartContainer(ctx context.Context, containerID string) error {
	return nanokube.Unimplemented()
}

func (d *driver) Status(ctx context.Context, verbose bool) (*v1.StatusResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) StopContainer(ctx context.Context, containerID string, timeout int64) error {
	return nanokube.Unimplemented()
}

func (d *driver) StopPodSandbox(ctx context.Context, podSandboxID string) error {
	return nanokube.Unimplemented()
}

func (d *driver) UpdateContainerResources(ctx context.Context, containerID string, resources *v1.ContainerResources) error {
	return nanokube.Unimplemented()
}

func (d *driver) UpdatePodSandboxResources(ctx context.Context, request *v1.UpdatePodSandboxResourcesRequest) (*v1.UpdatePodSandboxResourcesResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) UpdateRuntimeConfig(ctx context.Context, runtimeConfig *v1.RuntimeConfig) error {
	return nanokube.Unimplemented()
}

func (d *driver) Version(ctx context.Context, apiVersion string) (*v1.VersionResponse, error) {
	return &v1.VersionResponse{
		Version:           d.config.Version(),
		RuntimeName:       "awslambda",
		RuntimeVersion:    "0.0.0", // TODO from lambda
		RuntimeApiVersion: apiVersion,
	}, nil
}

func (d *driver) ExecHost(img string, cmd []string, mounts []string) (string, error) {
	return "", nanokube.Unimplemented()
}
