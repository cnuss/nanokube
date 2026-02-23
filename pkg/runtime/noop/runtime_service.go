package noop

import (
	"context"
	"time"

	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type RuntimeService struct{}

func NewRuntimeService() internalapi.RuntimeService {
	return &RuntimeService{}
}

func (s *RuntimeService) Version(context.Context, string) (*runtimeapi.VersionResponse, error) {
	return &runtimeapi.VersionResponse{
		Version:           "0.1.0",
		RuntimeName:       "nanokube",
		RuntimeVersion:    "0.1.0",
		RuntimeApiVersion: "v1",
	}, nil
}
func (s *RuntimeService) CreateContainer(context.Context, string, *runtimeapi.ContainerConfig, *runtimeapi.PodSandboxConfig) (string, error) {
	return "", nil
}
func (s *RuntimeService) StartContainer(context.Context, string) error { return nil }
func (s *RuntimeService) StopContainer(context.Context, string, int64) error {
	return nil
}
func (s *RuntimeService) RemoveContainer(context.Context, string) error { return nil }
func (s *RuntimeService) ListContainers(context.Context, *runtimeapi.ContainerFilter) ([]*runtimeapi.Container, error) {
	return nil, nil
}
func (s *RuntimeService) ContainerStatus(context.Context, string, bool) (*runtimeapi.ContainerStatusResponse, error) {
	return &runtimeapi.ContainerStatusResponse{}, nil
}
func (s *RuntimeService) UpdateContainerResources(context.Context, string, *runtimeapi.ContainerResources) error {
	return nil
}
func (s *RuntimeService) ExecSync(context.Context, string, []string, time.Duration) ([]byte, []byte, error) {
	return nil, nil, nil
}
func (s *RuntimeService) Exec(context.Context, *runtimeapi.ExecRequest) (*runtimeapi.ExecResponse, error) {
	return &runtimeapi.ExecResponse{}, nil
}
func (s *RuntimeService) Attach(context.Context, *runtimeapi.AttachRequest) (*runtimeapi.AttachResponse, error) {
	return &runtimeapi.AttachResponse{}, nil
}
func (s *RuntimeService) ReopenContainerLog(context.Context, string) error { return nil }
func (s *RuntimeService) CheckpointContainer(context.Context, *runtimeapi.CheckpointContainerRequest) error {
	return nil
}
func (s *RuntimeService) GetContainerEvents(context.Context, chan *runtimeapi.ContainerEventResponse, func(runtimeapi.RuntimeService_GetContainerEventsClient)) error {
	return nil
}
func (s *RuntimeService) RunPodSandbox(context.Context, *runtimeapi.PodSandboxConfig, string) (string, error) {
	return "", nil
}
func (s *RuntimeService) StopPodSandbox(context.Context, string) error   { return nil }
func (s *RuntimeService) RemovePodSandbox(context.Context, string) error { return nil }
func (s *RuntimeService) PodSandboxStatus(context.Context, string, bool) (*runtimeapi.PodSandboxStatusResponse, error) {
	return &runtimeapi.PodSandboxStatusResponse{}, nil
}
func (s *RuntimeService) ListPodSandbox(context.Context, *runtimeapi.PodSandboxFilter) ([]*runtimeapi.PodSandbox, error) {
	return nil, nil
}
func (s *RuntimeService) PortForward(context.Context, *runtimeapi.PortForwardRequest) (*runtimeapi.PortForwardResponse, error) {
	return &runtimeapi.PortForwardResponse{}, nil
}
func (s *RuntimeService) UpdatePodSandboxResources(context.Context, *runtimeapi.UpdatePodSandboxResourcesRequest) (*runtimeapi.UpdatePodSandboxResourcesResponse, error) {
	return &runtimeapi.UpdatePodSandboxResourcesResponse{}, nil
}
func (s *RuntimeService) ContainerStats(context.Context, string) (*runtimeapi.ContainerStats, error) {
	return &runtimeapi.ContainerStats{}, nil
}
func (s *RuntimeService) ListContainerStats(context.Context, *runtimeapi.ContainerStatsFilter) ([]*runtimeapi.ContainerStats, error) {
	return nil, nil
}
func (s *RuntimeService) PodSandboxStats(context.Context, string) (*runtimeapi.PodSandboxStats, error) {
	return &runtimeapi.PodSandboxStats{}, nil
}
func (s *RuntimeService) ListPodSandboxStats(context.Context, *runtimeapi.PodSandboxStatsFilter) ([]*runtimeapi.PodSandboxStats, error) {
	return nil, nil
}
func (s *RuntimeService) ListMetricDescriptors(context.Context) ([]*runtimeapi.MetricDescriptor, error) {
	return nil, nil
}
func (s *RuntimeService) ListPodSandboxMetrics(context.Context) ([]*runtimeapi.PodSandboxMetrics, error) {
	return nil, nil
}
func (s *RuntimeService) UpdateRuntimeConfig(context.Context, *runtimeapi.RuntimeConfig) error {
	return nil
}
func (s *RuntimeService) Status(context.Context, bool) (*runtimeapi.StatusResponse, error) {
	return &runtimeapi.StatusResponse{
		Status: &runtimeapi.RuntimeStatus{
			Conditions: []*runtimeapi.RuntimeCondition{
				{Type: "RuntimeReady", Status: true},
				{Type: "NetworkReady", Status: true},
			},
		},
	}, nil
}
func (s *RuntimeService) RuntimeConfig(context.Context) (*runtimeapi.RuntimeConfigResponse, error) {
	return &runtimeapi.RuntimeConfigResponse{}, nil
}
func (s *RuntimeService) Close() error { return nil }
