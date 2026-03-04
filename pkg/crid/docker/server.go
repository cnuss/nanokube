package docker

import (
	"context"

	"google.golang.org/grpc"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func NewServer(backend *DockerBackend) *Server {
	return &Server{
		backend: backend,
	}
}

type Server struct {
	runtimeapi.UnsafeImageServiceServer
	runtimeapi.UnsafeRuntimeServiceServer
	backend *DockerBackend
}

// Attach implements [v1.RuntimeServiceServer].
func (s *Server) Attach(context.Context, *runtimeapi.AttachRequest) (*runtimeapi.AttachResponse, error) {
	panic("unimplemented")
}

// CheckpointContainer implements [v1.RuntimeServiceServer].
func (s *Server) CheckpointContainer(context.Context, *runtimeapi.CheckpointContainerRequest) (*runtimeapi.CheckpointContainerResponse, error) {
	panic("unimplemented")
}

// ContainerStats implements [v1.RuntimeServiceServer].
func (s *Server) ContainerStats(context.Context, *runtimeapi.ContainerStatsRequest) (*runtimeapi.ContainerStatsResponse, error) {
	panic("unimplemented")
}

// ContainerStatus implements [v1.RuntimeServiceServer].
func (s *Server) ContainerStatus(context.Context, *runtimeapi.ContainerStatusRequest) (*runtimeapi.ContainerStatusResponse, error) {
	panic("unimplemented")
}

// CreateContainer implements [v1.RuntimeServiceServer].
func (s *Server) CreateContainer(context.Context, *runtimeapi.CreateContainerRequest) (*runtimeapi.CreateContainerResponse, error) {
	panic("unimplemented")
}

// Exec implements [v1.RuntimeServiceServer].
func (s *Server) Exec(context.Context, *runtimeapi.ExecRequest) (*runtimeapi.ExecResponse, error) {
	panic("unimplemented")
}

// ExecSync implements [v1.RuntimeServiceServer].
func (s *Server) ExecSync(context.Context, *runtimeapi.ExecSyncRequest) (*runtimeapi.ExecSyncResponse, error) {
	panic("unimplemented")
}

// GetContainerEvents implements [v1.RuntimeServiceServer].
func (s *Server) GetContainerEvents(*runtimeapi.GetEventsRequest, grpc.ServerStreamingServer[runtimeapi.ContainerEventResponse]) error {
	panic("unimplemented")
}

// ListContainerStats implements [v1.RuntimeServiceServer].
func (s *Server) ListContainerStats(context.Context, *runtimeapi.ListContainerStatsRequest) (*runtimeapi.ListContainerStatsResponse, error) {
	panic("unimplemented")
}

// ListContainers implements [v1.RuntimeServiceServer].
func (s *Server) ListContainers(context.Context, *runtimeapi.ListContainersRequest) (*runtimeapi.ListContainersResponse, error) {
	panic("unimplemented")
}

// ListMetricDescriptors implements [v1.RuntimeServiceServer].
func (s *Server) ListMetricDescriptors(context.Context, *runtimeapi.ListMetricDescriptorsRequest) (*runtimeapi.ListMetricDescriptorsResponse, error) {
	panic("unimplemented")
}

// ListPodSandbox implements [v1.RuntimeServiceServer].
func (s *Server) ListPodSandbox(context.Context, *runtimeapi.ListPodSandboxRequest) (*runtimeapi.ListPodSandboxResponse, error) {
	panic("unimplemented")
}

// ListPodSandboxMetrics implements [v1.RuntimeServiceServer].
func (s *Server) ListPodSandboxMetrics(context.Context, *runtimeapi.ListPodSandboxMetricsRequest) (*runtimeapi.ListPodSandboxMetricsResponse, error) {
	panic("unimplemented")
}

// ListPodSandboxStats implements [v1.RuntimeServiceServer].
func (s *Server) ListPodSandboxStats(context.Context, *runtimeapi.ListPodSandboxStatsRequest) (*runtimeapi.ListPodSandboxStatsResponse, error) {
	panic("unimplemented")
}

// PodSandboxStats implements [v1.RuntimeServiceServer].
func (s *Server) PodSandboxStats(context.Context, *runtimeapi.PodSandboxStatsRequest) (*runtimeapi.PodSandboxStatsResponse, error) {
	panic("unimplemented")
}

// PodSandboxStatus implements [v1.RuntimeServiceServer].
func (s *Server) PodSandboxStatus(context.Context, *runtimeapi.PodSandboxStatusRequest) (*runtimeapi.PodSandboxStatusResponse, error) {
	panic("unimplemented")
}

// PortForward implements [v1.RuntimeServiceServer].
func (s *Server) PortForward(context.Context, *runtimeapi.PortForwardRequest) (*runtimeapi.PortForwardResponse, error) {
	panic("unimplemented")
}

// RemoveContainer implements [v1.RuntimeServiceServer].
func (s *Server) RemoveContainer(context.Context, *runtimeapi.RemoveContainerRequest) (*runtimeapi.RemoveContainerResponse, error) {
	panic("unimplemented")
}

// RemovePodSandbox implements [v1.RuntimeServiceServer].
func (s *Server) RemovePodSandbox(context.Context, *runtimeapi.RemovePodSandboxRequest) (*runtimeapi.RemovePodSandboxResponse, error) {
	panic("unimplemented")
}

// ReopenContainerLog implements [v1.RuntimeServiceServer].
func (s *Server) ReopenContainerLog(context.Context, *runtimeapi.ReopenContainerLogRequest) (*runtimeapi.ReopenContainerLogResponse, error) {
	panic("unimplemented")
}

// RunPodSandbox implements [v1.RuntimeServiceServer].
func (s *Server) RunPodSandbox(context.Context, *runtimeapi.RunPodSandboxRequest) (*runtimeapi.RunPodSandboxResponse, error) {
	panic("unimplemented")
}

// RuntimeConfig implements [v1.RuntimeServiceServer].
func (s *Server) RuntimeConfig(context.Context, *runtimeapi.RuntimeConfigRequest) (*runtimeapi.RuntimeConfigResponse, error) {
	panic("unimplemented")
}

// StartContainer implements [v1.RuntimeServiceServer].
func (s *Server) StartContainer(context.Context, *runtimeapi.StartContainerRequest) (*runtimeapi.StartContainerResponse, error) {
	panic("unimplemented")
}

// Status implements [v1.RuntimeServiceServer].
func (s *Server) Status(context.Context, *runtimeapi.StatusRequest) (*runtimeapi.StatusResponse, error) {
	panic("unimplemented")
}

// StopContainer implements [v1.RuntimeServiceServer].
func (s *Server) StopContainer(context.Context, *runtimeapi.StopContainerRequest) (*runtimeapi.StopContainerResponse, error) {
	panic("unimplemented")
}

// StopPodSandbox implements [v1.RuntimeServiceServer].
func (s *Server) StopPodSandbox(context.Context, *runtimeapi.StopPodSandboxRequest) (*runtimeapi.StopPodSandboxResponse, error) {
	panic("unimplemented")
}

// UpdateContainerResources implements [v1.RuntimeServiceServer].
func (s *Server) UpdateContainerResources(context.Context, *runtimeapi.UpdateContainerResourcesRequest) (*runtimeapi.UpdateContainerResourcesResponse, error) {
	panic("unimplemented")
}

// UpdatePodSandboxResources implements [v1.RuntimeServiceServer].
func (s *Server) UpdatePodSandboxResources(context.Context, *runtimeapi.UpdatePodSandboxResourcesRequest) (*runtimeapi.UpdatePodSandboxResourcesResponse, error) {
	panic("unimplemented")
}

// UpdateRuntimeConfig implements [v1.RuntimeServiceServer].
func (s *Server) UpdateRuntimeConfig(context.Context, *runtimeapi.UpdateRuntimeConfigRequest) (*runtimeapi.UpdateRuntimeConfigResponse, error) {
	panic("unimplemented")
}

// Version implements [v1.RuntimeServiceServer].
func (s *Server) Version(context.Context, *runtimeapi.VersionRequest) (*runtimeapi.VersionResponse, error) {
	panic("unimplemented")
}

// ImageFsInfo implements [v1.ImageServiceServer].
func (s *Server) ImageFsInfo(context.Context, *runtimeapi.ImageFsInfoRequest) (*runtimeapi.ImageFsInfoResponse, error) {
	panic("unimplemented")
}

// ImageStatus implements [v1.ImageServiceServer].
func (s *Server) ImageStatus(context.Context, *runtimeapi.ImageStatusRequest) (*runtimeapi.ImageStatusResponse, error) {
	panic("unimplemented")
}

// ListImages implements [v1.ImageServiceServer].
func (s *Server) ListImages(context.Context, *runtimeapi.ListImagesRequest) (*runtimeapi.ListImagesResponse, error) {
	panic("unimplemented")
}

// PullImage implements [v1.ImageServiceServer].
func (s *Server) PullImage(context.Context, *runtimeapi.PullImageRequest) (*runtimeapi.PullImageResponse, error) {
	panic("unimplemented")
}

// RemoveImage implements [v1.ImageServiceServer].
func (s *Server) RemoveImage(context.Context, *runtimeapi.RemoveImageRequest) (*runtimeapi.RemoveImageResponse, error) {
	panic("unimplemented")
}

var _ runtimeapi.ImageServiceServer = &Server{}
var _ runtimeapi.RuntimeServiceServer = &Server{}
