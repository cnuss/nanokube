package cri

import (
	"context"
	"time"

	"google.golang.org/grpc"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/kubelet/pkg/cri/streaming"
)

type runtimeService struct {
	runtimeapi.UnimplementedRuntimeServiceServer
	backend      Backend
	streamServer streaming.Server
}

func (s *runtimeService) Version(ctx context.Context, req *runtimeapi.VersionRequest) (*runtimeapi.VersionResponse, error) {
	return s.backend.Version(ctx)
}

func (s *runtimeService) RunPodSandbox(ctx context.Context, req *runtimeapi.RunPodSandboxRequest) (*runtimeapi.RunPodSandboxResponse, error) {
	id, err := s.backend.RunPodSandbox(ctx, req.GetConfig(), req.GetRuntimeHandler())
	if err != nil {
		return nil, err
	}
	return &runtimeapi.RunPodSandboxResponse{PodSandboxId: id}, nil
}

func (s *runtimeService) StopPodSandbox(ctx context.Context, req *runtimeapi.StopPodSandboxRequest) (*runtimeapi.StopPodSandboxResponse, error) {
	if err := s.backend.StopPodSandbox(ctx, req.GetPodSandboxId()); err != nil {
		return nil, err
	}
	return &runtimeapi.StopPodSandboxResponse{}, nil
}

func (s *runtimeService) RemovePodSandbox(ctx context.Context, req *runtimeapi.RemovePodSandboxRequest) (*runtimeapi.RemovePodSandboxResponse, error) {
	if err := s.backend.RemovePodSandbox(ctx, req.GetPodSandboxId()); err != nil {
		return nil, err
	}
	return &runtimeapi.RemovePodSandboxResponse{}, nil
}

func (s *runtimeService) PodSandboxStatus(ctx context.Context, req *runtimeapi.PodSandboxStatusRequest) (*runtimeapi.PodSandboxStatusResponse, error) {
	return s.backend.PodSandboxStatus(ctx, req.GetPodSandboxId(), req.GetVerbose())
}

func (s *runtimeService) ListPodSandbox(ctx context.Context, req *runtimeapi.ListPodSandboxRequest) (*runtimeapi.ListPodSandboxResponse, error) {
	items, err := s.backend.ListPodSandbox(ctx, req.GetFilter())
	if err != nil {
		return nil, err
	}
	return &runtimeapi.ListPodSandboxResponse{Items: items}, nil
}

func (s *runtimeService) CreateContainer(ctx context.Context, req *runtimeapi.CreateContainerRequest) (*runtimeapi.CreateContainerResponse, error) {
	id, err := s.backend.CreateContainer(ctx, req.GetPodSandboxId(), req.GetConfig(), req.GetSandboxConfig())
	if err != nil {
		return nil, err
	}
	return &runtimeapi.CreateContainerResponse{ContainerId: id}, nil
}

func (s *runtimeService) StartContainer(ctx context.Context, req *runtimeapi.StartContainerRequest) (*runtimeapi.StartContainerResponse, error) {
	if err := s.backend.StartContainer(ctx, req.GetContainerId()); err != nil {
		return nil, err
	}
	return &runtimeapi.StartContainerResponse{}, nil
}

func (s *runtimeService) StopContainer(ctx context.Context, req *runtimeapi.StopContainerRequest) (*runtimeapi.StopContainerResponse, error) {
	if err := s.backend.StopContainer(ctx, req.GetContainerId(), req.GetTimeout()); err != nil {
		return nil, err
	}
	return &runtimeapi.StopContainerResponse{}, nil
}

func (s *runtimeService) RemoveContainer(ctx context.Context, req *runtimeapi.RemoveContainerRequest) (*runtimeapi.RemoveContainerResponse, error) {
	if err := s.backend.RemoveContainer(ctx, req.GetContainerId()); err != nil {
		return nil, err
	}
	return &runtimeapi.RemoveContainerResponse{}, nil
}

func (s *runtimeService) ListContainers(ctx context.Context, req *runtimeapi.ListContainersRequest) (*runtimeapi.ListContainersResponse, error) {
	containers, err := s.backend.ListContainers(ctx, req.GetFilter())
	if err != nil {
		return nil, err
	}
	return &runtimeapi.ListContainersResponse{Containers: containers}, nil
}

func (s *runtimeService) ContainerStatus(ctx context.Context, req *runtimeapi.ContainerStatusRequest) (*runtimeapi.ContainerStatusResponse, error) {
	return s.backend.ContainerStatus(ctx, req.GetContainerId(), req.GetVerbose())
}

func (s *runtimeService) UpdateContainerResources(ctx context.Context, req *runtimeapi.UpdateContainerResourcesRequest) (*runtimeapi.UpdateContainerResourcesResponse, error) {
	resources := &runtimeapi.ContainerResources{
		Linux:   req.GetLinux(),
		Windows: req.GetWindows(),
	}
	if err := s.backend.UpdateContainerResources(ctx, req.GetContainerId(), resources); err != nil {
		return nil, err
	}
	return &runtimeapi.UpdateContainerResourcesResponse{}, nil
}

func (s *runtimeService) ReopenContainerLog(ctx context.Context, req *runtimeapi.ReopenContainerLogRequest) (*runtimeapi.ReopenContainerLogResponse, error) {
	if err := s.backend.ReopenContainerLog(ctx, req.GetContainerId()); err != nil {
		return nil, err
	}
	return &runtimeapi.ReopenContainerLogResponse{}, nil
}

func (s *runtimeService) ExecSync(ctx context.Context, req *runtimeapi.ExecSyncRequest) (*runtimeapi.ExecSyncResponse, error) {
	stdout, stderr, err := s.backend.ExecSync(ctx, req.GetContainerId(), req.GetCmd(), time.Duration(req.GetTimeout())*time.Second)
	resp := &runtimeapi.ExecSyncResponse{Stdout: stdout, Stderr: stderr}
	if err != nil {
		// Return non-zero exit code for exec failures rather than gRPC error
		resp.ExitCode = -1
		return resp, nil
	}
	return resp, nil
}

func (s *runtimeService) Exec(ctx context.Context, req *runtimeapi.ExecRequest) (*runtimeapi.ExecResponse, error) {
	if s.streamServer == nil {
		return &runtimeapi.ExecResponse{}, nil
	}
	return s.streamServer.GetExec(req)
}

func (s *runtimeService) Attach(ctx context.Context, req *runtimeapi.AttachRequest) (*runtimeapi.AttachResponse, error) {
	if s.streamServer == nil {
		return &runtimeapi.AttachResponse{}, nil
	}
	return s.streamServer.GetAttach(req)
}

func (s *runtimeService) PortForward(ctx context.Context, req *runtimeapi.PortForwardRequest) (*runtimeapi.PortForwardResponse, error) {
	if s.streamServer == nil {
		return &runtimeapi.PortForwardResponse{}, nil
	}
	return s.streamServer.GetPortForward(req)
}

func (s *runtimeService) ContainerStats(ctx context.Context, req *runtimeapi.ContainerStatsRequest) (*runtimeapi.ContainerStatsResponse, error) {
	stats, err := s.backend.ContainerStats(ctx, req.GetContainerId())
	if err != nil {
		return nil, err
	}
	return &runtimeapi.ContainerStatsResponse{Stats: stats}, nil
}

func (s *runtimeService) ListContainerStats(ctx context.Context, req *runtimeapi.ListContainerStatsRequest) (*runtimeapi.ListContainerStatsResponse, error) {
	stats, err := s.backend.ListContainerStats(ctx, req.GetFilter())
	if err != nil {
		return nil, err
	}
	return &runtimeapi.ListContainerStatsResponse{Stats: stats}, nil
}

func (s *runtimeService) PodSandboxStats(ctx context.Context, req *runtimeapi.PodSandboxStatsRequest) (*runtimeapi.PodSandboxStatsResponse, error) {
	stats, err := s.backend.PodSandboxStats(ctx, req.GetPodSandboxId())
	if err != nil {
		return nil, err
	}
	return &runtimeapi.PodSandboxStatsResponse{Stats: stats}, nil
}

func (s *runtimeService) ListPodSandboxStats(ctx context.Context, req *runtimeapi.ListPodSandboxStatsRequest) (*runtimeapi.ListPodSandboxStatsResponse, error) {
	stats, err := s.backend.ListPodSandboxStats(ctx, req.GetFilter())
	if err != nil {
		return nil, err
	}
	return &runtimeapi.ListPodSandboxStatsResponse{Stats: stats}, nil
}

func (s *runtimeService) UpdateRuntimeConfig(ctx context.Context, req *runtimeapi.UpdateRuntimeConfigRequest) (*runtimeapi.UpdateRuntimeConfigResponse, error) {
	if err := s.backend.UpdateRuntimeConfig(ctx, req.GetRuntimeConfig()); err != nil {
		return nil, err
	}
	return &runtimeapi.UpdateRuntimeConfigResponse{}, nil
}

func (s *runtimeService) Status(ctx context.Context, req *runtimeapi.StatusRequest) (*runtimeapi.StatusResponse, error) {
	return s.backend.Status(ctx, req.GetVerbose())
}

func (s *runtimeService) CheckpointContainer(ctx context.Context, req *runtimeapi.CheckpointContainerRequest) (*runtimeapi.CheckpointContainerResponse, error) {
	if err := s.backend.CheckpointContainer(ctx, req.GetContainerId(), req.GetLocation(), req.GetTimeout()); err != nil {
		return nil, err
	}
	return &runtimeapi.CheckpointContainerResponse{}, nil
}

func (s *runtimeService) GetContainerEvents(req *runtimeapi.GetEventsRequest, stream grpc.ServerStreamingServer[runtimeapi.ContainerEventResponse]) error {
	eventsCh := make(chan *runtimeapi.ContainerEventResponse, 100)
	go func() {
		for ev := range eventsCh {
			if err := stream.Send(ev); err != nil {
				return
			}
		}
	}()
	return s.backend.GetContainerEvents(stream.Context(), eventsCh)
}

func (s *runtimeService) ListMetricDescriptors(ctx context.Context, req *runtimeapi.ListMetricDescriptorsRequest) (*runtimeapi.ListMetricDescriptorsResponse, error) {
	descriptors, err := s.backend.ListMetricDescriptors(ctx)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.ListMetricDescriptorsResponse{Descriptors: descriptors}, nil
}

func (s *runtimeService) ListPodSandboxMetrics(ctx context.Context, req *runtimeapi.ListPodSandboxMetricsRequest) (*runtimeapi.ListPodSandboxMetricsResponse, error) {
	metrics, err := s.backend.ListPodSandboxMetrics(ctx)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.ListPodSandboxMetricsResponse{PodMetrics: metrics}, nil
}

func (s *runtimeService) RuntimeConfig(ctx context.Context, req *runtimeapi.RuntimeConfigRequest) (*runtimeapi.RuntimeConfigResponse, error) {
	return s.backend.RuntimeConfig(ctx)
}

func (s *runtimeService) UpdatePodSandboxResources(ctx context.Context, req *runtimeapi.UpdatePodSandboxResourcesRequest) (*runtimeapi.UpdatePodSandboxResourcesResponse, error) {
	return s.backend.UpdatePodSandboxResources(ctx, req)
}
