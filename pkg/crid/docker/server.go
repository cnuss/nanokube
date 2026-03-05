package docker

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/go-connections/nat"
	"google.golang.org/grpc"
	"k8s.io/component-base/version"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const defaultPauseImage = "registry.k8s.io/pause:3.10"

func NewServer(backend *DockerBackend) *Server {
	return &Server{
		backend:    backend,
		logWriters: make(map[string]context.CancelFunc),
	}
}

type Server struct {
	runtimeapi.UnsafeImageServiceServer
	runtimeapi.UnsafeRuntimeServiceServer
	backend    *DockerBackend
	logMu      sync.Mutex
	logWriters map[string]context.CancelFunc
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
func (s *Server) RunPodSandbox(ctx context.Context, req *runtimeapi.RunPodSandboxRequest) (*runtimeapi.RunPodSandboxResponse, error) {
	logger.Trace().Str("name", req.Config.Metadata.Name).Str("namespace", req.Config.Metadata.Namespace).Str("uid", req.Config.Metadata.Uid).Msg("RunPodSandbox")

	config := req.GetConfig()
	meta := config.GetMetadata()

	b := s.backend.labels.NewBuilder(config.GetLabels()).
		WithSandbox(meta.GetUid()).
		WithPod(meta.GetName(), meta.GetNamespace(), meta.GetUid()).
		WithAnnotations(config.GetAnnotations())

	dockerConfig := &container.Config{
		Image:    defaultPauseImage,
		Hostname: config.GetHostname(),
		Labels:   b.BuildLabels(),
	}

	hostConfig := &container.HostConfig{
		IpcMode: container.IpcMode("shareable"),
	}

	// DNS
	if dns := config.GetDnsConfig(); dns != nil {
		hostConfig.DNS = dns.GetServers()
		hostConfig.DNSSearch = dns.GetSearches()
		hostConfig.DNSOptions = dns.GetOptions()
	}

	// Port mappings
	if pms := config.GetPortMappings(); len(pms) > 0 {
		dockerConfig.ExposedPorts = nat.PortSet{}
		hostConfig.PortBindings = nat.PortMap{}
		for _, pm := range pms {
			port := nat.Port(fmt.Sprintf("%d/%s", pm.GetContainerPort(), strings.ToLower(pm.GetProtocol().String())))
			dockerConfig.ExposedPorts[port] = struct{}{}
			hostPort := pm.GetHostPort()
			if hostPort == 0 {
				hostPort = pm.GetContainerPort()
			}
			hostConfig.PortBindings[port] = []nat.PortBinding{
				{HostIP: pm.GetHostIp(), HostPort: strconv.Itoa(int(hostPort))},
			}
		}
	}

	// Linux namespace options
	if linux := config.GetLinux(); linux != nil {
		if sc := linux.GetSecurityContext(); sc != nil {
			if sc.GetPrivileged() {
				hostConfig.Privileged = true
			}
			if ns := sc.GetNamespaceOptions(); ns != nil {
				if ns.GetNetwork() == runtimeapi.NamespaceMode_NODE {
					hostConfig.NetworkMode = "host"
				}
				if ns.GetPid() == runtimeapi.NamespaceMode_NODE {
					hostConfig.PidMode = "host"
				}
				if ns.GetIpc() == runtimeapi.NamespaceMode_NODE {
					hostConfig.IpcMode = "host"
				}
			}
		}
	}

	// Ensure pause image is available
	imgSpec := &runtimeapi.ImageSpec{Image: dockerConfig.Image}
	status, _ := s.ImageStatus(ctx, &runtimeapi.ImageStatusRequest{Image: imgSpec})
	if status.Image == nil {
		if _, err := s.PullImage(ctx, &runtimeapi.PullImageRequest{Image: imgSpec}); err != nil {
			return nil, fmt.Errorf("pull sandbox image: %w", err)
		}
	}

	resp, err := s.backend.client.ContainerCreate(ctx, dockerConfig, hostConfig, &network.NetworkingConfig{}, nil, b.BuildName())
	if err != nil {
		return nil, fmt.Errorf("create sandbox: %w", err)
	}

	logger.Debug().Str("id", resp.ID[:12]).Msg("sandbox created")
	return &runtimeapi.RunPodSandboxResponse{PodSandboxId: resp.ID}, nil
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
func (s *Server) Version(ctx context.Context, req *runtimeapi.VersionRequest) (*runtimeapi.VersionResponse, error) {
	v, err := s.backend.client.ServerVersion(ctx)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.VersionResponse{
		Version:           version.Get().GitVersion,
		RuntimeName:       string(s.backend.Name()),
		RuntimeVersion:    v.Version,
		RuntimeApiVersion: req.GetVersion(),
	}, nil
}

// ImageFsInfo implements [v1.ImageServiceServer].
func (s *Server) ImageFsInfo(ctx context.Context, req *runtimeapi.ImageFsInfoRequest) (*runtimeapi.ImageFsInfoResponse, error) {
	info, err := s.backend.client.Info(ctx)
	if err != nil {
		return &runtimeapi.ImageFsInfoResponse{}, nil
	}
	resp, err := s.ListImages(ctx, &runtimeapi.ListImagesRequest{})
	if err != nil {
		return &runtimeapi.ImageFsInfoResponse{}, nil
	}
	var totalSize uint64
	for _, img := range resp.Images {
		totalSize += img.Size
	}
	return &runtimeapi.ImageFsInfoResponse{
		ImageFilesystems: []*runtimeapi.FilesystemUsage{
			{
				FsId:      &runtimeapi.FilesystemIdentifier{Mountpoint: info.DockerRootDir},
				UsedBytes: &runtimeapi.UInt64Value{Value: totalSize},
			},
		},
	}, nil
}

// ImageStatus implements [v1.ImageServiceServer].
func (s *Server) ImageStatus(ctx context.Context, req *runtimeapi.ImageStatusRequest) (*runtimeapi.ImageStatusResponse, error) {
	ref := req.GetImage().GetImage()
	if ref == "" {
		return &runtimeapi.ImageStatusResponse{}, nil
	}

	inspect, err := s.backend.client.ImageInspect(ctx, ref)
	if err != nil {
		return &runtimeapi.ImageStatusResponse{}, nil
	}

	var repoTags []string
	for _, t := range inspect.RepoTags {
		if t != "<none>:<none>" && !strings.Contains(t, "@") {
			repoTags = append(repoTags, t)
		}
	}

	img := &runtimeapi.Image{
		Id:          inspect.ID,
		RepoTags:    repoTags,
		RepoDigests: inspect.RepoDigests,
		Size:        uint64(inspect.Size),
	}

	if inspect.Config != nil && inspect.Config.User != "" {
		userPart, _, _ := strings.Cut(inspect.Config.User, ":")
		if uid, err := strconv.ParseInt(userPart, 10, 64); err == nil {
			img.Uid = &runtimeapi.Int64Value{Value: uid}
		} else {
			img.Username = userPart
		}
	}

	resp := &runtimeapi.ImageStatusResponse{Image: img}
	if req.GetVerbose() {
		resp.Info = map[string]string{
			"architecture": inspect.Architecture,
			"os":           inspect.Os,
		}
	}
	return resp, nil
}

// ListImages implements [v1.ImageServiceServer].
func (s *Server) ListImages(ctx context.Context, req *runtimeapi.ListImagesRequest) (*runtimeapi.ListImagesResponse, error) {
	images, err := s.backend.client.ImageList(ctx, image.ListOptions{})
	if err != nil {
		return nil, err
	}
	result := make([]*runtimeapi.Image, len(images))
	for i, img := range images {
		result[i] = &runtimeapi.Image{
			Id:          img.ID,
			RepoTags:    img.RepoTags,
			RepoDigests: img.RepoDigests,
			Size:        uint64(img.Size),
		}
	}
	return &runtimeapi.ListImagesResponse{Images: result}, nil
}

// PullImage implements [v1.ImageServiceServer].
func (s *Server) PullImage(ctx context.Context, req *runtimeapi.PullImageRequest) (*runtimeapi.PullImageResponse, error) {
	ref := req.GetImage().GetImage()

	reader, err := s.backend.client.ImagePull(ctx, ref, image.PullOptions{})
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	io.Copy(io.Discard, reader)

	status, err := s.ImageStatus(ctx, &runtimeapi.ImageStatusRequest{Image: req.GetImage()})
	if err != nil {
		return nil, err
	}
	if status.Image == nil {
		return nil, fmt.Errorf("image %s not found after pull", ref)
	}
	return &runtimeapi.PullImageResponse{ImageRef: status.Image.Id}, nil
}

// RemoveImage implements [v1.ImageServiceServer].
func (s *Server) RemoveImage(ctx context.Context, req *runtimeapi.RemoveImageRequest) (*runtimeapi.RemoveImageResponse, error) {
	_, err := s.backend.client.ImageRemove(ctx, req.GetImage().GetImage(), image.RemoveOptions{Force: true, PruneChildren: true})
	if err != nil && errdefs.IsNotFound(err) {
		return &runtimeapi.RemoveImageResponse{}, nil
	}
	if err != nil {
		return nil, err
	}
	return &runtimeapi.RemoveImageResponse{}, nil
}

var (
	_ runtimeapi.ImageServiceServer   = &Server{}
	_ runtimeapi.RuntimeServiceServer = &Server{}
)
