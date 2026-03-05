package docker

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"strconv"
	"strings"

	"github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/go-connections/nat"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"google.golang.org/grpc"
	"k8s.io/component-base/version"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const defaultPauseImage = "registry.k8s.io/pause:3.10"

func NewServer(backend *DockerBackend) *Server {
	return &Server{backend: backend}
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
func (s *Server) CreateContainer(ctx context.Context, req *runtimeapi.CreateContainerRequest) (*runtimeapi.CreateContainerResponse, error) {
	logger.Trace().Str("name", req.Config.Metadata.Name).Str("sandbox", req.PodSandboxId).Str("image", req.Config.Image.Image).Msg("CreateContainer")

	config := req.GetConfig()
	sandboxID := req.GetPodSandboxId()
	meta := config.GetMetadata()

	if req.GetSandboxConfig() != nil {
		// TODO: reconcile sandbox config with actual sandbox state
		panic("CreateContainer: req.SandboxConfig reconciliation not implemented")
	}

	// Get the sandbox's actual Docker config as our base
	inspect, err := s.backend.client.ContainerInspect(ctx, sandboxID)
	if err != nil {
		return nil, wrapErr(err)
	}

	// Labels: start from sandbox, layer container labels on top
	name, labels, err := s.backend.labels.NewBuilder(inspect.Config.Labels).
		WithLabels(config.GetLabels()).
		WithContainer(sandboxID, meta.GetName()).
		WithAnnotations(config.GetAnnotations()).
		WithLogPath(config.GetLogPath(), req.GetSandboxConfig().GetLogDirectory()).Build()
	if err != nil {
		return nil, wrapErr(err)
	}

	// Override image, command, env from container config
	envs := make([]string, 0, len(config.GetEnvs()))
	for _, kv := range config.GetEnvs() {
		envs = append(envs, kv.GetKey()+"="+kv.GetValue())
	}

	dockerConfig := inspect.Config
	dockerConfig.Image = config.GetImage().GetImage()
	dockerConfig.Entrypoint = config.GetCommand()
	dockerConfig.Cmd = config.GetArgs()
	dockerConfig.Env = envs
	dockerConfig.WorkingDir = config.GetWorkingDir()
	dockerConfig.Labels = labels
	dockerConfig.StdinOnce = config.GetStdinOnce()
	dockerConfig.OpenStdin = config.GetStdin()
	dockerConfig.Tty = config.GetTty()

	// Host config: inherit sandbox, share namespaces
	hostConfig := inspect.HostConfig
	hostConfig.NetworkMode = container.NetworkMode("container:" + sandboxID)
	hostConfig.IpcMode = container.IpcMode("container:" + sandboxID)
	hostConfig.PidMode = container.PidMode("container:" + sandboxID)

	// Mounts: override with container-specific mounts
	hostConfig.Binds = nil
	for _, m := range config.GetMounts() {
		bind := m.GetHostPath() + ":" + m.GetContainerPath()
		if m.GetReadonly() {
			bind += ":ro"
		}
		hostConfig.Binds = append(hostConfig.Binds, bind)
	}

	// Linux resource overrides
	if linux := config.GetLinux(); linux != nil {
		if res := linux.GetResources(); res != nil {
			hostConfig.Resources.CPUShares = res.GetCpuShares()
			hostConfig.Resources.Memory = res.GetMemoryLimitInBytes()
			hostConfig.Resources.CPUQuota = res.GetCpuQuota()
			hostConfig.Resources.CPUPeriod = res.GetCpuPeriod()
		}
		if sc := linux.GetSecurityContext(); sc != nil {
			if sc.GetPrivileged() {
				hostConfig.Privileged = true
			}
			if sc.GetReadonlyRootfs() {
				hostConfig.ReadonlyRootfs = true
			}
		}
	}

	// Log config
	if config.GetLogPath() != "" {
		hostConfig.LogConfig = container.LogConfig{
			Type:   "json-file",
			Config: map[string]string{"max-size": "10m", "max-file": "3"},
		}
	}

	netConfig := &network.NetworkingConfig{}
	if inspect.NetworkSettings != nil {
		netConfig.EndpointsConfig = inspect.NetworkSettings.Networks
	}

	var platform *ocispec.Platform
	if inspect.ImageManifestDescriptor != nil {
		platform = inspect.ImageManifestDescriptor.Platform
	}

	resp, err := s.backend.client.ContainerCreate(ctx, dockerConfig, hostConfig, netConfig, platform, name)
	if err != nil {
		return nil, wrapErr(err)
	}

	logger.Debug().Str("id", resp.ID).Msg("container created")
	return &runtimeapi.CreateContainerResponse{ContainerId: resp.ID}, nil
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
func (s *Server) ListContainers(ctx context.Context, req *runtimeapi.ListContainersRequest) (*runtimeapi.ListContainersResponse, error) {
	logger.Trace().Msg("ListContainers")

	lb := s.backend.labels.NewBuilder(nil).WithType("container")
	f := s.backend.Into.Filters(lb)

	if filter := req.GetFilter(); filter != nil {
		if filter.Id != "" {
			f.Add("id", filter.Id)
		}
		if filter.PodSandboxId != "" {
			f.Add("label", s.backend.labels.SandboxIDFilter(filter.PodSandboxId))
		}
		if filter.State != nil {
			f.Add("status", s.backend.Into.ContainerStatus(filter.State.State))
		}
		for k, v := range filter.GetLabelSelector() {
			f.Add("label", k+"="+v)
		}
	}

	containers, err := s.backend.client.ContainerList(ctx, container.ListOptions{All: true, Filters: f})
	if err != nil {
		return nil, wrapErr(err)
	}

	var result []*runtimeapi.Container
	for _, c := range containers {
		result = append(result, s.backend.Into.Container(c))
	}
	return &runtimeapi.ListContainersResponse{Containers: result}, nil
}

// ListMetricDescriptors implements [v1.RuntimeServiceServer].
func (s *Server) ListMetricDescriptors(context.Context, *runtimeapi.ListMetricDescriptorsRequest) (*runtimeapi.ListMetricDescriptorsResponse, error) {
	panic("unimplemented")
}

// ListPodSandbox implements [v1.RuntimeServiceServer].
func (s *Server) ListPodSandbox(ctx context.Context, req *runtimeapi.ListPodSandboxRequest) (*runtimeapi.ListPodSandboxResponse, error) {
	logger.Trace().Msg("ListPodSandbox")

	f := s.backend.Into.Filters(s.backend.labels.NewBuilder(nil).WithType("sandbox"))

	if filter := req.GetFilter(); filter != nil {
		if filter.Id != "" {
			f.Add("id", filter.Id)
		}
		if filter.State != nil {
			for _, s := range s.backend.Into.PodStatuses(filter.State.State) {
				f.Add("status", s)
			}
		}
		for k, v := range filter.GetLabelSelector() {
			f.Add("label", k+"="+v)
		}
	}

	containers, err := s.backend.client.ContainerList(ctx, container.ListOptions{All: true, Filters: f})
	if err != nil {
		return nil, wrapErr(err)
	}

	var result []*runtimeapi.PodSandbox
	for _, c := range containers {
		result = append(result, s.backend.Into.PodSandbox(c))
	}
	return &runtimeapi.ListPodSandboxResponse{Items: result}, nil
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
func (s *Server) RemoveContainer(ctx context.Context, req *runtimeapi.RemoveContainerRequest) (*runtimeapi.RemoveContainerResponse, error) {
	logger.Trace().Str("id", req.ContainerId).Msg("RemoveContainer")
	id := req.GetContainerId()

	// Stop first (timeout 0 = immediate)
	s.StopContainer(ctx, &runtimeapi.StopContainerRequest{ContainerId: id, Timeout: 0})

	if err := s.backend.client.ContainerRemove(ctx, id, container.RemoveOptions{}); err != nil {
		if errdefs.IsNotFound(err) {
			return &runtimeapi.RemoveContainerResponse{}, nil
		}
		return nil, wrapErr(err)
	}
	return &runtimeapi.RemoveContainerResponse{}, nil
}

// RemovePodSandbox implements [v1.RuntimeServiceServer].
func (s *Server) RemovePodSandbox(ctx context.Context, req *runtimeapi.RemovePodSandboxRequest) (*runtimeapi.RemovePodSandboxResponse, error) {
	logger.Trace().Str("id", req.PodSandboxId).Msg("RemovePodSandbox")
	id := req.GetPodSandboxId()

	// Find and remove all containers belonging to this sandbox
	resp, err := s.ListContainers(ctx, &runtimeapi.ListContainersRequest{
		Filter: &runtimeapi.ContainerFilter{PodSandboxId: id},
	})
	if err != nil {
		return nil, wrapErr(err)
	}
	for _, c := range resp.Containers {
		s.RemoveContainer(ctx, &runtimeapi.RemoveContainerRequest{ContainerId: c.Id})
	}

	// Remove the sandbox itself
	s.RemoveContainer(ctx, &runtimeapi.RemoveContainerRequest{ContainerId: id})

	return &runtimeapi.RemovePodSandboxResponse{}, nil
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

	name, labels, err := s.backend.labels.NewBuilder(config.GetLabels()).
		WithSandbox(meta.GetUid()).
		WithPod(meta.GetName(), meta.GetNamespace(), meta.GetUid()).
		WithAnnotations(config.GetAnnotations()).
		Build()
	if err != nil {
		return nil, wrapErr(err)
	}

	dockerConfig := &container.Config{
		Image:    defaultPauseImage,
		Hostname: config.GetHostname(),
		Labels:   labels,
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
	imgReq := &runtimeapi.ImageStatusRequest{Image: imgSpec, Verbose: true}
	status, _ := s.ImageStatus(ctx, imgReq)
	if status.Image == nil {
		if _, err := s.PullImage(ctx, &runtimeapi.PullImageRequest{Image: imgSpec}); err != nil {
			return nil, wrapErr(err)
		}
		status, _ = s.ImageStatus(ctx, imgReq)
	}

	// Platform from image info
	var platform *ocispec.Platform
	if status.Info != nil {
		platform = &ocispec.Platform{OS: status.Info["os"], Architecture: status.Info["architecture"]}
	}

	resp, err := s.backend.client.ContainerCreate(ctx, dockerConfig, hostConfig, &network.NetworkingConfig{}, platform, name)
	if err != nil {
		return nil, wrapErr(err)
	}

	logger.Debug().Str("id", resp.ID).Msg("sandbox created")
	return &runtimeapi.RunPodSandboxResponse{PodSandboxId: resp.ID}, nil
}

// RuntimeConfig implements [v1.RuntimeServiceServer].
func (s *Server) RuntimeConfig(context.Context, *runtimeapi.RuntimeConfigRequest) (*runtimeapi.RuntimeConfigResponse, error) {
	panic("unimplemented")
}

// StartContainer implements [v1.RuntimeServiceServer].
func (s *Server) StartContainer(ctx context.Context, req *runtimeapi.StartContainerRequest) (*runtimeapi.StartContainerResponse, error) {
	logger.Trace().Str("id", req.ContainerId).Msg("StartContainer")
	id := req.GetContainerId()

	if err := s.backend.client.ContainerStart(ctx, id, container.StartOptions{}); err != nil {
		return nil, wrapErr(err)
	}

	s.backend.StartLogs(id)
	return &runtimeapi.StartContainerResponse{}, nil
}

// Status implements [v1.RuntimeServiceServer].
func (s *Server) Status(context.Context, *runtimeapi.StatusRequest) (*runtimeapi.StatusResponse, error) {
	panic("unimplemented")
}

// StopContainer implements [v1.RuntimeServiceServer].
func (s *Server) StopContainer(ctx context.Context, req *runtimeapi.StopContainerRequest) (*runtimeapi.StopContainerResponse, error) {
	logger.Trace().Str("id", req.ContainerId).Msg("StopContainer")
	id := req.GetContainerId()

	s.backend.StopLogs(id)

	t := int(req.GetTimeout())
	if err := s.backend.client.ContainerStop(ctx, id, container.StopOptions{Timeout: &t}); err != nil {
		if errdefs.IsNotFound(err) || errdefs.IsNotModified(err) {
			return &runtimeapi.StopContainerResponse{}, nil
		}
		return nil, wrapErr(err)
	}
	return &runtimeapi.StopContainerResponse{}, nil
}

// StopPodSandbox implements [v1.RuntimeServiceServer].
func (s *Server) StopPodSandbox(ctx context.Context, req *runtimeapi.StopPodSandboxRequest) (*runtimeapi.StopPodSandboxResponse, error) {
	logger.Trace().Str("id", req.PodSandboxId).Msg("StopPodSandbox")
	id := req.GetPodSandboxId()

	resp, err := s.ListContainers(ctx, &runtimeapi.ListContainersRequest{
		Filter: &runtimeapi.ContainerFilter{PodSandboxId: id},
	})
	if err != nil {
		return nil, wrapErr(err)
	}
	for _, c := range resp.Containers {
		s.StopContainer(ctx, &runtimeapi.StopContainerRequest{ContainerId: c.Id, Timeout: 0})
	}

	return &runtimeapi.StopPodSandboxResponse{}, nil
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
		return nil, wrapErr(err)
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
		return nil, wrapErr(err)
	}
	resp, err := s.ListImages(ctx, &runtimeapi.ListImagesRequest{})
	if err != nil {
		return nil, wrapErr(err)
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
		if errdefs.IsNotFound(err) {
			return &runtimeapi.ImageStatusResponse{}, nil
		}
		return nil, wrapErr(err)
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
		return nil, wrapErr(err)
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
		return nil, wrapErr(err)
	}
	defer reader.Close()
	io.Copy(io.Discard, reader)

	status, err := s.ImageStatus(ctx, &runtimeapi.ImageStatusRequest{Image: req.GetImage()})
	if err != nil {
		return nil, wrapErr(err)
	}
	if status.Image == nil {
		return nil, wrapErr(fmt.Errorf("image %s not found after pull", ref))
	}
	return &runtimeapi.PullImageResponse{ImageRef: status.Image.Id}, nil
}

// RemoveImage implements [v1.ImageServiceServer].
func (s *Server) RemoveImage(ctx context.Context, req *runtimeapi.RemoveImageRequest) (*runtimeapi.RemoveImageResponse, error) {
	ref := req.GetImage().GetImage()
	_, err := s.backend.client.ImageRemove(ctx, ref, image.RemoveOptions{Force: true, PruneChildren: true})
	if err != nil && errdefs.IsNotFound(err) {
		return &runtimeapi.RemoveImageResponse{}, nil
	}
	if err != nil {
		return nil, wrapErr(err)
	}
	return &runtimeapi.RemoveImageResponse{}, nil
}

// wrapErr wraps an error with the calling function's name.
func wrapErr(err error) error {
	pc, _, _, _ := runtime.Caller(1)
	name := runtime.FuncForPC(pc).Name()
	if idx := strings.LastIndex(name, "."); idx >= 0 {
		name = name[idx+1:]
	}
	return fmt.Errorf("%s: %w", name, err)
}

var (
	_ runtimeapi.ImageServiceServer   = &Server{}
	_ runtimeapi.RuntimeServiceServer = &Server{}
)
