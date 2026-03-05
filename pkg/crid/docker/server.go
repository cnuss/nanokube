package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/go-connections/nat"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"google.golang.org/grpc"
	"k8s.io/component-base/version"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	kubelettypes "k8s.io/kubelet/pkg/types"
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
func (s *Server) Attach(ctx context.Context, req *runtimeapi.AttachRequest) (*runtimeapi.AttachResponse, error) {
	logger.Warn().Str("container", req.GetContainerId()).Msg("Attach: unimplemented")
	return nil, fmt.Errorf("Attach: unimplemented")
}

// CheckpointContainer implements [v1.RuntimeServiceServer].
func (s *Server) CheckpointContainer(context.Context, *runtimeapi.CheckpointContainerRequest) (*runtimeapi.CheckpointContainerResponse, error) {
	panic("CheckpointContainer: unimplemented")
}

// ContainerStats implements [v1.RuntimeServiceServer].
func (s *Server) ContainerStats(ctx context.Context, req *runtimeapi.ContainerStatsRequest) (*runtimeapi.ContainerStatsResponse, error) {
	logger.Trace().Str("id", req.ContainerId).Msg("ContainerStats")
	id := req.GetContainerId()

	statusResp, err := s.ContainerStatus(ctx, &runtimeapi.ContainerStatusRequest{ContainerId: id})
	if err != nil {
		return nil, wrapErr(err)
	}
	cs := statusResp.Status

	resp, err := s.backend.client.ContainerStatsOneShot(ctx, id)
	if err != nil {
		return nil, wrapErr(err)
	}
	defer resp.Body.Close()

	var stats container.StatsResponse
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return nil, wrapErr(fmt.Errorf("decode docker stats: %w", err))
	}

	ts := time.Now().UnixNano()

	return &runtimeapi.ContainerStatsResponse{
		Stats: &runtimeapi.ContainerStats{
			Attributes: &runtimeapi.ContainerAttributes{
				Id:          id,
				Metadata:    cs.Metadata,
				Labels:      cs.Labels,
				Annotations: cs.Annotations,
			},
			Cpu: &runtimeapi.CpuUsage{
				Timestamp:            ts,
				UsageCoreNanoSeconds: &runtimeapi.UInt64Value{Value: stats.CPUStats.CPUUsage.TotalUsage},
			},
			Memory: &runtimeapi.MemoryUsage{
				Timestamp:       ts,
				WorkingSetBytes: &runtimeapi.UInt64Value{Value: stats.MemoryStats.Usage},
			},
			WritableLayer: &runtimeapi.FilesystemUsage{
				Timestamp: ts,
			},
		},
	}, nil
}

// ContainerStatus implements [v1.RuntimeServiceServer].
func (s *Server) ContainerStatus(ctx context.Context, req *runtimeapi.ContainerStatusRequest) (*runtimeapi.ContainerStatusResponse, error) {
	logger.Trace().Str("id", req.ContainerId).Msg("ContainerStatus")

	inspect, err := s.backend.client.ContainerInspect(ctx, req.GetContainerId())
	if err != nil {
		return nil, wrapErr(err)
	}

	createdAt := s.backend.Into.CreatedAt(inspect.Created)
	state := s.backend.Into.ContainerState(inspect.State.Status)

	status := &runtimeapi.ContainerStatus{
		Id: inspect.ID,
		Metadata: &runtimeapi.ContainerMetadata{
			Name: inspect.Config.Labels[kubelettypes.KubernetesContainerNameLabel],
		},
		State:       state,
		CreatedAt:   createdAt,
		Image:       &runtimeapi.ImageSpec{Image: inspect.Config.Image},
		ImageRef:    inspect.Image,
		Labels:      s.backend.labels.ExtractLabels(inspect.Config.Labels),
		Annotations: s.backend.labels.ExtractAnnotations(inspect.Config.Labels),
	}

	if logPath := s.backend.labels.LogPath(inspect.Config.Labels); logPath != "" {
		status.LogPath = logPath
	} else {
		status.LogPath = inspect.LogPath
	}

	if inspect.State.StartedAt != "" && inspect.State.StartedAt != "0001-01-01T00:00:00Z" {
		status.StartedAt = s.backend.Into.CreatedAt(inspect.State.StartedAt)
	}
	if inspect.State.FinishedAt != "" && inspect.State.FinishedAt != "0001-01-01T00:00:00Z" {
		status.FinishedAt = s.backend.Into.CreatedAt(inspect.State.FinishedAt)
	}
	if state == runtimeapi.ContainerState_CONTAINER_EXITED {
		status.ExitCode = int32(inspect.State.ExitCode)
		status.Reason = inspect.State.Error
	}

	for _, m := range inspect.Mounts {
		status.Mounts = append(status.Mounts, &runtimeapi.Mount{
			ContainerPath: m.Destination,
			HostPath:      m.Source,
			Readonly:      !m.RW,
		})
	}

	resp := &runtimeapi.ContainerStatusResponse{Status: status}
	if req.GetVerbose() {
		resp.Info = map[string]string{
			"pid": fmt.Sprintf("%d", inspect.State.Pid),
		}
	}
	return resp, nil
}

// CreateContainer implements [v1.RuntimeServiceServer].
func (s *Server) CreateContainer(ctx context.Context, req *runtimeapi.CreateContainerRequest) (*runtimeapi.CreateContainerResponse, error) {
	logger.Trace().Str("name", req.Config.Metadata.Name).Str("sandbox", req.PodSandboxId).Str("image", req.Config.Image.Image).Msg("CreateContainer")

	config := req.GetConfig()
	sandboxID := req.GetPodSandboxId()
	meta := config.GetMetadata()

	// Get sandbox status via CRI
	sandbox, err := s.PodSandboxStatus(ctx, &runtimeapi.PodSandboxStatusRequest{PodSandboxId: sandboxID})
	if err != nil {
		return nil, wrapErr(err)
	}
	status := sandbox.Status

	// Labels: start from sandbox, layer container labels on top
	name, labels, err := s.backend.labels.NewBuilder(nil).
		WithLabels(status.GetLabels()).
		WithLabels(config.GetLabels()).
		WithContainer(sandboxID, meta.GetName()).
		WithAnnotations(status.GetAnnotations()).
		WithAnnotations(config.GetAnnotations()).
		WithLogPath(config.GetLogPath()).
		WithPod(status.GetMetadata().GetName(), status.GetMetadata().GetNamespace(), status.GetMetadata().GetUid()).
		Build()
	if err != nil {
		return nil, wrapErr(err)
	}

	// Build Docker config from CRI container config
	envs := make([]string, 0, len(config.GetEnvs()))
	for _, kv := range config.GetEnvs() {
		envs = append(envs, kv.GetKey()+"="+kv.GetValue())
	}

	dockerConfig := &container.Config{
		Image:      config.GetImage().GetImage(),
		Entrypoint: config.GetCommand(),
		Cmd:        config.GetArgs(),
		Env:        envs,
		WorkingDir: config.GetWorkingDir(),
		Labels:     labels,
		StdinOnce:  config.GetStdinOnce(),
		OpenStdin:  config.GetStdin(),
		Tty:        config.GetTty(),
	}

	// Host config: share sandbox namespaces
	hostConfig := &container.HostConfig{
		NetworkMode: container.NetworkMode("container:" + sandboxID),
		IpcMode:     container.IpcMode("container:" + sandboxID),
		PidMode:     container.PidMode("container:" + sandboxID),
	}

	// Mounts
	for _, m := range config.GetMounts() {
		bind := m.GetHostPath() + ":" + m.GetContainerPath()
		if m.GetReadonly() {
			bind += ":ro"
		}
		hostConfig.Binds = append(hostConfig.Binds, bind)
	}

	// Linux resources
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

	resp, err := s.backend.client.ContainerCreate(ctx, dockerConfig, hostConfig, nil, nil, name)
	if err != nil {
		return nil, wrapErr(err)
	}

	logger.Debug().Str("id", resp.ID).Msg("container created")
	return &runtimeapi.CreateContainerResponse{ContainerId: resp.ID}, nil
}

// Exec implements [v1.RuntimeServiceServer].
func (s *Server) Exec(ctx context.Context, req *runtimeapi.ExecRequest) (*runtimeapi.ExecResponse, error) {
	logger.Warn().Str("container", req.GetContainerId()).Strs("cmd", req.GetCmd()).Msg("Exec: unimplemented")
	return nil, fmt.Errorf("Exec: unimplemented")
}

// ExecSync implements [v1.RuntimeServiceServer].
func (s *Server) ExecSync(ctx context.Context, req *runtimeapi.ExecSyncRequest) (*runtimeapi.ExecSyncResponse, error) {
	logger.Warn().Str("container", req.GetContainerId()).Strs("cmd", req.GetCmd()).Msg("ExecSync: unimplemented")
	return nil, fmt.Errorf("ExecSync: unimplemented")
}

// GetContainerEvents implements [v1.RuntimeServiceServer].
func (s *Server) GetContainerEvents(*runtimeapi.GetEventsRequest, grpc.ServerStreamingServer[runtimeapi.ContainerEventResponse]) error {
	panic("GetContainerEvents: unimplemented")
}

// ListContainerStats implements [v1.RuntimeServiceServer].
func (s *Server) ListContainerStats(ctx context.Context, req *runtimeapi.ListContainerStatsRequest) (*runtimeapi.ListContainerStatsResponse, error) {
	logger.Trace().Msg("ListContainerStats")

	filter := req.GetFilter()
	containers, err := s.ListContainers(ctx, &runtimeapi.ListContainersRequest{
		Filter: &runtimeapi.ContainerFilter{
			Id:            filter.GetId(),
			PodSandboxId:  filter.GetPodSandboxId(),
			LabelSelector: filter.GetLabelSelector(),
		},
	})
	if err != nil {
		return nil, wrapErr(err)
	}

	var stats []*runtimeapi.ContainerStats
	for _, c := range containers.Containers {
		resp, err := s.ContainerStats(ctx, &runtimeapi.ContainerStatsRequest{ContainerId: c.Id})
		if err != nil {
			logger.Warn().Str("id", c.Id).Err(err).Msg("ListContainerStats: skipping container")
			continue
		}
		stats = append(stats, resp.Stats)
	}
	return &runtimeapi.ListContainerStatsResponse{Stats: stats}, nil
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
			switch filter.State.State {
			case runtimeapi.ContainerState_CONTAINER_CREATED:
				f.Add("status", container.StateCreated)
			case runtimeapi.ContainerState_CONTAINER_RUNNING:
				f.Add("status", container.StateRunning)
			case runtimeapi.ContainerState_CONTAINER_EXITED:
				f.Add("status", container.StateRestarting)
				f.Add("status", container.StatePaused)
				f.Add("status", container.StateRemoving)
				f.Add("status", container.StateExited)
				f.Add("status", container.StateDead)
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

	var result []*runtimeapi.Container
	for _, c := range containers {
		result = append(result, s.backend.Into.Container(c))
	}
	return &runtimeapi.ListContainersResponse{Containers: result}, nil
}

// ListMetricDescriptors implements [v1.RuntimeServiceServer].
func (s *Server) ListMetricDescriptors(context.Context, *runtimeapi.ListMetricDescriptorsRequest) (*runtimeapi.ListMetricDescriptorsResponse, error) {
	panic("ListMetricDescriptors: unimplemented")
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
			if filter.State.State == runtimeapi.PodSandboxState_SANDBOX_READY {
				f.Add("status", container.StateRunning)
			} else {
				f.Add("status", container.StateCreated)
				f.Add("status", container.StateRestarting)
				f.Add("status", container.StatePaused)
				f.Add("status", container.StateRemoving)
				f.Add("status", container.StateExited)
				f.Add("status", container.StateDead)
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
	panic("ListPodSandboxMetrics: unimplemented")
}

// ListPodSandboxStats implements [v1.RuntimeServiceServer].
func (s *Server) ListPodSandboxStats(context.Context, *runtimeapi.ListPodSandboxStatsRequest) (*runtimeapi.ListPodSandboxStatsResponse, error) {
	panic("ListPodSandboxStats: unimplemented")
}

// PodSandboxStats implements [v1.RuntimeServiceServer].
func (s *Server) PodSandboxStats(context.Context, *runtimeapi.PodSandboxStatsRequest) (*runtimeapi.PodSandboxStatsResponse, error) {
	panic("PodSandboxStats: unimplemented")
}

// PodSandboxStatus implements [v1.RuntimeServiceServer].
func (s *Server) PodSandboxStatus(ctx context.Context, req *runtimeapi.PodSandboxStatusRequest) (*runtimeapi.PodSandboxStatusResponse, error) {
	logger.Trace().Str("id", req.PodSandboxId).Msg("PodSandboxStatus")

	inspect, err := s.backend.client.ContainerInspect(ctx, req.GetPodSandboxId())
	if err != nil {
		return nil, wrapErr(err)
	}

	createdAt := s.backend.Into.CreatedAt(inspect.Created)

	status := &runtimeapi.PodSandboxStatus{
		Id: inspect.ID,
		Metadata: &runtimeapi.PodSandboxMetadata{
			Name:      inspect.Config.Labels[kubelettypes.KubernetesPodNameLabel],
			Namespace: inspect.Config.Labels[kubelettypes.KubernetesPodNamespaceLabel],
			Uid:       inspect.Config.Labels[kubelettypes.KubernetesPodUIDLabel],
		},
		State:     s.backend.Into.PodState(inspect.State.Status),
		CreatedAt: createdAt,
		Network: &runtimeapi.PodSandboxNetworkStatus{
			Ip: getIPFromInspect(inspect),
		},
		Labels:      s.backend.labels.ExtractLabels(inspect.Config.Labels),
		Annotations: s.backend.labels.ExtractAnnotations(inspect.Config.Labels),
	}

	resp := &runtimeapi.PodSandboxStatusResponse{Status: status}
	if req.GetVerbose() {
		resp.Info = map[string]string{
			"pid": fmt.Sprintf("%d", inspect.State.Pid),
		}
	}
	return resp, nil
}

// PortForward implements [v1.RuntimeServiceServer].
func (s *Server) PortForward(context.Context, *runtimeapi.PortForwardRequest) (*runtimeapi.PortForwardResponse, error) {
	panic("PortForward: unimplemented")
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
func (s *Server) ReopenContainerLog(ctx context.Context, req *runtimeapi.ReopenContainerLogRequest) (*runtimeapi.ReopenContainerLogResponse, error) {
	logger.Trace().Str("id", req.ContainerId).Msg("ReopenContainerLog")
	id := req.GetContainerId()
	s.backend.StopLogs(id)
	s.backend.StartLogs(id)
	return &runtimeapi.ReopenContainerLogResponse{}, nil
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
		WithLogDirectory(config.GetLogDirectory()).
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

	if err := s.backend.client.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		s.backend.client.ContainerRemove(ctx, resp.ID, container.RemoveOptions{})
		return nil, wrapErr(err)
	}

	logger.Debug().Str("id", resp.ID).Msg("sandbox started")
	return &runtimeapi.RunPodSandboxResponse{PodSandboxId: resp.ID}, nil
}

// RuntimeConfig implements [v1.RuntimeServiceServer].
func (s *Server) RuntimeConfig(ctx context.Context, req *runtimeapi.RuntimeConfigRequest) (*runtimeapi.RuntimeConfigResponse, error) {
	logger.Trace().Msg("RuntimeConfig")
	return &runtimeapi.RuntimeConfigResponse{}, nil
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
func (s *Server) Status(ctx context.Context, req *runtimeapi.StatusRequest) (*runtimeapi.StatusResponse, error) {
	logger.Trace().Msg("Status")

	info, err := s.backend.client.Info(ctx)
	_, netErr := s.backend.client.NetworkInspect(ctx, "bridge", network.InspectOptions{})

	resp := &runtimeapi.StatusResponse{
		Status: &runtimeapi.RuntimeStatus{
			Conditions: []*runtimeapi.RuntimeCondition{
				{Type: "RuntimeReady", Status: err == nil, Reason: "DockerIsUp"},
				{Type: "NetworkReady", Status: netErr == nil, Reason: "BridgeNetworkReady"},
			},
		},
	}
	if req.GetVerbose() && err == nil {
		resp.Info = map[string]string{
			"storageDriver": info.Driver,
			"serverVersion": info.ServerVersion,
		}
	}
	return resp, nil
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
	panic("UpdateContainerResources: unimplemented")
}

// UpdatePodSandboxResources implements [v1.RuntimeServiceServer].
func (s *Server) UpdatePodSandboxResources(context.Context, *runtimeapi.UpdatePodSandboxResourcesRequest) (*runtimeapi.UpdatePodSandboxResourcesResponse, error) {
	panic("UpdatePodSandboxResources: unimplemented")
}

// UpdateRuntimeConfig implements [v1.RuntimeServiceServer].
func (s *Server) UpdateRuntimeConfig(ctx context.Context, req *runtimeapi.UpdateRuntimeConfigRequest) (*runtimeapi.UpdateRuntimeConfigResponse, error) {
	logger.Trace().Msg("UpdateRuntimeConfig")
	return &runtimeapi.UpdateRuntimeConfigResponse{}, nil
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
