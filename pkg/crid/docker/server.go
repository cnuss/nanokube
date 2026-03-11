package docker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/cnuss/nanokube/pkg/crid/backend"
	"github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"google.golang.org/grpc"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/kubelet/pkg/cri/streaming"
	kubelettypes "k8s.io/kubelet/pkg/types"
)

const defaultPauseImage = "registry.k8s.io/pause:3.10"

func NewServer(b *DockerBackend, parent backend.Backend) *Server {
	return &Server{backend: b, streaming: parent.Streaming(), log: component.NewLogger("docker-server")}
}

type Server struct {
	runtimeapi.UnsafeImageServiceServer
	runtimeapi.UnsafeRuntimeServiceServer
	log       component.Logger
	backend   *DockerBackend
	streaming streaming.Server
}

// Attach implements [v1.RuntimeServiceServer].
func (s *Server) Attach(ctx context.Context, req *runtimeapi.AttachRequest) (*runtimeapi.AttachResponse, error) {
	return s.streaming.GetAttach(req)
}

// CheckpointContainer implements [v1.RuntimeServiceServer].
func (s *Server) CheckpointContainer(ctx context.Context, req *runtimeapi.CheckpointContainerRequest) (*runtimeapi.CheckpointContainerResponse, error) {
	s.log.Warn().Str("method", "CheckpointContainer").Str("id", req.ContainerId).Msg("not implemented")
	return nil, wrapErr(fmt.Errorf("not implemented"))
}

// ContainerStats implements [v1.RuntimeServiceServer].
func (s *Server) ContainerStats(ctx context.Context, req *runtimeapi.ContainerStatsRequest) (*runtimeapi.ContainerStatsResponse, error) {
	s.log.Trace().Str("id", req.ContainerId).Msg("ContainerStats")
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
	s.log.Trace().Str("id", req.ContainerId).Msg("ContainerStatus")

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
	s.log.Trace().Str("name", req.Config.Metadata.Name).Str("sandbox", req.PodSandboxId).Str("image", req.Config.Image.Image).Msg("CreateContainer")

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

	s.log.Debug().Str("id", resp.ID).Msg("container created")
	return &runtimeapi.CreateContainerResponse{ContainerId: resp.ID}, nil
}

// Exec implements [v1.RuntimeServiceServer].
func (s *Server) Exec(ctx context.Context, req *runtimeapi.ExecRequest) (*runtimeapi.ExecResponse, error) {
	return s.streaming.GetExec(req)
}

// ExecSync implements [v1.RuntimeServiceServer].
func (s *Server) ExecSync(ctx context.Context, req *runtimeapi.ExecSyncRequest) (*runtimeapi.ExecSyncResponse, error) {
	s.log.Trace().Str("container", req.GetContainerId()).Strs("cmd", req.GetCmd()).Msg("ExecSync")
	id := req.GetContainerId()

	exec, err := s.backend.client.ContainerExecCreate(ctx, id, container.ExecOptions{
		Cmd:          req.GetCmd(),
		AttachStdout: true,
		AttachStderr: true,
	})
	if err != nil {
		return nil, wrapErr(err)
	}

	resp, err := s.backend.client.ContainerExecAttach(ctx, exec.ID, container.ExecAttachOptions{})
	if err != nil {
		return nil, wrapErr(err)
	}
	defer resp.Close()

	if timeout := req.GetTimeout(); timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
		defer cancel()
	}

	var stdout, stderr bytes.Buffer
	done := make(chan error, 1)
	go func() {
		_, err := stdcopy.StdCopy(&stdout, &stderr, resp.Reader)
		done <- err
	}()

	timedOut := false
	select {
	case <-ctx.Done():
		timedOut = true
	case err := <-done:
		if err != nil {
			return nil, wrapErr(err)
		}
	}

	ei, err := s.backend.client.ContainerExecInspect(context.Background(), exec.ID)
	if timedOut {
		if err == nil && ei.Pid > 0 {
			if err := s.backend.Run("busybox", []string{"kill", "-9", fmt.Sprintf("%d", ei.Pid)}, nil, true, func(string) error { return nil }); err != nil {
				s.log.Warn().Err(err).Int("pid", ei.Pid).Msg("ExecSync: failed to kill timed-out process")
			}
		}
		return &runtimeapi.ExecSyncResponse{
			Stdout:   stdout.Bytes(),
			Stderr:   stderr.Bytes(),
			ExitCode: -1,
		}, nil
	}

	var exitCode int32
	if err == nil {
		exitCode = int32(ei.ExitCode)
	}
	return &runtimeapi.ExecSyncResponse{
		Stdout:   stdout.Bytes(),
		Stderr:   stderr.Bytes(),
		ExitCode: exitCode,
	}, nil
}

// GetContainerEvents implements [v1.RuntimeServiceServer].
func (s *Server) GetContainerEvents(_ *runtimeapi.GetEventsRequest, stream grpc.ServerStreamingServer[runtimeapi.ContainerEventResponse]) error {
	s.log.Info().Msg("GetContainerEvents stream started")

	ctx := stream.Context()
	events := s.backend.parent.Subscribe()
	lp := s.backend.labels

	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-events:
			if !ok {
				return nil
			}
			if ev.Resource != backend.ResourceContainer {
				continue
			}

			var evType runtimeapi.ContainerEventType
			switch ev.Action {
			case backend.ActionCreate:
				evType = runtimeapi.ContainerEventType_CONTAINER_CREATED_EVENT
			case backend.ActionStart:
				evType = runtimeapi.ContainerEventType_CONTAINER_STARTED_EVENT
			case backend.ActionDie, backend.ActionStop, backend.ActionKill:
				evType = runtimeapi.ContainerEventType_CONTAINER_STOPPED_EVENT
			case backend.ActionDestroy, backend.ActionRemove:
				evType = runtimeapi.ContainerEventType_CONTAINER_DELETED_EVENT
			default:
				continue
			}

			// Skip sandbox containers — CRI events are for app containers only
			if ev.Attributes[lp.Prefix("type")] == "sandbox" {
				continue
			}

			containerID := ev.ID
			sandboxID := lp.SandboxID(ev.Attributes)

			var sandboxStatus *runtimeapi.PodSandboxStatus
			if sandboxID != "" {
				if resp, err := s.PodSandboxStatus(ctx, &runtimeapi.PodSandboxStatusRequest{PodSandboxId: sandboxID}); err == nil {
					sandboxStatus = resp.Status
				}
			}

			var containerStatuses []*runtimeapi.ContainerStatus
			if sandboxID != "" {
				if resp, err := s.ListContainers(ctx, &runtimeapi.ListContainersRequest{
					Filter: &runtimeapi.ContainerFilter{PodSandboxId: sandboxID},
				}); err == nil {
					for _, c := range resp.Containers {
						if st, err := s.ContainerStatus(ctx, &runtimeapi.ContainerStatusRequest{ContainerId: c.Id}); err == nil {
							containerStatuses = append(containerStatuses, st.Status)
						}
					}
				}
			}

			s.log.Debug().
				Str("container", containerID[:min(12, len(containerID))]).
				Str("action", string(ev.Action)).
				Int32("criEvent", int32(evType)).
				Msg("container event")

			if err := stream.Send(&runtimeapi.ContainerEventResponse{
				ContainerId:        containerID,
				ContainerEventType: evType,
				CreatedAt:          ev.TimeNano,
				PodSandboxStatus:   sandboxStatus,
				ContainersStatuses: containerStatuses,
			}); err != nil {
				return err
			}
		}
	}
}

// ListContainerStats implements [v1.RuntimeServiceServer].
func (s *Server) ListContainerStats(ctx context.Context, req *runtimeapi.ListContainerStatsRequest) (*runtimeapi.ListContainerStatsResponse, error) {
	s.log.Trace().Msg("ListContainerStats")

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
			s.log.Warn().Str("id", c.Id).Err(err).Msg("ListContainerStats: skipping container")
			continue
		}
		stats = append(stats, resp.Stats)
	}
	return &runtimeapi.ListContainerStatsResponse{Stats: stats}, nil
}

// ListContainers implements [v1.RuntimeServiceServer].
func (s *Server) ListContainers(ctx context.Context, req *runtimeapi.ListContainersRequest) (*runtimeapi.ListContainersResponse, error) {
	s.log.Trace().Msg("ListContainers")

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
	s.log.Warn().Str("method", "ListMetricDescriptors").Msg("not implemented")
	return nil, wrapErr(fmt.Errorf("not implemented"))
}

// ListPodSandbox implements [v1.RuntimeServiceServer].
func (s *Server) ListPodSandbox(ctx context.Context, req *runtimeapi.ListPodSandboxRequest) (*runtimeapi.ListPodSandboxResponse, error) {
	s.log.Trace().Msg("ListPodSandbox")

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
	s.log.Warn().Str("method", "ListPodSandboxMetrics").Msg("not implemented")
	return nil, wrapErr(fmt.Errorf("not implemented"))
}

// ListPodSandboxStats implements [v1.RuntimeServiceServer].
func (s *Server) ListPodSandboxStats(context.Context, *runtimeapi.ListPodSandboxStatsRequest) (*runtimeapi.ListPodSandboxStatsResponse, error) {
	s.log.Warn().Str("method", "ListPodSandboxStats").Msg("not implemented")
	return nil, wrapErr(fmt.Errorf("not implemented"))
}

// PodSandboxStats implements [v1.RuntimeServiceServer].
func (s *Server) PodSandboxStats(context.Context, *runtimeapi.PodSandboxStatsRequest) (*runtimeapi.PodSandboxStatsResponse, error) {
	s.log.Warn().Str("method", "PodSandboxStats").Msg("not implemented")
	return nil, wrapErr(fmt.Errorf("not implemented"))
}

// PodSandboxStatus implements [v1.RuntimeServiceServer].
func (s *Server) PodSandboxStatus(ctx context.Context, req *runtimeapi.PodSandboxStatusRequest) (*runtimeapi.PodSandboxStatusResponse, error) {
	s.log.Trace().Str("id", req.PodSandboxId).Msg("PodSandboxStatus")

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
func (s *Server) PortForward(ctx context.Context, req *runtimeapi.PortForwardRequest) (*runtimeapi.PortForwardResponse, error) {
	return s.streaming.GetPortForward(req)
}

// RemoveContainer implements [v1.RuntimeServiceServer].
func (s *Server) RemoveContainer(ctx context.Context, req *runtimeapi.RemoveContainerRequest) (*runtimeapi.RemoveContainerResponse, error) {
	s.log.Trace().Str("id", req.ContainerId).Msg("RemoveContainer")
	id := req.GetContainerId()
	s.backend.StopLogs(id)

	for {
		inspect, err := s.backend.client.ContainerInspect(ctx, id)
		if err != nil {
			return &runtimeapi.RemoveContainerResponse{}, nil
		}

		s.log.Info().Str("id", id[:min(12, len(id))]).Str("status", inspect.State.Status).Msg("removing container")
		switch inspect.State.Status {
		case "exited", "dead", "created":
			if err := s.backend.client.ContainerRemove(ctx, id, container.RemoveOptions{}); err != nil {
				s.log.Warn().Err(err).Str("id", id[:min(12, len(id))]).Msg("failed to remove container")
			}
		default:
			if err := s.backend.client.ContainerStop(ctx, id, container.StopOptions{}); err != nil {
				s.log.Warn().Err(err).Str("id", id).Msg("failed to stop container before removal")
			}
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// RemovePodSandbox implements [v1.RuntimeServiceServer].
func (s *Server) RemovePodSandbox(ctx context.Context, req *runtimeapi.RemovePodSandboxRequest) (*runtimeapi.RemovePodSandboxResponse, error) {
	s.log.Trace().Str("id", req.PodSandboxId).Msg("RemovePodSandbox")
	id := req.GetPodSandboxId()

	lb := s.backend.labels.NewBuilder(nil).WithSandbox(id).WithoutType()
	filters := s.backend.Into.Filters(lb)
	s.log.Info().Str("id", id[:min(12, len(id))]).Any("filters", filters).Msg("pruning containers")

	for {
		// Stop any running containers before pruning
		stopFilters := filters.Clone()
		stopFilters.Add("status", "running")
		stopFilters.Add("status", "paused")
		stopFilters.Add("status", "restarting")
		stopFilters.Add("status", "created")
		running, err := s.backend.client.ContainerList(ctx, container.ListOptions{Filters: stopFilters})
		if err != nil {
			s.log.Warn().Err(err).Str("id", id[:min(12, len(id))]).Msg("failed to list containers for pruning")
		} else if len(running) > 0 {
			// stop containers
			for _, c := range running {
				s.log.Info().Str("id", c.ID[:min(12, len(c.ID))]).Msg("stopping container before pruning")
				if err := s.backend.client.ContainerStop(ctx, c.ID, container.StopOptions{}); err != nil {
					s.log.Warn().Err(err).Str("id", c.ID[:min(12, len(c.ID))]).Msg("failed to stop container before pruning")
				}
			}
		} else {
			report, err := s.backend.client.ContainersPrune(ctx, filters)
			s.log.Info().Err(err).Any("report", report).Msg("prune attempt")
			if err != nil {
				s.log.Warn().Err(err).Str("id", id[:min(12, len(id))]).Msg("failed to prune containers")
			} else if len(report.ContainersDeleted) == 0 {
				s.log.Info().Str("id", id[:min(12, len(id))]).Msg("no containers to prune")
				break
			}
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}

	return &runtimeapi.RemovePodSandboxResponse{}, nil
}

// ReopenContainerLog implements [v1.RuntimeServiceServer].
func (s *Server) ReopenContainerLog(ctx context.Context, req *runtimeapi.ReopenContainerLogRequest) (*runtimeapi.ReopenContainerLogResponse, error) {
	s.log.Trace().Str("id", req.ContainerId).Msg("ReopenContainerLog")
	id := req.GetContainerId()
	s.backend.StopLogs(id)
	s.backend.StartLogs(id)
	return &runtimeapi.ReopenContainerLogResponse{}, nil
}

// RunPodSandbox implements [v1.RuntimeServiceServer].
func (s *Server) RunPodSandbox(ctx context.Context, req *runtimeapi.RunPodSandboxRequest) (*runtimeapi.RunPodSandboxResponse, error) {
	s.log.Trace().Str("name", req.Config.Metadata.Name).Str("namespace", req.Config.Metadata.Namespace).Str("uid", req.Config.Metadata.Uid).Msg("RunPodSandbox")

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
		s.log.Warn().Err(err).Str("id", resp.ID).Msg("failed to start sandbox container, removing")
		if _, err := s.RemovePodSandbox(ctx, &runtimeapi.RemovePodSandboxRequest{PodSandboxId: resp.ID}); err != nil {
			s.log.Warn().Err(err).Str("id", resp.ID).Msg("failed to remove sandbox container after failed start")
		}
		return nil, wrapErr(err)
	}

	s.log.Debug().Str("id", resp.ID).Msg("sandbox started")
	return &runtimeapi.RunPodSandboxResponse{PodSandboxId: resp.ID}, nil
}

// RuntimeConfig implements [v1.RuntimeServiceServer].
func (s *Server) RuntimeConfig(ctx context.Context, req *runtimeapi.RuntimeConfigRequest) (*runtimeapi.RuntimeConfigResponse, error) {
	s.log.Trace().Msg("RuntimeConfig")
	return &runtimeapi.RuntimeConfigResponse{}, nil
}

// StartContainer implements [v1.RuntimeServiceServer].
func (s *Server) StartContainer(ctx context.Context, req *runtimeapi.StartContainerRequest) (*runtimeapi.StartContainerResponse, error) {
	s.log.Trace().Str("id", req.ContainerId).Msg("StartContainer")
	id := req.GetContainerId()

	if err := s.backend.client.ContainerStart(ctx, id, container.StartOptions{}); err != nil {
		return nil, wrapErr(err)
	}

	s.backend.StartLogs(id)
	return &runtimeapi.StartContainerResponse{}, nil
}

// Status implements [v1.RuntimeServiceServer].
func (s *Server) Status(ctx context.Context, req *runtimeapi.StatusRequest) (*runtimeapi.StatusResponse, error) {
	s.log.Trace().Msg("Status")

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
	s.log.Trace().Str("id", req.ContainerId).Msg("StopContainer")
	id := req.GetContainerId()
	s.backend.StopLogs(id)

	t := int(req.GetTimeout())
	for {
		inspect, err := s.backend.client.ContainerInspect(ctx, id)
		if err != nil {
			return &runtimeapi.StopContainerResponse{}, nil
		}

		s.log.Info().Str("id", id[:min(12, len(id))]).Str("status", inspect.State.Status).Msg("stopping container")
		switch inspect.State.Status {
		case "exited", "dead":
			return &runtimeapi.StopContainerResponse{}, nil
		case "created":
			err := s.backend.client.ContainerRemove(ctx, id, container.RemoveOptions{})
			if err != nil {
				s.log.Warn().Err(err).Str("id", id).Msg("failed to remove container in created state")
			}
		default:
			err := s.backend.client.ContainerStop(ctx, id, container.StopOptions{Timeout: &t})
			if err != nil {
				s.log.Warn().Err(err).Str("id", id).Msg("failed to stop container")
			}
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// StopPodSandbox implements [v1.RuntimeServiceServer].
func (s *Server) StopPodSandbox(ctx context.Context, req *runtimeapi.StopPodSandboxRequest) (*runtimeapi.StopPodSandboxResponse, error) {
	s.log.Trace().Str("id", req.PodSandboxId).Msg("StopPodSandbox")
	id := req.GetPodSandboxId()

	resp, err := s.ListContainers(ctx, &runtimeapi.ListContainersRequest{
		Filter: &runtimeapi.ContainerFilter{PodSandboxId: id},
	})
	if err != nil {
		return nil, wrapErr(err)
	}
	for _, c := range resp.Containers {
		_, err := s.StopContainer(ctx, &runtimeapi.StopContainerRequest{ContainerId: c.Id, Timeout: 0})
		if err != nil {
			return nil, wrapErr(err)
		}
	}

	// Stop the sandbox container itself
	_, err = s.StopContainer(ctx, &runtimeapi.StopContainerRequest{ContainerId: id, Timeout: 0})
	if err != nil {
		return nil, wrapErr(err)
	}

	return &runtimeapi.StopPodSandboxResponse{}, nil
}

// UpdateContainerResources implements [v1.RuntimeServiceServer].
func (s *Server) UpdateContainerResources(ctx context.Context, req *runtimeapi.UpdateContainerResourcesRequest) (*runtimeapi.UpdateContainerResourcesResponse, error) {
	s.log.Trace().Str("id", req.ContainerId).Msg("UpdateContainerResources")
	resources := req.Linux
	if resources == nil {
		return &runtimeapi.UpdateContainerResourcesResponse{}, nil
	}
	_, err := s.backend.client.ContainerUpdate(ctx, req.ContainerId, container.UpdateConfig{
		Resources: container.Resources{
			CPUPeriod:  resources.CpuPeriod,
			CPUQuota:   resources.CpuQuota,
			CPUShares:  resources.CpuShares,
			Memory:     resources.MemoryLimitInBytes,
			MemorySwap: resources.MemoryLimitInBytes,
			CpusetCpus: resources.CpusetCpus,
			CpusetMems: resources.CpusetMems,
		},
	})
	if err != nil {
		return nil, wrapErr(err)
	}
	return &runtimeapi.UpdateContainerResourcesResponse{}, nil
}

// UpdatePodSandboxResources implements [v1.RuntimeServiceServer].
func (s *Server) UpdatePodSandboxResources(context.Context, *runtimeapi.UpdatePodSandboxResourcesRequest) (*runtimeapi.UpdatePodSandboxResourcesResponse, error) {
	s.log.Warn().Str("method", "UpdatePodSandboxResources").Msg("not implemented")
	return nil, wrapErr(fmt.Errorf("not implemented"))
}

// UpdateRuntimeConfig implements [v1.RuntimeServiceServer].
func (s *Server) UpdateRuntimeConfig(ctx context.Context, req *runtimeapi.UpdateRuntimeConfigRequest) (*runtimeapi.UpdateRuntimeConfigResponse, error) {
	s.log.Trace().Msg("UpdateRuntimeConfig")
	return &runtimeapi.UpdateRuntimeConfigResponse{}, nil
}

// Version implements [v1.RuntimeServiceServer].
func (s *Server) Version(ctx context.Context, req *runtimeapi.VersionRequest) (*runtimeapi.VersionResponse, error) {
	v, err := s.backend.client.ServerVersion(ctx)
	if err != nil {
		return nil, wrapErr(err)
	}
	return &runtimeapi.VersionResponse{
		Version:           req.GetVersion(),
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
