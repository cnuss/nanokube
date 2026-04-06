package docker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/cnuss/nanokube/pkg/crid/backend"
	"github.com/cnuss/nanokube/pkg/crid/labels"
	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	dockervolume "github.com/docker/docker/api/types/volume"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const defaultPauseImage = "busybox:latest"

type securityContext interface {
	GetPrivileged() bool
	GetSeccomp() *runtimeapi.SecurityProfile
}

// env translates CRI KeyValue envs to Docker env strings.
func (s *Server) env(criEnvs []*runtimeapi.KeyValue) []string {
	envs := make([]string, 0, len(criEnvs))
	for _, kv := range criEnvs {
		envs = append(envs, kv.GetKey()+"="+kv.GetValue())
	}
	return envs
}

// binds translates CRI mounts to Docker bind mount strings.
// Symlinks are resolved so Docker doesn't fail trying to mkdir over them.
func (s *Server) binds(current []string, mounts []*runtimeapi.Mount) []string {
	for _, m := range mounts {
		hostPath := m.GetHostPath()
		if resolved, err := filepath.EvalSymlinks(hostPath); err == nil {
			hostPath = resolved
		}
		bind := hostPath + ":" + m.GetContainerPath()
		if m.GetReadonly() {
			bind += ":ro"
		}
		current = append(current, bind)
	}
	return current
}

// user resolves the container user from CRI security context, falling back to sandbox annotations.
func (s *Server) user(sandboxConfig *container.Config, sc *runtimeapi.LinuxContainerSecurityContext) (string, error) {
	if sc.GetRunAsGroup() != nil && sc.GetRunAsUser() == nil {
		return "", status.Errorf(codes.InvalidArgument, "RunAsGroup requires RunAsUser")
	}
	var user string
	if uid := sc.GetRunAsUser(); uid != nil {
		user = strconv.FormatInt(uid.GetValue(), 10)
	}
	if gid := sc.GetRunAsGroup(); gid != nil {
		user += ":" + strconv.FormatInt(gid.GetValue(), 10)
	}
	if username := sc.GetRunAsUsername(); username != "" {
		user = username
	}
	if user == "" {
		annotations := s.backend.labels.ExtractAnnotations(sandboxConfig.Labels)
		user = s.backend.labels.SecurityContext(annotations)
	}
	return user, nil
}

// securityOpts translates CRI security context to Docker SecurityOpt entries.
func (s *Server) securityOpts(current []string, sc securityContext) []string {
	if sc == nil || sc.GetPrivileged() {
		return current
	}
	seccomp := sc.GetSeccomp()
	if seccomp == nil {
		return append(current, "seccomp=unconfined")
	}
	switch seccomp.GetProfileType() {
	case runtimeapi.SecurityProfile_Unconfined:
		return append(current, "seccomp=unconfined")
	case runtimeapi.SecurityProfile_Localhost:
		ref := seccomp.GetLocalhostRef()
		var data string
		err := s.backend.Run("busybox", []string{"cat", ref}, []string{ref + ":" + ref + ":ro"}, false, func(out string) error {
			data = out
			return nil
		})
		if err != nil {
			s.log.Warn().Err(err).Str("ref", ref).Msg("failed to read seccomp profile")
			return current
		}
		return append(current, "seccomp="+data)
	default:
		return current
	}
}

func NewServer(b *DockerBackend, parent backend.Backend) *Server {
	return &Server{
		backend: b,
		parent:  parent,
		log:     component.NewLogger("docker-server"),
	}
}

type Server struct {
	runtimeapi.UnsafeImageServiceServer
	runtimeapi.UnsafeRuntimeServiceServer
	log     component.Logger
	backend *DockerBackend
	parent  backend.Backend
}

// Attach implements [v1.RuntimeServiceServer].
func (s *Server) Attach(ctx context.Context, req *runtimeapi.AttachRequest) (*runtimeapi.AttachResponse, error) {
	if s.parent.Streaming() == nil {
		return nil, status.Error(codes.Unavailable, "streaming server not ready")
	}
	return s.parent.Streaming().GetAttach(req)
}

// CheckpointContainer implements [v1.RuntimeServiceServer].
func (s *Server) CheckpointContainer(ctx context.Context, req *runtimeapi.CheckpointContainerRequest) (*runtimeapi.CheckpointContainerResponse, error) {
	return nil, component.WrapErr(s.log, fmt.Errorf("not implemented"), req)
}

// ContainerStats implements [v1.RuntimeServiceServer].
func (s *Server) ContainerStats(ctx context.Context, req *runtimeapi.ContainerStatsRequest) (*runtimeapi.ContainerStatsResponse, error) {
	s.log.Trace().Str("id", req.ContainerId).Msg("ContainerStats")
	id := req.GetContainerId()

	statusResp, err := s.ContainerStatus(ctx, &runtimeapi.ContainerStatusRequest{ContainerId: id})
	if err != nil {
		return nil, component.WrapErr(s.log, err)
	}
	cs := statusResp.Status

	resp, err := s.backend.client.ContainerStatsOneShot(ctx, id)
	if err != nil {
		return nil, component.WrapErr(s.log, err)
	}
	defer resp.Body.Close()

	var stats container.StatsResponse
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return nil, component.WrapErr(s.log, fmt.Errorf("decode docker stats: %w", err))
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
		return nil, component.WrapErr(s.log, err)
	}

	createdAt := s.backend.Into.CreatedAt(inspect.Created)
	state := s.backend.Into.ContainerState(inspect.State.Status)

	status := &runtimeapi.ContainerStatus{
		Id: inspect.ID,
		Metadata: &runtimeapi.ContainerMetadata{
			Name:    s.backend.labels.GetName(inspect.Config.Labels),
			Attempt: s.backend.labels.Attempt(inspect.Config.Labels),
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
		if inspect.State.OOMKilled || (inspect.State.ExitCode == 137 && inspect.HostConfig.Memory > 0) {
			status.Reason = "OOMKilled"
		} else if inspect.State.ExitCode == 0 {
			status.Reason = "Completed"
		} else if inspect.State.Error != "" {
			status.Reason = inspect.State.Error
		} else {
			status.Reason = "Error"
		}
	}

	for _, m := range inspect.Mounts {
		status.Mounts = append(status.Mounts, &runtimeapi.Mount{
			ContainerPath: m.Destination,
			HostPath:      m.Source,
			Readonly:      !m.RW,
		})
	}

	s.log.Trace().Str("id", inspect.ID[:min(12, len(inspect.ID))]).Str("state", inspect.State.Status).Int32("exitCode", status.ExitCode).Str("reason", status.Reason).Msg("ContainerStatus")

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
	s.log.Info().Any("req", req).Msg("CreateContainer")
	config := req.GetConfig()
	sandboxID := req.GetPodSandboxId()
	meta := config.GetMetadata()

	// Inspect sandbox directly to get both CRI labels and internal labels (log directory)
	sandbox, err := s.backend.client.ContainerInspect(ctx, sandboxID)
	if err != nil {
		return nil, component.WrapErr(s.log, err)
	}

	// Labels: inherit sandbox labels, overlay container-specific fields
	labelBuilder := s.backend.labels.NewBuilder(sandbox.Config.Labels).
		WithType(labels.TypeContainer).
		WithName(meta.GetName()).
		WithParentUid(sandboxID).
		WithAttempt(meta.GetAttempt()).
		WithLabels(config.GetLabels()).
		WithAnnotations(config.GetAnnotations()).
		WithLogDirectory(s.backend.labels.LogDirectory(sandbox.Config.Labels)).
		WithLogPath(config.GetLogPath())

	name, labels, err := labelBuilder.Build()
	if err != nil {
		return nil, component.WrapErr(s.log, err)
	}

	// Deep copy sandbox configs so mutations don't affect the inspect result
	dockerConfig := new(container.Config)
	if b, err := json.Marshal(sandbox.Config); err == nil {
		json.Unmarshal(b, dockerConfig)
	}
	hostConfig := new(container.HostConfig)
	if b, err := json.Marshal(sandbox.HostConfig); err == nil {
		json.Unmarshal(b, hostConfig)
	}
	dockerConfig.Image = config.GetImage().GetImage()
	dockerConfig.Entrypoint = config.GetCommand()
	dockerConfig.Cmd = config.GetArgs()
	dockerConfig.Env = s.env(config.GetEnvs())
	dockerConfig.WorkingDir = config.GetWorkingDir()
	dockerConfig.Labels = labels
	dockerConfig.StdinOnce = config.GetStdinOnce()
	dockerConfig.OpenStdin = config.GetStdin()
	dockerConfig.Tty = config.GetTty()
	dockerConfig.Hostname = ""
	dockerConfig.ExposedPorts = nil
	if dockerConfig.User, err = s.user(sandbox.Config, config.GetLinux().GetSecurityContext()); err != nil {
		return nil, err
	}

	hostConfig.NetworkMode = container.NetworkMode("container:" + sandboxID)
	hostConfig.IpcMode = container.IpcMode("container:" + sandboxID)
	if config.GetLinux().GetSecurityContext().GetNamespaceOptions().GetPid() == runtimeapi.NamespaceMode_CONTAINER {
		hostConfig.PidMode = ""
	} else {
		hostConfig.PidMode = container.PidMode("container:" + sandboxID)
	}
	hostConfig.Binds = nil
	hostConfig.PortBindings = nil
	hostConfig.ExtraHosts = nil
	hostConfig.DNS = nil
	hostConfig.DNSSearch = nil
	hostConfig.DNSOptions = nil
	hostConfig.LogConfig = container.LogConfig{}
	hostConfig.Resources = container.Resources{}
	hostConfig.Resources.CPUShares = config.GetLinux().GetResources().GetCpuShares()
	hostConfig.Resources.Memory = config.GetLinux().GetResources().GetMemoryLimitInBytes()
	hostConfig.Resources.MemorySwap = config.GetLinux().GetResources().GetMemorySwapLimitInBytes()
	hostConfig.Resources.CPUQuota = config.GetLinux().GetResources().GetCpuQuota()
	hostConfig.Resources.CPUPeriod = config.GetLinux().GetResources().GetCpuPeriod()
	hostConfig.Privileged = config.GetLinux().GetSecurityContext().GetPrivileged()
	hostConfig.ReadonlyRootfs = config.GetLinux().GetSecurityContext().GetReadonlyRootfs()
	hostConfig.ReadonlyPaths = config.GetLinux().GetSecurityContext().GetReadonlyPaths()
	hostConfig.MaskedPaths = config.GetLinux().GetSecurityContext().GetMaskedPaths()
	for _, gid := range config.GetLinux().GetSecurityContext().GetSupplementalGroups() {
		hostConfig.GroupAdd = append(hostConfig.GroupAdd, strconv.FormatInt(gid, 10))
	}
	hostConfig.CapAdd = append(hostConfig.CapAdd, config.GetLinux().GetSecurityContext().GetCapabilities().GetAddCapabilities()...)
	hostConfig.CapDrop = append(hostConfig.CapDrop, config.GetLinux().GetSecurityContext().GetCapabilities().GetDropCapabilities()...)
	hostConfig.SecurityOpt = s.securityOpts(hostConfig.SecurityOpt, config.GetLinux().GetSecurityContext())
	if config.GetLinux().GetSecurityContext().GetNoNewPrivs() {
		hostConfig.SecurityOpt = append(hostConfig.SecurityOpt, "no-new-privileges")
	}
	hostConfig.Binds = s.binds(hostConfig.Binds, config.GetMounts())

	if config.GetLogPath() != "" {
		hostConfig.LogConfig = container.LogConfig{
			Type:   "json-file",
			Config: map[string]string{"max-size": "10m", "max-file": "3"},
		}
	}

	resp, err := s.backend.client.ContainerCreate(ctx, dockerConfig, hostConfig, nil, nil, name)
	for err != nil && errdefs.IsConflict(err) {
		labelBuilder = labelBuilder.Clone().IncrementAttempt()
		name, labels, err = labelBuilder.Build()
		dockerConfig.Labels = labels
		s.log.Info().Str("name", name).Msg("container name conflict, retrying")
		resp, err = s.backend.client.ContainerCreate(ctx, dockerConfig, hostConfig, nil, nil, name)
	}

	if err != nil {
		return nil, component.WrapErr(s.log, err)
	}

	s.log.Debug().Str("id", resp.ID).Msg("container created")
	return &runtimeapi.CreateContainerResponse{ContainerId: resp.ID}, nil
}

// Exec implements [v1.RuntimeServiceServer].
func (s *Server) Exec(ctx context.Context, req *runtimeapi.ExecRequest) (*runtimeapi.ExecResponse, error) {
	if s.parent.Streaming() == nil {
		return nil, status.Error(codes.Unavailable, "streaming server not ready")
	}
	return s.parent.Streaming().GetExec(req)
}

// ExecSync implements [v1.RuntimeServiceServer].
func (s *Server) ExecSync(ctx context.Context, req *runtimeapi.ExecSyncRequest) (*runtimeapi.ExecSyncResponse, error) {
	s.log.Info().Any("req", req).Msg("ExecSync")
	id := req.GetContainerId()
	cmd := req.GetCmd()

	// Detect sandbox sentinel: rewritten probes need to run in the pod sandbox
	// (which has busybox with wget/nc) instead of the app container.
	if len(cmd) > 0 && cmd[0] == backend.SandboxExecSentinel {
		cmd = cmd[1:]
		inspect, err := s.backend.client.ContainerInspect(ctx, id)
		if err != nil {
			return nil, component.WrapErr(s.log, err)
		}
		if sandboxID := s.backend.labels.ParentUID(inspect.Config.Labels); sandboxID != "" {
			s.log.Info().Str("container", id).Str("sandbox", sandboxID).Strs("cmd", cmd).Msg("routing probe to sandbox")
			id = sandboxID
		}
	}

	exec, err := s.backend.client.ContainerExecCreate(ctx, id, container.ExecOptions{
		Cmd:          cmd,
		AttachStdout: true,
		AttachStderr: true,
	})
	if err != nil {
		return nil, component.WrapErr(s.log, err)
	}

	resp, err := s.backend.client.ContainerExecAttach(ctx, exec.ID, container.ExecAttachOptions{})
	if err != nil {
		return nil, component.WrapErr(s.log, err)
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
			return nil, component.WrapErr(s.log, err)
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
func (s *Server) GetContainerEvents(req *runtimeapi.GetEventsRequest, stream grpc.ServerStreamingServer[runtimeapi.ContainerEventResponse]) error {
	return fmt.Errorf("GetContainerEvents not implemented")
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
		return nil, component.WrapErr(s.log, err)
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

	lb := s.backend.labels.NewBuilder(nil).WithType(labels.TypeContainer)
	f := s.backend.Into.Filters(lb)

	if filter := req.GetFilter(); filter != nil {
		if filter.Id != "" {
			f.Add("id", filter.Id)
		}
		if filter.PodSandboxId != "" {
			f.Add("label", s.backend.labels.ParentUIDFilter(filter.PodSandboxId))
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
	}

	containers, err := s.backend.client.ContainerList(ctx, container.ListOptions{All: true, Filters: f})
	if err != nil {
		return nil, component.WrapErr(s.log, err)
	}

	selector := req.GetFilter().GetLabelSelector()
	result := make([]*runtimeapi.Container, 0, len(containers))
	for _, c := range containers {
		if !s.matchLabels(c.Labels, selector) {
			continue
		}
		result = append(result, s.backend.Into.Container(c))
	}

	return &runtimeapi.ListContainersResponse{Containers: result}, nil
}

// ListMetricDescriptors implements [v1.RuntimeServiceServer].
func (s *Server) ListMetricDescriptors(_ context.Context, req *runtimeapi.ListMetricDescriptorsRequest) (*runtimeapi.ListMetricDescriptorsResponse, error) {
	return nil, component.WrapErr(s.log, fmt.Errorf("not implemented"), req)
}

// ListPodSandbox implements [v1.RuntimeServiceServer].
func (s *Server) ListPodSandbox(ctx context.Context, req *runtimeapi.ListPodSandboxRequest) (*runtimeapi.ListPodSandboxResponse, error) {
	s.log.Trace().Msg("ListPodSandbox")
	if req == nil {
		req = &runtimeapi.ListPodSandboxRequest{}
	}

	f := s.backend.Into.Filters(s.backend.labels.NewBuilder(nil).WithType(labels.TypeSandbox))

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
	}

	containers, err := s.backend.client.ContainerList(ctx, container.ListOptions{All: true, Filters: f})
	if err != nil {
		return nil, component.WrapErr(s.log, err)
	}

	selector := req.GetFilter().GetLabelSelector()
	var result []*runtimeapi.PodSandbox
	for _, c := range containers {
		if !s.matchLabels(c.Labels, selector) {
			continue
		}
		result = append(result, s.backend.Into.PodSandbox(c))
	}
	return &runtimeapi.ListPodSandboxResponse{Items: result}, nil
}

// ListPodSandboxMetrics implements [v1.RuntimeServiceServer].
func (s *Server) ListPodSandboxMetrics(_ context.Context, req *runtimeapi.ListPodSandboxMetricsRequest) (*runtimeapi.ListPodSandboxMetricsResponse, error) {
	return nil, component.WrapErr(s.log, fmt.Errorf("not implemented"), req)
}

// ListPodSandboxStats implements [v1.RuntimeServiceServer].
func (s *Server) ListPodSandboxStats(_ context.Context, req *runtimeapi.ListPodSandboxStatsRequest) (*runtimeapi.ListPodSandboxStatsResponse, error) {
	return nil, component.WrapErr(s.log, fmt.Errorf("not implemented"), req)
}

// PodSandboxStats implements [v1.RuntimeServiceServer].
func (s *Server) PodSandboxStats(_ context.Context, req *runtimeapi.PodSandboxStatsRequest) (*runtimeapi.PodSandboxStatsResponse, error) {
	return nil, component.WrapErr(s.log, fmt.Errorf("not implemented"), req)
}

// PodSandboxStatus implements [v1.RuntimeServiceServer].
func (s *Server) PodSandboxStatus(ctx context.Context, req *runtimeapi.PodSandboxStatusRequest) (*runtimeapi.PodSandboxStatusResponse, error) {
	s.log.Trace().Str("id", req.PodSandboxId).Msg("PodSandboxStatus")

	inspect, err := s.backend.client.ContainerInspect(ctx, req.GetPodSandboxId())
	if err != nil {
		return nil, component.WrapErr(s.log, err)
	}

	createdAt := s.backend.Into.CreatedAt(inspect.Created)

	// Build namespace options from Docker HostConfig
	nsOpts := &runtimeapi.NamespaceOption{
		Network: runtimeapi.NamespaceMode_POD,
		Pid:     runtimeapi.NamespaceMode_CONTAINER,
		Ipc:     runtimeapi.NamespaceMode_POD,
	}
	if inspect.HostConfig != nil {
		if inspect.HostConfig.NetworkMode == "host" {
			nsOpts.Network = runtimeapi.NamespaceMode_NODE
		}
		if inspect.HostConfig.PidMode == "host" {
			nsOpts.Pid = runtimeapi.NamespaceMode_NODE
		}
		if inspect.HostConfig.IpcMode == "host" {
			nsOpts.Ipc = runtimeapi.NamespaceMode_NODE
		}
	}

	status := &runtimeapi.PodSandboxStatus{
		Id: inspect.ID,
		Metadata: &runtimeapi.PodSandboxMetadata{
			Name:      s.backend.labels.GetName(inspect.Config.Labels),
			Namespace: s.backend.labels.Namespace(inspect.Config.Labels),
			Uid:       s.backend.labels.UID(inspect.Config.Labels),
		},
		State:     s.backend.Into.PodState(inspect.State.Status),
		CreatedAt: createdAt,
		Network: &runtimeapi.PodSandboxNetworkStatus{
			Ip:            getIPFromInspect(inspect, s.backend.labels),
			AdditionalIps: getAdditionalIPs(inspect),
		},
		Linux: &runtimeapi.LinuxPodSandboxStatus{
			Namespaces: &runtimeapi.Namespace{
				Options: nsOpts,
			},
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
	if s.parent.Streaming() == nil {
		return nil, status.Error(codes.Unavailable, "streaming server not ready")
	}
	return s.parent.Streaming().GetPortForward(req)
}

// RemoveContainer implements [v1.RuntimeServiceServer].
func (s *Server) RemoveContainer(ctx context.Context, req *runtimeapi.RemoveContainerRequest) (*runtimeapi.RemoveContainerResponse, error) {
	id := req.GetContainerId()
	s.log.Info().Str("id", id[:min(12, len(id))]).Msg("RemoveContainer")
	s.backend.StopLogs(id)

	if err := s.backend.client.ContainerRemove(ctx, id, container.RemoveOptions{Force: true}); err != nil {
		if !errdefs.IsNotFound(err) {
			return nil, component.WrapErr(s.log, err)
		}
	}

	return &runtimeapi.RemoveContainerResponse{}, nil
}

// RemovePodSandbox implements [v1.RuntimeServiceServer].
func (s *Server) RemovePodSandbox(ctx context.Context, req *runtimeapi.RemovePodSandboxRequest) (*runtimeapi.RemovePodSandboxResponse, error) {
	id := req.GetPodSandboxId()
	s.log.Info().Str("id", id[:min(12, len(id))]).Msg("RemovePodSandbox")

	// Check if sandbox exists — if not, return success (idempotent)
	status, err := s.PodSandboxStatus(ctx, &runtimeapi.PodSandboxStatusRequest{PodSandboxId: id})
	if status == nil || status.GetStatus() == nil {
		s.log.Debug().Str("id", id[:min(12, len(id))]).Msg("sandbox not found, nothing to remove")
		return &runtimeapi.RemovePodSandboxResponse{}, nil
	}
	if err != nil {
		return nil, component.WrapErr(s.log, err)
	}

	// Find all containers in the sandbox
	resp, err := s.ListContainers(ctx, &runtimeapi.ListContainersRequest{
		Filter: &runtimeapi.ContainerFilter{PodSandboxId: id},
	})
	if err != nil {
		return nil, component.WrapErr(s.log, err)
	}

	// Remove all containers in the sandbox
	for _, c := range resp.Containers {
		if _, err := s.RemoveContainer(ctx, &runtimeapi.RemoveContainerRequest{ContainerId: c.Id}); err != nil {
			return nil, component.WrapErr(s.log, err)
		}
	}

	// Remove the sandbox container itself
	if _, err := s.RemoveContainer(ctx, &runtimeapi.RemoveContainerRequest{ContainerId: id}); err != nil {
		return nil, component.WrapErr(s.log, err)
	}

	// Remove per-sandbox bridge networks
	if err := s.backend.parent.IPAM().DeallocateNetwork(ctx, status.GetStatus()); err != nil {
		s.log.Warn().Err(err).Str("sandbox", id[:min(12, len(id))]).Msg("failed to deallocate sandbox network")
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
	s.log.Info().Any("req", req).Msg("RunPodSandbox")
	config := req.GetConfig()
	meta := config.GetMetadata()

	name, labels, err := s.backend.labels.NewBuilder(config.GetLabels()).
		WithType(labels.TypeSandbox).WithName(meta.GetName()).WithNamespace(meta.GetNamespace()).WithUid(meta.GetUid()).
		WithAnnotations(config.GetAnnotations()).
		WithLogDirectory(config.GetLogDirectory()).
		Build()
	if err != nil {
		return nil, component.WrapErr(s.log, err)
	}

	var status *runtimeapi.PodSandboxStatus

	existing, _ := s.ListPodSandbox(ctx, &runtimeapi.ListPodSandboxRequest{
		Filter: &runtimeapi.PodSandboxFilter{
			LabelSelector: map[string]string{s.backend.labels.UIDKey(): meta.GetUid()},
		},
	})

	for _, sb := range existing.GetItems() {
		if sb.GetAnnotations()["kubernetes.io/config.hash"] == config.GetAnnotations()["kubernetes.io/config.hash"] {
			s.log.Info().Str("id", sb.Id[:min(12, len(sb.Id))]).Msg("reusing existing sandbox")
			resp, _ := s.PodSandboxStatus(ctx, &runtimeapi.PodSandboxStatusRequest{PodSandboxId: sb.Id})
			if resp.GetStatus() != nil {
				status = resp.GetStatus()
			}
		} else {
			s.log.Info().Str("id", sb.Id[:min(12, len(sb.Id))]).Msg("removing stale sandbox")
			s.RemovePodSandbox(ctx, &runtimeapi.RemovePodSandboxRequest{PodSandboxId: sb.Id})
		}
	}

	if status == nil {
		dockerConfig := &container.Config{
			Image:      defaultPauseImage,
			Entrypoint: []string{"tail", "-f", "/dev/null"},
			Hostname:   config.GetHostname(),
			Labels:     labels,
		}

		networkMode := backend.NetworkBridge
		if linux := config.GetLinux(); linux != nil {
			if ns := linux.GetSecurityContext().GetNamespaceOptions(); ns != nil && ns.GetNetwork() == runtimeapi.NamespaceMode_NODE {
				networkMode = backend.NetworkHost
			}
		}

		// TODO: set DNSNames on the per-sandbox network for Docker DNS discovery
		hostConfig := &container.HostConfig{
			IpcMode: container.IpcMode("shareable"),
		}

		// Linux namespace options
		if linux := config.GetLinux(); linux != nil {
			if sc := linux.GetSecurityContext(); sc != nil {
				hostConfig.Privileged = sc.GetPrivileged()
				hostConfig.SecurityOpt = s.securityOpts(hostConfig.SecurityOpt, sc)
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
			hostConfig.Sysctls = linux.GetSysctls()
		}

		// DNS — always pass CRI DNS config to Docker. For host-network, Docker
		// writes these directly to resolv.conf. For bridge-mode, Docker's embedded
		// DNS (127.0.0.11) uses them as upstream servers (ExtServers).
		networkingConfig := &network.NetworkingConfig{}
		if dns := config.GetDnsConfig(); dns != nil {
			hostConfig.DNS = dns.GetServers()
			hostConfig.DNSSearch = dns.GetSearches()
			hostConfig.DNSOptions = dns.GetOptions()
		}

		extraHosts := s.backend.parent.Hosts().ExtraHosts(ctx, networkMode)
		for _, extraHost := range s.backend.labels.ExtraHosts(config.GetAnnotations()) {
			// Host aliases — kubelet injects these as annotations via podAdmitHandler
			extraHosts = append(extraHosts, extraHost)
		}
		slices.Sort(extraHosts)
		hostConfig.ExtraHosts = slices.Compact(extraHosts)

		if networkMode != backend.NetworkHost {
			var networks []string

			// Forces only critest onto bridge
			if (len(config.Annotations)) == 0 {
				networks = []string{"bridge"}
			} else {
				if net, err := s.backend.parent.IPAM().AllocateNetwork(ctx, config); err == nil {
					networks = []string{net.Name, s.backend.parent.IPAM().StaticPodNet().Name}
				}
			}

			if len(config.GetPortMappings()) > 0 {
				dockerConfig.ExposedPorts = nat.PortSet{}
				hostConfig.PortBindings = nat.PortMap{}
				for _, pm := range config.GetPortMappings() {
					containerPort := pm.GetContainerPort()
					hostPort := pm.GetHostPort()
					if hostPort == 0 && len(config.Annotations) == 0 {
						// If host port is not specified and we do not have any annotations, we're in critest
						s.log.Info().Int32("containerPort", containerPort).Msg("host port not specified, defaulting to container port (critest compatibility)")
						hostPort = containerPort
					}
					port := nat.Port(fmt.Sprintf("%d/%s", containerPort, strings.ToLower(pm.GetProtocol().String())))
					dockerConfig.ExposedPorts[port] = struct{}{}
					hostConfig.PortBindings[port] = []nat.PortBinding{
						{HostIP: pm.GetHostIp(), HostPort: strconv.Itoa(int(hostPort))},
					}
				}
			}

			networkingConfig.EndpointsConfig = map[string]*network.EndpointSettings{}
			for _, networkName := range networks {
				networkingConfig.EndpointsConfig[networkName] = &network.EndpointSettings{}
			}
		}

		// Ensure pause image is available
		imgSpec := &runtimeapi.ImageSpec{Image: dockerConfig.Image}
		imgReq := &runtimeapi.ImageStatusRequest{Image: imgSpec, Verbose: true}
		image, _ := s.ImageStatus(ctx, imgReq)
		if image.Image == nil {
			if _, err := s.PullImage(ctx, &runtimeapi.PullImageRequest{Image: imgSpec}); err != nil {
				return nil, component.WrapErr(s.log, err)
			}
			image, _ = s.ImageStatus(ctx, imgReq)
		}

		// Platform from image info
		var platform *ocispec.Platform
		if image.Info != nil {
			platform = &ocispec.Platform{OS: image.Info["os"], Architecture: image.Info["architecture"]}
		}

		created, err := s.backend.client.ContainerCreate(ctx, dockerConfig, hostConfig, networkingConfig, platform, name)
		if err != nil {
			return nil, component.WrapErr(s.log, err)
		}

		resp, err := s.PodSandboxStatus(ctx, &runtimeapi.PodSandboxStatusRequest{PodSandboxId: created.ID})
		if err != nil {
			return nil, component.WrapErr(s.log, err)
		}

		status = resp.GetStatus()
	}

	if status.State != runtimeapi.PodSandboxState_SANDBOX_READY {
		if err := s.backend.client.ContainerStart(ctx, status.Id, container.StartOptions{}); err != nil {
			return nil, component.WrapErr(s.log, err)
		}

		resp, err := s.PodSandboxStatus(ctx, &runtimeapi.PodSandboxStatusRequest{PodSandboxId: status.Id})
		if err != nil {
			return nil, component.WrapErr(s.log, err)
		}
		status = resp.GetStatus()
	}

	s.log.Debug().Str("id", status.Id).Msg("sandbox started")
	return &runtimeapi.RunPodSandboxResponse{PodSandboxId: status.Id}, nil
}

// RuntimeConfig implements [v1.RuntimeServiceServer].
func (s *Server) RuntimeConfig(ctx context.Context, req *runtimeapi.RuntimeConfigRequest) (*runtimeapi.RuntimeConfigResponse, error) {
	s.log.Trace().Msg("RuntimeConfig")
	return &runtimeapi.RuntimeConfigResponse{}, nil
}

// StartContainer implements [v1.RuntimeServiceServer].
func (s *Server) StartContainer(ctx context.Context, req *runtimeapi.StartContainerRequest) (*runtimeapi.StartContainerResponse, error) {
	s.log.Info().Str("id", req.ContainerId).Msg("StartContainer")
	id := req.GetContainerId()

	if err := s.backend.client.ContainerStart(ctx, id, container.StartOptions{}); err != nil {
		return nil, component.WrapErr(s.log, err)
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
	id := req.GetContainerId()
	s.log.Trace().Str("id", id[:min(12, len(id))]).Msg("StopContainer")
	s.backend.StopLogs(id)

	t := int(req.GetTimeout())
	if err := s.backend.client.ContainerStop(ctx, id, container.StopOptions{Timeout: &t}); err != nil {
		if !errdefs.IsNotFound(err) {
			return nil, component.WrapErr(s.log, err)
		}
	}

	return &runtimeapi.StopContainerResponse{}, nil
}

// StopPodSandbox implements [v1.RuntimeServiceServer].
func (s *Server) StopPodSandbox(ctx context.Context, req *runtimeapi.StopPodSandboxRequest) (*runtimeapi.StopPodSandboxResponse, error) {
	id := req.GetPodSandboxId()
	s.log.Info().Str("id", id[:min(12, len(id))]).Msg("StopPodSandbox")

	resp, err := s.ListContainers(ctx, &runtimeapi.ListContainersRequest{
		Filter: &runtimeapi.ContainerFilter{PodSandboxId: id},
	})
	if err != nil {
		return nil, component.WrapErr(s.log, err)
	}
	for _, c := range resp.Containers {
		if _, err := s.StopContainer(ctx, &runtimeapi.StopContainerRequest{ContainerId: c.Id, Timeout: 0}); err != nil {
			return nil, component.WrapErr(s.log, err)
		}
	}

	// Stop the sandbox container itself
	if _, err := s.StopContainer(ctx, &runtimeapi.StopContainerRequest{ContainerId: id, Timeout: 0}); err != nil {
		return nil, component.WrapErr(s.log, err)
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
		return nil, component.WrapErr(s.log, err)
	}
	return &runtimeapi.UpdateContainerResourcesResponse{}, nil
}

// UpdatePodSandboxResources implements [v1.RuntimeServiceServer].
func (s *Server) UpdatePodSandboxResources(_ context.Context, req *runtimeapi.UpdatePodSandboxResourcesRequest) (*runtimeapi.UpdatePodSandboxResourcesResponse, error) {
	return nil, component.WrapErr(s.log, fmt.Errorf("not implemented"), req)
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
		return nil, component.WrapErr(s.log, err)
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
		return nil, component.WrapErr(s.log, err)
	}
	resp, err := s.ListImages(ctx, &runtimeapi.ListImagesRequest{})
	if err != nil {
		return nil, component.WrapErr(s.log, err)
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
		return nil, component.WrapErr(s.log, err)
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
		return nil, component.WrapErr(s.log, err)
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
		return nil, component.WrapErr(s.log, err)
	}
	defer reader.Close()
	io.Copy(io.Discard, reader)

	status, err := s.ImageStatus(ctx, &runtimeapi.ImageStatusRequest{Image: req.GetImage()})
	if err != nil {
		return nil, component.WrapErr(s.log, err)
	}
	if status.Image == nil {
		return nil, component.WrapErr(s.log, fmt.Errorf("image %s not found after pull", ref))
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
		return nil, component.WrapErr(s.log, err)
	}
	return &runtimeapi.RemoveImageResponse{}, nil
}

// --- CSI Controller ---

// CreateVolume implements [csipb.ControllerServer].
func (s *Server) CreateVolume(ctx context.Context, req *csipb.CreateVolumeRequest) (*csipb.CreateVolumeResponse, error) {
	name := req.GetName()
	s.log.Info().Str("name", name).Msg("CreateVolume")

	params := req.GetParameters()
	volName, volLabels, err := s.backend.labels.NewBuilder(nil).
		WithType(labels.TypeVolume).WithUid(name).
		WithName(params["name"]).
		WithNamespace(params["namespace"]).
		Build()
	if err != nil {
		return nil, component.WrapErr(s.log, fmt.Errorf("build volume labels: %w", err))
	}
	resp, err := s.backend.client.VolumeCreate(ctx, dockervolume.CreateOptions{
		Name:   volName,
		Labels: volLabels,
	})
	if err != nil {
		return nil, component.WrapErr(s.log, fmt.Errorf("create volume %q: %w", name, err))
	}

	return &csipb.CreateVolumeResponse{
		Volume: &csipb.Volume{
			VolumeId:      name,
			VolumeContext: map[string]string{"mountpoint": resp.Mountpoint},
		},
	}, nil
}

// DeleteVolume implements [csipb.ControllerServer].
func (s *Server) DeleteVolume(ctx context.Context, req *csipb.DeleteVolumeRequest) (*csipb.DeleteVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	s.log.Info().Str("volume", volumeID).Msg("DeleteVolume")

	volName, _, err := s.backend.labels.NewBuilder(nil).WithType(labels.TypeVolume).WithUid(volumeID).Build()
	if err != nil {
		return nil, component.WrapErr(s.log, fmt.Errorf("build volume name: %w", err))
	}
	if err := s.backend.client.VolumeRemove(ctx, volName, true); err != nil {
		return nil, component.WrapErr(s.log, fmt.Errorf("delete volume %q: %w", volumeID, err))
	}

	return &csipb.DeleteVolumeResponse{}, nil
}

// ControllerGetCapabilities implements [csipb.ControllerServer].
func (s *Server) ControllerGetCapabilities(_ context.Context, _ *csipb.ControllerGetCapabilitiesRequest) (*csipb.ControllerGetCapabilitiesResponse, error) {
	caps := []csipb.ControllerServiceCapability_RPC_Type{
		csipb.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csipb.ControllerServiceCapability_RPC_LIST_VOLUMES,
		csipb.ControllerServiceCapability_RPC_GET_VOLUME,
		csipb.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
	}
	result := make([]*csipb.ControllerServiceCapability, len(caps))
	for i, c := range caps {
		result[i] = &csipb.ControllerServiceCapability{
			Type: &csipb.ControllerServiceCapability_Rpc{
				Rpc: &csipb.ControllerServiceCapability_RPC{Type: c},
			},
		}
	}
	return &csipb.ControllerGetCapabilitiesResponse{Capabilities: result}, nil
}

// ControllerPublishVolume implements [csipb.ControllerServer].
// No-op for local Docker volumes — they're already available on the single node.
func (s *Server) ControllerPublishVolume(_ context.Context, _ *csipb.ControllerPublishVolumeRequest) (*csipb.ControllerPublishVolumeResponse, error) {
	return &csipb.ControllerPublishVolumeResponse{}, nil
}

// ControllerUnpublishVolume implements [csipb.ControllerServer].
// No-op for local Docker volumes.
func (s *Server) ControllerUnpublishVolume(_ context.Context, _ *csipb.ControllerUnpublishVolumeRequest) (*csipb.ControllerUnpublishVolumeResponse, error) {
	return &csipb.ControllerUnpublishVolumeResponse{}, nil
}

// ValidateVolumeCapabilities implements [csipb.ControllerServer].
func (s *Server) ValidateVolumeCapabilities(ctx context.Context, req *csipb.ValidateVolumeCapabilitiesRequest) (*csipb.ValidateVolumeCapabilitiesResponse, error) {
	volumeID := req.GetVolumeId()
	volName, _, err := s.backend.labels.NewBuilder(nil).WithType(labels.TypeVolume).WithUid(volumeID).Build()
	if err != nil {
		return nil, component.WrapErr(s.log, fmt.Errorf("build volume name: %w", err))
	}
	if _, err := s.backend.client.VolumeInspect(ctx, volName); err != nil {
		return nil, status.Errorf(codes.NotFound, "volume %q not found: %v", volumeID, err)
	}
	return &csipb.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csipb.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.GetVolumeCapabilities(),
		},
	}, nil
}

// ListVolumes implements [csipb.ControllerServer].
func (s *Server) ListVolumes(ctx context.Context, _ *csipb.ListVolumesRequest) (*csipb.ListVolumesResponse, error) {
	resp, err := s.backend.client.VolumeList(ctx, dockervolume.ListOptions{
		Filters: filters.NewArgs(
			filters.Arg("label", s.backend.labels.ManagedByFilter()),
			filters.Arg("label", s.backend.labels.TypeFilter("volume")),
		),
	})
	if err != nil {
		return nil, component.WrapErr(s.log, fmt.Errorf("list volumes: %w", err))
	}
	volumeIDKey := s.backend.labels.Prefix("volume-id")
	entries := make([]*csipb.ListVolumesResponse_Entry, 0, len(resp.Volumes))
	for _, v := range resp.Volumes {
		entries = append(entries, &csipb.ListVolumesResponse_Entry{
			Volume: &csipb.Volume{
				VolumeId:      v.Labels[volumeIDKey],
				VolumeContext: map[string]string{"mountpoint": v.Mountpoint},
			},
		})
	}
	return &csipb.ListVolumesResponse{Entries: entries}, nil
}

// GetCapacity implements [csipb.ControllerServer].
func (s *Server) GetCapacity(_ context.Context, _ *csipb.GetCapacityRequest) (*csipb.GetCapacityResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "GetCapacity not supported")
}

// CreateSnapshot implements [csipb.ControllerServer].
func (s *Server) CreateSnapshot(_ context.Context, _ *csipb.CreateSnapshotRequest) (*csipb.CreateSnapshotResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "snapshots not supported")
}

// DeleteSnapshot implements [csipb.ControllerServer].
func (s *Server) DeleteSnapshot(_ context.Context, _ *csipb.DeleteSnapshotRequest) (*csipb.DeleteSnapshotResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "snapshots not supported")
}

// ListSnapshots implements [csipb.ControllerServer].
func (s *Server) ListSnapshots(_ context.Context, _ *csipb.ListSnapshotsRequest) (*csipb.ListSnapshotsResponse, error) {
	return &csipb.ListSnapshotsResponse{}, nil
}

// ControllerExpandVolume implements [csipb.ControllerServer].
// No-op — Docker volumes are not fixed-size.
func (s *Server) ControllerExpandVolume(_ context.Context, req *csipb.ControllerExpandVolumeRequest) (*csipb.ControllerExpandVolumeResponse, error) {
	return &csipb.ControllerExpandVolumeResponse{
		CapacityBytes:         req.GetCapacityRange().GetRequiredBytes(),
		NodeExpansionRequired: false,
	}, nil
}

// ControllerGetVolume implements [csipb.ControllerServer].
func (s *Server) ControllerGetVolume(ctx context.Context, req *csipb.ControllerGetVolumeRequest) (*csipb.ControllerGetVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	volName, _, err := s.backend.labels.NewBuilder(nil).WithType(labels.TypeVolume).WithUid(volumeID).Build()
	if err != nil {
		return nil, component.WrapErr(s.log, fmt.Errorf("build volume name: %w", err))
	}
	v, err := s.backend.client.VolumeInspect(ctx, volName)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "volume %q not found: %v", volumeID, err)
	}
	return &csipb.ControllerGetVolumeResponse{
		Volume: &csipb.Volume{
			VolumeId:      volumeID,
			VolumeContext: map[string]string{"mountpoint": v.Mountpoint},
		},
		Status: &csipb.ControllerGetVolumeResponse_VolumeStatus{},
	}, nil
}

// ControllerModifyVolume implements [csipb.ControllerServer].
// No-op — Docker volumes have no mutable properties.
func (s *Server) ControllerModifyVolume(_ context.Context, _ *csipb.ControllerModifyVolumeRequest) (*csipb.ControllerModifyVolumeResponse, error) {
	return &csipb.ControllerModifyVolumeResponse{}, nil
}

func newNetworkSpec(inspect *network.Inspect) *backend.NetworkSpec {
	if inspect == nil || inspect.IPAM.Config == nil || len(inspect.IPAM.Config) == 0 {
		return nil
	}
	cfg := inspect.IPAM.Config[0]
	_, network, err := net.ParseCIDR(cfg.Subnet)
	if err != nil {
		return nil
	}
	return &backend.NetworkSpec{
		Name:    inspect.Name,
		Type:    backend.NetworkBridge,
		Gateway: net.ParseIP(cfg.Gateway),
		Network: *network,
	}
}

// --- Network Provider ---

func (s *Server) DefaultNetwork(ctx context.Context) backend.NetworkSpec {
	var resp network.Inspect
	var err error

	resp, err = s.backend.client.NetworkInspect(ctx, "bridge", network.InspectOptions{Verbose: true})
	if err == nil {
		networkSpec := newNetworkSpec(&resp)
		if networkSpec != nil {
			return *networkSpec
		}
	}

	s.log.Warn().Err(err).Msg("failed to get bridge network, trying host network")
	resp, err = s.backend.client.NetworkInspect(ctx, "host", network.InspectOptions{Verbose: true})
	if err == nil {
		networkSpec := newNetworkSpec(&resp)
		if networkSpec != nil {
			return *networkSpec
		}
	}

	s.log.Warn().Err(err).Msg("failed to get host network, trying none network")
	resp, err = s.backend.client.NetworkInspect(ctx, "none", network.InspectOptions{Verbose: true})
	if err == nil {
		networkSpec := newNetworkSpec(&resp)
		if networkSpec != nil {
			return *networkSpec
		}
	}

	s.log.Warn().Err(err).Msg("failed to get default networks, returning empty network spec")
	return backend.NetworkSpec{}
}

func (s *Server) GetNetwork(ctx context.Context, name string) (*backend.NetworkSpec, error) {
	netName, _, err := s.backend.Labels().NewBuilder(nil).
		WithType(labels.TypeNetwork).
		WithName(name).
		Build()
	if err != nil {
		return nil, component.WrapErr(s.log, err)
	}

	resp, err := s.backend.client.NetworkInspect(ctx, netName, network.InspectOptions{Verbose: true})
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil, nil
		}
		return nil, component.WrapErr(s.log, err)
	}

	var networkType backend.NetworkType
	switch resp.Driver {
	case "bridge":
		networkType = backend.NetworkBridge
	case "host":
		networkType = backend.NetworkHost
	default:
		return nil, component.WrapErr(s.log, fmt.Errorf("unsupported network %s: %s", resp.Name, resp.Driver))
	}

	isDefault := resp.Options["com.docker.network.bridge.default_bridge"] == "true"
	managedBy := resp.Labels[s.backend.labels.ManagedByKey()]
	if !isDefault && managedBy != s.backend.labels.Name() {
		return nil, component.WrapErr(s.log, fmt.Errorf("network %q is not managed by this runtime (managedBy=%q)", resp.Name, managedBy))
	}

	var network net.IPNet
	var gateway net.IP

	if resp.IPAM.Config != nil {
		for _, cfg := range resp.IPAM.Config {
			if cfg.Subnet != "" && cfg.Gateway != "" {
				gateway = net.ParseIP(cfg.Gateway)
				if gateway == nil {
					s.log.Warn().Str("gateway", cfg.Gateway).Msgf("failed to parse gateway for network %q, skipping", name)
					continue
				}
				_, ipNet, err := net.ParseCIDR(cfg.Subnet)
				if err != nil {
					s.log.Warn().Err(err).Str("subnet", cfg.Subnet).Msgf("failed to parse subnet for network %q, skipping", name)
					continue
				}
				network = *ipNet
			}
		}
	}

	return &backend.NetworkSpec{
		Name:    resp.Name,
		Type:    networkType,
		Gateway: gateway,
		Network: network,
	}, nil
}

// CreateNetwork implements [backend.NetworkProvider]. Idempotent — returns
// the network ID if it already exists.
func (s *Server) CreateNetwork(ctx context.Context, name string, net *net.IPNet, gateway *net.IP) (backend.NetworkSpec, error) {
	existing, _ := s.GetNetwork(ctx, name)
	if existing != nil {
		return *existing, nil
	}

	netName, netLabels, err := s.backend.labels.NewBuilder(nil).
		WithType(labels.TypeNetwork).
		WithName(name).
		Build()
	if err != nil {
		return backend.NetworkSpec{}, component.WrapErr(s.log, err)
	}

	createOptions := network.CreateOptions{
		Driver: "bridge",
		Labels: netLabels,
		Options: map[string]string{
			"com.docker.network.bridge.enable_icc":           "true",
			"com.docker.network.bridge.enable_ip_masquerade": "true",
			"com.docker.network.bridge.host_binding_ipv4":    "0.0.0.0",
			"com.docker.network.driver.mtu":                  "65535",
			// // DEVNOTE: Docker 29 added network isolation by default.
			// //          Disabling it with nat-unprotected.
			// //          Ref: https://github.com/moby/moby/pull/48597
			// "com.docker.network.bridge.gateway_mode_ipv4": "nat-unprotected",
		},
	}

	if net != nil {
		ipamConfig := network.IPAMConfig{Subnet: net.String()}
		if gateway != nil {
			ipamConfig.Gateway = gateway.String()
		}
		createOptions.IPAM = &network.IPAM{
			Driver: "default",
			Config: []network.IPAMConfig{ipamConfig},
		}
	}

	_, err = s.backend.client.NetworkCreate(ctx, netName, createOptions)
	if err != nil {
		return backend.NetworkSpec{}, component.WrapErr(s.log, err)
	}

	// DEVNOTE: Using original name
	network, err := s.GetNetwork(ctx, name)
	if err != nil {
		return backend.NetworkSpec{}, component.WrapErr(s.log, err)
	} else if network == nil {
		return backend.NetworkSpec{}, component.WrapErr(s.log, fmt.Errorf("network %q not found after creation", name))
	}

	return *network, nil
}

// RemoveNetwork implements [backend.NetworkProvider].
func (s *Server) RemoveNetwork(ctx context.Context, name string) error {
	netName, _, err := s.backend.labels.NewBuilder(nil).
		WithType(labels.TypeNetwork).
		WithName(name).
		Build()
	if err != nil {
		return component.WrapErr(s.log, err)
	}
	return s.backend.client.NetworkRemove(ctx, netName)
}

// ConnectNetwork implements [backend.NetworkProvider].
func (s *Server) ConnectNetwork(ctx context.Context, net, containerID string, aliases []string) error {
	return s.backend.client.NetworkConnect(ctx, net, containerID, &network.EndpointSettings{
		Aliases: aliases,
	})
}

// DisconnectNetwork implements [backend.NetworkProvider].
func (s *Server) DisconnectNetwork(ctx context.Context, net, containerID string) error {
	return s.backend.client.NetworkDisconnect(ctx, net, containerID, false)
}

// matchLabels returns true if the container's labels (including the packed
// labels blob) satisfy every key=value pair in the selector.
func (s *Server) matchLabels(dockerLabels map[string]string, selector map[string]string) bool {
	if len(selector) == 0 {
		return true
	}
	unpacked := s.backend.labels.ExtractLabels(dockerLabels)
	for k, v := range selector {
		if unpacked[k] == v || dockerLabels[k] == v {
			continue
		}
		return false
	}
	return true
}

var (
	_ runtimeapi.ImageServiceServer   = &Server{}
	_ runtimeapi.RuntimeServiceServer = &Server{}
	_ csipb.ControllerServer          = &Server{}
	_ backend.NetworkProvider         = &Server{}
)
