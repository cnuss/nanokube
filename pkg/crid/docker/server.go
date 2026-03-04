package docker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cnuss/nanokube/pkg/crid/backend"
	"github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/api/types/strslice"
	"github.com/docker/docker/api/types/volume"
	dockertypes "github.com/docker/docker/api/types"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
	"google.golang.org/grpc"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	kubelettypes "k8s.io/kubelet/pkg/types"
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

var _ runtimeapi.ImageServiceServer = &Server{}
var _ runtimeapi.RuntimeServiceServer = &Server{}

// --- Label helpers ---

func (s *Server) name() string {
	return string(s.backend.Name())
}

func (s *Server) managedByFilter() string {
	return s.backend.labels.ManagedByFilter()
}

func (s *Server) typeFilter(t string) string {
	return s.backend.labels.TypeFilter(t)
}

func (s *Server) extractLabels(dockerLabels map[string]string) map[string]string {
	return s.backend.labels.ExtractLabels(dockerLabels)
}

func (s *Server) extractAnnotations(dockerLabels map[string]string) map[string]string {
	return s.backend.labels.ExtractAnnotations(dockerLabels)
}

func (s *Server) mounts() backend.MountLookup {
	return s.backend.Mounts
}

// --- Runtime info ---

func (s *Server) Version(ctx context.Context, req *runtimeapi.VersionRequest) (*runtimeapi.VersionResponse, error) {
	v, err := s.backend.client.ServerVersion(ctx)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.VersionResponse{
		Version:           "0.1.0",
		RuntimeName:       s.name(),
		RuntimeVersion:    v.Version,
		RuntimeApiVersion: "v1",
	}, nil
}

func (s *Server) Status(ctx context.Context, req *runtimeapi.StatusRequest) (*runtimeapi.StatusResponse, error) {
	info, err := s.backend.client.Info(ctx)
	if err != nil {
		return nil, err
	}
	resp := &runtimeapi.StatusResponse{
		Status: &runtimeapi.RuntimeStatus{
			Conditions: []*runtimeapi.RuntimeCondition{
				{Type: "RuntimeReady", Status: true, Reason: "DockerIsUp"},
				{Type: "NetworkReady", Status: true, Reason: "DockerIsUp"},
			},
		},
	}
	if req.GetVerbose() {
		resp.Info = map[string]string{
			"storageDriver": info.Driver,
			"serverVersion": info.ServerVersion,
		}
	}
	return resp, nil
}

func (s *Server) UpdateRuntimeConfig(ctx context.Context, req *runtimeapi.UpdateRuntimeConfigRequest) (*runtimeapi.UpdateRuntimeConfigResponse, error) {
	return &runtimeapi.UpdateRuntimeConfigResponse{}, nil
}

func (s *Server) RuntimeConfig(ctx context.Context, req *runtimeapi.RuntimeConfigRequest) (*runtimeapi.RuntimeConfigResponse, error) {
	return &runtimeapi.RuntimeConfigResponse{}, nil
}

func (s *Server) ListMetricDescriptors(ctx context.Context, req *runtimeapi.ListMetricDescriptorsRequest) (*runtimeapi.ListMetricDescriptorsResponse, error) {
	return &runtimeapi.ListMetricDescriptorsResponse{}, nil
}

func (s *Server) ListPodSandboxMetrics(ctx context.Context, req *runtimeapi.ListPodSandboxMetricsRequest) (*runtimeapi.ListPodSandboxMetricsResponse, error) {
	return &runtimeapi.ListPodSandboxMetricsResponse{}, nil
}

func (s *Server) CheckpointContainer(ctx context.Context, req *runtimeapi.CheckpointContainerRequest) (*runtimeapi.CheckpointContainerResponse, error) {
	logger.Warn().Str("container", req.GetContainerId()).Msg("CheckpointContainer not implemented")
	return nil, fmt.Errorf("checkpoint not supported")
}

func (s *Server) UpdatePodSandboxResources(ctx context.Context, req *runtimeapi.UpdatePodSandboxResourcesRequest) (*runtimeapi.UpdatePodSandboxResourcesResponse, error) {
	logger.Warn().Msg("UpdatePodSandboxResources not implemented")
	return &runtimeapi.UpdatePodSandboxResourcesResponse{}, nil
}

// --- Streaming (delegated to backend for gRPC URL generation) ---

func (s *Server) Exec(ctx context.Context, req *runtimeapi.ExecRequest) (*runtimeapi.ExecResponse, error) {
	// The streaming server handles Exec via the backend's streaming.Runtime.
	// This gRPC method is not called directly; the kubelet uses the streaming URL.
	return nil, fmt.Errorf("exec streaming not handled via gRPC")
}

func (s *Server) Attach(ctx context.Context, req *runtimeapi.AttachRequest) (*runtimeapi.AttachResponse, error) {
	return nil, fmt.Errorf("attach streaming not handled via gRPC")
}

func (s *Server) PortForward(ctx context.Context, req *runtimeapi.PortForwardRequest) (*runtimeapi.PortForwardResponse, error) {
	return nil, fmt.Errorf("port-forward streaming not handled via gRPC")
}

// --- Events ---

func (s *Server) GetContainerEvents(req *runtimeapi.GetEventsRequest, stream grpc.ServerStreamingServer[runtimeapi.ContainerEventResponse]) error {
	ctx := stream.Context()
	logger.Info().Msg("starting Docker event stream")

	msgCh, errCh := s.backend.client.Events(ctx, events.ListOptions{
		Filters: filters.NewArgs(
			filters.Arg("type", string(events.ContainerEventType)),
			filters.Arg("label", s.managedByFilter()),
		),
	})

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errCh:
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("docker event stream: %w", err)
		case msg := <-msgCh:
			ev := s.dockerEventToCRI(ctx, msg)
			if ev != nil {
				if err := stream.Send(ev); err != nil {
					return err
				}
			}
		}
	}
}

func (s *Server) dockerEventToCRI(ctx context.Context, msg events.Message) *runtimeapi.ContainerEventResponse {
	var evType runtimeapi.ContainerEventType
	switch msg.Action {
	case events.ActionCreate:
		evType = runtimeapi.ContainerEventType_CONTAINER_CREATED_EVENT
	case events.ActionStart:
		evType = runtimeapi.ContainerEventType_CONTAINER_STARTED_EVENT
	case events.ActionDie, events.ActionStop, events.ActionKill:
		evType = runtimeapi.ContainerEventType_CONTAINER_STOPPED_EVENT
	case events.ActionDestroy, events.ActionRemove:
		evType = runtimeapi.ContainerEventType_CONTAINER_DELETED_EVENT
	default:
		return nil
	}

	containerID := msg.Actor.ID
	sandboxID := msg.Actor.Attributes[s.backend.labels.Prefix("sandbox-id")]

	// Skip sandbox containers
	if msg.Actor.Attributes[s.backend.labels.Prefix("type")] == "sandbox" {
		return nil
	}

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
				if statusResp, err := s.ContainerStatus(ctx, &runtimeapi.ContainerStatusRequest{ContainerId: c.Id}); err == nil {
					containerStatuses = append(containerStatuses, statusResp.Status)
				}
			}
		}
	}

	logger.Debug().
		Str("container", containerID[:min(12, len(containerID))]).
		Str("action", string(msg.Action)).
		Int32("criEvent", int32(evType)).
		Msg("docker event -> CRI")

	return &runtimeapi.ContainerEventResponse{
		ContainerId:        containerID,
		ContainerEventType: evType,
		CreatedAt:          msg.TimeNano,
		PodSandboxStatus:   sandboxStatus,
		ContainersStatuses: containerStatuses,
	}
}

// --- Probe helpers ---

func (s *Server) runProbe(ctx context.Context, img string, cmd []string, hostCfg *container.HostConfig) ([]byte, error) {
	_, err := s.backend.client.ImageInspect(ctx, img)
	if err != nil {
		reader, pullErr := s.backend.client.ImagePull(ctx, img, image.PullOptions{})
		if pullErr != nil {
			return nil, fmt.Errorf("pull %s: %w", img, pullErr)
		}
		io.Copy(io.Discard, reader)
		reader.Close()
	}

	hostCfg.AutoRemove = true

	resp, err := s.backend.client.ContainerCreate(ctx,
		&container.Config{Image: img, Cmd: cmd, AttachStdout: true, AttachStderr: true},
		hostCfg, nil, nil, "")
	if err != nil {
		return nil, fmt.Errorf("create probe container: %w", err)
	}

	attach, err := s.backend.client.ContainerAttach(ctx, resp.ID, container.AttachOptions{
		Stream: true, Stdout: true, Stderr: true,
	})
	if err != nil {
		s.removeContainer(ctx, resp.ID)
		return nil, fmt.Errorf("attach probe container: %w", err)
	}
	defer attach.Close()

	if err := s.backend.client.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return nil, fmt.Errorf("start probe container: %w", err)
	}

	var stdout, stderr bytes.Buffer
	stdcopy.StdCopy(&stdout, &stderr, attach.Reader)
	return stdout.Bytes(), nil
}

// --- Sandbox operations ---

func (s *Server) RunPodSandbox(ctx context.Context, req *runtimeapi.RunPodSandboxRequest) (*runtimeapi.RunPodSandboxResponse, error) {
	config := req.GetConfig()
	dockerConfig, hostConfig, netConfig := s.toSandboxContainerConfig(config)
	name := sandboxContainerName(config)

	logger.Debug().Str("name", name).Str("uid", config.GetMetadata().GetUid()).Msg("CRI RunPodSandbox")

	if _, err := s.backend.client.ImageInspect(ctx, dockerConfig.Image); err != nil {
		logger.Info().Str("image", dockerConfig.Image).Msg("pulling sandbox image")
		reader, pullErr := s.backend.client.ImagePull(ctx, dockerConfig.Image, image.PullOptions{})
		if pullErr != nil {
			return nil, fmt.Errorf("pull sandbox image: %w", pullErr)
		}
		io.Copy(io.Discard, reader)
		reader.Close()
	}

	resp, err := s.backend.client.ContainerCreate(ctx, dockerConfig, hostConfig, netConfig, nil, name)
	if err != nil {
		return nil, fmt.Errorf("create sandbox: %w", err)
	}

	if err := s.backend.client.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		s.removeContainer(ctx, resp.ID)
		return nil, fmt.Errorf("start sandbox: %w", err)
	}

	logger.Debug().Str("id", resp.ID[:12]).Str("name", name).Msg("CRI sandbox started")
	return &runtimeapi.RunPodSandboxResponse{PodSandboxId: resp.ID}, nil
}

func (s *Server) StopPodSandbox(ctx context.Context, req *runtimeapi.StopPodSandboxRequest) (*runtimeapi.StopPodSandboxResponse, error) {
	podSandboxID := req.GetPodSandboxId()
	logger.Debug().Str("id", podSandboxID[:12]).Msg("CRI StopPodSandbox")

	// Stop all containers in this sandbox via ListContainers
	containers, err := s.ListContainers(ctx, &runtimeapi.ListContainersRequest{
		Filter: &runtimeapi.ContainerFilter{PodSandboxId: podSandboxID},
	})
	if err == nil {
		for _, c := range containers.Containers {
			s.StopContainer(ctx, &runtimeapi.StopContainerRequest{ContainerId: c.Id})
		}
	}

	timeout := 10
	if err := s.backend.client.ContainerStop(ctx, podSandboxID, container.StopOptions{Timeout: &timeout}); err != nil {
		if errdefs.IsNotFound(err) || errdefs.IsNotModified(err) {
			return &runtimeapi.StopPodSandboxResponse{}, nil
		}
		return nil, err
	}
	return &runtimeapi.StopPodSandboxResponse{}, nil
}

func (s *Server) RemovePodSandbox(ctx context.Context, req *runtimeapi.RemovePodSandboxRequest) (*runtimeapi.RemovePodSandboxResponse, error) {
	podSandboxID := req.GetPodSandboxId()
	logger.Debug().Str("id", podSandboxID[:12]).Msg("CRI RemovePodSandbox")

	// Capture pod UID before removing the sandbox container (for volume cleanup)
	var podUID string
	if inspect, err := s.backend.client.ContainerInspect(ctx, podSandboxID); err == nil {
		podUID = inspect.Config.Labels[kubelettypes.KubernetesPodUIDLabel]
	}

	// Remove all containers in this sandbox via ListContainers
	containers, err := s.ListContainers(ctx, &runtimeapi.ListContainersRequest{
		Filter: &runtimeapi.ContainerFilter{PodSandboxId: podSandboxID},
	})
	if err == nil {
		for _, c := range containers.Containers {
			s.removeContainer(ctx, c.Id)
		}
	}

	if err := s.removeContainer(ctx, podSandboxID); err != nil {
		return nil, err
	}

	// Clean up volumes associated with this pod
	if podUID != "" {
		volumes, listErr := s.listVolumes(ctx, map[string]string{kubelettypes.KubernetesPodUIDLabel: podUID})
		if listErr == nil {
			for _, v := range volumes {
				if rmErr := s.removeVolume(ctx, v); rmErr != nil {
					logger.Warn().Err(rmErr).Str("volume", v).Msg("failed to remove pod volume")
				}
			}
		}
	}

	return &runtimeapi.RemovePodSandboxResponse{}, nil
}

func (s *Server) PodSandboxStatus(ctx context.Context, req *runtimeapi.PodSandboxStatusRequest) (*runtimeapi.PodSandboxStatusResponse, error) {
	podSandboxID := req.GetPodSandboxId()
	inspect, err := s.backend.client.ContainerInspect(ctx, podSandboxID)
	if err != nil {
		return nil, err
	}

	createdAt, _ := time.Parse(time.RFC3339Nano, inspect.Created)
	state := dockerStateToPodState(inspect.State.Running)

	metadata := &runtimeapi.PodSandboxMetadata{
		Name:      inspect.Config.Labels[kubelettypes.KubernetesPodNameLabel],
		Uid:       inspect.Config.Labels[kubelettypes.KubernetesPodUIDLabel],
		Namespace: inspect.Config.Labels[kubelettypes.KubernetesPodNamespaceLabel],
	}

	status := &runtimeapi.PodSandboxStatus{
		Id:        podSandboxID,
		Metadata:  metadata,
		State:     state,
		CreatedAt: createdAt.UnixNano(),
		Network: &runtimeapi.PodSandboxNetworkStatus{
			Ip: getIPFromInspect(inspect),
		},
		Labels:      s.extractLabels(inspect.Config.Labels),
		Annotations: s.extractAnnotations(inspect.Config.Labels),
	}

	resp := &runtimeapi.PodSandboxStatusResponse{Status: status}
	if req.GetVerbose() {
		resp.Info = map[string]string{
			"pid": fmt.Sprintf("%d", inspect.State.Pid),
		}
	}
	return resp, nil
}

func (s *Server) ListPodSandbox(ctx context.Context, req *runtimeapi.ListPodSandboxRequest) (*runtimeapi.ListPodSandboxResponse, error) {
	f := filters.NewArgs()
	f.Add("label", s.typeFilter("sandbox"))
	f.Add("label", s.managedByFilter())

	filter := req.GetFilter()
	if filter != nil {
		if filter.Id != "" {
			f.Add("id", filter.Id)
		}
		if filter.State != nil {
			if filter.State.State == runtimeapi.PodSandboxState_SANDBOX_READY {
				f.Add("status", "running")
			} else {
				f.Add("status", "exited")
			}
		}
		for k, v := range filter.GetLabelSelector() {
			f.Add("label", k+"="+v)
		}
	}

	containers, err := s.backend.client.ContainerList(ctx, container.ListOptions{
		All:     true,
		Filters: f,
	})
	if err != nil {
		return nil, err
	}

	var result []*runtimeapi.PodSandbox
	for _, c := range containers {
		createdAt := time.Unix(c.Created, 0)
		state := dockerStateToPodState(c.State == "running")
		result = append(result, &runtimeapi.PodSandbox{
			Id: c.ID,
			Metadata: &runtimeapi.PodSandboxMetadata{
				Name:      c.Labels[kubelettypes.KubernetesPodNameLabel],
				Uid:       c.Labels[kubelettypes.KubernetesPodUIDLabel],
				Namespace: c.Labels[kubelettypes.KubernetesPodNamespaceLabel],
			},
			State:       state,
			CreatedAt:   createdAt.UnixNano(),
			Labels:      s.extractLabels(c.Labels),
			Annotations: s.extractAnnotations(c.Labels),
		})
		logger.Debug().Str("id", c.ID[:12]).Str("uid", c.Labels[kubelettypes.KubernetesPodUIDLabel]).Str("name", c.Labels[kubelettypes.KubernetesPodNameLabel]).Str("state", c.State).Msg("CRI ListPodSandbox entry")
	}
	logger.Debug().Int("count", len(result)).Msg("CRI ListPodSandbox")
	return &runtimeapi.ListPodSandboxResponse{Items: result}, nil
}

// --- Container operations ---

func (s *Server) CreateContainer(ctx context.Context, req *runtimeapi.CreateContainerRequest) (*runtimeapi.CreateContainerResponse, error) {
	podSandboxID := req.GetPodSandboxId()
	config := req.GetConfig()
	sandboxConfig := req.GetSandboxConfig()
	dockerConfig, hostConfig := s.toContainerConfig(config, podSandboxID, sandboxConfig)
	name := containerName(sandboxConfig, config)

	logger.Debug().Str("name", name).Str("image", config.GetImage().GetImage()).Str("sandbox", podSandboxID[:12]).Msg("CRI CreateContainer")

	// Store the full CRI log path as a label for symlink creation on start
	if logPath := config.GetLogPath(); logPath != "" {
		logDir := sandboxConfig.GetLogDirectory()
		if logDir != "" {
			dockerConfig.Labels[s.backend.labels.Prefix("container.logPath")] = filepath.Join(logDir, logPath)
		}
	}

	for _, m := range config.GetMounts() {
		logger.Debug().Str("host", m.GetHostPath()).Str("container", m.GetContainerPath()).Bool("ro", m.GetReadonly()).Msg("CRI mount")
	}

	resp, err := s.backend.client.ContainerCreate(ctx, dockerConfig, hostConfig, nil, nil, name)
	if err != nil {
		return nil, fmt.Errorf("create container: %w", err)
	}

	logger.Debug().Str("id", resp.ID[:12]).Str("name", name).Msg("CRI container created")
	return &runtimeapi.CreateContainerResponse{ContainerId: resp.ID}, nil
}

func (s *Server) StartContainer(ctx context.Context, req *runtimeapi.StartContainerRequest) (*runtimeapi.StartContainerResponse, error) {
	containerID := req.GetContainerId()
	logger.Debug().Str("id", containerID[:12]).Msg("CRI StartContainer")
	if err := s.backend.client.ContainerStart(ctx, containerID, container.StartOptions{}); err != nil {
		s.removeContainer(ctx, containerID)
		return nil, fmt.Errorf("start container: %w", err)
	}
	// Start CRI log writer if a log path was configured
	inspect, err := s.backend.client.ContainerInspect(ctx, containerID)
	if err != nil {
		return &runtimeapi.StartContainerResponse{}, nil
	}
	logPathKey := s.backend.labels.Prefix("container.logPath")
	if criLogPath := inspect.Config.Labels[logPathKey]; criLogPath != "" {
		s.startLogWriter(containerID, criLogPath)
	}
	return &runtimeapi.StartContainerResponse{}, nil
}

func (s *Server) startLogWriter(containerID, logPath string) {
	os.MkdirAll(filepath.Dir(logPath), 0755)
	if f, err := os.Create(logPath); err == nil {
		f.Close()
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.logMu.Lock()
	if old, ok := s.logWriters[containerID]; ok {
		old()
	}
	s.logWriters[containerID] = cancel
	s.logMu.Unlock()
	go s.writeCRILog(ctx, containerID, logPath)
}

func (s *Server) stopLogWriter(containerID string) {
	s.logMu.Lock()
	if cancel, ok := s.logWriters[containerID]; ok {
		cancel()
		delete(s.logWriters, containerID)
	}
	s.logMu.Unlock()
}

func (s *Server) writeCRILog(ctx context.Context, containerID, logPath string) {
	defer s.stopLogWriter(containerID)
	reader, err := s.backend.client.ContainerLogs(ctx, containerID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
		Timestamps: true,
	})
	if err != nil {
		return
	}
	defer reader.Close()

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return
	}
	defer f.Close()

	hdr := make([]byte, 8)
	for {
		if _, err := io.ReadFull(reader, hdr); err != nil {
			return
		}
		stream := "stdout"
		if hdr[0] == 2 {
			stream = "stderr"
		}
		size := int(hdr[4])<<24 | int(hdr[5])<<16 | int(hdr[6])<<8 | int(hdr[7])
		if size <= 0 {
			continue
		}
		payload := make([]byte, size)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return
		}
		line := string(payload)
		ts := time.Now().Format(time.RFC3339Nano)
		msg := line
		if idx := strings.IndexByte(line, ' '); idx > 0 {
			ts = line[:idx]
			msg = line[idx+1:]
		}
		if !strings.HasSuffix(msg, "\n") {
			msg += "\n"
		}
		fmt.Fprintf(f, "%s %s F %s", ts, stream, msg)
		f.Sync()
	}
}

func (s *Server) StopContainer(ctx context.Context, req *runtimeapi.StopContainerRequest) (*runtimeapi.StopContainerResponse, error) {
	containerID := req.GetContainerId()
	timeout := int(req.GetTimeout())
	logger.Debug().Str("id", containerID[:12]).Int("timeout", timeout).Msg("CRI StopContainer")
	if err := s.backend.client.ContainerStop(ctx, containerID, container.StopOptions{Timeout: &timeout}); err != nil {
		if errdefs.IsNotFound(err) || errdefs.IsNotModified(err) {
			return &runtimeapi.StopContainerResponse{}, nil
		}
		return nil, err
	}
	return &runtimeapi.StopContainerResponse{}, nil
}

func (s *Server) RemoveContainer(ctx context.Context, req *runtimeapi.RemoveContainerRequest) (*runtimeapi.RemoveContainerResponse, error) {
	if err := s.removeContainer(ctx, req.GetContainerId()); err != nil {
		return nil, err
	}
	return &runtimeapi.RemoveContainerResponse{}, nil
}

func (s *Server) removeContainer(ctx context.Context, containerID string) error {
	logger.Debug().Str("id", containerID[:12]).Msg("CRI RemoveContainer")
	s.stopLogWriter(containerID)
	t := 0
	if err := s.backend.client.ContainerStop(ctx, containerID, container.StopOptions{Timeout: &t}); err != nil {
		if !errdefs.IsNotFound(err) && !errdefs.IsNotModified(err) {
			logger.Warn().Str("id", containerID[:12]).Err(err).Msg("failed to stop container before removal")
		}
	}
	if err := s.backend.client.ContainerRemove(ctx, containerID, container.RemoveOptions{}); err != nil {
		if errdefs.IsNotFound(err) {
			return nil
		}
		logger.Error().Str("id", containerID[:12]).Err(err).Msg("failed to remove container")
		return err
	}
	return nil
}

func (s *Server) ListContainers(ctx context.Context, req *runtimeapi.ListContainersRequest) (*runtimeapi.ListContainersResponse, error) {
	f := filters.NewArgs()
	f.Add("label", s.typeFilter("container"))
	f.Add("label", s.managedByFilter())

	filter := req.GetFilter()
	if filter != nil {
		if filter.Id != "" {
			f.Add("id", filter.Id)
		}
		if filter.PodSandboxId != "" {
			f.Add("label", s.backend.labels.Prefix("sandbox-id")+"="+filter.PodSandboxId)
		}
		if filter.State != nil {
			switch filter.State.State {
			case runtimeapi.ContainerState_CONTAINER_CREATED:
				f.Add("status", "created")
			case runtimeapi.ContainerState_CONTAINER_RUNNING:
				f.Add("status", "running")
			case runtimeapi.ContainerState_CONTAINER_EXITED:
				f.Add("status", "exited")
			}
		}
		for k, v := range filter.GetLabelSelector() {
			f.Add("label", k+"="+v)
		}
	}

	containers, err := s.backend.client.ContainerList(ctx, container.ListOptions{
		All:     true,
		Filters: f,
	})
	if err != nil {
		return nil, err
	}

	var result []*runtimeapi.Container
	for _, c := range containers {
		createdAt := time.Unix(c.Created, 0)
		state := dockerStateToContainerState(c.State)
		attempt, _ := strconv.ParseUint(c.Labels[s.backend.labels.Prefix("container.attempt")], 10, 32)
		result = append(result, &runtimeapi.Container{
			Id:           c.ID,
			PodSandboxId: c.Labels[s.backend.labels.Prefix("sandbox-id")],
			Metadata: &runtimeapi.ContainerMetadata{
				Name:    c.Labels[kubelettypes.KubernetesContainerNameLabel],
				Attempt: uint32(attempt),
			},
			Image: &runtimeapi.ImageSpec{
				Image: c.Image,
			},
			ImageRef:    c.ImageID,
			State:       state,
			CreatedAt:   createdAt.UnixNano(),
			Labels:      s.extractLabels(c.Labels),
			Annotations: s.extractAnnotations(c.Labels),
		})
	}
	if filter != nil && filter.PodSandboxId != "" {
		logger.Debug().Int("count", len(result)).Str("sandbox", filter.PodSandboxId[:12]).Msg("CRI ListContainers")
	} else {
		logger.Debug().Int("count", len(result)).Msg("CRI ListContainers")
	}
	return &runtimeapi.ListContainersResponse{Containers: result}, nil
}

func (s *Server) ContainerStatus(ctx context.Context, req *runtimeapi.ContainerStatusRequest) (*runtimeapi.ContainerStatusResponse, error) {
	containerID := req.GetContainerId()
	inspect, err := s.backend.client.ContainerInspect(ctx, containerID)
	if err != nil {
		return nil, err
	}

	createdAt, _ := time.Parse(time.RFC3339Nano, inspect.Created)
	state := dockerStateToContainerState(inspect.State.Status)
	logger.Debug().Str("id", containerID[:12]).Str("dockerState", inspect.State.Status).Int32("criState", int32(state)).Msg("CRI ContainerStatus")

	attempt, _ := strconv.ParseUint(inspect.Config.Labels[s.backend.labels.Prefix("container.attempt")], 10, 32)
	status := &runtimeapi.ContainerStatus{
		Id: containerID,
		Metadata: &runtimeapi.ContainerMetadata{
			Name:    inspect.Config.Labels[kubelettypes.KubernetesContainerNameLabel],
			Attempt: uint32(attempt),
		},
		State:     state,
		CreatedAt: createdAt.UnixNano(),
		Image: &runtimeapi.ImageSpec{
			Image: inspect.Config.Image,
		},
		ImageRef:    inspect.Image,
		Labels:      s.extractLabels(inspect.Config.Labels),
		Annotations: s.extractAnnotations(inspect.Config.Labels),
	}
	logPathKey := s.backend.labels.Prefix("container.logPath")
	if criLogPath := inspect.Config.Labels[logPathKey]; criLogPath != "" {
		status.LogPath = criLogPath
	} else {
		status.LogPath = inspect.LogPath
	}

	if inspect.State.StartedAt != "" && inspect.State.StartedAt != "0001-01-01T00:00:00Z" {
		startedAt, _ := time.Parse(time.RFC3339Nano, inspect.State.StartedAt)
		status.StartedAt = startedAt.UnixNano()
	}
	if inspect.State.FinishedAt != "" && inspect.State.FinishedAt != "0001-01-01T00:00:00Z" {
		finishedAt, _ := time.Parse(time.RFC3339Nano, inspect.State.FinishedAt)
		status.FinishedAt = finishedAt.UnixNano()
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

func (s *Server) UpdateContainerResources(ctx context.Context, req *runtimeapi.UpdateContainerResourcesRequest) (*runtimeapi.UpdateContainerResourcesResponse, error) {
	updateConfig := container.UpdateConfig{}
	if linux := req.GetLinux(); linux != nil {
		updateConfig.Resources.CPUShares = linux.GetCpuShares()
		updateConfig.Resources.Memory = linux.GetMemoryLimitInBytes()
		updateConfig.Resources.CPUQuota = linux.GetCpuQuota()
		updateConfig.Resources.CPUPeriod = linux.GetCpuPeriod()
	}
	_, err := s.backend.client.ContainerUpdate(ctx, req.GetContainerId(), updateConfig)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.UpdateContainerResourcesResponse{}, nil
}

func (s *Server) ReopenContainerLog(ctx context.Context, req *runtimeapi.ReopenContainerLogRequest) (*runtimeapi.ReopenContainerLogResponse, error) {
	containerID := req.GetContainerId()
	inspect, err := s.backend.client.ContainerInspect(ctx, containerID)
	if err != nil {
		return nil, err
	}
	logPathKey := s.backend.labels.Prefix("container.logPath")
	criLogPath := inspect.Config.Labels[logPathKey]
	if criLogPath == "" {
		return &runtimeapi.ReopenContainerLogResponse{}, nil
	}
	s.stopLogWriter(containerID)
	s.startLogWriter(containerID, criLogPath)
	return &runtimeapi.ReopenContainerLogResponse{}, nil
}

func (s *Server) ExecSync(ctx context.Context, req *runtimeapi.ExecSyncRequest) (*runtimeapi.ExecSyncResponse, error) {
	containerID := req.GetContainerId()
	cmd := req.GetCmd()
	timeout := time.Duration(req.GetTimeout()) * time.Second

	execConfig := container.ExecOptions{
		Cmd:          cmd,
		AttachStdout: true,
		AttachStderr: true,
	}

	exec, err := s.backend.client.ContainerExecCreate(ctx, containerID, execConfig)
	if err != nil {
		return nil, err
	}

	resp, err := s.backend.client.ContainerExecAttach(ctx, exec.ID, container.ExecAttachOptions{})
	if err != nil {
		return nil, err
	}
	defer resp.Close()

	var stdout, stderr bytes.Buffer

	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	done := make(chan error, 1)
	go func() {
		_, err := stdcopy.StdCopy(&stdout, &stderr, resp.Reader)
		done <- err
	}()

	select {
	case <-ctx.Done():
		inspectResp, err := s.backend.client.ContainerExecInspect(context.Background(), exec.ID)
		if err == nil && inspectResp.Pid > 0 {
			s.runProbe(context.Background(), "busybox",
				[]string{"kill", "-9", fmt.Sprintf("%d", inspectResp.Pid)},
				&container.HostConfig{
					Privileged: true,
					PidMode:    "host",
				})
		}
		return &runtimeapi.ExecSyncResponse{
			Stdout:   stdout.Bytes(),
			Stderr:   stderr.Bytes(),
			ExitCode: -1,
		}, fmt.Errorf("exec timeout")
	case err := <-done:
		if err != nil {
			return nil, err
		}
	}

	inspectResp, err := s.backend.client.ContainerExecInspect(ctx, exec.ID)
	if err != nil {
		return &runtimeapi.ExecSyncResponse{
			Stdout: stdout.Bytes(),
			Stderr: stderr.Bytes(),
		}, nil
	}
	return &runtimeapi.ExecSyncResponse{
		Stdout:   stdout.Bytes(),
		Stderr:   stderr.Bytes(),
		ExitCode: int32(inspectResp.ExitCode),
	}, nil
}

// --- Stats ---

func (s *Server) ContainerStats(ctx context.Context, req *runtimeapi.ContainerStatsRequest) (*runtimeapi.ContainerStatsResponse, error) {
	containerID := req.GetContainerId()
	stats, err := s.containerStats(ctx, containerID)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.ContainerStatsResponse{Stats: stats}, nil
}

func (s *Server) containerStats(ctx context.Context, containerID string) (*runtimeapi.ContainerStats, error) {
	resp, err := s.backend.client.ContainerStatsOneShot(ctx, containerID)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var stats container.StatsResponse
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return nil, fmt.Errorf("decode docker stats: %w", err)
	}

	inspect, err := s.backend.client.ContainerInspect(ctx, containerID)
	if err != nil {
		return nil, err
	}

	createdAt, _ := time.Parse(time.RFC3339Nano, inspect.Created)
	ts := stats.Read.UnixNano()
	if ts <= 0 {
		ts = createdAt.UnixNano()
	}

	cpuTotal := stats.CPUStats.CPUUsage.TotalUsage
	memUsage := stats.MemoryStats.Usage
	inactiveFile := stats.MemoryStats.Stats["inactive_file"]
	workingSet := memUsage - inactiveFile
	if inactiveFile > memUsage {
		workingSet = 0
	}
	rssBytes := stats.MemoryStats.Stats["rss"]

	return &runtimeapi.ContainerStats{
		Attributes: &runtimeapi.ContainerAttributes{
			Id: containerID,
			Metadata: &runtimeapi.ContainerMetadata{
				Name: inspect.Config.Labels[kubelettypes.KubernetesContainerNameLabel],
			},
			Labels:      s.extractLabels(inspect.Config.Labels),
			Annotations: s.extractAnnotations(inspect.Config.Labels),
		},
		Cpu: &runtimeapi.CpuUsage{
			Timestamp:            ts,
			UsageCoreNanoSeconds: &runtimeapi.UInt64Value{Value: cpuTotal},
		},
		Memory: &runtimeapi.MemoryUsage{
			Timestamp:       ts,
			WorkingSetBytes: &runtimeapi.UInt64Value{Value: workingSet},
			UsageBytes:      &runtimeapi.UInt64Value{Value: memUsage},
			RssBytes:        &runtimeapi.UInt64Value{Value: rssBytes},
		},
		WritableLayer: &runtimeapi.FilesystemUsage{
			Timestamp: createdAt.UnixNano(),
		},
	}, nil
}

func (s *Server) ListContainerStats(ctx context.Context, req *runtimeapi.ListContainerStatsRequest) (*runtimeapi.ListContainerStatsResponse, error) {
	filter := req.GetFilter()
	containers, err := s.ListContainers(ctx, &runtimeapi.ListContainersRequest{
		Filter: &runtimeapi.ContainerFilter{
			Id:            filter.GetId(),
			PodSandboxId:  filter.GetPodSandboxId(),
			LabelSelector: filter.GetLabelSelector(),
		},
	})
	if err != nil {
		return nil, err
	}
	var stats []*runtimeapi.ContainerStats
	for _, c := range containers.Containers {
		st, err := s.containerStats(ctx, c.Id)
		if err != nil {
			continue
		}
		stats = append(stats, st)
	}
	return &runtimeapi.ListContainerStatsResponse{Stats: stats}, nil
}

func (s *Server) PodSandboxStats(ctx context.Context, req *runtimeapi.PodSandboxStatsRequest) (*runtimeapi.PodSandboxStatsResponse, error) {
	return &runtimeapi.PodSandboxStatsResponse{
		Stats: &runtimeapi.PodSandboxStats{
			Attributes: &runtimeapi.PodSandboxAttributes{
				Id: req.GetPodSandboxId(),
			},
		},
	}, nil
}

func (s *Server) ListPodSandboxStats(ctx context.Context, req *runtimeapi.ListPodSandboxStatsRequest) (*runtimeapi.ListPodSandboxStatsResponse, error) {
	filter := req.GetFilter()
	sandboxes, err := s.ListPodSandbox(ctx, &runtimeapi.ListPodSandboxRequest{
		Filter: &runtimeapi.PodSandboxFilter{Id: filter.GetId()},
	})
	if err != nil {
		return nil, err
	}
	var stats []*runtimeapi.PodSandboxStats
	for _, sb := range sandboxes.Items {
		stats = append(stats, &runtimeapi.PodSandboxStats{
			Attributes: &runtimeapi.PodSandboxAttributes{Id: sb.Id},
		})
	}
	return &runtimeapi.ListPodSandboxStatsResponse{Stats: stats}, nil
}

// --- Image operations ---

func (s *Server) ListImages(ctx context.Context, req *runtimeapi.ListImagesRequest) (*runtimeapi.ListImagesResponse, error) {
	images, err := s.backend.client.ImageList(ctx, image.ListOptions{})
	if err != nil {
		return nil, err
	}
	var result []*runtimeapi.Image
	for _, img := range images {
		result = append(result, dockerImageToRuntimeImage(img))
	}
	return &runtimeapi.ListImagesResponse{Images: result}, nil
}

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

func (s *Server) PullImage(ctx context.Context, req *runtimeapi.PullImageRequest) (*runtimeapi.PullImageResponse, error) {
	ref := req.GetImage().GetImage()

	reader, err := s.backend.client.ImagePull(ctx, ref, image.PullOptions{})
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	decoder := json.NewDecoder(reader)
	for {
		var msg map[string]any
		if err := decoder.Decode(&msg); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}

	inspect, err := s.backend.client.ImageInspect(ctx, ref)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.PullImageResponse{ImageRef: inspect.ID}, nil
}

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

func (s *Server) ImageFsInfo(ctx context.Context, req *runtimeapi.ImageFsInfoRequest) (*runtimeapi.ImageFsInfoResponse, error) {
	usage, err := s.backend.client.DiskUsage(ctx, dockertypes.DiskUsageOptions{})
	if err != nil {
		return &runtimeapi.ImageFsInfoResponse{}, nil
	}

	var totalSize int64
	for _, img := range usage.Images {
		totalSize += img.Size
	}

	return &runtimeapi.ImageFsInfoResponse{
		ImageFilesystems: []*runtimeapi.FilesystemUsage{
			{
				FsId:      &runtimeapi.FilesystemIdentifier{Mountpoint: "/var/lib/docker"},
				UsedBytes: &runtimeapi.UInt64Value{Value: uint64(totalSize)},
			},
		},
	}, nil
}

// --- Volume helpers (not gRPC, used internally) ---

func (s *Server) createVolume(ctx context.Context, name string) (string, error) {
	resp, err := s.backend.client.VolumeCreate(ctx, volume.CreateOptions{
		Name: s.name() + "-" + name,
		Labels: map[string]string{
			s.backend.labels.Prefix("managed-by"):  s.name(),
			s.backend.labels.Prefix("volume.name"): name,
		},
	})
	if err != nil {
		return "", err
	}
	logger.Debug().Str("name", resp.Name).Msg("created volume")
	return resp.Name, nil
}

func (s *Server) removeVolume(ctx context.Context, name string) error {
	err := s.backend.client.VolumeRemove(ctx, name, true)
	if err != nil {
		return err
	}
	logger.Debug().Str("name", name).Msg("removed volume")
	return nil
}

func (s *Server) listVolumes(ctx context.Context, labelFilter map[string]string) ([]string, error) {
	f := filters.NewArgs()
	f.Add("label", s.managedByFilter())
	for k, v := range labelFilter {
		f.Add("label", k+"="+v)
	}

	resp, err := s.backend.client.VolumeList(ctx, volume.ListOptions{Filters: f})
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(resp.Volumes))
	for _, v := range resp.Volumes {
		names = append(names, v.Name)
	}
	return names, nil
}

// --- Conversion helpers ---

func (s *Server) sandboxLabels(config *runtimeapi.PodSandboxConfig) map[string]string {
	labels := map[string]string{
		s.backend.labels.Prefix("type"):       "sandbox",
		s.backend.labels.Prefix("managed-by"): s.name(),
		kubelettypes.KubernetesPodNameLabel:      config.GetMetadata().GetName(),
		kubelettypes.KubernetesPodUIDLabel:       config.GetMetadata().GetUid(),
	}
	if ns := config.GetMetadata().GetNamespace(); ns != "" {
		labels[kubelettypes.KubernetesPodNamespaceLabel] = ns
	}
	for k, v := range config.GetLabels() {
		labels[k] = v
	}
	s.backend.labels.Annotate(labels, config.GetAnnotations())
	return labels
}

func (s *Server) containerLabels(sandboxID string, config *runtimeapi.ContainerConfig, sandboxConfig *runtimeapi.PodSandboxConfig) map[string]string {
	labels := map[string]string{
		s.backend.labels.Prefix("type"):              "container",
		s.backend.labels.Prefix("managed-by"):        s.name(),
		s.backend.labels.Prefix("sandbox-id"):        sandboxID,
		s.backend.labels.Prefix("container.attempt"):  fmt.Sprintf("%d", config.GetMetadata().GetAttempt()),
		kubelettypes.KubernetesContainerNameLabel:       config.GetMetadata().GetName(),
		kubelettypes.KubernetesPodNameLabel:             sandboxConfig.GetMetadata().GetName(),
		kubelettypes.KubernetesPodNamespaceLabel:        sandboxConfig.GetMetadata().GetNamespace(),
		kubelettypes.KubernetesPodUIDLabel:              sandboxConfig.GetMetadata().GetUid(),
	}
	for k, v := range config.GetLabels() {
		labels[k] = v
	}
	s.backend.labels.Annotate(labels, config.GetAnnotations())
	return labels
}

func containerName(sandboxConfig *runtimeapi.PodSandboxConfig, config *runtimeapi.ContainerConfig) string {
	return fmt.Sprintf("k8s_%s_%s_%s_%d",
		config.GetMetadata().GetName(),
		sandboxConfig.GetMetadata().GetName(),
		sandboxConfig.GetMetadata().GetNamespace(),
		config.GetMetadata().GetAttempt(),
	)
}

func sandboxContainerName(config *runtimeapi.PodSandboxConfig) string {
	return fmt.Sprintf("k8s_POD_%s_%s_%s",
		config.GetMetadata().GetName(),
		config.GetMetadata().GetNamespace(),
		config.GetMetadata().GetUid(),
	)
}

func (s *Server) toContainerConfig(config *runtimeapi.ContainerConfig, sandboxID string, sandboxConfig *runtimeapi.PodSandboxConfig) (*container.Config, *container.HostConfig) {
	img := config.GetImage().GetImage()

	envs := make([]string, 0, len(config.GetEnvs()))
	for _, kv := range config.GetEnvs() {
		envs = append(envs, kv.GetKey()+"="+kv.GetValue())
	}

	dockerConfig := &container.Config{
		Image:      img,
		Entrypoint: strslice.StrSlice(config.GetCommand()),
		Cmd:        strslice.StrSlice(config.GetArgs()),
		Env:        envs,
		WorkingDir: config.GetWorkingDir(),
		Labels:     s.containerLabels(sandboxID, config, sandboxConfig),
		StdinOnce:  config.GetStdinOnce(),
		OpenStdin:  config.GetStdin(),
		Tty:        config.GetTty(),
	}

	hostConfig := &container.HostConfig{
		NetworkMode: container.NetworkMode("container:" + sandboxID),
		IpcMode:     container.IpcMode("container:" + sandboxID),
		PidMode:     container.PidMode("container:" + sandboxID),
	}

	mounts := s.mounts()
	for _, m := range config.GetMounts() {
		if mounts != nil {
			if opts, ok := mounts.GetTmpfs(m.GetHostPath()); ok {
				if hostConfig.Tmpfs == nil {
					hostConfig.Tmpfs = make(map[string]string)
				}
				hostConfig.Tmpfs[m.GetContainerPath()] = opts
				continue
			}
			if volName, ok := mounts.GetVolume(m.GetHostPath()); ok {
				bind := s.name() + "-" + volName + ":" + m.GetContainerPath()
				if m.GetReadonly() {
					bind += ":ro"
				}
				hostConfig.Binds = append(hostConfig.Binds, bind)
				continue
			}
		}
		bind := m.GetHostPath() + ":" + m.GetContainerPath()
		if m.GetReadonly() {
			bind += ":ro"
		}
		hostConfig.Binds = append(hostConfig.Binds, bind)
	}

	if linux := config.GetLinux(); linux != nil {
		if res := linux.GetResources(); res != nil {
			hostConfig.Resources.CPUShares = res.GetCpuShares()
			hostConfig.Resources.Memory = res.GetMemoryLimitInBytes()
			hostConfig.Resources.CPUQuota = res.GetCpuQuota()
			hostConfig.Resources.CPUPeriod = res.GetCpuPeriod()
			hostConfig.Resources.OomKillDisable = boolPtr(res.GetOomScoreAdj() == -998)
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

	if logPath := config.GetLogPath(); logPath != "" {
		hostConfig.LogConfig = container.LogConfig{
			Type: "json-file",
			Config: map[string]string{
				"max-size": "10m",
				"max-file": "3",
			},
		}
	}

	return dockerConfig, hostConfig
}

func (s *Server) toSandboxContainerConfig(config *runtimeapi.PodSandboxConfig) (*container.Config, *container.HostConfig, *network.NetworkingConfig) {
	dockerConfig := &container.Config{
		Image:    defaultPauseImage,
		Hostname: config.GetHostname(),
		Labels:   s.sandboxLabels(config),
	}

	hostConfig := &container.HostConfig{
		IpcMode: container.IpcMode("shareable"),
	}

	if dns := config.GetDnsConfig(); dns != nil {
		hostConfig.DNS = dns.GetServers()
		hostConfig.DNSSearch = dns.GetSearches()
		hostConfig.DNSOptions = dns.GetOptions()
	}

	if len(config.GetPortMappings()) > 0 {
		logger.Info().Interface("portMappings", config.GetPortMappings()).Msg("CRI port mappings input")
		dockerConfig.ExposedPorts = nat.PortSet{}
		hostConfig.PortBindings = nat.PortMap{}
		for _, pm := range config.GetPortMappings() {
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
		logger.Info().Interface("exposedPorts", dockerConfig.ExposedPorts).Interface("portBindings", hostConfig.PortBindings).Msg("Docker port config")
	}

	if linux := config.GetLinux(); linux != nil {
		if sc := linux.GetSecurityContext(); sc != nil {
			if sc.GetPrivileged() {
				hostConfig.Privileged = true
			}
		}
		if ns := linux.GetSecurityContext().GetNamespaceOptions(); ns != nil {
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

	return dockerConfig, hostConfig, &network.NetworkingConfig{}
}

// --- State converters ---

func dockerStateToContainerState(state string) runtimeapi.ContainerState {
	switch state {
	case "created":
		return runtimeapi.ContainerState_CONTAINER_CREATED
	case "running":
		return runtimeapi.ContainerState_CONTAINER_RUNNING
	default:
		return runtimeapi.ContainerState_CONTAINER_EXITED
	}
}

func dockerStateToPodState(running bool) runtimeapi.PodSandboxState {
	if running {
		return runtimeapi.PodSandboxState_SANDBOX_READY
	}
	return runtimeapi.PodSandboxState_SANDBOX_NOTREADY
}

func dockerImageToRuntimeImage(img image.Summary) *runtimeapi.Image {
	return &runtimeapi.Image{
		Id:          img.ID,
		RepoTags:    img.RepoTags,
		RepoDigests: img.RepoDigests,
		Size:        uint64(img.Size),
	}
}

func boolPtr(b bool) *bool {
	return &b
}

// --- Cleanup helpers (used by backend.Stop) ---

func (s *Server) RemoveContainers(ctx context.Context) ([]string, error) {
	f := filters.NewArgs()
	f.Add("label", s.typeFilter("container"))
	f.Add("label", s.managedByFilter())

	containers, err := s.backend.client.ContainerList(ctx, container.ListOptions{All: true, Filters: f})
	if err != nil {
		return nil, fmt.Errorf("list managed containers: %w", err)
	}

	var removed []string
	var errs []error
	for _, c := range containers {
		if err := s.removeContainer(ctx, c.ID); err != nil {
			errs = append(errs, fmt.Errorf("remove container %s: %w", c.ID[:12], err))
		} else {
			removed = append(removed, c.ID)
			logger.Info().Str("id", c.ID[:12]).Msg("cleanup: removed container")
		}
	}
	return removed, errors.Join(errs...)
}

func (s *Server) RemovePodSandboxes(ctx context.Context) ([]string, error) {
	f := filters.NewArgs()
	f.Add("label", s.typeFilter("sandbox"))
	f.Add("label", s.managedByFilter())

	sandboxes, err := s.backend.client.ContainerList(ctx, container.ListOptions{All: true, Filters: f})
	if err != nil {
		return nil, fmt.Errorf("list managed sandboxes: %w", err)
	}

	var removed []string
	var errs []error
	for _, sb := range sandboxes {
		if err := s.removeContainer(ctx, sb.ID); err != nil {
			errs = append(errs, fmt.Errorf("remove sandbox %s: %w", sb.ID[:12], err))
		} else {
			removed = append(removed, sb.ID)
			logger.Info().Str("id", sb.ID[:12]).Msg("cleanup: removed sandbox")
		}
	}
	return removed, errors.Join(errs...)
}

func (s *Server) RemoveVolumes(ctx context.Context) ([]string, error) {
	volumes, err := s.listVolumes(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("list managed volumes: %w", err)
	}

	var removed []string
	var errs []error
	for _, v := range volumes {
		if err := s.removeVolume(ctx, v); err != nil {
			errs = append(errs, fmt.Errorf("remove volume %s: %w", v, err))
		} else {
			removed = append(removed, v)
			logger.Info().Str("name", v).Msg("cleanup: removed volume")
		}
	}
	return removed, errors.Join(errs...)
}
