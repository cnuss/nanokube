package docker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cnuss/nanokube/pkg/component"
	critypes "github.com/cnuss/nanokube/pkg/cri/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/system"
	dockerclient "github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

var logger = component.NewLogger("docker")

// Backend implements cri.Backend using the Docker Engine API.
type Backend struct {
	dockerSocket string
	name         string // cluster name, used as managed-by label value
	client       *dockerclient.Client
	Mounts       critypes.MountLookup

	logMu      sync.Mutex
	logWriters map[string]context.CancelFunc // containerID -> cancel
}

// New creates a Docker backend that connects to the given Docker socket.
func New(dockerSocket, name string) *Backend {
	return &Backend{
		dockerSocket: dockerSocket,
		name:         name,
		logWriters:   make(map[string]context.CancelFunc),
	}
}

func (b *Backend) Name() string {
	return b.name
}

func (b *Backend) PluginName() string {
	return "docker.io/" + b.name
}

func (b *Backend) Hostname() string {
	if h, err := os.Hostname(); err == nil {
		return h
	}
	return "localhost"
}

func (b *Backend) Domain() string {
	return ""
}

func (b *Backend) Nameservers() []string {
	if b.client == nil {
		return []string{}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := b.RunProbe(ctx, "busybox", []string{"cat", "/etc/resolv.conf"}, nil)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to probe nameservers")
		return []string{}
	}
	var servers []string
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "nameserver ") {
			if s := strings.TrimSpace(line[len("nameserver "):]); s != "" {
				servers = append(servers, s)
			}
		}
	}
	return servers
}

func (b *Backend) Start(ctx context.Context) (component.Started, error) {
	httpClient := &http.Client{
		Transport: &loggingTransport{
			inner: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return net.Dial("unix", b.dockerSocket)
				},
			},
		},
	}
	var err error
	b.client, err = dockerclient.NewClientWithOpts(
		dockerclient.WithHost("unix://"+b.dockerSocket),
		dockerclient.WithAPIVersionNegotiation(),
		dockerclient.WithHTTPClient(httpClient),
	)
	if err != nil {
		return nil, fmt.Errorf("docker client: %w", err)
	}
	// Validate connectivity
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ping, err := b.client.Ping(pctx)
	if err != nil {
		return nil, fmt.Errorf("docker ping: %w", err)
	}
	logger.Info().Str("api", ping.APIVersion).Msg("docker backend connected")
	return component.Ready(), nil
}

func (b *Backend) Stop() component.Stopped {
	if b.client == nil {
		return component.Done()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Cleanup: containers → sandboxes → networks
	containers, err := b.RemoveContainers(ctx)
	logger.Info().Strs("ids", containers).Msg("cleanup: removed containers")
	if err != nil {
		logger.Warn().Err(err).Msg("cleanup: container removal errors")
	}

	sandboxes, err := b.RemovePodSandboxes(ctx)
	logger.Info().Strs("ids", sandboxes).Msg("cleanup: removed sandboxes")
	if err != nil {
		logger.Warn().Err(err).Msg("cleanup: sandbox removal errors")
	}

	// Stop all active log writers
	b.logMu.Lock()
	for id, cancel := range b.logWriters {
		cancel()
		delete(b.logWriters, id)
	}
	b.logMu.Unlock()

	b.client.Close()
	return component.Done()
}

func (b *Backend) Version(ctx context.Context) (*runtimeapi.VersionResponse, error) {
	v, err := b.client.ServerVersion(ctx)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.VersionResponse{
		Version:           "0.1.0",
		RuntimeName:       "docker",
		RuntimeVersion:    v.Version,
		RuntimeApiVersion: "v1",
	}, nil
}

func (b *Backend) Status(ctx context.Context, verbose bool) (*runtimeapi.StatusResponse, error) {
	info, err := b.client.Info(ctx)
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
	if verbose {
		resp.Info = infoMap(info)
	}
	return resp, nil
}

func infoMap(info system.Info) map[string]string {
	return map[string]string{
		"storageDriver": info.Driver,
		"serverVersion": info.ServerVersion,
	}
}

func (b *Backend) UpdateRuntimeConfig(ctx context.Context, config *runtimeapi.RuntimeConfig) error {
	return nil
}

func (b *Backend) RuntimeConfig(ctx context.Context) (*runtimeapi.RuntimeConfigResponse, error) {
	return &runtimeapi.RuntimeConfigResponse{}, nil
}

func (b *Backend) ListMetricDescriptors(ctx context.Context) ([]*runtimeapi.MetricDescriptor, error) {
	return nil, nil
}

func (b *Backend) ListPodSandboxMetrics(ctx context.Context) ([]*runtimeapi.PodSandboxMetrics, error) {
	return nil, nil
}

func (b *Backend) CheckpointContainer(ctx context.Context, containerID, location string, timeout int64) error {
	logger.Warn().Str("container", containerID).Msg("CheckpointContainer not implemented")
	return fmt.Errorf("checkpoint not supported")
}

func (b *Backend) GetContainerEvents(ctx context.Context, eventsCh chan *runtimeapi.ContainerEventResponse) error {
	logger.Info().Msg("starting Docker event stream")

	msgCh, errCh := b.client.Events(ctx, events.ListOptions{
		Filters: filters.NewArgs(
			filters.Arg("type", string(events.ContainerEventType)),
			filters.Arg("label", labelManagedBy+"="+b.name),
		),
	})

	for {
		select {
		case <-ctx.Done():
			close(eventsCh)
			return nil
		case err := <-errCh:
			close(eventsCh)
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("docker event stream: %w", err)
		case msg := <-msgCh:
			ev := b.dockerEventToCRI(ctx, msg)
			if ev != nil {
				select {
				case eventsCh <- ev:
				case <-ctx.Done():
					close(eventsCh)
					return nil
				}
			}
		}
	}
}

func (b *Backend) dockerEventToCRI(ctx context.Context, msg events.Message) *runtimeapi.ContainerEventResponse {
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
	sandboxID := msg.Actor.Attributes[labelSandboxID]

	// Skip sandbox containers — CRI events are for app containers only
	if msg.Actor.Attributes[labelContainerType] == containerTypeSandbox {
		return nil
	}

	// Build sandbox status
	var sandboxStatus *runtimeapi.PodSandboxStatus
	if sandboxID != "" {
		if resp, err := b.PodSandboxStatus(ctx, sandboxID, false); err == nil {
			sandboxStatus = resp.Status
		}
	}

	// Build container statuses for all containers in this sandbox
	var containerStatuses []*runtimeapi.ContainerStatus
	if sandboxID != "" {
		containers, err := b.ListContainers(ctx, &runtimeapi.ContainerFilter{
			PodSandboxId: sandboxID,
		})
		if err == nil {
			for _, c := range containers {
				if resp, err := b.ContainerStatus(ctx, c.Id, false); err == nil {
					containerStatuses = append(containerStatuses, resp.Status)
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

func (b *Backend) UpdatePodSandboxResources(ctx context.Context, req *runtimeapi.UpdatePodSandboxResourcesRequest) (*runtimeapi.UpdatePodSandboxResourcesResponse, error) {
	logger.Warn().Msg("UpdatePodSandboxResources not implemented")
	return &runtimeapi.UpdatePodSandboxResourcesResponse{}, nil
}

// RunProbe creates a short-lived container from image, executes cmd with
// optional bind mounts, and returns stdout. The container is removed after.
func (b *Backend) RunProbe(ctx context.Context, img string, cmd []string, binds []string) ([]byte, error) {
	hostCfg := &container.HostConfig{}
	if len(binds) > 0 {
		hostCfg.Binds = binds
	}
	return b.runProbe(ctx, img, cmd, hostCfg)
}

func (b *Backend) runProbe(ctx context.Context, img string, cmd []string, hostCfg *container.HostConfig) ([]byte, error) {
	// Ensure image is available
	_, err := b.client.ImageInspect(ctx, img)
	if err != nil {
		reader, pullErr := b.client.ImagePull(ctx, img, image.PullOptions{})
		if pullErr != nil {
			return nil, fmt.Errorf("pull %s: %w", img, pullErr)
		}
		io.Copy(io.Discard, reader)
		reader.Close()
	}

	hostCfg.AutoRemove = true

	resp, err := b.client.ContainerCreate(ctx,
		&container.Config{Image: img, Cmd: cmd, AttachStdout: true, AttachStderr: true},
		hostCfg, nil, nil, "")
	if err != nil {
		return nil, fmt.Errorf("create probe container: %w", err)
	}

	attach, err := b.client.ContainerAttach(ctx, resp.ID, container.AttachOptions{
		Stream: true, Stdout: true, Stderr: true,
	})
	if err != nil {
		b.RemoveContainer(ctx, resp.ID)
		return nil, fmt.Errorf("attach probe container: %w", err)
	}
	defer attach.Close()

	if err := b.client.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return nil, fmt.Errorf("start probe container: %w", err)
	}

	var stdout, stderr bytes.Buffer
	stdcopy.StdCopy(&stdout, &stderr, attach.Reader)
	return stdout.Bytes(), nil
}

// HostInfo returns host system information from Docker.
func (b *Backend) HostInfo(ctx context.Context) (int, int64, string, string, error) {
	info, err := b.client.Info(ctx)
	if err != nil {
		return 0, 0, "", "", fmt.Errorf("docker info: %w", err)
	}
	return info.NCPU, info.MemTotal, info.KernelVersion, info.OperatingSystem, nil
}

// HostIDs probes the host's boot ID, system UUID, and machine ID by running
// a busybox container with host namespace access and bind-mounted /etc + /sys.
func (b *Backend) HostIDs(ctx context.Context) (string, string, string, error) {
	out, err := b.runProbe(ctx, "busybox",
		[]string{"sh", "-c",
			// boot_id
			"cat /proc/sys/kernel/random/boot_id 2>/dev/null || echo; " +
				// system UUID: try DMI, then device-tree fallbacks (same order as cadvisor)
				"cat /host/sys/class/dmi/id/product_uuid 2>/dev/null || " +
				"cat /proc/device-tree/system-id 2>/dev/null || " +
				"cat /proc/device-tree/vm,uuid 2>/dev/null || echo; " +
				// machine-id
				"cat /etc/machine-id 2>/dev/null || echo",
		},
		&container.HostConfig{
			Privileged:  true,
			PidMode:     "host",
			NetworkMode: "host",
			IpcMode:     "host",
			Binds: []string{
				"/etc/machine-id:/etc/machine-id:ro",
				"/sys:/host/sys:ro",
			},
		})
	if err != nil {
		return "", "", "", fmt.Errorf("host-id probe: %w", err)
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")

	var bootID, systemUUID, machineID string
	if len(lines) > 0 {
		bootID = strings.TrimSpace(lines[0])
	}
	if len(lines) > 1 {
		systemUUID = strings.TrimSpace(lines[1])
	}
	if len(lines) > 2 {
		machineID = strings.TrimSpace(lines[2])
	}

	return bootID, systemUUID, machineID, nil
}
