package docker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/cnuss/nanokube/pkg/component"
	critypes "github.com/cnuss/nanokube/pkg/cri/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/system"
	dockerclient "github.com/docker/docker/client"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

var logger = component.NewLogger("docker")

// Backend implements cri.Backend using the Docker Engine API.
type Backend struct {
	dockerSocket string
	name         string // cluster name, used as managed-by label value
	client       *dockerclient.Client
	networkID    string // cluster bridge network ID
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
	return "localhost"
}

func (b *Backend) Domain() string {
	return ""
}

func (b *Backend) Nameservers() []string {
	return []string{}
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

	networks, err := b.RemoveNetworks(ctx)
	logger.Info().Strs("ids", networks).Msg("cleanup: removed networks")
	if err != nil {
		logger.Warn().Err(err).Msg("cleanup: network removal errors")
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
	logger.Warn().Msg("GetContainerEvents not implemented")
	<-ctx.Done()
	close(eventsCh)
	return nil
}

func (b *Backend) UpdatePodSandboxResources(ctx context.Context, req *runtimeapi.UpdatePodSandboxResourcesRequest) (*runtimeapi.UpdatePodSandboxResourcesResponse, error) {
	logger.Warn().Msg("UpdatePodSandboxResources not implemented")
	return &runtimeapi.UpdatePodSandboxResourcesResponse{}, nil
}

// RunProbe creates a short-lived container from image, executes cmd with
// optional bind mounts, and returns stdout. The container is removed after.
func (b *Backend) RunProbe(ctx context.Context, img string, cmd []string, binds []string) ([]byte, error) {
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

	hostCfg := &container.HostConfig{}
	if len(binds) > 0 {
		hostCfg.Binds = binds
	}

	resp, err := b.client.ContainerCreate(ctx,
		&container.Config{Image: img, Cmd: cmd},
		hostCfg, nil, nil, "")
	if err != nil {
		return nil, fmt.Errorf("create probe container: %w", err)
	}
	defer b.client.ContainerRemove(ctx, resp.ID, container.RemoveOptions{Force: true})

	if err := b.client.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return nil, fmt.Errorf("start probe container: %w", err)
	}

	waitCh, errCh := b.client.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)
	select {
	case <-waitCh:
	case err := <-errCh:
		if err != nil {
			return nil, fmt.Errorf("wait probe container: %w", err)
		}
	}

	logReader, err := b.client.ContainerLogs(ctx, resp.ID, container.LogsOptions{ShowStdout: true})
	if err != nil {
		return nil, fmt.Errorf("read probe logs: %w", err)
	}
	defer logReader.Close()

	var buf bytes.Buffer
	io.Copy(&buf, logReader)
	return stripLogHeaders(buf.Bytes()), nil
}

// stripLogHeaders removes Docker's 8-byte multiplexed stream headers.
// Each frame: [type(1) 0 0 0 size(4)] followed by size bytes of payload.
func stripLogHeaders(data []byte) []byte {
	var result []byte
	for len(data) >= 8 {
		size := int(data[4])<<24 | int(data[5])<<16 | int(data[6])<<8 | int(data[7])
		data = data[8:]
		if size > len(data) {
			size = len(data)
		}
		result = append(result, data[:size]...)
		data = data[size:]
	}
	if len(result) == 0 {
		return data
	}
	return result
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
	// Ensure image is available
	img := "busybox"
	_, err := b.client.ImageInspect(ctx, img)
	if err != nil {
		reader, pullErr := b.client.ImagePull(ctx, img, image.PullOptions{})
		if pullErr != nil {
			return "", "", "", fmt.Errorf("pull %s: %w", img, pullErr)
		}
		io.Copy(io.Discard, reader)
		reader.Close()
	}

	resp, err := b.client.ContainerCreate(ctx,
		&container.Config{
			Image: img,
			Cmd: []string{"sh", "-c",
				// boot_id
				"cat /proc/sys/kernel/random/boot_id 2>/dev/null || echo; " +
					// system UUID: try DMI, then device-tree fallbacks (same order as cadvisor)
					"cat /host/sys/class/dmi/id/product_uuid 2>/dev/null || " +
					"cat /proc/device-tree/system-id 2>/dev/null || " +
					"cat /proc/device-tree/vm,uuid 2>/dev/null || echo; " +
					// machine-id
					"cat /etc/machine-id 2>/dev/null || echo",
			},
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
		}, nil, nil, "")
	if err != nil {
		return "", "", "", fmt.Errorf("create host-id probe: %w", err)
	}
	defer b.client.ContainerRemove(ctx, resp.ID, container.RemoveOptions{Force: true})

	if err := b.client.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return "", "", "", fmt.Errorf("start host-id probe: %w", err)
	}

	waitCh, errCh := b.client.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)
	select {
	case <-waitCh:
	case err := <-errCh:
		if err != nil {
			return "", "", "", fmt.Errorf("wait host-id probe: %w", err)
		}
	}

	logReader, err := b.client.ContainerLogs(ctx, resp.ID, container.LogsOptions{ShowStdout: true})
	if err != nil {
		return "", "", "", fmt.Errorf("read host-id probe logs: %w", err)
	}
	defer logReader.Close()

	var buf bytes.Buffer
	io.Copy(&buf, logReader)
	lines := strings.Split(strings.TrimSpace(string(stripLogHeaders(buf.Bytes()))), "\n")

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
