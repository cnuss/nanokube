package docker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/docker/docker/api/types/system"
	dockerclient "github.com/docker/docker/client"
	"github.com/rs/zerolog/log"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// Backend implements cri.Backend using the Docker Engine API.
type Backend struct {
	dockerSocket string
	client       *dockerclient.Client

	logMu      sync.Mutex
	logWriters map[string]context.CancelFunc // containerID -> cancel
}

// New creates a Docker backend that connects to the given Docker socket.
func New(dockerSocket string) *Backend {
	return &Backend{
		dockerSocket: dockerSocket,
		logWriters:   make(map[string]context.CancelFunc),
	}
}

func (b *Backend) Init(ctx context.Context) error {
	var err error
	b.client, err = dockerclient.NewClientWithOpts(
		dockerclient.WithHost("unix://"+b.dockerSocket),
		dockerclient.WithAPIVersionNegotiation(),
	)
	if err != nil {
		return fmt.Errorf("docker client: %w", err)
	}
	// Validate connectivity
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ping, err := b.client.Ping(pctx)
	if err != nil {
		return fmt.Errorf("docker ping: %w", err)
	}
	log.Info().Str("api", ping.APIVersion).Msg("docker backend connected")
	return nil
}

func (b *Backend) Close() error {
	if b.client != nil {
		return b.client.Close()
	}
	return nil
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
	return fmt.Errorf("checkpoint not supported")
}

func (b *Backend) GetContainerEvents(ctx context.Context, eventsCh chan *runtimeapi.ContainerEventResponse) error {
	// Block until context is done; events are not implemented for Docker backend
	<-ctx.Done()
	close(eventsCh)
	return nil
}

func (b *Backend) UpdatePodSandboxResources(ctx context.Context, req *runtimeapi.UpdatePodSandboxResourcesRequest) (*runtimeapi.UpdatePodSandboxResourcesResponse, error) {
	return &runtimeapi.UpdatePodSandboxResourcesResponse{}, nil
}
