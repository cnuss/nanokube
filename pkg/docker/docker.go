package docker

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/cnuss/nanokube/pkg"
	"github.com/cnuss/nanokube/pkg/nanokube"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	v1 "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func Detect(config pkg.Config) pkg.Backend {
	home, _ := os.UserHomeDir()

	// TODO: Windows support
	// TODO: DOCKER_HOST env var support
	// TODO: Port number support
	sockets := []string{
		`//./pipe/docker_engine`, // TODO: Other windows named pipes?
		filepath.Join(string(os.PathSeparator), "var", "run", "docker.sock"),
		filepath.Join(string(os.PathSeparator), "run", "docker.sock"),
		filepath.Join(home, ".docker", "run", "docker.sock"),
		filepath.Join(home, ".colima", "default", "docker.sock"),
		filepath.Join(home, ".lima", "docker-actions-toolkit", "docker.sock"),
		filepath.Join(home, ".rd", "docker.sock"),
	}

	for _, socket := range sockets {
		if _, err := os.Stat(socket); err == nil {
			backend, err := newBackend(config, socket)
			if err != nil {
				continue
			}
			return backend
		}
	}

	return nil
}

func newBackend(config pkg.Config, socket string) (pkg.Backend, error) {
	client, err := newClient(config, socket)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(config.Context(), 5*time.Second)
	defer cancel()

	if _, err := client.Ping(ctx); err != nil {
		return nil, err
	}

	driver := &driver{config: config, client: client}

	return pkg.NewBackend(driver), nil
}

type driver struct {
	config pkg.Config
	client *client.Client
}

var _ pkg.Driver = &driver{}

func (d *driver) Name() string {
	return "docker"
}

func (d *driver) Context() context.Context {
	return d.config.Context()
}

func (d *driver) Options() nanokube.Options {
	return d.config.Options()
}

func (d *driver) Attach(ctx context.Context, req *v1.AttachRequest) (*v1.AttachResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) CheckpointContainer(ctx context.Context, options *v1.CheckpointContainerRequest) error {
	return nanokube.Unimplemented()
}

func (d *driver) Close() error {
	return nanokube.Unimplemented()
}

func (d *driver) ContainerStats(ctx context.Context, containerID string) (*v1.ContainerStats, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) ContainerStatus(ctx context.Context, containerID string, verbose bool) (*v1.ContainerStatusResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) CreateContainer(ctx context.Context, podSandboxID string, config *v1.ContainerConfig, sandboxConfig *v1.PodSandboxConfig) (string, error) {
	return "", nanokube.Unimplemented()
}

func (d *driver) Exec(ctx context.Context, request *v1.ExecRequest) (*v1.ExecResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) ExecSync(ctx context.Context, containerID string, cmd []string, timeout time.Duration) (stdout []byte, stderr []byte, err error) {
	return nil, nil, nanokube.Unimplemented()
}

func (d *driver) GetContainerEvents(ctx context.Context, containerEventsCh chan *v1.ContainerEventResponse, connectionEstablishedCallback func(v1.RuntimeService_GetContainerEventsClient)) error {
	return nanokube.Unimplemented()
}

func (d *driver) ImageFsInfo(ctx context.Context) (*v1.ImageFsInfoResponse, error) {
	info, err := d.client.Info(ctx)
	if err != nil {
		return nil, err
	}
	images, err := d.ListImages(ctx, nil)
	if err != nil {
		return nil, err
	}
	var totalSize uint64
	for _, img := range images {
		totalSize += img.Size
	}
	return &v1.ImageFsInfoResponse{
		ImageFilesystems: []*v1.FilesystemUsage{
			{
				FsId:      &v1.FilesystemIdentifier{Mountpoint: info.DockerRootDir},
				UsedBytes: &v1.UInt64Value{Value: totalSize},
			},
		},
	}, nil
}

func (d *driver) ImageStatus(ctx context.Context, image *v1.ImageSpec, verbose bool) (*v1.ImageStatusResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) ListContainerStats(ctx context.Context, filter *v1.ContainerStatsFilter) ([]*v1.ContainerStats, error) {
	return []*v1.ContainerStats{}, nanokube.Unimplemented()
}

func (d *driver) ListContainers(ctx context.Context, filter *v1.ContainerFilter) ([]*v1.Container, error) {
	tb := nanokube.NewTagBuilder(d.Name(), nil).WithType(nanokube.ResourceContainer)
	f := filters.NewArgs()
	for k, v := range tb.InternalTags() {
		if v != "" {
			f.Add("label", k+"="+v)
		}
	}

	if filter != nil {
		if filter.Id != "" {
			f.Add("id", filter.Id)
		}
		if filter.PodSandboxId != "" {
			f.Add("label", nanokube.TagParentUIDFilter(d.Name(), filter.PodSandboxId))
		}
		if filter.State != nil {
			switch filter.State.State {
			case v1.ContainerState_CONTAINER_CREATED:
				f.Add("status", container.StateCreated)
			case v1.ContainerState_CONTAINER_RUNNING:
				f.Add("status", container.StateRunning)
			case v1.ContainerState_CONTAINER_EXITED:
				f.Add("status", container.StateRestarting)
				f.Add("status", container.StatePaused)
				f.Add("status", container.StateRemoving)
				f.Add("status", container.StateExited)
				f.Add("status", container.StateDead)
			}
		}
	}

	containers, err := d.client.ContainerList(ctx, container.ListOptions{All: true, Filters: f})
	if err != nil {
		return nil, err
	}

	selector := filter.GetLabelSelector()
	result := make([]*v1.Container, 0, len(containers))
	for _, c := range containers {
		if !d.matchLabels(c.Labels, selector) {
			continue
		}
		prefix := d.Name()
		var containerState v1.ContainerState
		switch c.State {
		case "created":
			containerState = v1.ContainerState_CONTAINER_CREATED
		case "running":
			containerState = v1.ContainerState_CONTAINER_RUNNING
		default:
			containerState = v1.ContainerState_CONTAINER_EXITED
		}
		result = append(result, &v1.Container{
			Id:           c.ID,
			PodSandboxId: nanokube.TagParentUID(prefix, c.Labels),
			Metadata: &v1.ContainerMetadata{
				Name:    nanokube.TagName(prefix, c.Labels),
				Attempt: nanokube.TagAttempt(prefix, c.Labels),
			},
			Image:       &v1.ImageSpec{Image: c.Image},
			ImageRef:    c.ImageID,
			State:       containerState,
			CreatedAt:   c.Created * int64(time.Second),
			Labels:      nanokube.TagExtractLabels(prefix, c.Labels),
			Annotations: nanokube.TagExtractAnnotations(prefix, c.Labels),
		})
	}

	return result, nil
}

func (d *driver) matchLabels(dockerLabels map[string]string, selector map[string]string) bool {
	if len(selector) == 0 {
		return true
	}
	unpacked := nanokube.TagExtractLabels(d.Name(), dockerLabels)
	for k, v := range selector {
		if unpacked[k] == v || dockerLabels[k] == v {
			continue
		}
		return false
	}
	return true
}

func (d *driver) ListImages(ctx context.Context, filter *v1.ImageFilter) ([]*v1.Image, error) {
	images, err := d.client.ImageList(ctx, image.ListOptions{})
	if err != nil {
		return nil, err
	}
	result := make([]*v1.Image, len(images))
	for i, img := range images {
		result[i] = &v1.Image{
			Id:          img.ID,
			RepoTags:    img.RepoTags,
			RepoDigests: img.RepoDigests,
			Size:        uint64(img.Size),
		}
	}
	return result, nil
}

func (d *driver) ListMetricDescriptors(ctx context.Context) ([]*v1.MetricDescriptor, error) {
	return []*v1.MetricDescriptor{}, nanokube.Unimplemented()
}

func (d *driver) ListPodSandbox(ctx context.Context, filter *v1.PodSandboxFilter) ([]*v1.PodSandbox, error) {
	tb := nanokube.NewTagBuilder(d.Name(), nil).WithType(nanokube.ResourceSandbox)
	f := filters.NewArgs()
	for k, v := range tb.InternalTags() {
		if v != "" {
			f.Add("label", k+"="+v)
		}
	}

	if filter != nil {
		if filter.Id != "" {
			f.Add("id", filter.Id)
		}
		if filter.State != nil {
			if filter.State.State == v1.PodSandboxState_SANDBOX_READY {
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

	containers, err := d.client.ContainerList(ctx, container.ListOptions{All: true, Filters: f})
	if err != nil {
		return nil, err
	}

	selector := filter.GetLabelSelector()
	var result []*v1.PodSandbox
	for _, c := range containers {
		if !d.matchLabels(c.Labels, selector) {
			continue
		}
		prefix := d.Name()
		podState := v1.PodSandboxState_SANDBOX_NOTREADY
		if c.State == "running" {
			podState = v1.PodSandboxState_SANDBOX_READY
		}
		result = append(result, &v1.PodSandbox{
			Id: c.ID,
			Metadata: &v1.PodSandboxMetadata{
				Name:      nanokube.TagName(prefix, c.Labels),
				Namespace: nanokube.TagNamespace(prefix, c.Labels),
				Uid:       nanokube.TagUID(prefix, c.Labels),
			},
			State:       podState,
			CreatedAt:   c.Created * int64(time.Second),
			Labels:      nanokube.TagExtractLabels(prefix, c.Labels),
			Annotations: nanokube.TagExtractAnnotations(prefix, c.Labels),
		})
	}
	return result, nil
}

func (d *driver) ListPodSandboxMetrics(ctx context.Context) ([]*v1.PodSandboxMetrics, error) {
	return []*v1.PodSandboxMetrics{}, nanokube.Unimplemented()
}

func (d *driver) ListPodSandboxStats(ctx context.Context, filter *v1.PodSandboxStatsFilter) ([]*v1.PodSandboxStats, error) {
	return []*v1.PodSandboxStats{}, nanokube.Unimplemented()
}

func (d *driver) PodSandboxStats(ctx context.Context, podSandboxID string) (*v1.PodSandboxStats, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) PodSandboxStatus(ctx context.Context, podSandboxID string, verbose bool) (*v1.PodSandboxStatusResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) PortForward(ctx context.Context, request *v1.PortForwardRequest) (*v1.PortForwardResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) PullImage(ctx context.Context, image *v1.ImageSpec, auth *v1.AuthConfig, podSandboxConfig *v1.PodSandboxConfig) (string, error) {
	return "", nanokube.Unimplemented()
}

func (d *driver) RemoveContainer(ctx context.Context, containerID string) error {
	return nanokube.Unimplemented()
}

func (d *driver) RemoveImage(ctx context.Context, image *v1.ImageSpec) error {
	return nanokube.Unimplemented()
}

func (d *driver) RemovePodSandbox(ctx context.Context, podSandboxID string) error {
	return nanokube.Unimplemented()
}

func (d *driver) ReopenContainerLog(ctx context.Context, ContainerID string) error {
	return nanokube.Unimplemented()
}

func (d *driver) RunPodSandbox(ctx context.Context, config *v1.PodSandboxConfig, runtimeHandler string) (string, error) {
	return "", nanokube.Unimplemented()
}

func (d *driver) RuntimeConfig(ctx context.Context) (*v1.RuntimeConfigResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) StartContainer(ctx context.Context, containerID string) error {
	return nanokube.Unimplemented()
}

func (d *driver) Status(ctx context.Context, verbose bool) (*v1.StatusResponse, error) {
	info, err := d.client.Info(ctx)
	_, netErr := d.client.NetworkInspect(ctx, "bridge", network.InspectOptions{})

	resp := &v1.StatusResponse{
		Status: &v1.RuntimeStatus{
			Conditions: []*v1.RuntimeCondition{
				{Type: "RuntimeReady", Status: err == nil, Reason: "DockerIsUp"},
				{Type: "NetworkReady", Status: netErr == nil, Reason: "BridgeNetworkReady"},
			},
		},
	}
	if verbose && err == nil {
		resp.Info = map[string]string{
			"storageDriver": info.Driver,
			"serverVersion": info.ServerVersion,
		}
	}
	return resp, nil
}

func (d *driver) StopContainer(ctx context.Context, containerID string, timeout int64) error {
	return nanokube.Unimplemented()
}

func (d *driver) StopPodSandbox(ctx context.Context, podSandboxID string) error {
	return nanokube.Unimplemented()
}

func (d *driver) UpdateContainerResources(ctx context.Context, containerID string, resources *v1.ContainerResources) error {
	return nanokube.Unimplemented()
}

func (d *driver) UpdatePodSandboxResources(ctx context.Context, request *v1.UpdatePodSandboxResourcesRequest) (*v1.UpdatePodSandboxResourcesResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) UpdateRuntimeConfig(ctx context.Context, runtimeConfig *v1.RuntimeConfig) error {
	return nanokube.Unimplemented()
}

func (d *driver) Version(ctx context.Context, version string) (*v1.VersionResponse, error) {
	v, err := d.client.ServerVersion(ctx)
	if err != nil {
		return nil, err
	}
	return &v1.VersionResponse{
		Version:           version,
		RuntimeName:       d.Name(),
		RuntimeVersion:    v.APIVersion,
		RuntimeApiVersion: "v1",
	}, nil
}
