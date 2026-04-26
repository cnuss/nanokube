package podman

import (
	"context"
	"net"
	"net/url"
	"os"
	"time"

	"github.com/cnuss/nanokube/pkg"
	"github.com/cnuss/nanokube/pkg/nanokube"
	v1 "github.com/cnuss/nanokube/pkg/v1"
	"github.com/emicklei/go-restful/v3"
	criv1 "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func Detect(config v1.Config) v1.Backend {
	sockets := []string{}

	for _, socket := range sockets {
		if _, err := os.Stat(socket); err == nil {
			backend, err := NewBackend(config)
			if err != nil {
				continue
			}
			return backend
		}
	}

	return nil
}

func NewBackend(config v1.Config) (v1.Backend, error) {
	_, cancel := context.WithTimeout(config.Context(), 5*time.Second)
	defer cancel()

	// TODO: Setup client and ping

	driver := &driver{config: config}

	return pkg.NewBackend(v1.PodmanBackend, driver), nil
}

type driver struct {
	config v1.Config
}

var _ v1.Driver = &driver{}

func (d *driver) Name() string {
	return "todo" // similar to "name" from docker info response
}

func (d *driver) Context() context.Context {
	return d.config.Context()
}

func (d *driver) Options() v1.Options {
	return d.config.Options()
}

func (d *driver) Service() *restful.WebService {
	nanokube.Unimplemented()
	return nil
}

func (d *driver) CgroupRoot() string {
	nanokube.Unimplemented()
	return "/"
}

func (d *driver) Attach(ctx context.Context, req *criv1.AttachRequest) (*criv1.AttachResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) CheckpointContainer(ctx context.Context, options *criv1.CheckpointContainerRequest) error {
	return nanokube.Unimplemented()
}

func (d *driver) Close() error {
	return nanokube.Unimplemented()
}

func (d *driver) ContainerStats(ctx context.Context, containerID string) (*criv1.ContainerStats, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) ContainerStatus(ctx context.Context, containerID string, verbose bool) (*criv1.ContainerStatusResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) CreateContainer(ctx context.Context, podSandboxID string, config *criv1.ContainerConfig, sandboxConfig *criv1.PodSandboxConfig) (string, error) {
	return "", nanokube.Unimplemented()
}

func (d *driver) Exec(ctx context.Context, request *criv1.ExecRequest) (*criv1.ExecResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) ExecSync(ctx context.Context, containerID string, cmd []string, timeout time.Duration) (stdout []byte, stderr []byte, err error) {
	return nil, nil, nanokube.Unimplemented()
}

func (d *driver) GetContainerEvents(ctx context.Context, containerEventsCh chan *criv1.ContainerEventResponse, connectionEstablishedCallback func(criv1.RuntimeService_GetContainerEventsClient)) error {
	return nanokube.Unimplemented()
}

func (d *driver) ImageFsInfo(ctx context.Context) (*criv1.ImageFsInfoResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) ImageStatus(ctx context.Context, image *criv1.ImageSpec, verbose bool) (*criv1.ImageStatusResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) ListContainerStats(ctx context.Context, filter *criv1.ContainerStatsFilter) ([]*criv1.ContainerStats, error) {
	return []*criv1.ContainerStats{}, nanokube.Unimplemented()
}

func (d *driver) ListContainers(ctx context.Context, filter *criv1.ContainerFilter) ([]*criv1.Container, error) {
	return []*criv1.Container{}, nanokube.Unimplemented()
}

func (d *driver) ListImages(ctx context.Context, filter *criv1.ImageFilter) ([]*criv1.Image, error) {
	return []*criv1.Image{}, nanokube.Unimplemented()
}

func (d *driver) ListMetricDescriptors(ctx context.Context) ([]*criv1.MetricDescriptor, error) {
	return []*criv1.MetricDescriptor{}, nanokube.Unimplemented()
}

func (d *driver) ListPodSandbox(ctx context.Context, filter *criv1.PodSandboxFilter) ([]*criv1.PodSandbox, error) {
	return []*criv1.PodSandbox{}, nanokube.Unimplemented()
}

func (d *driver) ListPodSandboxMetrics(ctx context.Context) ([]*criv1.PodSandboxMetrics, error) {
	return []*criv1.PodSandboxMetrics{}, nanokube.Unimplemented()
}

func (d *driver) ListPodSandboxStats(ctx context.Context, filter *criv1.PodSandboxStatsFilter) ([]*criv1.PodSandboxStats, error) {
	return []*criv1.PodSandboxStats{}, nanokube.Unimplemented()
}

func (d *driver) PodSandboxStats(ctx context.Context, podSandboxID string) (*criv1.PodSandboxStats, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) PodSandboxStatus(ctx context.Context, podSandboxID string, verbose bool) (*criv1.PodSandboxStatusResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) PortForward(ctx context.Context, request *criv1.PortForwardRequest) (*criv1.PortForwardResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) PullImage(ctx context.Context, image *criv1.ImageSpec, auth *criv1.AuthConfig, podSandboxConfig *criv1.PodSandboxConfig) (string, error) {
	return "", nanokube.Unimplemented()
}

func (d *driver) RemoveContainer(ctx context.Context, containerID string) error {
	return nanokube.Unimplemented()
}

func (d *driver) RemoveImage(ctx context.Context, image *criv1.ImageSpec) error {
	return nanokube.Unimplemented()
}

func (d *driver) RemovePodSandbox(ctx context.Context, podSandboxID string) error {
	return nanokube.Unimplemented()
}

func (d *driver) ReopenContainerLog(ctx context.Context, ContainerID string) error {
	return nanokube.Unimplemented()
}

func (d *driver) RunPodSandbox(ctx context.Context, config *criv1.PodSandboxConfig, runtimeHandler string) (string, error) {
	return "", nanokube.Unimplemented()
}

func (d *driver) RuntimeConfig(ctx context.Context) (*criv1.RuntimeConfigResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) StartContainer(ctx context.Context, containerID string) error {
	return nanokube.Unimplemented()
}

func (d *driver) Status(ctx context.Context, verbose bool) (*criv1.StatusResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) StopContainer(ctx context.Context, containerID string, timeout int64) error {
	return nanokube.Unimplemented()
}

func (d *driver) StopPodSandbox(ctx context.Context, podSandboxID string) error {
	return nanokube.Unimplemented()
}

func (d *driver) UpdateContainerResources(ctx context.Context, containerID string, resources *criv1.ContainerResources) error {
	return nanokube.Unimplemented()
}

func (d *driver) UpdatePodSandboxResources(ctx context.Context, request *criv1.UpdatePodSandboxResourcesRequest) (*criv1.UpdatePodSandboxResourcesResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) UpdateRuntimeConfig(ctx context.Context, runtimeConfig *criv1.RuntimeConfig) error {
	return nanokube.Unimplemented()
}

func (d *driver) Version(ctx context.Context, apiVersion string) (*criv1.VersionResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) ExecOnHost(ctx context.Context, img string, cmd []string, mounts []v1.Path) (string, error) {
	return "", nanokube.Unimplemented()
}

func (d *driver) ExecOnNetwork(ctx context.Context, net v1.AllocatedNetwork, image string, cmd []string, portMap []v1.PortMap) (string, error) {
	return "", nanokube.Unimplemented()
}

func (d *driver) CreateNetwork(ctx context.Context, name string, networkType v1.NetworkType, net *net.IPNet, gateway *net.IP) error {
	return nanokube.Unimplemented()
}

func (d *driver) DefaultNetwork(ctx context.Context) string {
	nanokube.Unimplemented()
	return ""
}

func (d *driver) GetNetwork(ctx context.Context, name string) (*string, *v1.NetworkType, *net.IP, *net.IPNet, error) {
	return nil, nil, nil, nil, nanokube.Unimplemented()
}

func (d *driver) RemoveNetwork(ctx context.Context, name string) error {
	return nanokube.Unimplemented()
}

func (d *driver) LogStream(containerID string, status *criv1.ContainerStatus) v1.LogStream {
	nanokube.Unimplemented()
	return nil
}

func (d *driver) WithBaseURL(baseURL *url.URL) v1.Driver {
	nanokube.Unimplemented()
	return d
}

func (d *driver) BaseURL() *url.URL {
	nanokube.Unimplemented()
	return nil
}

func (d *driver) WithNetwork(network v1.Network) v1.Driver {
	nanokube.Unimplemented()
	return d
}

func (d *driver) Network() v1.Network {
	nanokube.Unimplemented()
	return nil
}
