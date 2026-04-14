package docker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cnuss/nanokube/pkg"
	"github.com/cnuss/nanokube/pkg/nanokube"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
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

	cgroupRoot     string
	cgroupRootOnce sync.Once
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

func (d *driver) CgroupRoot() string {
	d.cgroupRootOnce.Do(func() {
		info, err := d.client.Info(d.Context())
		if err != nil {
			d.cgroupRoot = "/"
			return
		}
		switch info.CgroupDriver {
		case "systemd":
			d.cgroupRoot = "/system.slice/docker.service"
		default:
			d.cgroupRoot = "/docker"
		}
	})
	return d.cgroupRoot
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
	containers, err := d.ListContainers(ctx, &v1.ContainerFilter{
		Id:            filter.GetId(),
		PodSandboxId:  filter.GetPodSandboxId(),
		LabelSelector: filter.GetLabelSelector(),
	})
	if err != nil {
		return nil, err
	}

	var stats []*v1.ContainerStats
	for _, c := range containers {
		s, err := d.ContainerStats(ctx, c.Id)
		if err != nil {
			continue
		}
		stats = append(stats, s)
	}
	return stats, nil
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
	meta := config.GetMetadata()

	name, labels, err := nanokube.NewTagBuilder(d.Name(), config.GetLabels()).
		WithType(nanokube.ResourceSandbox).WithName(meta.GetName()).WithNamespace(meta.GetNamespace()).WithUID(meta.GetUid()).
		WithAnnotations(config.GetAnnotations()).
		WithLogDirectory(config.GetLogDirectory()).
		Build()
	if err != nil {
		return "", err
	}

	var status *v1.PodSandboxStatus

	existing, _ := d.ListPodSandbox(ctx, &v1.PodSandboxFilter{
		LabelSelector: map[string]string{nanokube.TagUIDKey(d.Name()): meta.GetUid()},
	})

	for _, sb := range existing {
		if sb.GetAnnotations()["kubernetes.io/config.hash"] == config.GetAnnotations()["kubernetes.io/config.hash"] {
			resp, _ := d.PodSandboxStatus(ctx, sb.Id, true)
			if resp.GetStatus() != nil {
				status = resp.GetStatus()
			}
		} else {
			d.RemovePodSandbox(ctx, sb.Id)
		}
	}

	if status == nil {
		dockerConfig := &container.Config{
			Image:      "busybox", // TODO(partial): create nanokube/pause with minimal tooling
			Entrypoint: []string{"tail", "-f", "/dev/null"},
			Hostname:   config.GetHostname(),
			Labels:     labels,
		}

		networkMode := container.NetworkMode("bridge")
		if linux := config.GetLinux(); linux != nil {
			if ns := linux.GetSecurityContext().GetNamespaceOptions(); ns != nil && ns.GetNetwork() == v1.NamespaceMode_NODE {
				networkMode = container.NetworkMode("host")
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
				// TODO(partial): port securityOpts (seccomp profile handling)
				if ns := sc.GetNamespaceOptions(); ns != nil {
					if ns.GetNetwork() == v1.NamespaceMode_NODE {
						hostConfig.NetworkMode = "host"
					}
					if ns.GetPid() == v1.NamespaceMode_NODE {
						hostConfig.PidMode = "host"
					}
					if ns.GetIpc() == v1.NamespaceMode_NODE {
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

		// DEVNOTE: old impl also merged host-level extra hosts here:
		//   extraHosts := s.backend.parent.Hosts().ExtraHosts(ctx, networkMode)
		//   for _, extraHost := range s.backend.labels.ExtraHosts(config.GetAnnotations()) {
		//       extraHosts = append(extraHosts, extraHost)
		//   }
		//   slices.Sort(extraHosts)
		//   hostConfig.ExtraHosts = slices.Compact(extraHosts)
		extraHosts := nanokube.TagExtraHosts(d.Name(), config.GetAnnotations())
		slices.Sort(extraHosts)
		hostConfig.ExtraHosts = slices.Compact(extraHosts)

		if networkMode != container.NetworkMode("host") {
			networks := []string{"bridge"}
			if networksStr, ok := config.GetAnnotations()[nanokube.TagKey(d.Name(), nanokube.KeyNetworks)]; ok {
				networks = strings.Split(networksStr, ",")
			}

			if len(config.GetPortMappings()) > 0 {
				dockerConfig.ExposedPorts = nat.PortSet{}
				hostConfig.PortBindings = nat.PortMap{}
				for _, pm := range config.GetPortMappings() {
					containerPort := pm.GetContainerPort()
					hostPort := pm.GetHostPort()
					if hostPort == 0 && len(config.Annotations) == 0 {
						// If host port is not specified and we do not have any annotations, we're in critest
						// TODO(partial): log host port defaulting for critest compatibility
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
		imageSpec := &v1.ImageSpec{Image: dockerConfig.Image}
		image, err := d.ImageStatus(ctx, imageSpec, false)
		if err != nil || image.Image == nil {
			if _, err := d.PullImage(ctx, imageSpec, nil, nil); err != nil {
				return "", err
			}
			image, err = d.ImageStatus(ctx, imageSpec, false)
			if err != nil {
				return "", err
			}
		}

		// Platform from image info
		var platform *ocispec.Platform
		if image != nil && image.Info != nil {
			platform = &ocispec.Platform{OS: image.Info["os"], Architecture: image.Info["architecture"]}
		}

		created, err := d.client.ContainerCreate(ctx, dockerConfig, hostConfig, networkingConfig, platform, name)
		if err != nil {
			return "", err
		}

		resp, err := d.PodSandboxStatus(ctx, created.ID, true)
		if err != nil {
			return "", err
		}

		status = resp.GetStatus()
	}

	if status.State != v1.PodSandboxState_SANDBOX_READY {
		if err := d.client.ContainerStart(ctx, status.Id, container.StartOptions{}); err != nil {
			return "", err
		}

		resp, err := d.PodSandboxStatus(ctx, status.Id, true)
		if err != nil {
			return "", err
		}
		status = resp.GetStatus()
	}

	return status.Id, nil
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

func (d *driver) ExecHost(img string, cmd []string, mounts []nanokube.Path) (string, error) {
	reader, err := d.client.ImagePull(d.Context(), img, image.PullOptions{})
	if err != nil {
		return "", err
	}
	io.Copy(io.Discard, reader)
	reader.Close()

	hostConfig := &container.HostConfig{
		AutoRemove: true,
		Binds: func() []string {
			var binds []string
			for _, mount := range mounts {
				binds = append(binds, fmt.Sprintf("%s:/host%s", string(mount), string(mount)))
			}
			return binds
		}(),
		NetworkMode: "host",
		PidMode:     "host",
		IpcMode:     "host",
		Privileged:  true,
	}

	resp, err := d.client.ContainerCreate(d.Context(), &container.Config{
		Image:        img,
		Cmd:          cmd,
		AttachStdout: true,
		AttachStderr: true,
	}, hostConfig, nil, nil, "")
	if err != nil {
		return "", err
	}

	attach, err := d.client.ContainerAttach(d.Context(), resp.ID, container.AttachOptions{Stream: true, Stdout: true, Stderr: true})
	if err != nil {
		return "", err
	}
	defer attach.Close()

	if err := d.client.ContainerStart(d.Context(), resp.ID, container.StartOptions{}); err != nil {
		return "", err
	}

	var stdout, stderr bytes.Buffer
	stdcopy.StdCopy(&stdout, &stderr, attach.Reader)

	waitCh, errCh := d.client.ContainerWait(d.Context(), resp.ID, container.WaitConditionNotRunning)
	select {
	case result := <-waitCh:
		if result.StatusCode != 0 {
			return "", fmt.Errorf("exit code %d: %s", result.StatusCode, strings.TrimSpace(stderr.String()))
		}
	case err := <-errCh:
		if err != nil {
			return "", fmt.Errorf("waiting for container: %w", err)
		}
	}

	return strings.TrimSpace(stdout.String()), nil
}

func (d *driver) CreateNetwork(ctx context.Context, name string, networkType nanokube.NetworkType, net *net.IPNet, gateway *net.IP) error {
	existing, _, _, _, _ := d.GetNetwork(ctx, name)
	if existing != nil {
		return nil
	}

	netName, netLabels, err := nanokube.NewTagBuilder(d.Name(), nil).WithType(nanokube.ResourceNetwork).WithName(name).Build()
	if err != nil {
		return err
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

	_, err = d.client.NetworkCreate(ctx, netName, createOptions)
	return err
}

func (d *driver) DefaultNetwork(ctx context.Context) string {
	var resp network.Inspect
	var err error

	resp, err = d.client.NetworkInspect(ctx, "bridge", network.InspectOptions{Verbose: true})
	if err == nil {
		return resp.Name
	}

	resp, err = d.client.NetworkInspect(ctx, "host", network.InspectOptions{Verbose: true})
	if err == nil {
		return resp.Name
	}

	resp, err = d.client.NetworkInspect(ctx, "none", network.InspectOptions{Verbose: true})
	if err == nil {
		return resp.Name
	}

	nanokube.Log.Warn("failed to get default networks")
	return ""
}

func (d *driver) GetNetwork(ctx context.Context, name string) (*string, *nanokube.NetworkType, *net.IP, *net.IPNet, error) {
	netName, _, err := nanokube.NewTagBuilder(d.Name(), nil).WithType(nanokube.ResourceNetwork).WithName(name).Build()
	if err != nil {
		return nil, nil, nil, nil, err
	}
	resp, err := d.client.NetworkInspect(ctx, netName, network.InspectOptions{Verbose: true})
	if err != nil {
		return nil, nil, nil, nil, err
	}

	var networkType nanokube.NetworkType
	switch resp.Driver {
	case "bridge":
		networkType = nanokube.NetworkBridge
	case "host":
		networkType = nanokube.NetworkHost
	default:
		return nil, nil, nil, nil, fmt.Errorf("unsupported network driver: %s", resp.Driver)
	}

	isDefault := resp.Options["com.docker.network.bridge.default_bridge"] == "true"
	if !isDefault && !nanokube.TagIsManaged(d.Name(), resp.Labels) {
		return nil, nil, nil, nil, fmt.Errorf("network %s is not managed by nanokube", name)
	}

	var network net.IPNet
	var gateway net.IP

	if resp.IPAM.Config != nil {
		for _, cfg := range resp.IPAM.Config {
			if cfg.Subnet != "" && cfg.Gateway != "" {
				gateway = net.ParseIP(cfg.Gateway)
				if gateway == nil {
					continue
				}
				_, ipNet, err := net.ParseCIDR(cfg.Subnet)
				if err != nil {
					continue
				}
				network = *ipNet
				break
			}
		}
	}

	return &resp.Name, &networkType, &gateway, &network, nil
}

func (d *driver) RemoveNetwork(ctx context.Context, name string) error {
	netName, _, err := nanokube.NewTagBuilder(d.Name(), nil).WithType(nanokube.ResourceNetwork).WithName(name).Build()
	if err != nil {
		return err
	}
	return d.client.NetworkRemove(ctx, netName)
}
