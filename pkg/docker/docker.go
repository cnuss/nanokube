package docker

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cnuss/nanokube/pkg"
	"github.com/cnuss/nanokube/pkg/nanokube"
	"github.com/containerd/errdefs"
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

	name     string
	nameOnce sync.Once

	cgroupRoot     string
	cgroupRootOnce sync.Once

	logStreams sync.Map // containerID -> *LogStream
}

var _ pkg.Driver = &driver{}

func (d *driver) Name() string {
	d.nameOnce.Do(func() {
		d.name = "docker"
		info, err := d.client.Info(d.Context())
		if err == nil {
			d.name = fmt.Sprintf("%s.%s", info.Name, d.name)
		}
	})
	return d.name
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
	cs, err := d.ContainerStatus(ctx, containerID, false)
	if err != nil {
		return nil, err
	}

	resp, err := d.client.ContainerStatsOneShot(ctx, containerID)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var stats container.StatsResponse
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return nil, fmt.Errorf("decode docker stats: %w", err)
	}

	ts := time.Now().UnixNano()

	return &v1.ContainerStats{
		Attributes: &v1.ContainerAttributes{
			Id:          containerID,
			Metadata:    cs.Status.Metadata,
			Labels:      cs.Status.Labels,
			Annotations: cs.Status.Annotations,
		},
		Cpu: &v1.CpuUsage{
			Timestamp:            ts,
			UsageCoreNanoSeconds: &v1.UInt64Value{Value: stats.CPUStats.CPUUsage.TotalUsage},
		},
		Memory: &v1.MemoryUsage{
			Timestamp:       ts,
			WorkingSetBytes: &v1.UInt64Value{Value: stats.MemoryStats.Usage},
		},
		WritableLayer: &v1.FilesystemUsage{
			Timestamp: ts,
		},
	}, nil
}

func (d *driver) ContainerStatus(ctx context.Context, containerID string, verbose bool) (*v1.ContainerStatusResponse, error) {
	inspect, err := d.client.ContainerInspect(ctx, containerID)
	if err != nil {
		return nil, err
	}

	createdAt, _ := time.Parse(time.RFC3339Nano, inspect.Created)

	var state v1.ContainerState
	switch inspect.State.Status {
	case "created":
		state = v1.ContainerState_CONTAINER_CREATED
	case "running":
		state = v1.ContainerState_CONTAINER_RUNNING
	default:
		state = v1.ContainerState_CONTAINER_EXITED
	}

	status := &v1.ContainerStatus{
		Id: inspect.ID,
		Metadata: &v1.ContainerMetadata{
			Name:    nanokube.TagName(d.Name(), inspect.Config.Labels),
			Attempt: nanokube.TagAttempt(d.Name(), inspect.Config.Labels),
		},
		State:       state,
		CreatedAt:   createdAt.UnixNano(),
		Image:       &v1.ImageSpec{Image: inspect.Config.Image},
		ImageRef:    inspect.Image,
		Labels:      nanokube.TagExtractLabels(d.Name(), inspect.Config.Labels),
		Annotations: nanokube.TagExtractAnnotations(d.Name(), inspect.Config.Labels),
	}

	if logPath := nanokube.TagLogPath(d.Name(), inspect.Config.Labels); logPath != "" {
		status.LogPath = logPath
	} else {
		status.LogPath = inspect.LogPath
	}

	if inspect.State.StartedAt != "" && inspect.State.StartedAt != "0001-01-01T00:00:00Z" {
		t, _ := time.Parse(time.RFC3339Nano, inspect.State.StartedAt)
		status.StartedAt = t.UnixNano()
	}
	if inspect.State.FinishedAt != "" && inspect.State.FinishedAt != "0001-01-01T00:00:00Z" {
		t, _ := time.Parse(time.RFC3339Nano, inspect.State.FinishedAt)
		status.FinishedAt = t.UnixNano()
	}
	if state == v1.ContainerState_CONTAINER_EXITED {
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
		status.Mounts = append(status.Mounts, &v1.Mount{
			ContainerPath: m.Destination,
			HostPath:      m.Source,
			Readonly:      !m.RW,
		})
	}

	resp := &v1.ContainerStatusResponse{Status: status}
	if verbose {
		resp.Info = map[string]string{
			"pid": fmt.Sprintf("%d", inspect.State.Pid),
		}
	}
	return resp, nil
}

func (d *driver) CreateContainer(ctx context.Context, podSandboxID string, config *v1.ContainerConfig, sandboxConfig *v1.PodSandboxConfig) (string, error) {
	meta := config.GetMetadata()

	sandbox, err := d.client.ContainerInspect(ctx, podSandboxID)
	if err != nil {
		return "", err
	}

	tb := nanokube.NewTagBuilder(d.Name()).
		WithType(nanokube.ResourceContainer).
		WithName(meta.GetName()).
		WithSandboxUID(podSandboxID).
		WithAttempt(meta.GetAttempt()).
		WithLabels(config.GetLabels()).
		WithAnnotations(config.GetAnnotations()).
		WithLogDirectory(nanokube.TagLogDirectory(d.Name(), sandbox.Config.Labels)).
		WithLogPath(config.GetLogPath())

	name, labels, err := tb.Build()
	if err != nil {
		return "", err
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
	dockerConfig.WorkingDir = config.GetWorkingDir()
	dockerConfig.Labels = labels
	dockerConfig.StdinOnce = config.GetStdinOnce()
	dockerConfig.OpenStdin = config.GetStdin()
	dockerConfig.Tty = config.GetTty()
	dockerConfig.Hostname = ""
	dockerConfig.ExposedPorts = nil

	// Environment variables
	dockerConfig.Env = make([]string, 0, len(config.GetEnvs()))
	for _, kv := range config.GetEnvs() {
		dockerConfig.Env = append(dockerConfig.Env, kv.GetKey()+"="+kv.GetValue())
	}

	// User: CRI security context, falling back to pod-level annotation
	sc := config.GetLinux().GetSecurityContext()
	if sc.GetRunAsGroup() != nil && sc.GetRunAsUser() == nil {
		return "", fmt.Errorf("RunAsGroup requires RunAsUser")
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
		user = nanokube.TagSecurityContext(d.Name(), nanokube.TagExtractAnnotations(d.Name(), sandbox.Config.Labels))
	}
	dockerConfig.User = user

	// Namespaces: share network/ipc with sandbox, pid per container by default
	hostConfig.NetworkMode = container.NetworkMode("container:" + podSandboxID)
	hostConfig.IpcMode = container.IpcMode("container:" + podSandboxID)
	if sc.GetNamespaceOptions().GetPid() == v1.NamespaceMode_CONTAINER {
		hostConfig.PidMode = ""
	} else {
		hostConfig.PidMode = container.PidMode("container:" + podSandboxID)
	}

	// Clear sandbox-specific settings
	hostConfig.Binds = nil
	hostConfig.PortBindings = nil
	hostConfig.ExtraHosts = nil
	hostConfig.DNS = nil
	hostConfig.DNSSearch = nil
	hostConfig.DNSOptions = nil
	hostConfig.LogConfig = container.LogConfig{}

	// Resources
	hostConfig.Resources = container.Resources{
		CPUShares:  config.GetLinux().GetResources().GetCpuShares(),
		Memory:     config.GetLinux().GetResources().GetMemoryLimitInBytes(),
		MemorySwap: config.GetLinux().GetResources().GetMemorySwapLimitInBytes(),
		CPUQuota:   config.GetLinux().GetResources().GetCpuQuota(),
		CPUPeriod:  config.GetLinux().GetResources().GetCpuPeriod(),
	}

	// Security
	hostConfig.Privileged = sc.GetPrivileged()
	hostConfig.ReadonlyRootfs = sc.GetReadonlyRootfs()
	hostConfig.ReadonlyPaths = sc.GetReadonlyPaths()
	hostConfig.MaskedPaths = sc.GetMaskedPaths()
	hostConfig.GroupAdd = nil
	for _, gid := range sc.GetSupplementalGroups() {
		hostConfig.GroupAdd = append(hostConfig.GroupAdd, strconv.FormatInt(gid, 10))
	}
	hostConfig.CapAdd = append([]string{}, sc.GetCapabilities().GetAddCapabilities()...)
	hostConfig.CapDrop = append([]string{}, sc.GetCapabilities().GetDropCapabilities()...)
	// TODO(partial): port securityOpts (seccomp profile handling)
	if sc.GetNoNewPrivs() {
		hostConfig.SecurityOpt = append(hostConfig.SecurityOpt, "no-new-privileges")
	}

	// Mounts: resolve symlinks so Docker doesn't fail trying to mkdir over them
	for _, m := range config.GetMounts() {
		hostPath := m.GetHostPath()
		if resolved, err := filepath.EvalSymlinks(hostPath); err == nil {
			hostPath = resolved
		}
		bind := hostPath + ":" + m.GetContainerPath()
		if m.GetReadonly() {
			bind += ":ro"
		}
		hostConfig.Binds = append(hostConfig.Binds, bind)
	}

	// Log config
	if config.GetLogPath() != "" {
		hostConfig.LogConfig = container.LogConfig{
			Type:   "json-file",
			Config: map[string]string{"max-size": "10m", "max-file": "3"},
		}
	}

	resp, err := d.client.ContainerCreate(ctx, dockerConfig, hostConfig, nil, nil, name)
	for err != nil && errdefs.IsConflict(err) {
		tb = tb.Clone().IncrementAttempt()
		name, labels, err = tb.Build()
		dockerConfig.Labels = labels
		resp, err = d.client.ContainerCreate(ctx, dockerConfig, hostConfig, nil, nil, name)
	}
	if err != nil {
		return "", err
	}

	return resp.ID, nil
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
	ref := image.GetImage()
	if ref == "" {
		return &v1.ImageStatusResponse{}, nil
	}

	inspect, err := d.client.ImageInspect(ctx, ref)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return &v1.ImageStatusResponse{}, nil
		}
		return nil, err
	}

	var repoTags []string
	for _, t := range inspect.RepoTags {
		if t != "<none>:<none>" && !strings.Contains(t, "@") {
			repoTags = append(repoTags, t)
		}
	}

	img := &v1.Image{
		Id:          inspect.ID,
		RepoTags:    repoTags,
		RepoDigests: inspect.RepoDigests,
		Size:        uint64(inspect.Size),
	}

	if inspect.Config != nil && inspect.Config.User != "" {
		userPart, _, _ := strings.Cut(inspect.Config.User, ":")
		if uid, err := strconv.ParseInt(userPart, 10, 64); err == nil {
			img.Uid = &v1.Int64Value{Value: uid}
		} else {
			img.Username = userPart
		}
	}

	resp := &v1.ImageStatusResponse{Image: img}
	if verbose {
		resp.Info = map[string]string{
			"architecture": inspect.Architecture,
			"os":           inspect.Os,
		}
	}
	return resp, nil
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
	tb := nanokube.NewTagBuilder(d.Name()).WithType(nanokube.ResourceContainer)
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
			f.Add("label", nanokube.TagSandboxUIDFilter(d.Name(), filter.PodSandboxId))
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
			PodSandboxId: nanokube.TagSandboxUID(prefix, c.Labels),
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
	tb := nanokube.NewTagBuilder(d.Name()).WithType(nanokube.ResourceSandbox)
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
	inspect, err := d.client.ContainerInspect(ctx, podSandboxID)
	if err != nil {
		return nil, err
	}

	createdAt, _ := time.Parse(time.RFC3339Nano, inspect.Created)

	nsOpts := &v1.NamespaceOption{
		Network: v1.NamespaceMode_POD,
		Pid:     v1.NamespaceMode_CONTAINER,
		Ipc:     v1.NamespaceMode_POD,
	}
	if inspect.HostConfig != nil {
		if inspect.HostConfig.NetworkMode == "host" {
			nsOpts.Network = v1.NamespaceMode_NODE
		}
		if inspect.HostConfig.PidMode == "host" {
			nsOpts.Pid = v1.NamespaceMode_NODE
		}
		if inspect.HostConfig.IpcMode == "host" {
			nsOpts.Ipc = v1.NamespaceMode_NODE
		}
	}

	podState := v1.PodSandboxState_SANDBOX_NOTREADY
	if inspect.State.Status == "running" {
		podState = v1.PodSandboxState_SANDBOX_READY
	}

	// Determine primary IP
	var ip string
	if inspect.HostConfig != nil && inspect.HostConfig.NetworkMode == "host" {
		ip = "127.0.0.1"
	} else if inspect.NetworkSettings != nil {
		if len(nanokube.TagExtractAnnotations(d.Name(), inspect.Config.Labels)) > 0 {
			for name, n := range inspect.NetworkSettings.Networks {
				if name != "bridge" && name != "host" && name != "none" && n.IPAddress != "" {
					ip = n.IPAddress
					break
				}
			}
		}
		if ip == "" {
			for _, bindings := range inspect.NetworkSettings.Ports {
				for _, b := range bindings {
					if b.HostIP == "" || b.HostIP == "0.0.0.0" {
						ip = "127.0.0.1"
						break
					}
				}
				if ip != "" {
					break
				}
			}
		}
		if ip == "" {
			for _, n := range inspect.NetworkSettings.Networks {
				if n.IPAddress != "" {
					ip = n.IPAddress
					break
				}
			}
		}
	}

	// Additional IPs
	var additionalIPs []*v1.PodIP
	if inspect.NetworkSettings != nil {
		for name, n := range inspect.NetworkSettings.Networks {
			if name == "host" || name == "none" || n.IPAddress == "" {
				continue
			}
			additionalIPs = append(additionalIPs, &v1.PodIP{Ip: n.IPAddress})
		}
	}

	status := &v1.PodSandboxStatus{
		Id: inspect.ID,
		Metadata: &v1.PodSandboxMetadata{
			Name:      nanokube.TagName(d.Name(), inspect.Config.Labels),
			Namespace: nanokube.TagNamespace(d.Name(), inspect.Config.Labels),
			Uid:       nanokube.TagUID(d.Name(), inspect.Config.Labels),
		},
		State:     podState,
		CreatedAt: createdAt.UnixNano(),
		Network: &v1.PodSandboxNetworkStatus{
			Ip:            ip,
			AdditionalIps: additionalIPs,
		},
		Linux: &v1.LinuxPodSandboxStatus{
			Namespaces: &v1.Namespace{
				Options: nsOpts,
			},
		},
		Labels:      nanokube.TagExtractLabels(d.Name(), inspect.Config.Labels),
		Annotations: nanokube.TagExtractAnnotations(d.Name(), inspect.Config.Labels),
	}

	resp := &v1.PodSandboxStatusResponse{Status: status}
	if verbose {
		resp.Info = map[string]string{
			"pid": fmt.Sprintf("%d", inspect.State.Pid),
		}
	}
	return resp, nil
}

func (d *driver) PortForward(ctx context.Context, request *v1.PortForwardRequest) (*v1.PortForwardResponse, error) {
	return nil, nanokube.Unimplemented()
}

func (d *driver) PullImage(ctx context.Context, img *v1.ImageSpec, auth *v1.AuthConfig, podSandboxConfig *v1.PodSandboxConfig) (string, error) {
	reader, err := d.client.ImagePull(ctx, img.GetImage(), image.PullOptions{})
	if err != nil {
		return "", err
	}
	defer reader.Close()
	io.Copy(io.Discard, reader)

	status, err := d.ImageStatus(ctx, img, false)
	if err != nil {
		return "", err
	}
	if status.Image == nil {
		return "", fmt.Errorf("image %s not found after pull", img.GetImage())
	}
	return status.Image.Id, nil
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

func (d *driver) ReopenContainerLog(ctx context.Context, containerID string) error {
	logStream := d.LogStream(containerID)
	if logStream == nil {
		return fmt.Errorf("container %s log stream not found", containerID)
	}
	logStream.Stop()
	logStream.Start()
	return nil
}

func (d *driver) RunPodSandbox(ctx context.Context, config *v1.PodSandboxConfig, runtimeHandler string) (string, error) {
	meta := config.GetMetadata()

	name, labels, err := nanokube.NewTagBuilder(d.Name()).
		WithType(nanokube.ResourceSandbox).WithName(meta.GetName()).WithNamespace(meta.GetNamespace()).WithUID(meta.GetUid()).
		WithLabels(config.GetLabels()).WithAnnotations(config.GetAnnotations()).
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
	if err := d.client.ContainerStart(ctx, containerID, container.StartOptions{}); err != nil {
		return err
	}
	// TODO(incomplete): StartLogs(containerID) — stream container logs to pod log directory
	return nil
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
	logStream := d.LogStream(containerID)
	if logStream != nil {
		logStream.Destroy()
	}

	t := int(timeout)
	err := d.client.ContainerStop(ctx, containerID, container.StopOptions{Timeout: &t})
	if err != nil && !errdefs.IsNotFound(err) {
		return err
	}

	return nil
}

func (d *driver) StopPodSandbox(ctx context.Context, podSandboxID string) error {
	containers, err := d.ListContainers(ctx, &v1.ContainerFilter{PodSandboxId: podSandboxID})
	if err != nil {
		return err
	}
	for _, c := range containers {
		d.StopContainer(ctx, c.Id, 0)
	}
	return d.StopContainer(ctx, podSandboxID, 0)
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

	netName, netLabels, err := nanokube.NewTagBuilder(d.Name()).WithType(nanokube.ResourceNetwork).WithName(name).Build()
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
	netName, _, err := nanokube.NewTagBuilder(d.Name()).WithType(nanokube.ResourceNetwork).WithName(name).Build()
	if err != nil {
		return nil, nil, nil, nil, err
	}
	resp, err := d.client.NetworkInspect(ctx, netName, network.InspectOptions{Verbose: true})
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil, nil, nil, nil, nil
		}
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
	netName, _, err := nanokube.NewTagBuilder(d.Name()).WithType(nanokube.ResourceNetwork).WithName(name).Build()
	if err != nil {
		return err
	}
	return d.client.NetworkRemove(ctx, netName)
}

func (d *driver) LogStream(containerID string) *nanokube.LogStream {
	if logStream, ok := d.logStreams.Load(containerID); ok {
		return logStream.(*nanokube.LogStream)
	}

	inspect, err := d.client.ContainerInspect(d.Context(), containerID)
	if err != nil {
		return nil
	}

	var ctx context.Context = d.Context()
	var reader io.ReadCloser
	var readerCancel context.CancelFunc
	crioutR, crioutW := io.Pipe()
	stdoutR, stdoutW := io.Pipe()
	stderrR, stderrW := io.Pipe()
	var started, stopped, destroyed atomic.Bool = atomic.Bool{}, atomic.Bool{}, atomic.Bool{}

	stop := func() {
		if !started.Load() || stopped.Load() || destroyed.Load() {
			return
		}
		stopped.Store(true)
		if readerCancel != nil {
			readerCancel()
		}
		if reader != nil {
			reader.Close()
		}
	}

	start := func() {
		if destroyed.Load() {
			return
		}
		started.Store(true)
		stopped.Store(false)

		ctx, readerCancel = context.WithCancel(ctx)
		reader, err = d.client.ContainerLogs(ctx, inspect.ID, container.LogsOptions{
			ShowStdout: true,
			ShowStderr: true,
			Follow:     true,
			Timestamps: true,
		})
		if err != nil {
			stop()
			return
		}

		go func() {
			defer stop()

			// Docker multiplexes stdout/stderr with an 8-byte header per frame:
			// [stream_type(1)][0][0][0][size(4 big-endian)]
			hdr := make([]byte, 8)
			for {
				if _, err := io.ReadFull(reader, hdr); err != nil {
					return
				}

				streamType := "stdout"
				if hdr[0] == 2 {
					streamType = "stderr"
				}

				size := binary.BigEndian.Uint32(hdr[4:8])
				if size == 0 {
					continue
				}

				buf := make([]byte, size)
				if _, err := io.ReadFull(reader, buf); err != nil {
					return
				}

				line := string(buf)
				switch streamType {
				case "stdout":
					if _, err := stdoutW.Write([]byte(line)); err != nil {
						return
					}
				case "stderr":
					if _, err := stderrW.Write([]byte(line)); err != nil {
						return
					}
				}

				// Normalize to "[ts] [stdout|stderr] F message\n" format for easier parsing by log processors.
				ts := time.Now().Format(time.RFC3339Nano)
				msg := line
				if before, after, ok := strings.Cut(line, " "); ok {
					ts = before
					msg = after
				}

				if _, err := time.Parse(time.RFC3339Nano, ts); err != nil {
					ts = time.Now().Format(time.RFC3339Nano)
				}

				if !strings.HasSuffix(msg, "\n") {
					msg += "\n"
				}

				if _, err := fmt.Fprintf(crioutW, "%s %s F %s", ts, streamType, msg); err != nil {
					return
				}
			}
		}()
	}

	destroy := func() {
		if destroyed.Load() {
			return
		}
		stop()
		destroyed.Store(true)
		crioutW.Close()
		stdoutW.Close()
		stderrW.Close()
		d.logStreams.Delete(containerID)
	}

	stream := &nanokube.LogStream{
		Criout:  crioutR,
		Stdout:  stdoutR,
		Stderr:  stderrR,
		Stop:    stop,
		Start:   start,
		Destroy: destroy,
	}

	d.logStreams.Store(containerID, stream)
	return stream
}
