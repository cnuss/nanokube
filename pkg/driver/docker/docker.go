package docker

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cnuss/nanokube/pkg"
	"github.com/cnuss/nanokube/pkg/nanokube"
	"github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/api/types/volume"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
	"github.com/emicklei/go-restful/v3"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	criv1 "k8s.io/cri-api/pkg/apis/runtime/v1"
	tools "k8s.io/cri-streaming/pkg/streaming/remotecommand"
	utilexec "k8s.io/utils/exec"

	v1 "github.com/cnuss/nanokube/pkg/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/klog/v2"
)

// streamIdleTimeout matches upstream cri/streaming server's default.
const streamIdleTimeout = 4 * time.Hour

func Detect(ctx context.Context) v1.BackendFunc {
	return func(nano v1.Nanokube) v1.Backend {
		home, _ := os.UserHomeDir()

		// TODO: Windows support
		// TODO: DOCKER_HOST env var support
		// TODO: Port number support
		// TODO: Split Colima/Lima/RD into separate Detect functions
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
			if _, err := os.Stat(socket); err != nil {
				klog.V(2).InfoS("docker socket not present", "socket", socket, "error", err)
				continue
			}
			backend, err := NewBackend(ctx, nano, socket)
			if err != nil {
				klog.ErrorS(err, "docker socket present but unusable", "socket", socket)
				continue
			}
			klog.InfoS("docker socket accepted", "socket", socket)
			return backend
		}

		return nil
	}
}

func NewBackend(ctx context.Context, nano v1.Nanokube, socket string) (v1.Backend, error) {
	dclient, err := newClient(ctx, socket)
	if err != nil {
		return nil, err
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if _, err := dclient.Ping(pingCtx); err != nil {
		return nil, err
	}

	return pkg.NewBackend(v1.DockerBackend, &driver{
		nano:            nano,
		ctx:             ctx,
		client:          dclient,
		baseURLProvided: make(chan struct{}),
		streamsProvided: make(chan struct{}),
		networkProvided: make(chan struct{}),
	}, nano), nil
}

type driver struct {
	nano   v1.Nanokube
	ctx    context.Context
	client *client.Client

	name     string
	nameOnce sync.Once

	cgroupRoot     string
	cgroupRootOnce sync.Once

	logStreams sync.Map // containerID -> *LogStream

	streams         nanokube.Streams
	streamsOnce     sync.Once
	streamsProvided chan struct{}

	baseURL         *url.URL
	baseURLOnce     sync.Once
	baseURLProvided chan struct{}

	network         v1.Network
	networkOnce     sync.Once
	networkProvided chan struct{}
}

var _ v1.Driver = &driver{}

func (d *driver) Name() string {
	d.nameOnce.Do(func() {
		d.name = "unknown"
		if info, err := d.client.Info(d.Context()); err == nil {
			d.name = info.Name
		}
	})
	return d.name
}

func (d *driver) Context() context.Context {
	return d.nano
}

func (d *driver) Service() *restful.WebService {
	<-d.baseURLProvided
	d.streamsOnce.Do(func() {
		d.streams = nanokube.NewStreams(d)
		close(d.streamsProvided)
	})
	return d.streams.Service()
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

func (d *driver) Attach(ctx context.Context, req *criv1.AttachRequest) (*criv1.AttachResponse, error) {
	<-d.streamsProvided
	return &criv1.AttachResponse{
		Url: d.streams.New().WithAttach(req, func(ctx context.Context, stream nanokube.Stream, stdin bool, in io.Reader, stdout bool, out io.WriteCloser, stderr bool, err io.WriteCloser, resize <-chan tools.TerminalSize) <-chan nanokube.Done {
			ctx, cancel := context.WithCancel(ctx)
			done := make(chan nanokube.Done, 1)
			resizer := <-stream.Resizer(ctx, resize)

			go func() {
				defer close(done)
				defer resizer.Done()

				attachResp, e := d.client.ContainerAttach(ctx, req.GetContainerId(), container.AttachOptions{
					Stdin:  true,
					Stdout: true,
					Stderr: true,
					Stream: true,
				})
				if e != nil {
					done <- nanokube.Done{Cancel: cancel, Err: fmt.Errorf("attach: %w", e)}
					return
				}
				defer attachResp.Close()

				resizer.WithHandler(func(height, width uint) {
					d.client.ContainerResize(ctx, req.GetContainerId(), container.ResizeOptions{
						Height: height,
						Width:  width,
					})
				})

				cancel, e := stream.ProxyStream(ctx, req.GetTty(), stdin, in, stdout, out, stderr, err, &nanokube.Proxy{
					Conn:       attachResp.Conn,
					Reader:     attachResp.Reader,
					CloseWrite: attachResp.CloseWrite,
				})
				if e != nil {
					done <- nanokube.Done{Cancel: cancel, Err: fmt.Errorf("proxy stream: %w", e)}
					return
				}

				done <- nanokube.Done{Cancel: cancel}
			}()
			return done
		}).URL(),
	}, nil
}

func (d *driver) CheckpointContainer(ctx context.Context, options *criv1.CheckpointContainerRequest) error {
	return nanokube.Unimplemented()
}

func (d *driver) Close(ctx context.Context) error {
	return nanokube.Unimplemented()
}

func (d *driver) ContainerStats(ctx context.Context, containerID string) (*criv1.ContainerStats, error) {
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

	return &criv1.ContainerStats{
		Attributes: &criv1.ContainerAttributes{
			Id:          containerID,
			Metadata:    cs.Status.Metadata,
			Labels:      cs.Status.Labels,
			Annotations: cs.Status.Annotations,
		},
		Cpu: &criv1.CpuUsage{
			Timestamp:            ts,
			UsageCoreNanoSeconds: &criv1.UInt64Value{Value: stats.CPUStats.CPUUsage.TotalUsage},
		},
		Memory: &criv1.MemoryUsage{
			Timestamp:       ts,
			WorkingSetBytes: &criv1.UInt64Value{Value: stats.MemoryStats.Usage},
		},
		WritableLayer: &criv1.FilesystemUsage{
			Timestamp: ts,
		},
	}, nil
}

func (d *driver) ContainerStatus(ctx context.Context, containerID string, verbose bool) (*criv1.ContainerStatusResponse, error) {
	inspect, err := d.client.ContainerInspect(ctx, containerID)
	if err != nil {
		return nil, err
	}

	createdAt, _ := time.Parse(time.RFC3339Nano, inspect.Created)

	var state criv1.ContainerState
	switch inspect.State.Status {
	case "created":
		state = criv1.ContainerState_CONTAINER_CREATED
	case "running":
		state = criv1.ContainerState_CONTAINER_RUNNING
	default:
		state = criv1.ContainerState_CONTAINER_EXITED
	}

	tb := nanokube.NewTagBuilder(d).WithTags(inspect.Config.Labels)

	status := &criv1.ContainerStatus{
		Id: inspect.ID,
		Metadata: &criv1.ContainerMetadata{
			Name:    tb.Name(),
			Attempt: tb.Attempt(),
		},
		State:       state,
		CreatedAt:   createdAt.UnixNano(),
		Image:       &criv1.ImageSpec{Image: inspect.Config.Image},
		ImageRef:    inspect.Image,
		Labels:      tb.Labels(),
		Annotations: tb.Annotations(),
	}

	if logPath := tb.LogPath(); logPath != "" {
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
	if state == criv1.ContainerState_CONTAINER_EXITED {
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
		status.Mounts = append(status.Mounts, &criv1.Mount{
			ContainerPath: m.Destination,
			HostPath:      m.Source,
			Readonly:      !m.RW,
		})
	}

	resp := &criv1.ContainerStatusResponse{Status: status}
	if verbose {
		resp.Info = map[string]string{
			"sandboxID": tb.SandboxUID(),
			"pid":       fmt.Sprintf("%d", inspect.State.Pid),
		}
	}
	return resp, nil
}

func (d *driver) CreateContainer(ctx context.Context, podSandboxID string, config *criv1.ContainerConfig, sandboxConfig *criv1.PodSandboxConfig) (string, error) {
	meta := config.GetMetadata()

	sandbox, err := d.client.ContainerInspect(ctx, podSandboxID)
	if err != nil {
		return "", err
	}

	tb := nanokube.NewTagBuilder(d).
		WithType(nanokube.ResourceContainer).
		WithName(meta.GetName()).
		WithSandboxUID(podSandboxID).
		WithAttempt(meta.GetAttempt()).
		WithContainerConfig(config).
		WithPodSandboxConfig(sandboxConfig)

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

	// drop sandbox-inherited env so the workload image's PATH isn't shadowed
	dockerConfig.Env = nil
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
		user = nanokube.NewTagBuilder(d).WithTags(sandbox.Config.Labels).SecurityContext()
	}
	dockerConfig.User = user

	// Namespaces: share network/ipc with sandbox, pid per container by default
	hostConfig.NetworkMode = container.NetworkMode("container:" + podSandboxID)
	hostConfig.IpcMode = container.IpcMode("container:" + podSandboxID)
	if sc.GetNamespaceOptions().GetPid() == criv1.NamespaceMode_CONTAINER {
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

func (d *driver) Exec(ctx context.Context, request *criv1.ExecRequest) (*criv1.ExecResponse, error) {
	<-d.streamsProvided
	return &criv1.ExecResponse{
		Url: d.streams.New().WithExec(request, func(ctx context.Context, stream nanokube.Stream, stdin bool, in io.Reader, stdout bool, out io.WriteCloser, stderr bool, err io.WriteCloser, resize <-chan tools.TerminalSize, timeout time.Duration) <-chan nanokube.Done {
			ctx, cancel := context.WithCancel(ctx)
			done := make(chan nanokube.Done, 1)
			resizer := <-stream.Resizer(ctx, resize)

			go func() {
				defer close(done)
				defer resizer.Done()

				createResp, e := d.client.ContainerExecCreate(ctx, request.GetContainerId(), container.ExecOptions{
					Cmd:          request.GetCmd(),
					AttachStdin:  true,
					AttachStdout: true,
					AttachStderr: true,
					Tty:          request.GetTty(),
					ConsoleSize:  resizer.ConsoleSize(),
				})
				if e != nil {
					done <- nanokube.Done{Cancel: cancel, Err: fmt.Errorf("exec create: %w", e)}
					return
				}

				attachResp, e := d.client.ContainerExecAttach(ctx, createResp.ID, container.ExecAttachOptions{
					Tty: request.GetTty(),
					ConsoleSize: resizer.WithHandler(func(height, width uint) {
						d.client.ContainerExecResize(ctx, createResp.ID, container.ResizeOptions{
							Height: height,
							Width:  width,
						})
					}).ConsoleSize(),
				})
				if e != nil {
					done <- nanokube.Done{Cancel: cancel, Err: fmt.Errorf("exec attach: %w", e)}
					return
				}
				defer attachResp.Close()

				cancel, e := stream.ProxyStream(ctx, request.GetTty(), request.GetStdin(), in, request.GetStdout(), out, request.GetStderr(), err, &nanokube.Proxy{
					Conn:       attachResp.Conn,
					Reader:     attachResp.Reader,
					CloseWrite: attachResp.CloseWrite,
				})
				if e != nil {
					done <- nanokube.Done{Cancel: cancel, Err: fmt.Errorf("proxy stream: %w", e)}
					return
				}

				inspect, e := d.client.ContainerExecInspect(ctx, createResp.ID)
				if e != nil {
					done <- nanokube.Done{Cancel: cancel, Err: fmt.Errorf("exec inspect: %w", e)}
					return
				}

				if inspect.ExitCode != 0 {
					done <- nanokube.Done{Cancel: cancel, Code: int(inspect.ExitCode), Err: fmt.Errorf("command exited with code %d", inspect.ExitCode)}
					return
				}

				done <- nanokube.Done{Cancel: cancel, Code: 0}
			}()

			return done
		}).URL(),
	}, nil
}

func (d *driver) ExecSync(ctx context.Context, containerID string, cmd []string, timeout time.Duration) ([]byte, []byte, error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	if len(cmd) > 0 && cmd[0] == v1.SandboxExecSentinel {
		cmd = cmd[1:]
		status, err := d.ContainerStatus(ctx, containerID, true)
		if err != nil {
			return nil, nil, fmt.Errorf("lookup container status: %w", err)
		}
		sandboxID := status.GetInfo()["sandboxID"]
		if sandboxID == "" {
			return nil, nil, fmt.Errorf("no sandbox for container %s", containerID)
		}
		containerID = sandboxID
	}

	exec, err := d.client.ContainerExecCreate(ctx, containerID, container.ExecOptions{
		Cmd: cmd, AttachStdout: true, AttachStderr: true,
	})
	if err != nil {
		return nil, nil, err
	}

	attach, err := d.client.ContainerExecAttach(ctx, exec.ID, container.ExecAttachOptions{})
	if err != nil {
		return nil, nil, err
	}
	defer attach.Close()

	var stdout, stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdout, &stderr, attach.Reader); err != nil && ctx.Err() == nil {
		return stdout.Bytes(), stderr.Bytes(), err
	}
	if ctx.Err() != nil {
		return stdout.Bytes(), stderr.Bytes(), fmt.Errorf("%w: %q timed out after %s", context.DeadlineExceeded, strings.Join(cmd, " "), timeout)
	}

	inspect, err := d.client.ContainerExecInspect(ctx, exec.ID)
	if err != nil {
		return stdout.Bytes(), stderr.Bytes(), err
	}
	if inspect.ExitCode != 0 {
		return stdout.Bytes(), stderr.Bytes(),
			utilexec.CodeExitError{
				Err:  fmt.Errorf("command %q exited with %d: %s", strings.Join(cmd, " "), inspect.ExitCode, stderr.String()),
				Code: inspect.ExitCode,
			}
	}
	return stdout.Bytes(), stderr.Bytes(), nil
}

func (d *driver) GetContainerEvents(ctx context.Context, containerEventsCh chan *criv1.ContainerEventResponse, connectionEstablishedCallback func(criv1.RuntimeService_GetContainerEventsClient)) error {
	return nanokube.Unimplemented()
}

func (d *driver) ImageFsInfo(ctx context.Context) (*criv1.ImageFsInfoResponse, error) {
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
	return &criv1.ImageFsInfoResponse{
		ImageFilesystems: []*criv1.FilesystemUsage{
			{
				FsId:      &criv1.FilesystemIdentifier{Mountpoint: info.DockerRootDir},
				UsedBytes: &criv1.UInt64Value{Value: totalSize},
			},
		},
	}, nil
}

func (d *driver) ImageStatus(ctx context.Context, image *criv1.ImageSpec, verbose bool) (*criv1.ImageStatusResponse, error) {
	ref := image.GetImage()
	if ref == "" {
		return &criv1.ImageStatusResponse{}, nil
	}

	inspect, err := d.client.ImageInspect(ctx, ref)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return &criv1.ImageStatusResponse{}, nil
		}
		return nil, err
	}

	var repoTags []string
	for _, t := range inspect.RepoTags {
		if t != "<none>:<none>" && !strings.Contains(t, "@") {
			repoTags = append(repoTags, t)
		}
	}

	img := &criv1.Image{
		Id:          inspect.ID,
		RepoTags:    repoTags,
		RepoDigests: inspect.RepoDigests,
		Size:        uint64(inspect.Size),
	}

	if inspect.Config != nil && inspect.Config.User != "" {
		userPart, _, _ := strings.Cut(inspect.Config.User, ":")
		if uid, err := strconv.ParseInt(userPart, 10, 64); err == nil {
			img.Uid = &criv1.Int64Value{Value: uid}
		} else {
			img.Username = userPart
		}
	}

	resp := &criv1.ImageStatusResponse{Image: img}
	if verbose {
		resp.Info = map[string]string{
			"architecture": inspect.Architecture,
			"os":           inspect.Os,
		}
	}
	return resp, nil
}

func (d *driver) ListContainerStats(ctx context.Context, filter *criv1.ContainerStatsFilter) ([]*criv1.ContainerStats, error) {
	containers, err := d.ListContainers(ctx, &criv1.ContainerFilter{
		Id:            filter.GetId(),
		PodSandboxId:  filter.GetPodSandboxId(),
		LabelSelector: filter.GetLabelSelector(),
	})
	if err != nil {
		return nil, err
	}

	var stats []*criv1.ContainerStats
	for _, c := range containers {
		s, err := d.ContainerStats(ctx, c.Id)
		if err != nil {
			continue
		}
		stats = append(stats, s)
	}
	return stats, nil
}

func (d *driver) ListContainers(ctx context.Context, filter *criv1.ContainerFilter) ([]*criv1.Container, error) {
	tb := nanokube.NewTagBuilder(d).WithType(nanokube.ResourceContainer)
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
			f.Add("label", nanokube.NewTagBuilder(d).SandboxUIDFilter(filter.PodSandboxId))
		}
		if filter.State != nil {
			switch filter.State.State {
			case criv1.ContainerState_CONTAINER_CREATED:
				f.Add("status", container.StateCreated)
			case criv1.ContainerState_CONTAINER_RUNNING:
				f.Add("status", container.StateRunning)
			case criv1.ContainerState_CONTAINER_EXITED:
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
		return []*criv1.Container{}, err
	}

	selector := filter.GetLabelSelector()
	result := make([]*criv1.Container, 0, len(containers))
	for _, c := range containers {
		if !d.matchLabels(c.Labels, selector) {
			continue
		}
		tb := nanokube.NewTagBuilder(d).WithTags(c.Labels)
		var containerState criv1.ContainerState
		switch c.State {
		case "created":
			containerState = criv1.ContainerState_CONTAINER_CREATED
		case "running":
			containerState = criv1.ContainerState_CONTAINER_RUNNING
		default:
			containerState = criv1.ContainerState_CONTAINER_EXITED
		}
		result = append(result, &criv1.Container{
			Id:           c.ID,
			PodSandboxId: tb.SandboxUID(),
			Metadata: &criv1.ContainerMetadata{
				Name:    tb.Name(),
				Attempt: tb.Attempt(),
			},
			Image:       &criv1.ImageSpec{Image: c.Image},
			ImageRef:    c.ImageID,
			State:       containerState,
			CreatedAt:   c.Created * int64(time.Second),
			Labels:      tb.Labels(),
			Annotations: tb.Annotations(),
		})
	}

	return result, nil
}

func (d *driver) matchLabels(dockerLabels map[string]string, selector map[string]string) bool {
	if len(selector) == 0 {
		return true
	}
	unpacked := nanokube.NewTagBuilder(d).WithTags(dockerLabels).Labels()
	for k, v := range selector {
		if unpacked[k] == v || dockerLabels[k] == v {
			continue
		}
		return false
	}
	return true
}

func (d *driver) ListImages(ctx context.Context, filter *criv1.ImageFilter) ([]*criv1.Image, error) {
	images, err := d.client.ImageList(ctx, image.ListOptions{})
	if err != nil {
		return nil, err
	}
	result := make([]*criv1.Image, len(images))
	for i, img := range images {
		result[i] = &criv1.Image{
			Id:          img.ID,
			RepoTags:    img.RepoTags,
			RepoDigests: img.RepoDigests,
			Size:        uint64(img.Size),
		}
	}
	return result, nil
}

func (d *driver) ListMetricDescriptors(ctx context.Context) ([]*criv1.MetricDescriptor, error) {
	return []*criv1.MetricDescriptor{}, nanokube.Unimplemented()
}

func (d *driver) ListPodSandbox(ctx context.Context, filter *criv1.PodSandboxFilter) ([]*criv1.PodSandbox, error) {
	tb := nanokube.NewTagBuilder(d).WithType(nanokube.ResourceSandbox)
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
			if filter.State.State == criv1.PodSandboxState_SANDBOX_READY {
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
	var result []*criv1.PodSandbox
	for _, c := range containers {
		if !d.matchLabels(c.Labels, selector) {
			continue
		}
		tb := nanokube.NewTagBuilder(d).WithTags(c.Labels)
		podState := criv1.PodSandboxState_SANDBOX_NOTREADY
		if c.State == "running" {
			podState = criv1.PodSandboxState_SANDBOX_READY
		}
		result = append(result, &criv1.PodSandbox{
			Id: c.ID,
			Metadata: &criv1.PodSandboxMetadata{
				Name:      tb.Name(),
				Namespace: tb.Namespace(),
				Uid:       tb.UID(),
			},
			State:       podState,
			CreatedAt:   c.Created * int64(time.Second),
			Labels:      tb.Labels(),
			Annotations: tb.Annotations(),
		})
	}
	return result, nil
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
	inspect, err := d.client.ContainerInspect(ctx, podSandboxID)
	if err != nil {
		return nil, err
	}

	createdAt, _ := time.Parse(time.RFC3339Nano, inspect.Created)

	nsOpts := &criv1.NamespaceOption{
		Network: criv1.NamespaceMode_POD,
		Pid:     criv1.NamespaceMode_CONTAINER,
		Ipc:     criv1.NamespaceMode_POD,
	}
	if inspect.HostConfig != nil {
		if inspect.HostConfig.NetworkMode == "host" {
			nsOpts.Network = criv1.NamespaceMode_NODE
		}
		if inspect.HostConfig.PidMode == "host" {
			nsOpts.Pid = criv1.NamespaceMode_NODE
		}
		if inspect.HostConfig.IpcMode == "host" {
			nsOpts.Ipc = criv1.NamespaceMode_NODE
		}
	}

	podState := criv1.PodSandboxState_SANDBOX_NOTREADY
	if inspect.State.Status == "running" {
		podState = criv1.PodSandboxState_SANDBOX_READY
	}

	// Determine primary IP
	tb := nanokube.NewTagBuilder(d).WithTags(inspect.Config.Labels)

	var ip string
	if inspect.HostConfig != nil && inspect.HostConfig.NetworkMode == "host" {
		ip = "127.0.0.1"
	} else if inspect.NetworkSettings != nil {
		if len(tb.Annotations()) > 0 {
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
	var additionalIPs []*criv1.PodIP
	if inspect.NetworkSettings != nil {
		for name, n := range inspect.NetworkSettings.Networks {
			if ip == n.IPAddress || name == "host" || name == "none" || n.IPAddress == "" {
				continue
			}
			additionalIPs = append(additionalIPs, &criv1.PodIP{Ip: n.IPAddress})
		}
	}

	status := &criv1.PodSandboxStatus{
		Id: inspect.ID,
		Metadata: &criv1.PodSandboxMetadata{
			Name:      tb.Name(),
			Namespace: tb.Namespace(),
			Uid:       tb.UID(),
		},
		State:     podState,
		CreatedAt: createdAt.UnixNano(),
		Network: &criv1.PodSandboxNetworkStatus{
			Ip:            ip,
			AdditionalIps: additionalIPs,
		},
		Linux: &criv1.LinuxPodSandboxStatus{
			Namespaces: &criv1.Namespace{
				Options: nsOpts,
			},
		},
		Labels:      tb.Labels(),
		Annotations: tb.Annotations(),
	}

	resp := &criv1.PodSandboxStatusResponse{Status: status}
	if verbose {
		resp.Info = map[string]string{
			"pid": fmt.Sprintf("%d", inspect.State.Pid),
		}
	}
	return resp, nil
}

func (d *driver) PortForward(ctx context.Context, request *criv1.PortForwardRequest) (*criv1.PortForwardResponse, error) {
	<-d.streamsProvided
	<-d.networkProvided

	return &criv1.PortForwardResponse{
		Url: d.streams.New().WithForward(request, func(ctx context.Context, stream nanokube.Stream, port int32, closer io.ReadWriteCloser) <-chan nanokube.Done {
			done := make(chan nanokube.Done, 1)
			ctx, cancel := context.WithCancel(ctx)
			status, err := d.PodSandboxStatus(ctx, request.GetPodSandboxId(), false)
			if err != nil || status == nil {
				done <- nanokube.Done{Cancel: cancel, Err: fmt.Errorf("get pod sandbox status: %w", err)}
				return done
			}
			network, err := d.network.FromStatus(ctx, status.GetStatus())
			if err != nil {
				done <- nanokube.Done{Cancel: cancel, Err: fmt.Errorf("get network info: %w", err)}
				return done
			}
			go func() {
				klog.InfoS("port foraward", "network", network, "port", port)
				// TODO(incomplete): docker run --rm --network static-network.nanokube -p 1234:1234 alpine/socat TCP-LISTEN:1234,fork,reuseaddr TCP:172.18.0.1:8080
				// TODO(incomplete): figure out API Server doing SPDY, SPDY headers are stripped by cloudflare
				done <- nanokube.Done{Cancel: cancel, Err: nanokube.Unimplemented()}
			}()
			return done
		}).URL(),
	}, nil
}

func (d *driver) PullImage(ctx context.Context, img *criv1.ImageSpec, auth *criv1.AuthConfig, podSandboxConfig *criv1.PodSandboxConfig) (string, error) {
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
	if err := d.client.ContainerRemove(ctx, containerID, container.RemoveOptions{Force: true}); err != nil && !errdefs.IsNotFound(err) {
		return err
	}
	if logStream, ok := d.logStreams.Load(containerID); ok {
		logStream.(v1.LogStream).Stop()
	}
	return nil
}

func (d *driver) RemoveImage(ctx context.Context, image *criv1.ImageSpec) error {
	return nanokube.Unimplemented()
}

func (d *driver) RemovePodSandbox(ctx context.Context, podSandboxID string) error {
	// DEVNOTE: Best effort, ignores errors for complete cleanup attempt
	status := func() *criv1.PodSandboxStatus {
		resp, _ := d.PodSandboxStatus(ctx, podSandboxID, false)
		if resp == nil {
			return &criv1.PodSandboxStatus{}
		}
		return resp.GetStatus()
	}()

	containers, _ := d.ListContainers(ctx, &criv1.ContainerFilter{
		PodSandboxId: podSandboxID,
	})

	for _, c := range containers {
		d.RemoveContainer(ctx, c.Id)
	}

	d.RemoveContainer(ctx, podSandboxID)

	network, _ := d.Network().FromStatus(ctx, status)
	if network != nil {
		network.Deallocate()
	}

	return nil
}

func (d *driver) ReopenContainerLog(ctx context.Context, containerID string) error {
	if logStream, ok := d.logStreams.Load(containerID); ok {
		logStream.(v1.LogStream).Stop()
		logStream.(v1.LogStream).Start()
		return nil
	}
	return fmt.Errorf("container log stream not found for container %s", containerID)
}

func (d *driver) RunPodSandbox(ctx context.Context, config *criv1.PodSandboxConfig, runtimeHandler string) (string, error) {
	meta := config.GetMetadata()

	name, labels, err := nanokube.NewTagBuilder(d).
		WithType(nanokube.ResourceSandbox).
		WithName(meta.GetName()).
		WithNamespace(meta.GetNamespace()).
		WithUID(meta.GetUid()).
		WithPodSandboxConfig(config).
		Build()
	if err != nil {
		return "", err
	}

	var status *criv1.PodSandboxStatus

	existing, _ := d.ListPodSandbox(ctx, &criv1.PodSandboxFilter{
		LabelSelector: map[string]string{nanokube.NewTagBuilder(d).UIDKey(): meta.GetUid()},
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
			Domainname: meta.GetNamespace() + ".svc.cluster.local",
			Labels:     labels,
		}

		networkMode := container.NetworkMode("bridge")
		if linux := config.GetLinux(); linux != nil {
			if ns := linux.GetSecurityContext().GetNamespaceOptions(); ns != nil && ns.GetNetwork() == criv1.NamespaceMode_NODE {
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
					if ns.GetNetwork() == criv1.NamespaceMode_NODE {
						hostConfig.NetworkMode = "host"
					}
					if ns.GetPid() == criv1.NamespaceMode_NODE {
						hostConfig.PidMode = "host"
					}
					if ns.GetIpc() == criv1.NamespaceMode_NODE {
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
		configTb := nanokube.NewTagBuilder(d).WithTags(config.GetAnnotations())
		extraHosts := configTb.ExtraHosts()
		slices.Sort(extraHosts)
		hostConfig.ExtraHosts = slices.Compact(extraHosts)

		if networkMode != container.NetworkMode("host") {
			networks := []string{"bridge"}
			if networkStr, ok := config.GetAnnotations()[configTb.Key(nanokube.KeyNetwork)]; ok {
				networks = []string{networkStr}
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
		imageSpec := &criv1.ImageSpec{Image: dockerConfig.Image}
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

	if status.State != criv1.PodSandboxState_SANDBOX_READY {
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

func (d *driver) RuntimeConfig(ctx context.Context) (*criv1.RuntimeConfigResponse, error) {
	return &criv1.RuntimeConfigResponse{
		Linux: &criv1.LinuxRuntimeConfiguration{
			CgroupDriver: criv1.CgroupDriver_CGROUPFS,
		},
	}, nil
}

func (d *driver) StartContainer(ctx context.Context, containerID string) error {
	if err := d.client.ContainerStart(ctx, containerID, container.StartOptions{}); err != nil {
		return err
	}
	if logStream, ok := d.logStreams.Load(containerID); ok {
		logStream.(v1.LogStream).Start()
	}
	return nil
}

func (d *driver) Status(ctx context.Context, verbose bool) (*criv1.StatusResponse, error) {
	info, err := d.client.Info(ctx)
	_, netErr := d.client.NetworkInspect(ctx, "bridge", network.InspectOptions{})

	resp := &criv1.StatusResponse{
		Status: &criv1.RuntimeStatus{
			Conditions: []*criv1.RuntimeCondition{
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
	t := int(timeout)
	err := d.client.ContainerStop(ctx, containerID, container.StopOptions{Timeout: &t})
	if err != nil && !errdefs.IsNotFound(err) {
		return err
	}

	if logStream, ok := d.logStreams.Load(containerID); ok {
		logStream.(v1.LogStream).Stop()
	}

	return nil
}

func (d *driver) StopPodSandbox(ctx context.Context, podSandboxID string) error {
	containers, err := d.ListContainers(ctx, &criv1.ContainerFilter{PodSandboxId: podSandboxID})
	if err != nil {
		return err
	}
	for _, c := range containers {
		d.StopContainer(ctx, c.Id, 0)
	}
	return d.StopContainer(ctx, podSandboxID, 0)
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

func (d *driver) Version(ctx context.Context, version string) (*criv1.VersionResponse, error) {
	v, err := d.client.ServerVersion(ctx)
	if err != nil {
		return nil, err
	}
	return &criv1.VersionResponse{
		Version:           version,
		RuntimeName:       d.Name(),
		RuntimeVersion:    v.APIVersion,
		RuntimeApiVersion: "v1",
	}, nil
}

func (d *driver) ExecOnHost(ctx context.Context, img string, cmd []string, mounts []v1.Path) (string, error) {
	reader, err := d.client.ImagePull(ctx, img, image.PullOptions{})
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

	resp, err := d.client.ContainerCreate(ctx, &container.Config{
		Image:        img,
		Cmd:          cmd,
		AttachStdout: true,
		AttachStderr: true,
	}, hostConfig, nil, nil, "")
	if err != nil {
		return "", err
	}

	attach, err := d.client.ContainerAttach(ctx, resp.ID, container.AttachOptions{Stream: true, Stdout: true, Stderr: true})
	if err != nil {
		return "", err
	}
	defer attach.Close()

	if err := d.client.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return "", err
	}

	var stdout, stderr bytes.Buffer
	stdcopy.StdCopy(&stdout, &stderr, attach.Reader)

	waitCh, errCh := d.client.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)
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

func (d *driver) ExecOnNetwork(ctx context.Context, net v1.AllocatedNetwork, img string, cmd []string, portMap []v1.PortMap) (string, error) {
	reader, err := d.client.ImagePull(ctx, img, image.PullOptions{})
	if err != nil {
		return "", err
	}
	io.Copy(io.Discard, reader)
	reader.Close()

	hostConfig := &container.HostConfig{
		AutoRemove: true,
	}

	networkID := net.ID()
	networkConfig := &network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{
			networkID: {},
		},
	}

	resp, err := d.client.ContainerCreate(ctx, &container.Config{
		Image:        img,
		Cmd:          cmd,
		AttachStdout: true,
		AttachStderr: true,
	}, hostConfig, networkConfig, nil, "")
	if err != nil {
		return "", err
	}

	attach, err := d.client.ContainerAttach(ctx, resp.ID, container.AttachOptions{Stream: true, Stdout: true, Stderr: true})
	if err != nil {
		return "", err
	}
	defer attach.Close()

	if err := d.client.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return "", err
	}

	var stdout, stderr bytes.Buffer
	stdcopy.StdCopy(&stdout, &stderr, attach.Reader)

	waitCh, errCh := d.client.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)
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

func (d *driver) GetNetwork(ctx context.Context, id string) (*v1.NetworkType, *net.IP, *net.IPNet, error) {
	inspect, err := d.client.NetworkInspect(ctx, id, network.InspectOptions{})
	if err != nil {
		return nil, nil, nil, err
	}
	networkType := v1.NetworkType(inspect.Driver)
	config := inspect.IPAM.Config
	if len(config) == 0 || config[0].Subnet == "" {
		return nil, nil, nil, fmt.Errorf("invalid ipam config: %v", config)
	}
	_, parsed, err := net.ParseCIDR(config[0].Subnet)
	if err != nil {
		return nil, nil, nil, err
	}
	gateway := net.ParseIP(config[0].Gateway)
	return &networkType, &gateway, parsed, nil
}

func (d *driver) CreateNetwork(ctx context.Context, networkType v1.NetworkType, net *net.IPNet, gateway *net.IP) (string, error) {
	nameSuffix := string(networkType)
	if net != nil {
		nameSuffix = strings.ReplaceAll(net.IP.String(), ".", "-")
	}
	netName, netLabels, err := nanokube.NewTagBuilder(d).WithType(nanokube.ResourceNetwork).
		WithName(nameSuffix).
		Build()
	if err != nil {
		return "", err
	}
	createOptions := network.CreateOptions{
		Driver: string(networkType),
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

	created, err := d.client.NetworkCreate(ctx, netName, createOptions)
	if err != nil {
		return "", err
	}
	return created.ID, nil
}

func (d *driver) RemoveNetwork(ctx context.Context, id string) error {
	return d.client.NetworkRemove(ctx, id)
}

func (d *driver) LogStream(containerID string, status *criv1.ContainerStatus) v1.LogStream {
	if logStream, ok := d.logStreams.Load(containerID); ok {
		return logStream.(v1.LogStream)
	}

	inspect, err := d.client.ContainerInspect(d.Context(), containerID)
	if err != nil {
		return nil
	}

	source := func(ctx context.Context) (io.Reader, io.Reader, func(), error) {
		reader, err := d.client.ContainerLogs(ctx, inspect.ID, container.LogsOptions{
			ShowStdout: true,
			ShowStderr: true,
			Since:      "0",
			Follow:     true,
			Timestamps: true,
		})
		if err != nil {
			return nil, nil, nil, err
		}
		// Docker multiplexes stdout/stderr on one stream with an 8-byte header
		// per frame: [stream_type(1)][0][0][0][size(4 big-endian)]. Demux into
		// two pipes so LogStreamImpl can read each as its own line stream.
		stdoutR, stdoutW, err := os.Pipe()
		if err != nil {
			return nil, nil, nil, err
		}
		stderrR, stderrW, err := os.Pipe()
		if err != nil {
			return nil, nil, nil, err
		}
		go func() {
			defer stdoutW.Close()
			defer stderrW.Close()

			// Per-stream accumulators so frames that straddle line boundaries
			// still emit whole CRI-formatted lines to the respective pipes.
			var stdoutBuf, stderrBuf []byte

			flush := func(accum *[]byte, dst *os.File, streamType string) error {
				for {
					i := bytes.IndexByte(*accum, '\n')
					if i < 0 {
						return nil
					}
					line := (*accum)[:i+1]
					*accum = (*accum)[i+1:]

					// Docker with Timestamps:true prefixes each line with an RFC3339Nano ts.
					// Peel it off if present; stamp now as fallback.
					ts := time.Now().Format(time.RFC3339Nano)
					msg := string(line)
					if before, after, ok := strings.Cut(msg, " "); ok {
						if _, perr := time.Parse(time.RFC3339Nano, before); perr == nil {
							ts = before
							msg = after
						}
					}
					if !strings.HasSuffix(msg, "\n") {
						msg += "\n"
					}
					if _, err := fmt.Fprintf(dst, "%s %s F %s", ts, streamType, msg); err != nil {
						return err
					}
				}
			}

			hdr := make([]byte, 8)
			for {
				if _, err := io.ReadFull(reader, hdr); err != nil {
					return
				}
				size := binary.BigEndian.Uint32(hdr[4:8])
				if size == 0 {
					continue
				}
				payload := make([]byte, size)
				if _, err := io.ReadFull(reader, payload); err != nil {
					return
				}
				if hdr[0] == 2 {
					stderrBuf = append(stderrBuf, payload...)
					if err := flush(&stderrBuf, stderrW, "stderr"); err != nil {
						return
					}
				} else {
					stdoutBuf = append(stdoutBuf, payload...)
					if err := flush(&stdoutBuf, stdoutW, "stdout"); err != nil {
						return
					}
				}
			}
		}()
		return stdoutR, stderrR, func() { reader.Close() }, nil
	}

	stream := nanokube.NewLogStream(d.Context(), source, status)
	d.logStreams.Store(containerID, stream)
	return stream
}

func (d *driver) WithBaseURL(baseURL *url.URL) v1.Driver {
	d.baseURLOnce.Do(func() {
		d.baseURL = baseURL
		close(d.baseURLProvided)
	})
	return d
}

func (d *driver) BaseURL() *url.URL {
	<-d.baseURLProvided
	return d.baseURL
}

func (d *driver) WithNetwork(network v1.Network) v1.Driver {
	d.networkOnce.Do(func() {
		d.network = network
		close(d.networkProvided)
	})
	return d
}

func (d *driver) Network() v1.Network {
	<-d.networkProvided
	return d.network
}

func (d *driver) ClaimVolume(backend v1.Backend, client v1.Client, pvc *corev1.PersistentVolumeClaim) *corev1ac.PersistentVolumeClaimApplyConfiguration {
	name, labels, err := nanokube.NewTagBuilder(backend.Driver()).
		WithType(nanokube.ResourceVolume).
		WithName(fmt.Sprintf("pvc-%s-%s", pvc.Namespace, pvc.Name)).
		WithUID(string(pvc.UID)).
		Build()
	if err != nil {
		return nil
	}

	// TODO(incomplete): make sure there's enough space on the system for the requested volume size
	if pvc.Spec.StorageClassName == nil && pvc.Spec.VolumeName == "" {
		pv, err := client.CoreV1().PersistentVolumes().Apply(d.Context(), corev1ac.PersistentVolume(name).
			WithLabels(labels).
			WithSpec(corev1ac.PersistentVolumeSpec().
				WithStorageClassName("").
				WithCapacity(corev1.ResourceList{
					"storage": *pvc.Spec.Resources.Requests.Storage(),
				}).
				WithAccessModes(pvc.Spec.AccessModes...).
				WithPersistentVolumeReclaimPolicy(corev1.PersistentVolumeReclaimDelete). // TODO(premium): retain if paid
				WithLocal(corev1ac.LocalVolumeSource().
					WithFSType(string(backend.Name())). // So we can look up the backend in host.go's mounter setup/teardown
					WithPath(name)).                    // So we can look set a determinstic name in createvolume below
				WithNodeAffinity(corev1ac.VolumeNodeAffinity().
					WithRequired(corev1ac.NodeSelector().
						WithNodeSelectorTerms(corev1ac.NodeSelectorTerm().
							WithMatchExpressions(
								corev1ac.NodeSelectorRequirement().
									WithKey("kubernetes.io/hostname").
									WithOperator(corev1.NodeSelectorOpIn).
									// TODO(incomplete): better selection than just NodeRef.Name
									WithValues(backend.Nanokube().NodeRef().Name),
							))))),
			metav1.ApplyOptions{FieldManager: string(backend.Name())})
		if err != nil {
			klog.ErrorS(err, "failed to apply volume")
			return nil
		}

		return corev1ac.
			PersistentVolumeClaim(pvc.Name, pvc.Namespace).
			WithLabels(labels).
			WithSpec(corev1ac.PersistentVolumeClaimSpec().
				WithStorageClassName("").
				WithVolumeName(pv.Name))
	}

	return nil
}

func (d *driver) CreateVolume(lvs *corev1.LocalVolumeSource) error {
	if lvs == nil {
		return fmt.Errorf("local volume source is nil")
	}

	_, labels, err := nanokube.NewTagBuilder(d).
		WithType(nanokube.ResourceVolume).
		WithName(lvs.Path).
		Build()
	if err != nil {
		return err
	}

	_, err = d.client.VolumeCreate(d.Context(), volume.CreateOptions{
		Name:   lvs.Path,
		Labels: labels,
	})
	if err != nil {
		return err
	}

	return nil
}

func (d *driver) DeleteVolume(lvs *corev1.LocalVolumeSource) error {
	if lvs == nil {
		return fmt.Errorf("local volume source is nil")
	}

	err := d.client.VolumeRemove(d.Context(), lvs.Path, true)
	if err != nil && !errdefs.IsNotFound(err) {
		return err
	}

	return nil
}

func (d *driver) ReleaseVolume(backend v1.Backend, client v1.Client, pvc *corev1.PersistentVolumeClaim) error {
	name, _, err := nanokube.NewTagBuilder(backend.Driver()).
		WithType(nanokube.ResourceVolume).
		WithName(fmt.Sprintf("pvc-%s-%s", pvc.Namespace, pvc.Name)).
		WithUID(string(pvc.UID)).
		Build()
	if err != nil {
		return err
	}

	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != "" || pvc.Spec.VolumeName != name {
		// not ours
		return nil
	}

	pvs := client.CoreV1().PersistentVolumes()
	pv, err := func() (*corev1.PersistentVolume, error) {
		pv, err := pvs.Get(d.Context(), name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil, nil
			}
			return nil, err
		}

		if pv == nil {
			return nil, nil
		}

		if pv.Spec.Local != nil && pv.Spec.NodeAffinity != nil && pv.Spec.NodeAffinity.Required != nil {
			for _, term := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
				for _, expr := range term.MatchExpressions {
					if expr.Key == "kubernetes.io/hostname" && expr.Operator == corev1.NodeSelectorOpIn {
						for _, v := range expr.Values {
							// TODO(incomplete): better selection than just NodeRef.Name
							if v == backend.Nanokube().NodeRef().Name {
								return pv, nil
							}
						}
					}
				}
			}
		}

		return nil, nil
	}()
	if err != nil {
		return err
	}
	if pv == nil {
		return nil
	}

	if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain && pv.Spec.Local != nil {
		if err := d.DeleteVolume(pv.Spec.Local); err != nil {
			return err
		}
	}

	if err := pvs.Delete(d.Context(), name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	return nil
}
