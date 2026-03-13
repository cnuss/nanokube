package docker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/cnuss/nanokube/pkg/crid/backend"
	"github.com/cnuss/nanokube/pkg/crid/labels"
	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	dockertypes "github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	dockervolume "github.com/docker/docker/api/types/volume"
	dockerclient "github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"k8s.io/client-go/tools/remotecommand"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	utilexec "k8s.io/utils/exec"
)

type DockerBackend struct {
	ctx        context.Context
	log        component.Logger
	dataDir    string
	client     *dockerclient.Client
	server     *Server
	labels     labels.LabelProvider
	logs       *logWriter
	Into       DockerInto
	Mounts     backend.MountLookup
	parent     backend.Backend
	serverOnce sync.Once
}

var _ backend.Driver = &DockerBackend{}

func (b *DockerBackend) DataDir() string {
	return b.dataDir
}

// Cleanup force-removes all containers and volumes managed by this backend.
func (b *DockerBackend) Cleanup(ctx context.Context) error {
	b.log.Info().Msg("cleanup: removing managed containers and volumes")
	f := filters.NewArgs()
	f.Add("label", b.labels.ManagedByFilter())
	var errs []error

	containers, err := b.client.ContainerList(ctx, container.ListOptions{All: true, Filters: f})
	if err != nil {
		return err
	}
	for _, c := range containers {
		b.log.Info().Str("id", c.ID[:min(12, len(c.ID))]).Strs("names", c.Names).Msg("cleanup: removing container")
		if err := b.client.ContainerRemove(ctx, c.ID, container.RemoveOptions{Force: true}); err != nil {
			errs = append(errs, fmt.Errorf("remove container %s: %w", c.ID[:min(12, len(c.ID))], err))
		}
	}

	volumes, err := b.client.VolumeList(ctx, dockervolume.ListOptions{Filters: f})
	if err != nil {
		return err
	}
	for _, v := range volumes.Volumes {
		b.log.Info().Str("name", v.Name).Msg("cleanup: removing volume")
		if err := b.client.VolumeRemove(ctx, v.Name, true); err != nil {
			errs = append(errs, fmt.Errorf("remove volume %s: %w", v.Name, err))
		}
	}

	return errors.Join(errs...)
}

func Detect(ctx context.Context, name, dataDir string) backend.Backend {
	home, _ := os.UserHomeDir()
	paths := []string{
		// TODO: OS Agnostic
		"/var/run/docker.sock",
		"/run/docker.sock",
		filepath.Join(home, ".docker", "run", "docker.sock"),
		filepath.Join(home, ".colima", "default", "docker.sock"),
		filepath.Join(home, ".rd", "docker.sock"),
	}

	socket := func() *string {
		for _, s := range paths {
			if _, err := os.Stat(s); err == nil {
				return &s
			}
		}
		return nil
	}()

	if socket == nil {
		return nil
	}

	dataDir = filepath.Join(dataDir, string(backend.Docker))
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil
	}

	httpClient := &http.Client{
		Transport: &loggingTransport{
			inner: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return net.Dial("unix", *socket)
				},
			},
			log: component.NewLogger("dockerapi"),
		},
	}

	client, err := dockerclient.NewClientWithOpts(dockerclient.WithHost("unix://"+*socket), dockerclient.WithAPIVersionNegotiation(), dockerclient.WithHTTPClient(httpClient))
	if err != nil {
		return nil
	}
	if _, err := client.Ping(ctx); err != nil {
		client.Close()
		return nil
	}
	lp := labels.NewLabels(name)
	d := &DockerBackend{ctx: ctx, client: client, dataDir: dataDir, labels: lp, log: component.NewLogger("docker"), logs: newLogWriter(ctx, client), Into: DockerInto{labels: lp}}
	b := backend.NewBackend(name, d)
	d.parent = b
	return b
}

func (b *DockerBackend) Name() backend.Runtime {
	return backend.Docker
}

func (b *DockerBackend) init() {
	b.serverOnce.Do(func() { b.server = NewServer(b, b.parent) })
}

func (b *DockerBackend) Labels() labels.LabelProvider {
	return b.labels
}

// StartLogs inspects the container for a CRI log path label and starts
// a log writer if present. Best-effort; errors are logged and ignored.
func (b *DockerBackend) StartLogs(containerID string) {
	inspect, err := b.client.ContainerInspect(b.ctx, containerID)
	if err != nil {
		b.log.Warn().Str("id", containerID[:12]).Err(err).Msg("failed to start log writer")
		return
	}
	if logPath := b.labels.LogPath(inspect.Config.Labels); logPath != "" {
		b.logs.Start(containerID, logPath)
	}
}

// StopLogs stops the log writer for a container.
func (b *DockerBackend) StopLogs(containerID string) {
	b.logs.Stop(containerID)
}

func (b *DockerBackend) ImageServer() runtimeapi.ImageServiceServer {
	b.init()
	return b.server
}

func (b *DockerBackend) ContainerServer() runtimeapi.RuntimeServiceServer {
	b.init()
	return b.server
}

func (b *DockerBackend) VolumeServer() csipb.ControllerServer {
	b.init()
	return b.server
}

// Exec implements [backend.Driver] / [streaming.Runtime].
func (b *DockerBackend) Exec(ctx context.Context, containerID string, cmd []string, stdin io.Reader, stdout, stderr io.WriteCloser, tty bool, resize <-chan remotecommand.TerminalSize) error {
	execConfig := container.ExecOptions{
		Cmd:          cmd,
		AttachStdin:  stdin != nil,
		AttachStdout: stdout != nil,
		AttachStderr: stderr != nil,
		Tty:          tty,
	}

	b.log.Info().Str("container", containerID[:12]).Strs("cmd", cmd).Bool("tty", tty).Msg("exec")

	createResp, err := b.client.ContainerExecCreate(ctx, containerID, execConfig)
	if err != nil {
		return err
	}

	attachResp, err := b.client.ContainerExecAttach(ctx, createResp.ID, container.ExecAttachOptions{Tty: tty})
	if err != nil {
		return err
	}
	defer attachResp.Close()

	if tty && resize != nil {
		go func() {
			for size := range resize {
				b.client.ContainerExecResize(ctx, createResp.ID, container.ResizeOptions{
					Height: uint(size.Height),
					Width:  uint(size.Width),
				})
			}
		}()
	}

	proxyStreams(tty, stdin, stdout, stderr, attachResp)

	inspect, err := b.client.ContainerExecInspect(ctx, createResp.ID)
	if err != nil {
		return nil
	}
	if inspect.ExitCode != 0 {
		b.log.Info().Str("container", containerID[:12]).Int("code", inspect.ExitCode).Msg("exec exited")
		return utilexec.CodeExitError{
			Err:  fmt.Errorf("command terminated with exit code %d", inspect.ExitCode),
			Code: inspect.ExitCode,
		}
	}
	return nil
}

// Attach implements [backend.Driver] / [streaming.Runtime].
func (b *DockerBackend) Attach(ctx context.Context, containerID string, stdin io.Reader, stdout, stderr io.WriteCloser, tty bool, resize <-chan remotecommand.TerminalSize) error {
	opts := container.AttachOptions{
		Stream: true,
		Stdin:  stdin != nil,
		Stdout: stdout != nil,
		Stderr: stderr != nil,
	}

	b.log.Info().Str("container", containerID[:12]).Msg("attach")

	attachResp, err := b.client.ContainerAttach(ctx, containerID, opts)
	if err != nil {
		return err
	}
	defer attachResp.Close()

	if tty && resize != nil {
		go func() {
			for size := range resize {
				b.client.ContainerResize(ctx, containerID, container.ResizeOptions{
					Height: uint(size.Height),
					Width:  uint(size.Width),
				})
			}
		}()
	}

	return proxyStreams(tty, stdin, stdout, stderr, attachResp)
}

// PortForward implements [backend.Driver] / [streaming.Runtime].
func (b *DockerBackend) PortForward(ctx context.Context, podSandboxID string, port int32, stream io.ReadWriteCloser) error {
	inspect, err := b.client.ContainerInspect(ctx, podSandboxID)
	if err != nil {
		return err
	}

	ip := getIPFromInspect(inspect)
	if ip == "" {
		return fmt.Errorf("no IP address for sandbox %s", podSandboxID)
	}

	b.log.Info().Str("sandbox", podSandboxID[:12]).Int32("port", port).Str("ip", ip).Msg("port-forward")

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", ip, port), 10*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		io.Copy(conn, stream)
	}()
	go func() {
		defer wg.Done()
		io.Copy(stream, conn)
		stream.Close()
	}()
	wg.Wait()
	return nil
}

// RunCmd implements [backend.Driver].
func (b *DockerBackend) Run(img string, cmd []string, binds []string, host bool, cb func(string) error) error {
	reader, err := b.client.ImagePull(b.ctx, img, image.PullOptions{})
	if err != nil {
		return wrapErr(err)
	}
	io.Copy(io.Discard, reader)
	reader.Close()

	hostConfig := &container.HostConfig{AutoRemove: true, Binds: binds}
	if host {
		hostConfig.NetworkMode = "host"
		hostConfig.PidMode = "host"
		hostConfig.Privileged = true
	}

	resp, err := b.client.ContainerCreate(b.ctx, &container.Config{
		Image:        img,
		Cmd:          cmd,
		AttachStdout: true,
		AttachStderr: true,
	}, hostConfig, nil, nil, "")
	if err != nil {
		return wrapErr(err)
	}

	attach, err := b.client.ContainerAttach(b.ctx, resp.ID, container.AttachOptions{Stream: true, Stdout: true, Stderr: true})
	if err != nil {
		return wrapErr(err)
	}
	defer attach.Close()

	if err := b.client.ContainerStart(b.ctx, resp.ID, container.StartOptions{}); err != nil {
		return wrapErr(err)
	}

	var stdout, stderr bytes.Buffer
	stdcopy.StdCopy(&stdout, &stderr, attach.Reader)

	waitCh, errCh := b.client.ContainerWait(b.ctx, resp.ID, container.WaitConditionNotRunning)
	select {
	case result := <-waitCh:
		if result.StatusCode != 0 {
			return fmt.Errorf("exit code %d: %s", result.StatusCode, strings.TrimSpace(stderr.String()))
		}
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("waiting for container: %w", err)
		}
	}

	if err := cb(strings.TrimSpace(stdout.String())); err != nil {
		return err
	}
	return nil
}

// Events implements [backend.Driver]. Streams Docker engine events filtered
// to containers, images, volumes, and networks managed by this backend.
func (b *DockerBackend) Events(ctx context.Context) (<-chan backend.Event, <-chan error) {
	out := make(chan backend.Event, 64)
	outErr := make(chan error, 1)

	msgCh, errCh := b.client.Events(ctx, events.ListOptions{
		Filters: filters.NewArgs(
			filters.Arg("label", b.labels.ManagedByFilter()),
		),
	})

	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-errCh:
				if ctx.Err() != nil {
					return
				}
				outErr <- err
				return
			case msg, ok := <-msgCh:
				if !ok {
					return
				}
				ev, ok := dockerEventToBackend(msg)
				if !ok {
					continue
				}
				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return out, outErr
}

var dockerResourceMap = map[events.Type]backend.EventResource{
	events.ContainerEventType: backend.ResourceContainer,
	events.ImageEventType:     backend.ResourceImage,
	events.VolumeEventType:    backend.ResourceVolume,
	events.NetworkEventType:   backend.ResourceNetwork,
}

var dockerActionMap = map[events.Action]backend.EventAction{
	events.ActionCreate:     backend.ActionCreate,
	events.ActionStart:      backend.ActionStart,
	events.ActionRestart:    backend.ActionRestart,
	events.ActionStop:       backend.ActionStop,
	events.ActionKill:       backend.ActionKill,
	events.ActionDie:        backend.ActionDie,
	events.ActionOOM:        backend.ActionOOM,
	events.ActionPause:      backend.ActionPause,
	events.ActionUnPause:    backend.ActionUnpause,
	events.ActionDestroy:    backend.ActionDestroy,
	events.ActionRemove:     backend.ActionRemove,
	events.ActionPull:       backend.ActionPull,
	events.ActionPush:       backend.ActionPush,
	events.ActionTag:        backend.ActionTag,
	events.ActionUnTag:      backend.ActionUntag,
	events.ActionDelete:     backend.ActionDelete,
	events.ActionMount:      backend.ActionMount,
	events.ActionUnmount:    backend.ActionUnmount,
	events.ActionConnect:    backend.ActionConnect,
	events.ActionDisconnect: backend.ActionDisconnect,
	events.ActionExecCreate: backend.ActionExecCreate,
	events.ActionExecStart:  backend.ActionExecStart,
	events.ActionExecDie:    backend.ActionExecDie,

	events.ActionHealthStatus:          backend.ActionHealthStatus,
	events.ActionHealthStatusRunning:   backend.ActionHealthStatusRunning,
	events.ActionHealthStatusHealthy:   backend.ActionHealthStatusHealthy,
	events.ActionHealthStatusUnhealthy: backend.ActionHealthStatusUnhealthy,
}

func dockerEventToBackend(msg events.Message) (backend.Event, bool) {
	resource, ok := dockerResourceMap[msg.Type]
	if !ok {
		return backend.Event{}, false
	}
	action, ok := dockerActionMap[msg.Action]
	if !ok {
		return backend.Event{}, false
	}
	return backend.Event{
		Resource:   resource,
		Action:     action,
		ID:         msg.Actor.ID,
		Attributes: msg.Actor.Attributes,
		TimeNano:   msg.TimeNano,
	}, true
}

// proxyStreams proxies stdin/stdout/stderr to/from a hijacked Docker connection.
func proxyStreams(tty bool, stdin io.Reader, stdout, stderr io.WriteCloser, resp dockertypes.HijackedResponse) error {
	var wg sync.WaitGroup

	if stdin != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			io.Copy(resp.Conn, stdin)
			resp.CloseWrite()
		}()
	}

	if tty {
		if stdout != nil {
			io.Copy(stdout, resp.Reader)
			stdout.Close()
		}
	} else {
		stdcopy.StdCopy(stdout, stderr, resp.Reader)
		if stdout != nil {
			stdout.Close()
		}
		if stderr != nil {
			stderr.Close()
		}
	}

	if closer, ok := stdin.(io.Closer); ok {
		closer.Close()
	}
	resp.Conn.Close()

	wg.Wait()
	return nil
}

// getIPFromInspect extracts the container IP from Docker inspect response.
// On Docker Desktop (macOS), bridge IPs are not routable from the host,
// so we return 127.0.0.1 when on bridge with published ports.
func getIPFromInspect(inspect container.InspectResponse) string {
	if inspect.HostConfig != nil && inspect.HostConfig.NetworkMode == "host" {
		return ""
	}
	if inspect.NetworkSettings != nil {
		if _, onBridge := inspect.NetworkSettings.Networks["bridge"]; onBridge {
			for _, bindings := range inspect.NetworkSettings.Ports {
				for _, b := range bindings {
					if b.HostIP == "" || b.HostIP == "0.0.0.0" {
						return "127.0.0.1"
					}
				}
			}
		}
		for _, n := range inspect.NetworkSettings.Networks {
			if n.IPAddress != "" {
				return n.IPAddress
			}
		}
	}
	return ""
}
