package docker

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/cnuss/nanokube/pkg/crid/backend"
	"github.com/cnuss/nanokube/pkg/crid/labels"
	dockertypes "github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	dockerclient "github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"k8s.io/client-go/tools/remotecommand"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	utilexec "k8s.io/utils/exec"
)

var logger = component.NewLogger("crid-docker")

type DockerBackend struct {
	dataDir    string
	client     *dockerclient.Client
	server     *Server
	labels     labels.LabelProvider
	Mounts     backend.MountLookup
	serverOnce sync.Once
}

var _ backend.Driver = &DockerBackend{}

func (b *DockerBackend) DataDir() string {
	return b.dataDir
}

func Detect(ctx context.Context, dataDir string) backend.Backend {
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
	if err := os.MkdirAll(dataDir, 0755); err != nil {
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
	b := backend.NewBackend(&DockerBackend{client: client, dataDir: dataDir, labels: labels.NewLabels(string(backend.Docker))})
	return b
}

func (b *DockerBackend) Name() backend.Runtime {
	return backend.Docker
}

func (b *DockerBackend) init() {
	b.serverOnce.Do(func() { b.server = NewServer(b) })
}

func (b *DockerBackend) Labels() labels.LabelProvider {
	return b.labels
}

func (b *DockerBackend) ImageServer() runtimeapi.ImageServiceServer {
	b.init()
	return b.server
}

func (b *DockerBackend) ContainerServer() runtimeapi.RuntimeServiceServer {
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

	logger.Info().Str("container", containerID[:12]).Strs("cmd", cmd).Bool("tty", tty).Msg("exec")

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
		logger.Info().Str("container", containerID[:12]).Int("code", inspect.ExitCode).Msg("exec exited")
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

	logger.Info().Str("container", containerID[:12]).Msg("attach")

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

	logger.Info().Str("sandbox", podSandboxID[:12]).Int32("port", port).Str("ip", ip).Msg("port-forward")

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
