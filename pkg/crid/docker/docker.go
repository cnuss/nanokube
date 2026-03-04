package docker

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/cnuss/nanokube/pkg/crid/backend"
	"github.com/cnuss/nanokube/pkg/crid/labels"
	dockerclient "github.com/docker/docker/client"
	"k8s.io/client-go/tools/remotecommand"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type DockerBackend struct {
	dataDir    string
	client     *dockerclient.Client
	server     *Server
	labels     labels.LabelProvider
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

// Attach implements [backend.Driver].
func (b *DockerBackend) Attach(ctx context.Context, containerID string, in io.Reader, out io.WriteCloser, err io.WriteCloser, tty bool, resize <-chan remotecommand.TerminalSize) error {
	panic("unimplemented")
}

// Exec implements [backend.Driver].
func (b *DockerBackend) Exec(ctx context.Context, containerID string, cmd []string, in io.Reader, out io.WriteCloser, err io.WriteCloser, tty bool, resize <-chan remotecommand.TerminalSize) error {
	panic("unimplemented")
}

// PortForward implements [backend.Driver].
func (b *DockerBackend) PortForward(ctx context.Context, podSandboxID string, port int32, stream io.ReadWriteCloser) error {
	panic("unimplemented")
}
