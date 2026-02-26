package cri

import (
	"context"
	"fmt"
	"net/http"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/cnuss/nanokube/pkg/cri/docker"
	"google.golang.org/grpc"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/kubelet/pkg/cri/streaming"
	"net"
	"os"
	"path/filepath"
)

var logger = component.NewLogger("cri")

type CRI struct {
	dataDir      string
	name         string
	clean        bool
	socketPath   string
	backend      Backend
	streaming    streaming.Runtime
	streamServer streaming.Server
	grpcServer   *grpc.Server
}

func NewCRI(dataDir, name string, clean bool) *CRI {
	socketPath := filepath.Join(dataDir, "cri.sock")
	backend, streamingRT := detectBackend(name)
	return &CRI{
		dataDir:    dataDir,
		name:       name,
		clean:      clean,
		socketPath: socketPath,
		backend:    backend,
		streaming:  streamingRT,
	}
}

func (c *CRI) Start(ctx context.Context) (component.Started, error) {
	if c.backend == nil {
		return nil, fmt.Errorf("no container runtime detected (checked Docker, Podman)")
	}

	logger.Info().Msg("starting CRI server")

	if err := c.backend.Init(ctx); err != nil {
		return nil, fmt.Errorf("cri backend init: %w", err)
	}

	// When --clean is active, remove all managed Docker resources from a
	// previous run. os.RemoveAll(dataDir) only wipes local files; containers,
	// volumes, and networks live in Docker and must be removed via the API.
	// Cleanup removes containers + sandboxes + networks; volumes are removed
	// separately since they intentionally persist across normal restarts.
	if c.clean {
		if volumes, err := c.backend.RemoveVolumes(ctx); err != nil {
			logger.Warn().Err(err).Msg("clean: failed to remove volumes")
		} else if len(volumes) > 0 {
			logger.Info().Strs("volumes", volumes).Msg("clean: removed volumes")
		}
	}

	// Start streaming HTTP server for exec/attach/portforward
	if c.streaming != nil {
		streamCfg := streaming.DefaultConfig
		streamCfg.Addr = "127.0.0.1:0"
		var err error
		c.streamServer, err = streaming.NewServer(streamCfg, c.streaming)
		if err != nil {
			return nil, fmt.Errorf("cri streaming server: %w", err)
		}
		go func() {
			if err := c.streamServer.Start(true); err != nil && err != http.ErrServerClosed {
				logger.Error().Err(err).Msg("cri streaming server exited")
			}
		}()
		logger.Info().Msg("cri streaming server started")
	}

	// Remove stale socket
	os.Remove(c.socketPath)

	lis, err := net.Listen("unix", c.socketPath)
	if err != nil {
		return nil, fmt.Errorf("cri listen %s: %w", c.socketPath, err)
	}

	c.grpcServer = grpc.NewServer()
	runtimeapi.RegisterRuntimeServiceServer(c.grpcServer, &runtimeService{backend: c.backend, streamServer: c.streamServer})
	runtimeapi.RegisterImageServiceServer(c.grpcServer, &imageService{backend: c.backend})

	go func() {
		logger.Info().Str("socket", c.socketPath).Msg("cri server listening")
		if err := c.grpcServer.Serve(lis); err != nil {
			logger.Error().Err(err).Msg("cri server exited")
		}
	}()

	return component.Ready(), nil
}

func (c *CRI) Stop() component.Stopped {
	return component.Closed("unix", c.socketPath, func() {
		if err := Cleanup(c.backend); err != nil {
			logger.Warn().Err(err).Msg("cleanup: errors during teardown")
		}
		c.stop()
	})
}

func (c *CRI) stop() {
	if c.streamServer != nil {
		c.streamServer.Stop()
	}
	if c.grpcServer != nil {
		c.grpcServer.GracefulStop()
	}
	if c.backend != nil {
		c.backend.Close()
	}
	os.Remove(c.socketPath)
}

func (c *CRI) Hostname() string {
	return "localhost"
}

func (c *CRI) Endpoint() string {
	if c.backend != nil {
		return "unix://" + c.socketPath
	}
	return ""
}

func (c *CRI) Domain() string {
	return ""
}

func (c *CRI) Nameservers() []string {
	return []string{}
}

// RuntimeBackend returns the underlying container runtime backend, or nil.
func (c *CRI) RuntimeBackend() Backend {
	return c.backend
}

// detectBackend probes for Docker then Podman and returns the first available backend
// along with its streaming runtime (for exec/attach/portforward).
func detectBackend(name string) (Backend, streaming.Runtime) {
	if sock := detectDockerSocket(); sock != "" {
		logger.Info().Str("socket", sock).Msg("detected Docker runtime")
		b := docker.New(sock, name)
		return b, docker.NewStreamingRuntime(b)
	}
	if sock := detectPodmanSocket(); sock != "" {
		logger.Info().Str("socket", sock).Msg("detected Podman runtime")
		// TODO: wire podman.New(sock) once backend is implemented
		_ = sock
	}
	return nil, nil
}

func detectDockerSocket() string {
	home, _ := os.UserHomeDir()
	paths := []string{
		"/var/run/docker.sock",
		"/run/docker.sock",
		filepath.Join(home, ".docker", "run", "docker.sock"),
		filepath.Join(home, ".colima", "default", "docker.sock"),
		filepath.Join(home, ".rd", "docker.sock"),
	}
	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

func detectPodmanSocket() string {
	home, _ := os.UserHomeDir()
	xdg := os.Getenv("XDG_RUNTIME_DIR")
	paths := []string{
		"/var/run/podman/podman.sock",
		"/run/podman/podman.sock",
		filepath.Join(home, ".local", "share", "podman", "podman.sock"),
	}
	if xdg != "" {
		paths = append(paths, filepath.Join(xdg, "podman", "podman.sock"))
	}
	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}
