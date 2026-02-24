package cri

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"

	"github.com/cnuss/nanokube/pkg/cri/docker"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/kubelet/pkg/cri/streaming"
)

type CRI struct {
	dataDir      string
	socketPath   string
	backend      Backend
	streaming    streaming.Runtime
	streamServer streaming.Server
	grpcServer   *grpc.Server
}

func NewCRI(dataDir string) *CRI {
	socketPath := filepath.Join(dataDir, "cri.sock")
	backend, streaming := detectBackend()
	return &CRI{
		dataDir:    dataDir,
		socketPath: socketPath,
		backend:    backend,
		streaming:  streaming,
	}
}

func (c *CRI) Start(ctx context.Context) error {
	if c.backend == nil {
		log.Warn().Msg("no container runtime detected; CRI server disabled")
		return nil
	}

	log.Info().Msg("starting CRI server")

	if err := c.backend.Init(ctx); err != nil {
		return fmt.Errorf("cri backend init: %w", err)
	}

	// Start streaming HTTP server for exec/attach/portforward
	if c.streaming != nil {
		streamCfg := streaming.DefaultConfig
		streamCfg.Addr = "127.0.0.1:0"
		var err error
		c.streamServer, err = streaming.NewServer(streamCfg, c.streaming)
		if err != nil {
			return fmt.Errorf("cri streaming server: %w", err)
		}
		go func() {
			if err := c.streamServer.Start(true); err != nil {
				log.Error().Err(err).Msg("cri streaming server exited")
			}
		}()
		log.Info().Msg("cri streaming server started")
	}

	// Remove stale socket
	os.Remove(c.socketPath)

	lis, err := net.Listen("unix", c.socketPath)
	if err != nil {
		return fmt.Errorf("cri listen %s: %w", c.socketPath, err)
	}

	c.grpcServer = grpc.NewServer()
	runtimeapi.RegisterRuntimeServiceServer(c.grpcServer, &runtimeService{backend: c.backend, streamServer: c.streamServer})
	runtimeapi.RegisterImageServiceServer(c.grpcServer, &imageService{backend: c.backend})

	go func() {
		log.Info().Str("socket", c.socketPath).Msg("cri server listening")
		if err := c.grpcServer.Serve(lis); err != nil {
			log.Error().Err(err).Msg("cri server exited")
		}
	}()

	go func() {
		<-ctx.Done()
		c.stop()
	}()

	return nil
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

// detectBackend probes for Docker then Podman and returns the first available backend
// along with its streaming runtime (for exec/attach/portforward).
func detectBackend() (Backend, streaming.Runtime) {
	if sock := detectDockerSocket(); sock != "" {
		log.Info().Str("socket", sock).Msg("detected Docker runtime")
		b := docker.New(sock)
		return b, docker.NewStreamingRuntime(b)
	}
	if sock := detectPodmanSocket(); sock != "" {
		log.Info().Str("socket", sock).Msg("detected Podman runtime")
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
