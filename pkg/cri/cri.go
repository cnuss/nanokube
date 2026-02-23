package cri

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// Server is a CRI gRPC server backed by a container engine.
type Server struct {
	socketPath string
	backend    Backend
	grpc       *grpc.Server
}

// NewServer creates a CRI server. The socket is placed in dataDir/cri.sock.
func NewServer(dataDir string, backend Backend) *Server {
	return &Server{
		socketPath: filepath.Join(dataDir, "cri.sock"),
		backend:    backend,
	}
}

// SocketPath returns the Unix socket path for the CRI endpoint.
func (s *Server) SocketPath() string {
	return s.socketPath
}

// Start initialises the backend, creates the Unix socket, registers gRPC
// services, and begins serving in a background goroutine.
func (s *Server) Start(ctx context.Context) error {
	if err := s.backend.Init(ctx); err != nil {
		return fmt.Errorf("cri backend init: %w", err)
	}

	// Remove stale socket
	os.Remove(s.socketPath)

	lis, err := net.Listen("unix", s.socketPath)
	if err != nil {
		return fmt.Errorf("cri listen %s: %w", s.socketPath, err)
	}

	s.grpc = grpc.NewServer()
	runtimeapi.RegisterRuntimeServiceServer(s.grpc, &runtimeService{backend: s.backend})
	runtimeapi.RegisterImageServiceServer(s.grpc, &imageService{backend: s.backend})

	go func() {
		log.Info().Str("socket", s.socketPath).Msg("cri server listening")
		if err := s.grpc.Serve(lis); err != nil {
			log.Error().Err(err).Msg("cri server exited")
		}
	}()

	// Shut down when context is cancelled
	go func() {
		<-ctx.Done()
		s.Stop()
	}()

	return nil
}

// Stop gracefully shuts down the gRPC server and cleans up.
func (s *Server) Stop() {
	if s.grpc != nil {
		s.grpc.GracefulStop()
	}
	s.backend.Close()
	os.Remove(s.socketPath)
}

// DetectDockerSocket probes well-known Docker socket paths and returns the
// first one that exists. Returns "" if none found. OS-agnostic (no exec/shell).
func DetectDockerSocket() string {
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
			log.Debug().Str("socket", p).Msg("found docker socket")
			return p
		}
	}
	return ""
}

// DetectPodmanSocket probes well-known Podman socket paths and returns the
// first one that exists. Returns "" if none found.
func DetectPodmanSocket() string {
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
			log.Debug().Str("socket", p).Msg("found podman socket")
			return p
		}
	}
	return ""
}
