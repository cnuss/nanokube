package internal

import (
	"context"
	"os"
	"path/filepath"

	"github.com/Mirantis/cri-dockerd/cmd"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

func findDockerSocket() string {
	// Check DOCKER_HOST env var first
	if host := os.Getenv("DOCKER_HOST"); host != "" {
		return host
	}

	// Common socket locations
	home, _ := os.UserHomeDir()
	candidates := []string{
		"/var/run/docker.sock",                                                           // Linux / older Docker Desktop
		filepath.Join(home, ".docker/run/docker.sock"),                                   // Docker Desktop macOS
		filepath.Join(home, ".docker/desktop/docker.sock"),                               // Docker Desktop alternative
		filepath.Join(home, "Library/Containers/com.docker.docker/Data/docker.raw.sock"), // Docker Desktop macOS raw
	}

	for _, sock := range candidates {
		if _, err := os.Stat(sock); err == nil {
			return "unix://" + sock
		}
	}

	// Default fallback
	return "unix:///var/run/docker.sock"
}

type Docker struct {
	config *Config
	cmd    *cobra.Command
}

func NewDocker(config *Config) *Docker {
	return &Docker{
		config: config,
	}
}

func (d *Docker) Start(ctx context.Context) error {
	dockerSock := findDockerSocket()
	log.Info().Str("docker", dockerSock).Msg("starting cri-dockerd")

	// Create a stop channel that closes when context is done
	stopCh := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(stopCh)
	}()

	d.cmd = cmd.NewDockerCRICommand(stopCh)
	d.cmd.SetContext(ctx)

	args := []string{
		"--docker-endpoint=" + dockerSock,
		"--container-runtime-endpoint=unix://" + d.config.DataDir + "/cri-dockerd.sock",
		"--cri-dockerd-root-directory=" + d.config.DataDir + "/cri-dockerd",
		"--network-plugin=",
	}

	d.cmd.SetArgs(args)

	go d.cmd.ExecuteContext(ctx)

	log.Info().Msg("cri-dockerd started")
	return nil
}
