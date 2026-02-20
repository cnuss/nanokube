package internal

import (
	"context"

	"github.com/Mirantis/cri-dockerd/cmd"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

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
	log.Info().Msg("starting cri-dockerd")

	stopCh := make(chan struct{})
	d.cmd = cmd.NewDockerCRICommand(stopCh)

	args := []string{
		// TODO: add flags here
	}

	d.cmd.SetArgs(args)

	go func() {
		<-ctx.Done()
		close(stopCh)
	}()

	go d.cmd.Execute()

	// cri-dockerd doesn't have an HTTP health endpoint, so just return
	log.Info().Msg("cri-dockerd started")
	return nil
}
