package internal

import (
	"github.com/cnuss/nanokube/pkg/config"
	"github.com/cnuss/nanokube/pkg/cri"
	"github.com/rs/zerolog/log"
)

type CRI struct {
	config *config.Config
	server *cri.Server
}

func NewCRI(config *config.Config, server *cri.Server) *CRI {
	return &CRI{
		config: config,
		server: server,
	}
}

func (c *CRI) Start() error {
	log.Info().Msg("starting cri server")
	return c.server.Start(c.config.Ctx)
}
