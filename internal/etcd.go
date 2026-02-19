package internal

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"

	"github.com/rs/zerolog/log"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Etcd struct {
	DataDir string
	Verbose bool
	server  *embed.Etcd
}

func NewEtcd(dataDir string, verbose bool) *Etcd {
	return &Etcd{
		DataDir: dataDir,
		Verbose: verbose,
	}
}

func (e *Etcd) Start(ctx context.Context) error {
	log.Info().Str("dataDir", e.DataDir).Msg("starting etcd")

	cfg := embed.NewConfig()
	cfg.Dir = filepath.Join(e.DataDir, "etcd")

	if e.Verbose {
		cfg.LogLevel = "info"
	} else {
		cfg.LogLevel = "error"
		zapCfg := zap.NewProductionConfig()
		zapCfg.Level = zap.NewAtomicLevelAt(zapcore.PanicLevel)
		logger, err := zapCfg.Build()
		if err != nil {
			return fmt.Errorf("failed to create logger: %w", err)
		}
		cfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(logger)
	}

	clientURL, _ := url.Parse("http://127.0.0.1:2379")
	peerURL, _ := url.Parse("http://127.0.0.1:2380")

	cfg.ListenClientUrls = []url.URL{*clientURL}
	cfg.AdvertiseClientUrls = []url.URL{*clientURL}
	cfg.ListenPeerUrls = []url.URL{*peerURL}
	cfg.AdvertisePeerUrls = []url.URL{*peerURL}
	cfg.InitialCluster = "default=" + peerURL.String()

	var err error
	e.server, err = embed.StartEtcd(cfg)
	if err != nil {
		return fmt.Errorf("failed to start etcd: %w", err)
	}

	select {
	case <-e.server.Server.ReadyNotify():
		log.Info().Msg("etcd is ready")
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func (e *Etcd) Stop() error {
	log.Info().Msg("stopping etcd")
	if e.server != nil {
		e.server.Close()
	}
	return nil
}
