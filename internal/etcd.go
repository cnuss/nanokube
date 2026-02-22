package internal

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"time"

	"github.com/rs/zerolog/log"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Etcd struct {
	config *Config
	server *embed.Etcd
}

func NewEtcd(config *Config) *Etcd {
	return &Etcd{
		config: config,
	}
}

func (e *Etcd) Start(ctx context.Context) error {
	log.Info().Str("dataDir", e.config.DataDir).Msg("starting etcd")

	cfg := embed.NewConfig()
	cfg.Dir = filepath.Join(e.config.DataDir, "etcd")

	if e.config.Verbose {
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

	clientURL, _ := url.Parse("https://127.0.0.1:2379")
	peerURL, _ := url.Parse("https://127.0.0.1:2380")

	cfg.ListenClientUrls = []url.URL{*clientURL}
	cfg.AdvertiseClientUrls = []url.URL{*clientURL}
	cfg.ListenPeerUrls = []url.URL{*peerURL}
	cfg.AdvertisePeerUrls = []url.URL{*peerURL}
	cfg.InitialCluster = "default=" + peerURL.String()

	// Client TLS
	cfg.ClientTLSInfo = transport.TLSInfo{
		CertFile:      e.config.CertPath,
		KeyFile:       e.config.KeyPath,
		TrustedCAFile: e.config.CertPath,
	}

	// Peer TLS
	cfg.PeerTLSInfo = transport.TLSInfo{
		CertFile:      e.config.CertPath,
		KeyFile:       e.config.KeyPath,
		TrustedCAFile: e.config.CertPath,
	}

	var err error
	e.server, err = embed.StartEtcd(cfg)
	if err != nil {
		return fmt.Errorf("failed to start etcd: %w", err)
	}

	select {
	case <-e.server.Server.ReadyNotify():
		log.Info().Msg("etcd server ready")
	case <-ctx.Done():
		return ctx.Err()
	}

	// Wait for client connectivity
	tlsInfo := transport.TLSInfo{
		CertFile:      e.config.CertPath,
		KeyFile:       e.config.KeyPath,
		TrustedCAFile: e.config.CertPath,
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		return fmt.Errorf("failed to create tls config: %w", err)
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"https://127.0.0.1:2379"},
		TLS:         tlsConfig,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("failed to create etcd client: %w", err)
	}
	defer client.Close()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			ctxTimeout, cancel := context.WithTimeout(ctx, 2*time.Second)
			_, err := client.Get(ctxTimeout, "health")
			cancel()
			if err == nil {
				log.Info().Msg("etcd is ready")
				return nil
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}
