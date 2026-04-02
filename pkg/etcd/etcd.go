package etcd

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/cnuss/nanokube/pkg/config"
	"go.etcd.io/etcd/server/v3/embed"
)

var logger = component.NewLogger("etcd")

type Etcd struct {
	certs   *config.Certs
	dataDir string
	server  *embed.Etcd
}

func NewEtcd(certs *config.Certs, dataDir string) *Etcd {
	return &Etcd{
		certs:   certs,
		dataDir: filepath.Join(dataDir, "etcd"),
	}
}

func (e *Etcd) Start(ctx context.Context) (component.Started, error) {
	logger.Info().Str("dataDir", e.dataDir).Msg("starting etcd")

	cfg := embed.NewConfig()
	cfg.Dir = e.dataDir

	clientURL, _ := url.Parse("https://0.0.0.0:2379")
	peerURL, _ := url.Parse("https://127.0.0.1:2380")

	cfg.ListenClientUrls = []url.URL{*clientURL}
	cfg.AdvertiseClientUrls = []url.URL{*clientURL}
	cfg.ListenPeerUrls = []url.URL{*peerURL}
	cfg.AdvertisePeerUrls = []url.URL{*peerURL}
	cfg.InitialCluster = "default=" + peerURL.String()

	// Restart: if WAL exists, this is an existing cluster
	if _, err := os.Stat(filepath.Join(e.dataDir, "member", "wal")); err == nil {
		cfg.ClusterState = embed.ClusterStateFlagExisting
	}

	cfg.ClientTLSInfo.CertFile = e.certs.CertPath()
	cfg.ClientTLSInfo.KeyFile = e.certs.KeyPath()
	cfg.ClientTLSInfo.TrustedCAFile = e.certs.CertPath()
	cfg.PeerTLSInfo.CertFile = e.certs.CertPath()
	cfg.PeerTLSInfo.KeyFile = e.certs.KeyPath()
	cfg.PeerTLSInfo.TrustedCAFile = e.certs.CertPath()

	cfg.AutoCompactionRetention = "0"
	cfg.LogLevel = "fatal"

	var err error
	e.server, err = embed.StartEtcd(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to start etcd: %w", err)
	}

	select {
	case <-e.server.Server.ReadyNotify():
		logger.Info().Msg("etcd ready")
	case <-time.After(30 * time.Second):
		e.server.Close()
		return nil, fmt.Errorf("etcd took too long to start")
	case <-ctx.Done():
		e.server.Close()
		return nil, ctx.Err()
	}

	return component.Ready(), nil
}

func (e *Etcd) Stop(ctx context.Context) component.Stopped {
	logger.Info().Msg("stopping etcd")
	if e.server != nil {
		e.server.Close()
	}
	return component.Done()
}
