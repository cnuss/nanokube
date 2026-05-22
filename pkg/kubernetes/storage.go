package kubernetes

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync"

	v1 "github.com/cnuss/nanokube/pkg/v1"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"k8s.io/klog/v2"
)

type StorageCommand struct {
	*Command
	nano v1.Nanokube

	port     int
	portOnce sync.Once
}

func NewStorageCommand(nano v1.Nanokube) *StorageCommand {
	c := &StorageCommand{nano: nano}
	command := &cobra.Command{
		Use: "etcd",
		RunE: func(cmd *cobra.Command, args []string) error {
			dataDir := c.nano.Options().DataDirAt(v1.DataDirEtcd)
			port := c.Port()
			clientURL, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", port))
			peerURL, _ := url.Parse("http://127.0.0.1:0")

			cfg := embed.NewConfig()
			cfg.Dir = filepath.Join(dataDir, "data")
			cfg.ListenClientUrls = []url.URL{*clientURL}
			cfg.AdvertiseClientUrls = []url.URL{*clientURL}
			cfg.ListenPeerUrls = []url.URL{*peerURL}
			cfg.AdvertisePeerUrls = []url.URL{*peerURL}
			cfg.InitialCluster = "default=" + peerURL.String()
			if _, err := os.Stat(filepath.Join(cfg.Dir, "member", "wal")); err == nil {
				cfg.ClusterState = embed.ClusterStateFlagExisting
			}
			cfg.AutoCompactionRetention = "0"
			cfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(zap.New(zapcore.NewCore(
				zapcore.NewConsoleEncoder(zap.NewProductionEncoderConfig()),
				zapcore.AddSync(klogWriter{}),
				zapcore.FatalLevel,
			)))

			server, err := embed.StartEtcd(cfg)
			if err != nil {
				return err
			}
			<-server.Server.ReadyNotify()
			<-cmd.Context().Done()
			server.Close()
			return nil
		},
	}
	c.Command = newCommand(nano, command)
	return c
}

func (c *StorageCommand) Port() int {
	c.portOnce.Do(func() {
		port, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			panic(err)
		}
		c.port = port.Addr().(*net.TCPAddr).Port
		port.Close()
	})
	return c.port
}

type klogWriter struct{}

func (klogWriter) Write(p []byte) (int, error) {
	klog.V(2).Info(string(p))
	return len(p), nil
}

func (klogWriter) Sync() error { return nil }
