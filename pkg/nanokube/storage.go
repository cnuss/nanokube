package nanokube

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	_ "unsafe"

	v1 "github.com/cnuss/nanokube/pkg/v1"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/kubernetes"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	_ "k8s.io/apiserver/pkg/storage/storagebackend/factory"
	"k8s.io/klog/v2"
)

//go:linkname newETCD3Client k8s.io/apiserver/pkg/storage/storagebackend/factory.newETCD3Client
var newETCD3Client func(c storagebackend.TransportConfig) (*kubernetes.Client, error)

var (
	storage     v1.Storage
	storageOnce sync.Once
)

func init() {
	// HACK: override newETCD3Client to use our embedded etcd server.
	//
	// newETCD3Client is an unexported internal of the k8s 1.36 storagebackend
	// factory, bound via go:linkname above. If a kubernetes bump renames or
	// changes its signature, the linkname target goes nil and this assignment
	// panics at init — loud by design, so the coupling can't drift silently.
	if newETCD3Client == nil {
		panic("storage: newETCD3Client linkname target is nil; kubernetes factory internals changed")
	}
	newETCD3Client = func(cfg storagebackend.TransportConfig) (*kubernetes.Client, error) {
		client := StorageRef().WithTransportConfig(cfg).Client()
		if client == nil {
			return nil, fmt.Errorf("embedded etcd client unavailable")
		}
		return client, nil
	}
}

type StorageImpl struct {
	ctx    context.Context
	cancel context.CancelCauseFunc

	cfg         storagebackend.TransportConfig
	cfgOnce     sync.Once
	cfgProvided chan struct{}

	portOnce sync.Once
	port     int
}

var _ v1.Storage = &StorageImpl{}

func StorageRef() v1.Storage {
	storageOnce.Do(func() {
		ctx, cancel := context.WithCancelCause(context.Background())
		storage = &StorageImpl{
			ctx:         ctx,
			cancel:      cancel,
			cfgProvided: make(chan struct{}),
		}
	})
	return storage
}

func (s *StorageImpl) Cancel(reason error) {
	s.cancel(reason)
}

// Client builds a fresh etcd client per call. The k8s storagebackend factory
// (newETCD3Client) owns the client it receives and Close()s it in each
// resource's destroyFunc, so callers must NOT share one: a single closed
// connection would break every other resource's in-flight requests.
func (s *StorageImpl) Client() *kubernetes.Client {
	<-Await(s.ctx, s.cfgProvided)
	kc, err := kubernetes.New(clientv3.Config{
		Endpoints: s.Endpoints(),
	})
	if err != nil {
		s.cancel(fmt.Errorf("build etcd client: %w", err))
		return nil
	}
	return kc
}

func (s *StorageImpl) WithTransportConfig(cfg storagebackend.TransportConfig) v1.Storage {
	s.cfgOnce.Do(func() {
		s.cfg = cfg

		// HACK: embed datadir into kube-apiserver's etcd server list
		dataDir := strings.TrimPrefix(s.cfg.ServerList[0], "file://")
		peerURL, _ := url.Parse("http://127.0.0.1:0")

		cfg := embed.NewConfig()
		cfg.Dir = filepath.Join(dataDir, "data")
		cfg.ListenClientUrls = s.ClientURLs()
		cfg.AdvertiseClientUrls = s.ClientURLs()
		cfg.ListenPeerUrls = []url.URL{*peerURL}
		cfg.AdvertisePeerUrls = []url.URL{*peerURL}
		cfg.InitialCluster = "default=" + peerURL.String()
		// Restart: if WAL exists, this is an existing cluster
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
			s.cancel(err)
			return
		}

		go func() {
			<-s.ctx.Done()
			klog.InfoS("shutting down storage")
			server.Close()
			klog.InfoS("storage is done")
		}()

		<-Await(s.ctx, server.Server.ReadyNotify())
		klog.InfoS("storage is ready", "port", s.Port())

		close(s.cfgProvided)
	})
	return s
}

func (s *StorageImpl) SetConfig(config *server.Config) *server.Config {
	// TODO(partial): in case we want to intercept API Server Config
	return config
}

func (s *StorageImpl) Port() int {
	s.portOnce.Do(func() {
		s.port = func() int {
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				s.cancel(err)
				return 0
			}
			defer l.Close()
			return l.Addr().(*net.TCPAddr).Port
		}()
	})
	return s.port
}

func (s *StorageImpl) Endpoints() []string {
	return []string{fmt.Sprintf("http://127.0.0.1:%d", s.Port())}
}

func (s *StorageImpl) ClientURLs() []url.URL {
	endpoints := s.Endpoints()
	urls := make([]url.URL, len(endpoints))
	for i, ep := range endpoints {
		u, _ := url.Parse(ep)
		urls[i] = *u
	}
	return urls
}

type klogWriter struct{}

func (klogWriter) Write(p []byte) (int, error) {
	klog.V(2).Info(string(p))
	return len(p), nil
}

func (klogWriter) Sync() error { return nil }
