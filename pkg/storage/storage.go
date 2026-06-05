package storage

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	_ "unsafe"

	v1 "github.com/cnuss/nanokube/pkg/v1"
	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/drivers/sqlite"
	kine "github.com/k3s-io/kine/pkg/server"
	"go.etcd.io/etcd/client/v3/kubernetes"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/server"
	kubestorage "k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"
	"k8s.io/client-go/tools/cache"
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
	if newETCD3Client == nil {
		panic("storage: newETCD3Client linkname target is nil; kubernetes factory internals changed")
	}
	newETCD3Client = func(cfg storagebackend.TransportConfig) (*kubernetes.Client, error) {
		kubernetes := StorageRef().WithTransport(cfg).Client().Kubernetes()
		if kubernetes == nil {
			return nil, fmt.Errorf("embedded etcd client unavailable")
		}
		return kubernetes, nil
	}
}

func await(ctx context.Context, sig <-chan struct{}) {
	select {
	case <-sig:
	case <-ctx.Done():
		return
	}
}

type StorageImpl struct {
	ctx    context.Context
	cancel context.CancelCauseFunc

	clientOnce sync.Once
	client     v1.StorageClient

	embeddedOnce sync.Once
	embedded     *embed.Etcd

	sqliteOnce   sync.Once
	sqliteBridge *kine.KVServerBridge

	transport         storagebackend.TransportConfig
	transportOnce     sync.Once
	transportProvided chan struct{}

	serverConfig         *server.Config
	serverConfigOnce     sync.Once
	serverConfigProvided chan struct{}
	rest                 generic.RESTOptionsGetter

	portOnce sync.Once
	port     int
}

var _ v1.Storage = &StorageImpl{}

func StorageRef() v1.Storage {
	storageOnce.Do(func() {
		ctx, cancel := context.WithCancelCause(context.Background())
		storage = &StorageImpl{
			ctx:                  ctx,
			cancel:               cancel,
			transportProvided:    make(chan struct{}),
			serverConfigProvided: make(chan struct{}),
		}
	})
	return storage
}

func (s *StorageImpl) Cancel(reason error) {
	s.cancel(reason)
}

func (s *StorageImpl) Client() v1.StorageClient {
	s.clientOnce.Do(func() {
		await(s.ctx, s.transportProvided)
		url, err := url.Parse(s.transport.ServerList[0])
		if err != nil {
			s.cancel(fmt.Errorf("invalid transport server list URL: %w", err))
			return
		}

		if url.Scheme == "embed" {
			s.client = NewClient(s.ctx).WithServer(s.embeddedEtcd(strings.TrimPrefix(s.transport.ServerList[0], "embed://")).Server)
			return
		}

		if url.Scheme == "sqlite" {
			s.client = NewClient(s.ctx).WithBridge(s.sqliteEtcd(strings.TrimPrefix(s.transport.ServerList[0], "sqlite://")))
			return
		}

		s.cancel(fmt.Errorf("unsupported transport scheme: %s", url.Scheme))
	})
	return s.client
}

func (s *StorageImpl) WithTransport(cfg storagebackend.TransportConfig) v1.Storage {
	s.transportOnce.Do(func() {
		s.transport = cfg
		close(s.transportProvided)
	})
	return s
}

// sqliteEtcd lazily builds a kine SQLite-backed bridge that implements the
// etcdserverpb KV/Watch/Lease/Maintenance/Cluster servers over a local SQLite
// database, once a transport config has been provided. dataDir is a directory;
// the database lives at dataDir/state.db. Returns nil if setup failed (ctx is
// then cancelled with the cause).
func (s *StorageImpl) sqliteEtcd(dataDir string) *kine.KVServerBridge {
	s.sqliteOnce.Do(func() {
		await(s.ctx, s.transportProvided)

		if err := os.MkdirAll(dataDir, 0o700); err != nil {
			s.cancel(fmt.Errorf("sqlite: create data dir: %w", err))
			return
		}
		dsn := filepath.Join(dataDir, "state.db") + "?_journal=WAL&cache=shared&_busy_timeout=30000&_txlock=immediate"

		// Compaction params normally come from kine's CLI defaults; calling the
		// driver directly we must set them or backend.Start rejects the zero
		// values ("compact-batch-size 0 too small"). Values mirror kine's flag
		// defaults in pkg/app/app.go (v0.16.2). CompactInterval stays 0 so the
		// apiserver drives compaction, matching kine's own default.
		_, backend, err := sqlite.New(s.ctx, &sync.WaitGroup{}, &drivers.Config{
			DataSourceName:        dsn,
			CompactInterval:       0,
			CompactIntervalJitter: 0,
			CompactTimeout:        5 * time.Second,
			CompactMinRetain:      1000,
			CompactBatchSize:      1000,
			PollBatchSize:         500,
		})
		if err != nil {
			s.cancel(fmt.Errorf("sqlite: create backend: %w", err))
			return
		}

		if err := backend.Start(s.ctx); err != nil {
			s.cancel(fmt.Errorf("sqlite: start backend: %w", err))
			return
		}

		s.sqliteBridge = kine.New(backend, "unix", 5*time.Second, "3.6.11")
		klog.InfoS("storage is ready", "backend", "sqlite", "path", dsn)
	})
	return s.sqliteBridge
}

func (s *StorageImpl) embeddedEtcd(dataDir string) *embed.Etcd {
	s.embeddedOnce.Do(func() {
		await(s.ctx, s.transportProvided)
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

		await(s.ctx, server.Server.ReadyNotify())
		klog.InfoS("storage is ready", "backend", "embedded", "path", cfg.Dir, "port", s.Port())

		s.embedded = server
	})
	return s.embedded
}

func (s *StorageImpl) Transport() storagebackend.TransportConfig {
	await(s.ctx, s.transportProvided)
	return s.transport
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

func (s *StorageImpl) WithServerConfig(cfg *server.Config) v1.Storage {
	s.serverConfigOnce.Do(func() {
		s.serverConfig = cfg
		s.rest = s.serverConfig.RESTOptionsGetter
		s.serverConfig.RESTOptionsGetter = s
		close(s.serverConfigProvided)
	})
	return s
}

func (s *StorageImpl) ServerConfig() *server.Config {
	await(s.ctx, s.serverConfigProvided)
	return s.serverConfig
}

func (s *StorageImpl) GetRESTOptions(resource schema.GroupResource, example runtime.Object) (generic.RESTOptions, error) {
	await(s.ctx, s.serverConfigProvided)
	opts, err := s.rest.GetRESTOptions(resource, example)
	if err != nil {
		return opts, err
	}
	decorator := opts.Decorator
	opts.Decorator = func(
		config *storagebackend.ConfigForResource,
		resourcePrefix string,
		keyFunc func(obj runtime.Object) (string, error),
		newFunc func() runtime.Object,
		newListFunc func() runtime.Object,
		getAttrsFunc kubestorage.AttrFunc,
		trigger kubestorage.IndexerFuncs,
		indexers *cache.Indexers,
	) (kubestorage.Interface, factory.DestroyFunc, error) {
		inner, destroy, err := decorator(config, resourcePrefix, keyFunc, newFunc, newListFunc, getAttrsFunc, trigger, indexers)
		if err != nil {
			return inner, destroy, err
		}
		klog.V(2).InfoS("intercepted storage decorator call", "resourcePrefix", resourcePrefix)
		return inner, destroy, nil
	}
	return opts, nil
}

type klogWriter struct{}

func (klogWriter) Write(p []byte) (int, error) {
	klog.V(2).Info(string(p))
	return len(p), nil
}

func (klogWriter) Sync() error { return nil }
