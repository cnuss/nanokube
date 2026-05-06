package nanokube

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/klog/v2"

	v1 "github.com/cnuss/nanokube/pkg/v1"
)

type StorageFactoryImpl struct {
	ctx    context.Context
	config v1.Config

	defaultStorageFactory         *serverstorage.DefaultStorageFactory
	defaultStorageFactoryProvided chan struct{}

	port     int
	portOnce sync.Once

	runOnce sync.Once
	ready   chan struct{}
}

var _ v1.StorageFactory = &StorageFactoryImpl{}

func NewStorageFactory(config v1.Config) v1.StorageFactory {
	return &StorageFactoryImpl{
		ctx:                           config.Context(),
		config:                        config,
		defaultStorageFactoryProvided: make(chan struct{}),
		ready:                         make(chan struct{}),
	}
}

func (s *StorageFactoryImpl) Ready() <-chan struct{} {
	return s.ready
}

func (s *StorageFactoryImpl) WithDefault(factory *serverstorage.DefaultStorageFactory) v1.StorageFactory {
	s.defaultStorageFactory = factory
	close(s.defaultStorageFactoryProvided)
	return s
}

func (s *StorageFactoryImpl) Default() *serverstorage.DefaultStorageFactory {
	<-s.defaultStorageFactoryProvided

	s.runOnce.Do(func() {
		dataDir := s.config.Options().DataDirAt(v1.DataDirEtcd)
		port := s.Port()

		clientURL, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", port))
		peerURL, _ := url.Parse("http://127.0.0.1:0")

		cfg := embed.NewConfig()
		cfg.Dir = filepath.Join(dataDir, "data")
		cfg.ListenClientUrls = []url.URL{*clientURL}
		cfg.AdvertiseClientUrls = []url.URL{*clientURL}
		cfg.ListenPeerUrls = []url.URL{*peerURL}
		cfg.AdvertisePeerUrls = []url.URL{*peerURL}
		cfg.InitialCluster = "default=" + peerURL.String()
		// Restart: if WAL exists, this is an existing cluster
		if _, err := os.Stat(filepath.Join(cfg.Dir, "member", "wal")); err == nil {
			cfg.ClusterState = embed.ClusterStateFlagExisting
		}

		cfg.AutoCompactionRetention = "0"

		var etcdLevel zapcore.Level
		switch {
		case s.config.Options().Verbosity() >= 3:
			etcdLevel = zapcore.DebugLevel
		case s.config.Options().Verbosity() >= 2:
			etcdLevel = zapcore.InfoLevel
		case s.config.Options().Verbosity() >= 1:
			etcdLevel = zapcore.WarnLevel
		default:
			etcdLevel = zapcore.FatalLevel
		}
		cfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(zap.New(zapcore.NewCore(
			zapcore.NewConsoleEncoder(zap.NewProductionEncoderConfig()),
			zapcore.AddSync(klogWriter{}),
			etcdLevel,
		)))

		server, err := embed.StartEtcd(cfg)
		if err != nil {
			klog.Fatalf("Failed to start etcd: %v", err)
		}

		select {
		case <-server.Server.ReadyNotify():
			Log.Info("storage is ready", "port", port)
			close(s.ready)
		case <-time.After(30 * time.Second):
			server.Close()
			klog.Fatalf("etcd took too long to start")
		case <-s.ctx.Done():
			server.Close()
			return
		}

		go func() {
			<-s.ctx.Done()
			server.Close()
		}()
	})

	<-s.Ready()
	return s.defaultStorageFactory
}

func (s *StorageFactoryImpl) ServerList() []string {
	return []string{fmt.Sprintf("http://127.0.0.1:%d", s.Port())}
}

func (s *StorageFactoryImpl) Port() int {
	s.portOnce.Do(func() {
		port, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			klog.Fatalf("Failed to find free port for etcd: %v", err)
		}
		s.port = port.Addr().(*net.TCPAddr).Port
		port.Close()
	})
	return s.port
}

func (s *StorageFactoryImpl) Backends() []serverstorage.Backend {
	backends := s.Default().Backends()
	// TODO: intercept backends
	return backends
}

func (s *StorageFactoryImpl) Configs() []storagebackend.Config {
	configs := s.Default().Configs()
	// TODO: intercept configs
	return configs
}

func (s *StorageFactoryImpl) NewConfig(groupResource schema.GroupResource, obj runtime.Object) (*storagebackend.ConfigForResource, error) {
	config, err := s.Default().NewConfig(groupResource, obj)
	// TODO: intercept
	return config, err
}

func (s *StorageFactoryImpl) ResourcePrefix(groupResource schema.GroupResource) string {
	resourcePrefix := s.Default().ResourcePrefix(groupResource)
	// TODO intercept
	return resourcePrefix
}

type klogWriter struct{}

func (klogWriter) Write(p []byte) (int, error) {
	klog.V(2).Info(string(p))
	return len(p), nil
}

func (klogWriter) Sync() error { return nil }

// // TODO: make StorageFactoryImpl return Storage
// type Storage interface {
// 	storage.Interface
// }

// type StorageImpl struct {
// 	inner storage.Interface
// }

// var _ Storage = &StorageImpl{}

// func (s *StorageImpl) CompactRevision() int64 {
// 	return s.inner.CompactRevision()
// }

// func (s *StorageImpl) Create(ctx context.Context, key string, obj runtime.Object, out runtime.Object, ttl uint64) error {
// 	return s.inner.Create(ctx, key, obj, out, ttl)
// }

// func (s *StorageImpl) Delete(ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions, validateDeletion storage.ValidateObjectFunc, cachedExistingObject runtime.Object, opts storage.DeleteOptions) error {
// 	return s.inner.Delete(ctx, key, out, preconditions, validateDeletion, cachedExistingObject, opts)
// }

// func (s *StorageImpl) EnableResourceSizeEstimation(keysFunc storage.KeysFunc) error {
// 	return s.inner.EnableResourceSizeEstimation(keysFunc)
// }

// func (s *StorageImpl) Get(ctx context.Context, key string, opts storage.GetOptions, objPtr runtime.Object) error {
// 	return s.inner.Get(ctx, key, opts, objPtr)
// }

// func (s *StorageImpl) GetCurrentResourceVersion(ctx context.Context) (uint64, error) {
// 	return s.inner.GetCurrentResourceVersion(ctx)
// }

// func (s *StorageImpl) GetList(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
// 	return s.inner.GetList(ctx, key, opts, listObj)
// }

// func (s *StorageImpl) GuaranteedUpdate(ctx context.Context, key string, destination runtime.Object, ignoreNotFound bool, preconditions *storage.Preconditions, tryUpdate storage.UpdateFunc, cachedExistingObject runtime.Object) error {
// 	return s.inner.GuaranteedUpdate(ctx, key, destination, ignoreNotFound, preconditions, tryUpdate, cachedExistingObject)
// }

// func (s *StorageImpl) ReadinessCheck() error {
// 	return s.inner.ReadinessCheck()
// }

// func (s *StorageImpl) RequestWatchProgress(ctx context.Context) error {
// 	return s.inner.RequestWatchProgress(ctx)
// }

// func (s *StorageImpl) Stats(ctx context.Context) (storage.Stats, error) {
// 	return s.inner.Stats(ctx)
// }

// func (s *StorageImpl) Versioner() storage.Versioner {
// 	return s.inner.Versioner()
// }

// func (s *StorageImpl) Watch(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
// 	return s.inner.Watch(ctx, key, opts)
// }
