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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	serveroptions "k8s.io/apiserver/pkg/server/options"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubeapiserver"
)

type StorageFactory interface {
	serverstorage.StorageFactory
	Default() *serverstorage.DefaultStorageFactory
	Run(ctx context.Context)
	Port() int
}

type StorageFactoryImpl struct {
	options Options

	inner     *serverstorage.DefaultStorageFactory
	innerOnce sync.Once

	config        *kubeapiserver.StorageFactoryConfig
	storageConfig *storagebackend.Config

	port     int
	portOnce sync.Once
}

var _ StorageFactory = &StorageFactoryImpl{}

func NewStorageFactory(options Options) StorageFactory {
	factory := &StorageFactoryImpl{
		options:       options,
		config:        kubeapiserver.NewStorageFactoryConfig(),
		storageConfig: storagebackend.NewDefaultConfig(fmt.Sprintf("/%s", options.Name()), nil),
	}
	factory.storageConfig.Transport.ServerList = []string{fmt.Sprintf("http://localhost:%d", factory.Port())}

	return factory
}

func (s *StorageFactoryImpl) Run(ctx context.Context) {
	dataDir := s.options.DataDirAt(DataDirEtcd)
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
	cfg.LogLevel = "fatal"

	server, err := embed.StartEtcd(cfg)
	if err != nil {
		klog.Fatalf("Failed to start etcd: %v", err)
	}

	select {
	case <-server.Server.ReadyNotify():
		klog.V(2).Infof("etcd ready on port %d", port)
	case <-time.After(30 * time.Second):
		server.Close()
		klog.Fatalf("etcd took too long to start")
	case <-ctx.Done():
		server.Close()
		return
	}

	<-ctx.Done()
	server.Close()
}

func (s *StorageFactoryImpl) Default() *serverstorage.DefaultStorageFactory {
	s.innerOnce.Do(func() {
		etcd := serveroptions.NewEtcdOptions(s.storageConfig)
		factoryComplete := s.config.Complete(etcd)

		inner, err := factoryComplete.New()
		if err != nil {
			klog.Fatalf("Failed to create storage factory: %v", err)
		}

		s.inner = inner
	})
	return s.inner
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
	return s.Default().Backends()
}

func (s *StorageFactoryImpl) Configs() []storagebackend.Config {
	return s.Default().Configs()
}

func (s *StorageFactoryImpl) NewConfig(groupResource schema.GroupResource, obj runtime.Object) (*storagebackend.ConfigForResource, error) {
	return s.Default().NewConfig(groupResource, obj)
}

func (s *StorageFactoryImpl) ResourcePrefix(groupResource schema.GroupResource) string {
	return s.Default().ResourcePrefix(groupResource)
}

// TODO: make StorageFactoryImpl return Storage
type Storage interface {
	storage.Interface
}

type StorageImpl struct {
	inner storage.Interface
}

var _ Storage = &StorageImpl{}

func (s *StorageImpl) CompactRevision() int64 {
	return s.inner.CompactRevision()
}

func (s *StorageImpl) Create(ctx context.Context, key string, obj runtime.Object, out runtime.Object, ttl uint64) error {
	return s.inner.Create(ctx, key, obj, out, ttl)
}

func (s *StorageImpl) Delete(ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions, validateDeletion storage.ValidateObjectFunc, cachedExistingObject runtime.Object, opts storage.DeleteOptions) error {
	return s.inner.Delete(ctx, key, out, preconditions, validateDeletion, cachedExistingObject, opts)
}

func (s *StorageImpl) EnableResourceSizeEstimation(keysFunc storage.KeysFunc) error {
	return s.inner.EnableResourceSizeEstimation(keysFunc)
}

func (s *StorageImpl) Get(ctx context.Context, key string, opts storage.GetOptions, objPtr runtime.Object) error {
	return s.inner.Get(ctx, key, opts, objPtr)
}

func (s *StorageImpl) GetCurrentResourceVersion(ctx context.Context) (uint64, error) {
	return s.inner.GetCurrentResourceVersion(ctx)
}

func (s *StorageImpl) GetList(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
	return s.inner.GetList(ctx, key, opts, listObj)
}

func (s *StorageImpl) GuaranteedUpdate(ctx context.Context, key string, destination runtime.Object, ignoreNotFound bool, preconditions *storage.Preconditions, tryUpdate storage.UpdateFunc, cachedExistingObject runtime.Object) error {
	return s.inner.GuaranteedUpdate(ctx, key, destination, ignoreNotFound, preconditions, tryUpdate, cachedExistingObject)
}

func (s *StorageImpl) ReadinessCheck() error {
	return s.inner.ReadinessCheck()
}

func (s *StorageImpl) RequestWatchProgress(ctx context.Context) error {
	return s.inner.RequestWatchProgress(ctx)
}

func (s *StorageImpl) Stats(ctx context.Context) (storage.Stats, error) {
	return s.inner.Stats(ctx)
}

func (s *StorageImpl) Versioner() storage.Versioner {
	return s.inner.Versioner()
}

func (s *StorageImpl) Watch(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
	return s.inner.Watch(ctx, key, opts)
}
