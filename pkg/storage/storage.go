package storage

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/cnuss/nanokube/pkg/nanokube"
	v1 "github.com/cnuss/nanokube/pkg/v1"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/storage"
	kubestorage "k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

type StorageImpl struct {
	nano v1.Nanokube

	config        *server.Config
	configOnce    sync.Once
	inner         generic.RESTOptionsGetter
	innerProvided chan struct{}

	servers     []string
	serversOnce sync.Once

	started      atomic.Bool
	ready        chan struct{}
	shutdown     chan struct{}
	shutdownOnce sync.Once
	done         chan struct{}
}

type StorageClientImpl struct {
	storage  v1.Storage
	inner    kubestorage.Interface
	resource schema.GroupResource
}

var _ v1.StorageClient = &StorageClientImpl{}

func NewStorage(nano v1.Nanokube) v1.Storage {
	return &StorageImpl{
		nano:          nano,
		innerProvided: make(chan struct{}),
		ready:         make(chan struct{}),
		shutdown:      make(chan struct{}),
		done:          make(chan struct{}),
	}
}

var _ v1.Storage = &StorageImpl{}

func (s *StorageImpl) Ready() <-chan struct{} {
	return s.ready
}

func (s *StorageImpl) Shutdown() {
	s.shutdownOnce.Do(func() { close(s.shutdown) })
	if s.started.Load() {
		<-s.done
	}
}

func (s *StorageImpl) SetConfig(config *server.Config) *server.Config {
	s.configOnce.Do(func() {
		s.inner = config.RESTOptionsGetter
		close(s.innerProvided)
		config.RESTOptionsGetter = s
		s.config = config
	})
	return s.config
}

func (s *StorageImpl) WithResource(inner kubestorage.Interface, resource schema.GroupResource) v1.StorageClient {
	return &StorageClientImpl{
		storage:  s,
		inner:    inner,
		resource: resource,
	}
}

func (s *StorageImpl) GetRESTOptions(resource schema.GroupResource, example runtime.Object) (generic.RESTOptions, error) {
	<-nanokube.Await(s.nano, s.innerProvided)
	opts, err := s.inner.GetRESTOptions(resource, example)
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
		getAttrsFunc storage.AttrFunc,
		trigger storage.IndexerFuncs,
		indexers *cache.Indexers,
	) (kubestorage.Interface, factory.DestroyFunc, error) {
		inner, destroy, err := decorator(config, resourcePrefix, keyFunc, newFunc, newListFunc, getAttrsFunc, trigger, indexers)
		if err != nil {
			return inner, destroy, err
		}
		klog.Infof("Wrapping storage for %s", resource)
		return s.WithResource(inner, resource), destroy, nil
	}
	return opts, nil
}

func (s *StorageImpl) Servers() []string {
	s.serversOnce.Do(func() {
		dataDir := s.nano.Options().DataDirAt(v1.DataDirEtcd)
		port := func() int {
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				s.nano.CancelErr(err)
				return 0
			}
			defer l.Close()
			return l.Addr().(*net.TCPAddr).Port
		}()

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
		case s.nano.Options().Verbosity() >= 3:
			etcdLevel = zapcore.DebugLevel
		case s.nano.Options().Verbosity() >= 2:
			etcdLevel = zapcore.InfoLevel
		case s.nano.Options().Verbosity() >= 1:
			etcdLevel = zapcore.WarnLevel
		default:
			etcdLevel = zapcore.FatalLevel
		}

		cfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(zap.New(zapcore.NewCore(
			zapcore.NewConsoleEncoder(zap.NewProductionEncoderConfig()),
			zapcore.AddSync(klogWriter{}),
			etcdLevel,
		)))

		s.started.Store(true)
		go func() {
			defer close(s.done)
			server, err := embed.StartEtcd(cfg)
			if err != nil {
				s.nano.CancelErr(err)
				return
			}
			<-nanokube.Await(s.nano, server.Server.ReadyNotify())
			if s.nano.Err() == nil {
				klog.InfoS("storage is ready", "port", port)
			}
			close(s.ready)
			<-s.shutdown
			server.Close()
			klog.InfoS("storage is done")
		}()

		s.servers = []string{fmt.Sprintf("http://127.0.0.1:%d", port)}
	})

	return s.servers
}

type klogWriter struct{}

func (klogWriter) Write(p []byte) (int, error) {
	klog.V(2).Info(string(p))
	return len(p), nil
}

func (klogWriter) Sync() error { return nil }

func (sc *StorageClientImpl) Versioner() storage.Versioner {
	// klog.Infof("Storage.Versioner %s", sc.resource)
	return sc.inner.Versioner()
}

func (sc *StorageClientImpl) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
	// klog.Infof("Storage.Create %s key=%s", sc.resource, key)
	return sc.inner.Create(ctx, key, obj, out, ttl)
}

func (sc *StorageClientImpl) Delete(ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions, validateDeletion storage.ValidateObjectFunc, cachedExistingObject runtime.Object, opts storage.DeleteOptions) error {
	// klog.Infof("Storage.Delete %s key=%s", sc.resource, key)
	return sc.inner.Delete(ctx, key, out, preconditions, validateDeletion, cachedExistingObject, opts)
}

func (sc *StorageClientImpl) Watch(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
	// klog.Infof("Storage.Watch %s key=%s", sc.resource, key)
	return sc.inner.Watch(ctx, key, opts)
}

func (sc *StorageClientImpl) Get(ctx context.Context, key string, opts storage.GetOptions, objPtr runtime.Object) error {
	// klog.Infof("Storage.Get %s key=%s", sc.resource, key)
	return sc.inner.Get(ctx, key, opts, objPtr)
}

func (sc *StorageClientImpl) GetList(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
	// klog.Infof("Storage.GetList %s key=%s", sc.resource, key)
	return sc.inner.GetList(ctx, key, opts, listObj)
}

func (sc *StorageClientImpl) GuaranteedUpdate(ctx context.Context, key string, destination runtime.Object, ignoreNotFound bool, preconditions *storage.Preconditions, tryUpdate storage.UpdateFunc, cachedExistingObject runtime.Object) error {
	// klog.Infof("Storage.GuaranteedUpdate %s key=%s", sc.resource, key)
	return sc.inner.GuaranteedUpdate(ctx, key, destination, ignoreNotFound, preconditions, tryUpdate, cachedExistingObject)
}

func (sc *StorageClientImpl) Stats(ctx context.Context) (storage.Stats, error) {
	// klog.Infof("Storage.Stats %s", sc.resource)
	return sc.inner.Stats(ctx)
}

func (sc *StorageClientImpl) ReadinessCheck() error {
	return sc.inner.ReadinessCheck()
}

func (sc *StorageClientImpl) RequestWatchProgress(ctx context.Context) error {
	// klog.Infof("Storage.RequestWatchProgress %s", sc.resource)
	return sc.inner.RequestWatchProgress(ctx)
}

func (sc *StorageClientImpl) GetCurrentResourceVersion(ctx context.Context) (uint64, error) {
	// klog.Infof("Storage.GetCurrentResourceVersion %s", sc.resource)
	return sc.inner.GetCurrentResourceVersion(ctx)
}

func (sc *StorageClientImpl) EnableResourceSizeEstimation(fn storage.KeysFunc) error {
	// klog.Infof("Storage.EnableResourceSizeEstimation %s", sc.resource)
	return sc.inner.EnableResourceSizeEstimation(fn)
}

func (sc *StorageClientImpl) CompactRevision() int64 {
	// klog.Infof("Storage.CompactRevision %s", sc.resource)
	return sc.inner.CompactRevision()
}
