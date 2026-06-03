package storage

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cnuss/nanokube/pkg/nanokube"
	v1 "github.com/cnuss/nanokube/pkg/v1"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/kubernetes"
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
	"k8s.io/apiserver/pkg/storage/etcd3"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"
	"k8s.io/apiserver/pkg/storage/value"
	"k8s.io/apiserver/pkg/storage/value/encrypt/identity"
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

	client     *clientv3.Client
	clientOnce sync.Once

	kubeClient     *kubernetes.Client
	kubeClientOnce sync.Once

	started      atomic.Bool
	ready        chan struct{}
	shutdown     chan struct{}
	shutdownOnce sync.Once
	done         chan struct{}
}

type StorageClientImpl struct {
	client      clientv3.Client
	storage     v1.Storage
	inner       kubestorage.Interface
	resource    schema.GroupResource
	codec       runtime.Codec
	transformer value.Transformer
	// pathPrefix is the etcd key prefix for all keys (e.g. "/registry/"),
	// normalized to start at "/" and end in "/".
	pathPrefix string
	// resourcePrefix is the per-resource key segment (e.g. "/pods").
	resourcePrefix string
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

func (s *StorageImpl) WithResource(inner kubestorage.Interface, config *storagebackend.ConfigForResource, resourcePrefix string) v1.StorageClient {
	// Match upstream etcd3 store: pathPrefix starts at "/" and ends in "/".
	pathPrefix := path.Join("/", config.Prefix)
	if !strings.HasSuffix(pathPrefix, "/") {
		pathPrefix += "/"
	}
	return &StorageClientImpl{
		client:         *s.Client(),
		storage:        s,
		inner:          inner,
		resource:       config.GroupResource,
		codec:          config.Codec,
		transformer:    config.Transformer,
		pathPrefix:     pathPrefix,
		resourcePrefix: resourcePrefix,
	}
}

func (s *StorageImpl) GetRESTOptions(resource schema.GroupResource, example runtime.Object) (generic.RESTOptions, error) {
	<-nanokube.Await(s.nano, s.innerProvided)
	opts, err := s.inner.GetRESTOptions(resource, example)
	if err != nil {
		return opts, err
	}
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
		// Build a raw etcd3 store directly against the embedded etcd instead of
		// delegating to the apiserver's decorator, mirroring newETCD3Storage.
		transformer := config.Transformer
		if transformer == nil {
			transformer = identity.NewEncryptCheckTransformer()
		}
		versioner := storage.APIObjectVersioner{}
		decoder := etcd3.NewDefaultDecoder(config.Codec, versioner)
		inner, err := etcd3.New(
			s.kube(),
			nil, // compactor: embedded etcd handles compaction
			config.Codec,
			newFunc,
			newListFunc,
			config.Prefix,
			resourcePrefix,
			config.GroupResource,
			transformer,
			config.LeaseManagerConfig,
			decoder,
			versioner,
		)
		if err != nil {
			return nil, nil, err
		}
		klog.Infof("Wrapping storage for %s", resource)
		return s.WithResource(inner, config, resourcePrefix), inner.Close, nil
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

func (s *StorageImpl) Client() *clientv3.Client {
	s.clientOnce.Do(func() {
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   s.Servers(),
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			s.nano.CancelErr(err)
			return
		}
		s.client = client
	})
	return s.client
}

// kube wraps the shared clientv3 client in the etcd3 kubernetes.Client helper
// (single-RTT optimistic ops) required by etcd3.New. It reuses Client() so
// there is only one connection to the embedded etcd.
func (s *StorageImpl) kube() *kubernetes.Client {
	s.kubeClientOnce.Do(func() {
		c := s.Client()
		if c == nil {
			return
		}
		kc := &kubernetes.Client{Client: c}
		kc.Kubernetes = kc
		s.kubeClient = kc
	})
	return s.kubeClient
}

type klogWriter struct{}

func (klogWriter) Write(p []byte) (int, error) {
	klog.V(2).Info(string(p))
	return len(p), nil
}

func (klogWriter) Sync() error { return nil }

// -- kubestorage.Interface METHODS --

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

// -- VERSIONER METHODS --

func (sc *StorageClientImpl) ObjectResourceVersion(obj runtime.Object) (uint64, error) {
	return sc.inner.Versioner().ObjectResourceVersion(obj)
}

func (sc *StorageClientImpl) ParseResourceVersion(resourceVersion string) (uint64, error) {
	return sc.inner.Versioner().ParseResourceVersion(resourceVersion)
}

func (sc *StorageClientImpl) PrepareObjectForStorage(obj runtime.Object) error {
	return sc.inner.Versioner().PrepareObjectForStorage(obj)
}

func (sc *StorageClientImpl) UpdateList(obj runtime.Object, resourceVersion uint64, continueValue string, remainingItemCount *int64) error {
	return sc.inner.Versioner().UpdateList(obj, resourceVersion, continueValue, remainingItemCount)
}

func (sc *StorageClientImpl) UpdateObject(obj runtime.Object, resourceVersion uint64) error {
	return sc.inner.Versioner().UpdateObject(obj, resourceVersion)
}

// -- TRANSFORMER METHODS (value.Transformer) --

func (sc *StorageClientImpl) TransformFromStorage(ctx context.Context, data []byte, dataCtx value.Context) ([]byte, bool, error) {
	return sc.transformer.TransformFromStorage(ctx, data, dataCtx)
}

func (sc *StorageClientImpl) TransformToStorage(ctx context.Context, data []byte, dataCtx value.Context) ([]byte, error) {
	return sc.transformer.TransformToStorage(ctx, data, dataCtx)
}

// -- SERIALIZER METHODS (runtime.Serializer == runtime.Codec) --

func (sc *StorageClientImpl) Encode(obj runtime.Object, w io.Writer) error {
	return sc.codec.Encode(obj, w)
}

func (sc *StorageClientImpl) Decode(data []byte, defaults *schema.GroupVersionKind, into runtime.Object) (runtime.Object, *schema.GroupVersionKind, error) {
	return sc.codec.Decode(data, defaults, into)
}

func (sc *StorageClientImpl) Identifier() runtime.Identifier {
	return sc.codec.Identifier()
}

// -- ACCESSORS --

func (sc *StorageClientImpl) PathPrefix() string {
	return sc.pathPrefix
}

func (sc *StorageClientImpl) GroupResource() schema.GroupResource {
	return sc.resource
}

func (sc *StorageClientImpl) ResourcePrefix() string {
	return sc.resourcePrefix
}
