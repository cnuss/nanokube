package backend

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cnuss/nanokube/pkg/component"
	"github.com/cnuss/nanokube/pkg/crid/labels"
	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	tp "go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
	storagev1ac "k8s.io/client-go/applyconfigurations/storage/v1"
	"k8s.io/client-go/tools/record"
	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	remote "k8s.io/cri-client/pkg"
	"k8s.io/klog/v2"
	"k8s.io/kubelet/pkg/cri/streaming"
	"k8s.io/kubernetes/pkg/kubelet/cadvisor"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	"k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/prober"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/kubernetes/pkg/volume/util/subpath"
	"k8s.io/mount-utils"
)

type Runtime string

const (
	Docker Runtime = "docker"
	Podman Runtime = "podman"
)

type Network string

const (
	NetworkHost   Network = "Host"
	NetworkBridge Network = "Bridge"
)

// EventResource classifies the source of an event.
type EventResource string

const (
	ResourceContainer EventResource = "container"
	ResourceImage     EventResource = "image"
	ResourceVolume    EventResource = "volume"
	ResourceNetwork   EventResource = "network"
)

// EventAction classifies what happened.
type EventAction string

const (
	ActionCreate     EventAction = "create"
	ActionStart      EventAction = "start"
	ActionRestart    EventAction = "restart"
	ActionStop       EventAction = "stop"
	ActionKill       EventAction = "kill"
	ActionDie        EventAction = "die"
	ActionOOM        EventAction = "oom"
	ActionPause      EventAction = "pause"
	ActionUnpause    EventAction = "unpause"
	ActionDestroy    EventAction = "destroy"
	ActionRemove     EventAction = "remove"
	ActionPull       EventAction = "pull"
	ActionPush       EventAction = "push"
	ActionTag        EventAction = "tag"
	ActionUntag      EventAction = "untag"
	ActionDelete     EventAction = "delete"
	ActionMount      EventAction = "mount"
	ActionUnmount    EventAction = "unmount"
	ActionConnect    EventAction = "connect"
	ActionDisconnect EventAction = "disconnect"

	ActionExecCreate EventAction = "exec_create"
	ActionExecStart  EventAction = "exec_start"
	ActionExecDie    EventAction = "exec_die"

	ActionHealthStatus          EventAction = "health_status"
	ActionHealthStatusRunning   EventAction = "health_status: running"
	ActionHealthStatusHealthy   EventAction = "health_status: healthy"
	ActionHealthStatusUnhealthy EventAction = "health_status: unhealthy"
)

// Event is a generic lifecycle event emitted by a Driver.
type Event struct {
	Resource   EventResource
	Action     EventAction
	ID         string
	Attributes map[string]string
	TimeNano   int64
}

type Into[State any, Container any] interface {
	Container(Container) *runtimeapi.Container
	PodSandbox(Container) *runtimeapi.PodSandbox
	ContainerState(State) runtimeapi.ContainerState
	PodState(State) runtimeapi.PodSandboxState
}

// Driver is what docker/podman implement
type Driver interface {
	streaming.Runtime

	Name() Runtime
	DataDir() string
	Labels() labels.LabelProvider
	Domain() string

	ImageServer() runtimeapi.ImageServiceServer
	ContainerServer() runtimeapi.RuntimeServiceServer
	VolumeServer() csipb.ControllerServer

	Run(img string, cmd []string, binds []string, host bool, cb func(string) error) error
	Events(ctx context.Context) (<-chan Event, <-chan error)
	Cleanup(ctx context.Context) error
}

// Backend is the full interface consumers use
type Backend interface {
	Name() Runtime
	Context() context.Context

	Start(ctx context.Context, broadcaster record.EventBroadcaster, pluginsDir, registrationDir string) error
	Stop(ctx context.Context) error

	Streaming() streaming.Server
	Labels() labels.LabelProvider
	Images() internalapi.ImageManagerService
	Containers() internalapi.RuntimeService
	ContainerManager() cm.ContainerManager
	Mounter() mount.Interface
	Cadvisor() cadvisor.Interface
	OS() container.OSInterface
	Subpath() subpath.Interface
	HostUtils() hostutil.HostUtils
	EventBroadcaster() record.EventBroadcaster
	EventRecorder() record.EventRecorder
	Prober() prober.Manager

	// Storage
	CSI() *CSI
	StorageClass() *storagev1ac.StorageClassApplyConfiguration

	// Host information — probed from inside the container runtime
	HostInfo() (*HostInfo, error)

	// ExtraHosts returns hostname:ip entries to inject into container /etc/hosts.
	ExtraHosts(network Network) []string
	SetExtraHosts(fn func(Network) []string)

	// Subscribe returns a channel that receives all container lifecycle events.
	// Each subscriber gets its own channel; closing the context unsubscribes.
	Subscribe() <-chan Event

	// Cleanup force-removes all containers managed by this backend.
	Cleanup(ctx context.Context) error
}

var _ Backend = &BackendImpl{}

// BackendImpl adds shared behavior on top of a Driver
type BackendImpl struct {
	Driver
	// identity
	name string
	// context
	ctx context.Context

	// servers
	grpc      *grpc.Server
	streaming streaming.Server

	// clients
	log           component.Logger
	klog          klog.Logger
	images        internalapi.ImageManagerService
	containers    internalapi.RuntimeService
	cm            cm.ContainerManager
	mounter       mount.Interface
	cadvisor      cadvisor.Interface
	os            container.OSInterface
	subpath       subpath.Interface
	hostUtils     hostutil.HostUtils
	eventRecorder record.EventRecorder
	prober        prober.Manager
	traceProvider tp.TracerProvider
	hostInfo      *HostInfo
	csi           *CSI

	// hosts
	extraHosts func(Network) []string

	// events
	broadcaster record.EventBroadcaster
	subscribers []chan Event

	// node lifecycle
	nodeReady     chan struct{}
	nodeReadyOnce sync.Once

	// sync
	mu sync.Mutex
}

func NewBackend(name string, d Driver) Backend {
	backend := &BackendImpl{
		Driver:        d,
		name:          name,
		log:           component.NewLogger(string(d.Name())),
		klog:          klog.Background(),
		traceProvider: tp.NewTracerProvider(),
		nodeReady:     make(chan struct{}),
	}
	return backend
}

func (b *BackendImpl) Context() context.Context {
	return b.ctx
}

func (b *BackendImpl) Start(ctx context.Context, broadcaster record.EventBroadcaster, pluginsDir, registrationDir string) error {
	b.log.Info().Str("backend", string(b.Name())).Msg("starting")
	b.ctx = ctx
	b.broadcaster = broadcaster

	os.MkdirAll(b.DataDir(), 0o755)
	os.Remove(b.socket())
	lis, err := net.Listen("unix", b.socket())
	if err != nil {
		return fmt.Errorf("failed to listen on socket %s: %w", b.socket(), err)
	}

	streamCfg := streaming.DefaultConfig
	streamCfg.Addr = "127.0.0.1:0"
	b.streaming, err = streaming.NewServer(streamCfg, b.Driver)
	if err != nil {
		return fmt.Errorf("failed to start streaming server: %w", err)
	}
	go func() {
		b.log.Info().Msg("backend streaming server starting")
		if err := b.streaming.Start(true); err != nil {
			b.log.Info().Err(err).Msg("backend streaming server exited")
		}
	}()

	b.grpc = grpc.NewServer()
	runtimeapi.RegisterRuntimeServiceServer(b.grpc, b.ContainerServer())
	runtimeapi.RegisterImageServiceServer(b.grpc, b.ImageServer())

	go func() {
		b.log.Info().Str("socket", b.socket()).Msg("backend gRPC server listening")
		if err := b.grpc.Serve(lis); err != nil {
			b.log.Error().Err(err).Msg("backend gRPC server exited")
		}
	}()

	// Start consuming driver events and fan out to subscribers
	eventCh, errCh := b.Driver.Events(ctx)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-errCh:
				if ctx.Err() != nil {
					return
				}
				b.log.Error().Err(err).Msg("driver event stream error")
				return
			case ev, ok := <-eventCh:
				if !ok {
					return
				}
				b.log.Debug().
					Str("resource", string(ev.Resource)).
					Str("action", string(ev.Action)).
					Str("id", ev.ID).
					Msg("event")
				b.mu.Lock()
				for _, sub := range b.subscribers {
					select {
					case sub <- ev:
					default:
					}
				}
				b.mu.Unlock()
			}
		}
	}()

	// Start CSI driver — endpoint starts now, registration deferred until node ready
	if err := b.CSI().Start(ctx, pluginsDir, registrationDir); err != nil {
		return fmt.Errorf("csi: %w", err)
	}

	b.log.Info().Msg("backend started")
	return nil
}

func (b *BackendImpl) SignalNodeReady() {
	b.nodeReadyOnce.Do(func() { close(b.nodeReady) })
}

func (b *BackendImpl) Subscribe() <-chan Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan Event, 64)
	b.subscribers = append(b.subscribers, ch)
	return ch
}

func (b *BackendImpl) Stop(ctx context.Context) error {
	b.log.Info().Str("backend", string(b.Name())).Msg("stopping")

	if b.grpc != nil {
		done := make(chan struct{})
		go func() { b.grpc.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-ctx.Done():
			b.grpc.Stop()
		}
		os.Remove(b.socket())
	}

	if b.streaming != nil {
		err := b.streaming.Stop()
		if err != nil {
			b.log.Info().Err(err).Msg("backend streaming server exited")
		}
	}

	b.log.Info().Str("backend", string(b.Name())).Msg("backend stopped")
	return nil
}

func (b *BackendImpl) Streaming() streaming.Server {
	return b.streaming
}

func (b *BackendImpl) Labels() labels.LabelProvider {
	return b.Driver.Labels()
}

func (b *BackendImpl) Images() internalapi.ImageManagerService {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.images == nil {
		runtimeService, err := remote.NewRemoteImageService(b.socket(), 30*time.Second, b.traceProvider, &b.klog)
		if err != nil {
			b.log.Warn().Err(err).Msg("failed to create image service client")
			return nil
		}
		b.images = runtimeService
	}
	return b.images
}

func (b *BackendImpl) Containers() internalapi.RuntimeService {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.containers == nil {
		runtimeService, err := remote.NewRemoteRuntimeService(b.socket(), 30*time.Second, b.traceProvider, &b.klog)
		if err != nil {
			b.log.Warn().Err(err).Msg("failed to create runtime service client")
			return nil
		}
		b.containers = runtimeService
	}
	return b.containers
}

func (b *BackendImpl) ContainerManager() cm.ContainerManager {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cm == nil {
		b.cm = NewContainerManager(b)
	}
	return b.cm
}

func (b *BackendImpl) Mounter() mount.Interface {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.mounter == nil {
		b.mounter = NewMounter(b)
	}
	return b.mounter
}

func (b *BackendImpl) Cadvisor() cadvisor.Interface {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cadvisor == nil {
		b.cadvisor = NewCadvisor(b)
	}
	return b.cadvisor
}

func (b *BackendImpl) OS() container.OSInterface {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.os == nil {
		b.os = NewOS(b)
	}
	return b.os
}

func (b *BackendImpl) Subpath() subpath.Interface {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.subpath == nil {
		b.subpath = NewSubpath(b)
	}
	return b.subpath
}

func (b *BackendImpl) HostUtils() hostutil.HostUtils {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.hostUtils == nil {
		b.hostUtils = NewHostUtils(b)
	}
	return b.hostUtils
}

func (b *BackendImpl) EventBroadcaster() record.EventBroadcaster {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.broadcaster
}

func (b *BackendImpl) EventRecorder() record.EventRecorder {
	b.mu.Lock()
	if b.eventRecorder != nil {
		defer b.mu.Unlock()
		return b.eventRecorder
	}
	b.mu.Unlock()

	// Build outside the lock — NewEventRecorder calls Subscribe() which
	// also takes b.mu, so we must not hold it here.
	er := NewEventRecorder(b)

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.eventRecorder == nil {
		b.eventRecorder = er
	}
	return b.eventRecorder
}

func (b *BackendImpl) Prober() prober.Manager {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.prober == nil {
		b.prober = NewProber(b)
	}
	return b.prober
}

func (b *BackendImpl) CSI() *CSI {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.csi == nil {
		b.csi = NewCSI(b)
	}
	return b.csi
}

func (b *BackendImpl) StorageClass() *storagev1ac.StorageClassApplyConfiguration {
	return b.CSI().StorageClass()
}

func (b *BackendImpl) HostInfo() (*HostInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.hostInfo == nil {
		var err error
		b.hostInfo, err = NewHostInfo(b.Driver)
		if err != nil {
			b.log.Warn().Err(err).Msg("failed to probe host info")
			return nil, err
		}
		b.log.Trace().Any("hostinfo", b.hostInfo).Msg("host info probed successfully")
	}
	return b.hostInfo, nil
}

func (b *BackendImpl) ExtraHosts(network Network) []string {
	if b.extraHosts != nil {
		return b.extraHosts(network)
	}
	return nil
}

func (b *BackendImpl) SetExtraHosts(fn func(Network) []string) {
	b.extraHosts = fn
}

func (b *BackendImpl) socket() string {
	return filepath.Join(b.DataDir(), "cri.sock")
}
