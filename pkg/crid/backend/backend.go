package backend

import (
	"context"
	"errors"
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
	v1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	remote "k8s.io/cri-client/pkg"
	"k8s.io/klog/v2"
	"k8s.io/kubelet/pkg/cri/streaming"
	"k8s.io/kubernetes/pkg/kubelet/cadvisor"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	"k8s.io/kubernetes/pkg/kubelet/container"

	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/kubernetes/pkg/volume/util/subpath"
	"k8s.io/mount-utils"
)

type Runtime string

const (
	Docker Runtime = "docker"
	Podman Runtime = "podman"
)

// DetectFunc probes for a container runtime and returns a Backend if found.
type DetectFunc func(ctx context.Context, name, dataDir string) Backend

// Runtimes maps supported runtimes to their detect functions.
// Populated by runtime packages via init().
var Runtimes = map[Runtime]DetectFunc{}

type NetworkType string

const (
	NetworkHost   NetworkType = "Host"
	NetworkBridge NetworkType = "Bridge"
)

type NetworkSpec struct {
	Name    string
	Type    NetworkType
	Gateway net.IP
	Network net.IPNet
}

type NetworkProvider interface {
	DefaultNetwork(ctx context.Context) NetworkSpec
	GetNetwork(ctx context.Context, name string) (*NetworkSpec, error)
	CreateNetwork(ctx context.Context, name string, net *net.IPNet, gateway *net.IP) (NetworkSpec, error)
	RemoveNetwork(ctx context.Context, name string) error
	ConnectNetwork(ctx context.Context, network, containerID string, aliases []string) error
	DisconnectNetwork(ctx context.Context, network, containerID string) error
}

type Hosts interface {
	Log() component.Logger
	Hostname() string
	WithContext(ctx context.Context) Hosts
	WithHost(name string, addrs []string) Hosts
	WithBackend(runtime Runtime, backend Backend) Hosts
	Entries(ctx context.Context, network NetworkType) map[string][]string
	ExtraHosts(ctx context.Context, network NetworkType) []string
	HostAliases(ctx context.Context, network NetworkType) []v1.HostAlias
}

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

// Event is a generic lifecycle event emitted by a Driver.
type Event struct {
	Resource   EventResource
	Action     EventAction
	ID         string
	Attributes map[string]string
	TimeNano   int64
}

// EventStream is a single event channel pair returned by Driver.Events.
type EventStream struct {
	Events <-chan Event
	Errors <-chan error
}

type Into[State any, Container any, InternalEvent any] interface {
	Container(Container) *runtimeapi.Container
	PodSandbox(Container) *runtimeapi.PodSandbox
	ContainerState(State) runtimeapi.ContainerState
	PodState(State) runtimeapi.PodSandboxState
	Event(InternalEvent) *Event
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
	Networks() NetworkProvider

	Run(img string, cmd []string, binds []string, host bool, cb func(string) error) error
	Events(ctx context.Context) *EventStream
}

// Backend is the full interface consumers use
type Backend interface {
	Name() Runtime
	Context() context.Context

	Start(ctx context.Context, hosts Hosts, broadcaster record.EventBroadcaster, pluginsDir, registrationDir string, clean bool) error
	Stop(ctx context.Context) error
	clean() error

	Streaming() streaming.Server
	Labels() labels.LabelProvider
	Images() internalapi.ImageManagerService
	Containers() internalapi.RuntimeService
	Volumes() csipb.ControllerServer
	ContainerManager() cm.ContainerManager
	Mounter() mount.Interface
	Cadvisor() cadvisor.Interface
	OS() container.OSInterface
	Subpath() subpath.Interface
	HostUtils() hostutil.HostUtils
	EventBroadcaster() record.EventBroadcaster
	EventRecorder() *EventRecorderImpl

	// Storage
	CSI() CSI

	// Host information — probed from inside the container runtime
	HostInfo() (*HostInfo, error)
	HostnameOverride() string

	// Hosts returns the host provider for /etc/hosts injection.
	Hosts() Hosts

	// Subscribe returns a channel that receives all container lifecycle events.
	// Each subscriber gets its own channel; closing the context unsubscribes.
	Subscribe() <-chan Event

	// Networking
	Networks() NetworkProvider
	IPAM() Ipam

	// KubeClient
	KubeClient() clientset.Interface
	WithKubeClient(client clientset.Interface) Backend
}

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
	volumes       csipb.ControllerServer
	cm            cm.ContainerManager
	mounter       mount.Interface
	cadvisor      cadvisor.Interface
	os            container.OSInterface
	subpath       subpath.Interface
	hostUtils     hostutil.HostUtils
	eventRecorder *EventRecorderImpl

	traceProvider tp.TracerProvider
	hostInfo      *HostInfo
	csi           CSI

	// hosts
	hosts    Hosts
	ipam     Ipam
	ipamOnce sync.Once

	// events
	broadcaster   record.EventBroadcaster
	subscribers   []chan Event
	subscribersMu sync.Mutex
	streamOnce    sync.Once

	// sync
	mu sync.Mutex

	// kube client
	kubeClient clientset.Interface
}

func NewBackend(name string, d Driver) Backend {
	backend := &BackendImpl{
		Driver:        d,
		name:          name,
		log:           component.NewLogger(string(d.Name())),
		klog:          klog.Background(),
		traceProvider: tp.NewTracerProvider(),
	}
	return backend
}

func (b *BackendImpl) Context() context.Context {
	return b.ctx
}

func (b *BackendImpl) Start(ctx context.Context, hosts Hosts, broadcaster record.EventBroadcaster, pluginsDir, registrationDir string, clean bool) error {
	b.ctx = ctx
	b.hosts = hosts
	b.broadcaster = broadcaster

	os.MkdirAll(b.DataDir(), 0o755)
	os.Remove(b.socket())
	lis, err := net.Listen("unix", b.socket())
	if err != nil {
		return component.WrapErr(b.log, fmt.Errorf("listen on socket %s: %w", b.socket(), err))
	}

	streamCfg := streaming.DefaultConfig
	streamCfg.Addr = "127.0.0.1:0"
	b.streaming, err = streaming.NewServer(streamCfg, b.Driver)
	if err != nil {
		return component.WrapErr(b.log, err)
	}
	go func() {
		if err := b.streaming.Start(true); err != nil {
			b.log.Debug().Err(err).Msg("backend streaming server exited")
		}
	}()

	b.grpc = grpc.NewServer()
	runtimeapi.RegisterRuntimeServiceServer(b.grpc, b.ContainerServer())
	runtimeapi.RegisterImageServiceServer(b.grpc, b.ImageServer())

	go func() {
		if err := b.grpc.Serve(lis); err != nil {
			b.log.Error().Err(err).Msg("backend gRPC server exited")
		}
	}()

	// Start CSI driver — endpoint starts now, registration deferred until node ready
	if err := b.CSI().Start(ctx, pluginsDir, registrationDir); err != nil {
		return component.WrapErr(b.log, err)
	}

	b.log.Debug().Str("backend", string(b.Name())).Str("socket", b.socket()).Msg("backend started")
	return nil
}

func (b *BackendImpl) clean() error {
	b.log.Debug().Str("backend", string(b.Name())).Msg("clean")
	containerServer := b.ContainerServer()
	volumeServer := b.VolumeServer()
	var errs []error

	// Remove sandboxes (and thus all containers) via CRI Server
	if sandboxes, err := containerServer.ListPodSandbox(b.ctx, nil); err == nil {
		for _, sb := range sandboxes.GetItems() {
			b.log.Debug().Str("id", sb.Id[:min(12, len(sb.Id))]).Msg("removing sandbox")
			if _, err := containerServer.RemovePodSandbox(b.ctx, &runtimeapi.RemovePodSandboxRequest{PodSandboxId: sb.Id}); err != nil {
				errs = append(errs, fmt.Errorf("remove sandbox %s: %w", sb.Id, err))
			}
		}
	}

	// Remove volumes via CSI Server
	if volumes, err := volumeServer.ListVolumes(b.ctx, &csipb.ListVolumesRequest{}); err == nil {
		for _, vol := range volumes.GetEntries() {
			b.log.Debug().Str("id", vol.GetVolume().GetVolumeId()[:min(12, len(vol.GetVolume().GetVolumeId()))]).Msg("removing volume")
			if _, err := volumeServer.DeleteVolume(b.ctx, &csipb.DeleteVolumeRequest{VolumeId: vol.GetVolume().GetVolumeId()}); err != nil {
				errs = append(errs, fmt.Errorf("delete volume %s: %w", vol.GetVolume().GetVolumeId(), err))
			}
		}
	}

	return errors.Join(errs...)
}

func (b *BackendImpl) Subscribe() <-chan Event {
	b.streamOnce.Do(func() {
		stream := b.Driver.Events(b.ctx)
		go func() {
			for {
				select {
				case <-b.ctx.Done():
					return
				case err := <-stream.Errors:
					if b.ctx.Err() != nil {
						return
					}
					b.log.Error().Err(err).Msg("driver event stream error")
					b.subscribersMu.Lock()
					for _, sub := range b.subscribers {
						close(sub)
					}
					b.subscribers = nil
					b.subscribersMu.Unlock()
					return
				case ev, ok := <-stream.Events:
					if !ok {
						return
					}
					if ev.Resource == ResourceContainer && !b.Labels().IsManaged(ev.Attributes) {
						continue
					}
					b.subscribersMu.Lock()
					for _, sub := range b.subscribers {
						select {
						case sub <- ev:
						default:
						}
					}
					b.subscribersMu.Unlock()
				}
			}
		}()
	})

	b.subscribersMu.Lock()
	defer b.subscribersMu.Unlock()
	ch := make(chan Event)
	b.subscribers = append(b.subscribers, ch)
	return ch
}

func (b *BackendImpl) Stop(ctx context.Context) error {
	containers := b.ContainerServer()
	if containers == nil {
		return nil
	}

	// Stop running sandboxes via CRI
	if running, err := containers.ListPodSandbox(ctx, &runtimeapi.ListPodSandboxRequest{Filter: &runtimeapi.PodSandboxFilter{State: &runtimeapi.PodSandboxStateValue{State: runtimeapi.PodSandboxState_SANDBOX_READY}}}); err == nil {
		for _, sb := range running.GetItems() {
			b.log.Debug().Str("id", sb.Id[:min(12, len(sb.Id))]).Msg("stopping sandbox")
			if _, err := containers.StopPodSandbox(ctx, &runtimeapi.StopPodSandboxRequest{PodSandboxId: sb.Id}); err != nil {
				b.log.Error().Str("id", sb.Id[:min(12, len(sb.Id))]).Err(err).Msg("failed to stop sandbox")
			}
		}
	}

	// Remove stopped sandboxes via CRI
	if stopped, err := containers.ListPodSandbox(ctx, &runtimeapi.ListPodSandboxRequest{Filter: &runtimeapi.PodSandboxFilter{State: &runtimeapi.PodSandboxStateValue{State: runtimeapi.PodSandboxState_SANDBOX_NOTREADY}}}); err == nil {
		for _, sb := range stopped.GetItems() {
			b.log.Debug().Str("id", sb.Id[:min(12, len(sb.Id))]).Msg("removing sandbox")
			if _, err := containers.RemovePodSandbox(ctx, &runtimeapi.RemovePodSandboxRequest{PodSandboxId: sb.Id}); err != nil {
				b.log.Error().Str("id", sb.Id[:min(12, len(sb.Id))]).Err(err).Msg("failed to remove sandbox")
			}
		}
	}

	if b.grpc != nil {
		done := make(chan struct{})
		go func() { b.grpc.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-ctx.Done():
			b.grpc.Stop()
		}
	}

	if b.streaming != nil {
		err := b.streaming.Stop()
		if err != nil {
			b.log.Debug().Err(err).Msg("backend streaming server exited")
		}
	}

	os.Remove(b.socket())
	b.log.Debug().Str("backend", string(b.Name())).Msg("backend stopped")
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

func (b *BackendImpl) Volumes() csipb.ControllerServer {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.volumes == nil {
		b.volumes = b.VolumeServer()
	}
	return b.volumes
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

func (b *BackendImpl) EventRecorder() *EventRecorderImpl {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.eventRecorder == nil {
		b.eventRecorder = NewEventRecorder(b)
	}
	return b.eventRecorder
}

func (b *BackendImpl) CSI() CSI {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.csi == nil {
		b.csi = NewCSI(b)
	}
	return b.csi
}

func (b *BackendImpl) HostInfo() (*HostInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.hostInfo == nil {
		var err error
		b.hostInfo, err = NewHostInfo(b.Driver, b.hosts)
		if err != nil {
			b.log.Warn().Err(err).Msg("failed to probe host info")
			return nil, err
		}
		b.log.Debug().Str("hostname", b.hostInfo.Hostname).Str("domain", b.hostInfo.Domain).Str("machineId", b.hostInfo.MachineID).Msg("host info probed successfully")
	}
	return b.hostInfo, nil
}

func (b *BackendImpl) HostnameOverride() string {
	info, err := b.HostInfo()
	if err != nil {
		return ""
	}
	if info.MachineID != "" {
		return fmt.Sprintf("%s-%s", info.Hostname, info.MachineID)
	}
	return info.Hostname
}

func (b *BackendImpl) Hosts() Hosts {
	return b.hosts
}

func (b *BackendImpl) IPAM() Ipam {
	b.ipamOnce.Do(func() {
		b.ipam = NewIpam(b.Driver)
	})
	return b.ipam
}

func (b *BackendImpl) WithKubeClient(client clientset.Interface) Backend {
	b.kubeClient = client
	return b
}

func (b *BackendImpl) KubeClient() clientset.Interface {
	return b.kubeClient
}

func (b *BackendImpl) socket() string {
	return filepath.Join(b.DataDir(), "cri.sock")
}

var _ Backend = &BackendImpl{}
