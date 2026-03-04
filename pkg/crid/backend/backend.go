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
	tp "go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
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
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/kubernetes/pkg/volume/util/subpath"
)

type Runtime string

const (
	Docker Runtime = "docker"
	Podman Runtime = "podman"
)

// Driver is what docker/podman implement
type Driver interface {
	streaming.Runtime

	Name() Runtime
	DataDir() string

	ImageServer() runtimeapi.ImageServiceServer
	ContainerServer() runtimeapi.RuntimeServiceServer
}

// Backend is the full interface consumers use
type Backend interface {
	Name() Runtime

	Start(ctx context.Context) error
	Stop(ctx context.Context) error

	Images() internalapi.ImageManagerService
	Containers() internalapi.RuntimeService
	ContainerManager() cm.ContainerManager
	Volumes() volume.VolumePlugin
	Cadvisor() cadvisor.Interface
	OS() container.OSInterface
	Subpath() subpath.Interface
	HostUtils() hostutil.HostUtils
	EventRecorder() record.EventRecorder
	Prober() prober.Manager

	RunProbe(cmd []string) (string, error)
	RunHostProbe(cmd []string) (string, error)
}

var _ Backend = &BackendImpl{}

// BackendImpl adds shared behavior on top of a Driver
type BackendImpl struct {
	Driver

	// servers
	grpc      *grpc.Server
	streaming streaming.Server

	// clients
	log           component.Logger
	klog          klog.Logger
	images        internalapi.ImageManagerService
	containers    internalapi.RuntimeService
	cm            cm.ContainerManager
	volumes       volume.VolumePlugin
	cadvisor      cadvisor.Interface
	os            container.OSInterface
	subpath       subpath.Interface
	hostUtils     hostutil.HostUtils
	eventRecorder record.EventRecorder
	prober        prober.Manager
	traceProvider tp.TracerProvider

	// sync
	mu sync.Mutex
}

func NewBackend(d Driver) Backend {
	backend := &BackendImpl{
		Driver:        d,
		log:           component.NewLogger(string(d.Name())),
		klog:          klog.Background(),
		traceProvider: tp.NewTracerProvider(),
	}
	return backend
}

func (b *BackendImpl) Start(ctx context.Context) error {
	b.log.Info().Str("backend", string(b.Name())).Msg("starting")

	os.MkdirAll(b.DataDir(), 0755)
	os.Remove(b.socket())
	lis, err := net.Listen("unix", b.socket())
	if err != nil {
		return fmt.Errorf("failed to listen on socket %s: %w", b.socket(), err)
	}

	b.grpc = grpc.NewServer()
	runtimeapi.RegisterRuntimeServiceServer(b.grpc, b.ContainerServer())
	runtimeapi.RegisterImageServiceServer(b.grpc, b.ImageServer())

	go func() {
		b.log.Info().Str("socket", b.socket()).Msg("backend gRPC server listening")
		if err := b.grpc.Serve(lis); err != nil {
			b.log.Error().Err(err).Msg("backend gRPC server exited")
		}
	}()

	streamCfg := streaming.DefaultConfig
	streamCfg.Addr = "127.0.0.1:0"
	b.streaming, err = streaming.NewServer(streamCfg, b.Driver)
	if err != nil {
		b.grpc.Stop()
		return fmt.Errorf("failed to start streaming server: %w", err)
	}
	go func() {
		b.log.Info().Msg("backend streaming server starting")
		if err := b.streaming.Start(true); err != nil {
			b.log.Error().Err(err).Msg("backend streaming server exited")
		}
	}()
	b.log.Info().Msg("backend started")
	return nil
}

func (b *BackendImpl) Stop(ctx context.Context) error {
	b.log.Info().Str("backend", string(b.Name())).Msg("stopping")

	// Clean up pod sandboxes before tearing down gRPC
	if rt := b.Containers(); rt != nil {
		sandboxes, err := rt.ListPodSandbox(ctx, nil)
		if err != nil {
			b.log.Error().Err(err).Msg("failed to list pod sandboxes")
		}
		for _, sb := range sandboxes {
			b.log.Info().Str("id", sb.Id).Str("name", sb.Metadata.Name).Msg("cleaning up pod sandbox")
			if err := rt.StopPodSandbox(ctx, sb.Id); err != nil {
				b.log.Error().Err(err).Str("id", sb.Id).Msg("failed to stop pod sandbox")
			}
			if err := rt.RemovePodSandbox(ctx, sb.Id); err != nil {
				b.log.Error().Err(err).Str("id", sb.Id).Msg("failed to remove pod sandbox")
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
		os.Remove(b.socket())
	}

	if b.streaming != nil {
		err := b.streaming.Stop()
		if err != nil {
			b.log.Error().Err(err).Msg("backend streaming server exited with error")
		}
	}

	b.log.Info().Str("backend", string(b.Name())).Msg("backend stopped")
	return nil
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

func (b *BackendImpl) Volumes() volume.VolumePlugin {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.volumes == nil {
		b.volumes = NewVolumes(b)
	}
	return b.volumes
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

func (b *BackendImpl) EventRecorder() record.EventRecorder {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.eventRecorder == nil {
		b.eventRecorder = NewEventRecorder(b)
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

func (b *BackendImpl) RunProbe(cmd []string) (string, error) {
	return "", fmt.Errorf("probe not implemented for backend %s", b.Name())
}

func (b *BackendImpl) RunHostProbe(cmd []string) (string, error) {
	return "", fmt.Errorf("host probe not implemented for backend %s", b.Name())
}

func (b *BackendImpl) socket() string {
	return filepath.Join(b.DataDir(), "cri.sock")
}
